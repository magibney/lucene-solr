/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.index;

import java.io.IOException;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;

/**
 *
 * @author magibney
 */
class CapturedFieldInvertState {
  
  private static final int MAX_VINT_BYTES = Integer.BYTES + 1;
  private final ByteArrayDataOutput intWriter = new ByteArrayDataOutput();

  private State head;
  private State createTail;
  
  State captureState(FieldInvertState input, boolean firstInSegment, State previousState) {
    State ret = head;
    if (ret != null) {
      head = ret.trackNextRef;
    } else {
      ret = new State(intWriter, createTail);
      createTail = ret;
    }
    ret.init(input, firstInSegment, previousState);
    return ret;
  }
  
  void resetPool() {
    this.head = createTail;
  }

  static final class State {
    private final ByteArrayDataOutput intWriter;
    int position;
    int offset;
    boolean firstInSegment;
    boolean firstInDoc;
    boolean hasDecreasingEndPosition;
    int endPosition;
    int maxPositionLength;
    boolean initializeSlicesFromFieldState;
    final BytesRef firstMaxPositionLengthSlice = new BytesRef();
    final BytesRef secondMaxPositionLengthSlice = new BytesRef();
    AttributeSource.State attState;
    private final State trackNextRef;

    private State(ByteArrayDataOutput intWriter, State trackNextRef) {
      this.intWriter = intWriter;
      this.trackNextRef = trackNextRef;
    }

    private void init(FieldInvertState input, boolean firstInSegment, State previousState) {
      this.position = input.position;
      this.offset = input.offset;
      this.attState = input.attributeSource.captureState();
      this.firstInSegment = firstInSegment;
      if (previousState == null) {
        this.initializeSlicesFromFieldState = false;
        this.firstInDoc = true;
        this.hasDecreasingEndPosition = false;
        this.maxPositionLength = input.positionLengthAttribute == null ? 0 : input.positionLengthAttribute.getPositionLength();
        if (input.positionLengthAttribute == null) {
          this.maxPositionLength = 0;
          this.endPosition = -1;
        } else {
          this.maxPositionLength = input.positionLengthAttribute.getPositionLength();
          this.endPosition = position + maxPositionLength;
        }
      } else {
        this.firstInDoc = false;
        if (previousState.maxPositionLength <= 0) {
          this.maxPositionLength = 0;
        } else {
          final int inputPositionLength = input.positionLengthAttribute.getPositionLength();
          this.maxPositionLength = Math.max(previousState.maxPositionLength, inputPositionLength);
          this.hasDecreasingEndPosition = previousState.hasDecreasingEndPosition
              || previousState.endPosition > (endPosition = position + inputPositionLength);
        }
        if (previousState.firstInDoc) {
          this.initializeSlicesFromFieldState = true;
        } else {
          this.initializeSlicesFromFieldState = false;
          copyBytesRef(previousState.firstMaxPositionLengthSlice, firstMaxPositionLengthSlice);
          copyBytesRef(previousState.secondMaxPositionLengthSlice, secondMaxPositionLengthSlice);
        }
      }
    }

    private static void copyBytesRef(BytesRef from, BytesRef to) {
      to.bytes = from.bytes;
      to.offset = from.offset;
      to.length = from.length;
    }
    
    void restoreState(FieldInvertState output) {
      output.allocatePositionLength = false;
      output.position = this.position;
      output.offset = this.offset;
      output.attributeSource.restoreState(this.attState);
      if (initializeSlicesFromFieldState) {
        initializeSlicesFromFieldState = false;
        copyBytesRef(output.firstMaxPositionLengthSlice, firstMaxPositionLengthSlice);
        copyBytesRef(output.secondMaxPositionLengthSlice, secondMaxPositionLengthSlice);
      }
    }
    
    void restoreState(FieldInvertState output, int nextPosition, boolean flush) {
      output.position = this.position;
      output.offset = this.offset;
      output.attributeSource.restoreState(this.attState);
      transformPayload(output, nextPosition - this.position, flush);
    }
    
    private void transformPayload(FieldInvertState fieldState, int offset, boolean flush) {
      PayloadAttribute payloadAtt = fieldState.payloadAttribute;
      final BytesRef extantPayload = payloadAtt.getPayload();
      final int extantEndIndex = extantPayload.offset + extantPayload.length;
      final int overhead = firstInDoc ? MAX_VINT_BYTES + Integer.BYTES : MAX_VINT_BYTES;
      final int requiredCapacity = extantEndIndex + overhead;
      byte[] extantBytes = extantPayload.bytes;
      final byte[] dest;
      if (extantBytes.length >= requiredCapacity) {
        dest = extantBytes;
      } else {
        extantPayload.bytes = dest = new byte[requiredCapacity];
        System.arraycopy(extantBytes, 0, dest, 0, extantEndIndex);
      }
      intWriter.reset(dest, extantEndIndex, overhead);
      try {
        intWriter.writeVInt(offset);
      } catch (IOException ex) {
        throw new RuntimeException("this won't happen", ex);
      }
      if (firstInDoc) {
        if (!flush) {
          // allocate space for writing maxPositionLength for this doc.
          fieldState.allocatePositionLength = true;
        }
      } else if (flush) {
        writeMaxPositionLength(hasDecreasingEndPosition ? ~maxPositionLength : maxPositionLength);
      }
      extantPayload.length += (intWriter.getPosition() - extantEndIndex);
    }

    void writeMaxPositionLength(final int maxPosLen) {
      final BytesRef firstSlice = firstMaxPositionLengthSlice;
      final byte[] firstSliceBytes = firstSlice.bytes;
      final int firstSliceOffset = firstSlice.offset;
      final int firstSliceLength = firstSlice.length;
      int shift = Integer.SIZE;
      int i = 0;
      byte testByte = 0;
      do {
        assert ++testByte == firstSliceBytes[firstSliceOffset + i] : "testByte/"+testByte+" != "+firstSliceBytes[firstSliceOffset + i];
        firstSliceBytes[firstSliceOffset + i] = (byte)(maxPosLen >>> (shift -= Byte.SIZE));
      } while (++i < firstSliceLength);
      if (i < Integer.BYTES) {
        final BytesRef secondSlice = secondMaxPositionLengthSlice;
        final byte[] secondSliceBytes = secondSlice.bytes;
        final int secondSliceOffset = secondSlice.offset;
        final int secondSliceLength = secondSlice.length;
        i = 0;
        do {
          assert ++testByte == secondSliceBytes[secondSliceOffset + i] : "testByte/"+testByte+" != "+secondSliceBytes[secondSliceOffset + i];
          secondSliceBytes[secondSliceOffset + i] = (byte)(maxPosLen >>> (shift -= Byte.SIZE));
        } while (++i < secondSliceLength);
      }
    }

  }
}
