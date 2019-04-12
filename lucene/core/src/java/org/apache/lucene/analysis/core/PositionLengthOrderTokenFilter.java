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
package org.apache.lucene.analysis.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.search.spans.TermSpans;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

/**
 *
 */
public class PositionLengthOrderTokenFilter extends TokenFilter {
  private static final int MAX_VINT_BYTES = Integer.SIZE + 1;
  private final PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
  private final PositionLengthAttribute posLengthAtt = addAttribute(PositionLengthAttribute.class);
  private final ByteArrayDataOutput positionLengthWriter = new ByteArrayDataOutput();
  private final PayloadAttribute payloadAtt = addAttribute(PayloadAttribute.class);
  private ArrayList<AttributeStateEntry> stored = null;
  private int storedIdx = -1;
  private int positionIncrement = -1;
  private State inputState;
  private int inputPositionIncrement = -1;
  private int inputPositionLength = -1;
  private final byte[] magicNumber;

  public PositionLengthOrderTokenFilter(TokenStream input, boolean indexLookahead) {
    super(input);
    this.magicNumber = ArrayUtil.copyOfSubArray(TermSpans.MAGIC_NUMBER, 0, TermSpans.MAGIC_NUMBER.length);
    if (indexLookahead) {
      this.magicNumber[TermSpans.MAGIC_STABLE_LENGTH] = TermSpans.ENCODE_LOOKAHEAD;
    }
  }

  @Override
  public void reset() throws IOException {
    positionIncrement = -1;
    inputState = null;
    inputPositionIncrement = -1;
    inputPositionLength = -1;
    if (stored != null) {
      stored.clear();
      storedIdx = -1;
    }
    super.reset();
  }

  private BytesRef encodePositionLength(int length) {
    final byte[] bytes;
    if (length == 1) {
      bytes = ArrayUtil.growExact(magicNumber, 3);
      bytes[2] = 1;
      return new BytesRef(bytes);
    } else {
      bytes = ArrayUtil.growExact(magicNumber, magicNumber.length + MAX_VINT_BYTES);
    }
    positionLengthWriter.reset(bytes, magicNumber.length, MAX_VINT_BYTES);
    try {
      positionLengthWriter.writeVInt(length);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    return new BytesRef(bytes, 0, positionLengthWriter.getPosition());
  }

  private boolean clearStored() {
    storedIdx = -1;
    stored.clear();
    return false;
  }

  @Override
  public final boolean incrementToken() throws IOException {
    AttributeStateEntry ase;
    int posLength;
    if (inputPositionIncrement > 0) {
      if (storedIdx >= 0 && (++storedIdx < stored.size() || clearStored())) {
        ase = stored.get(storedIdx);
        restoreState(ase.state);
        payloadAtt.setPayload(encodePositionLength(ase.positionLength));
        return true;
      } else {
        int restorePosIncr = inputPositionIncrement;
        inputPositionIncrement = -1;
        if (inputPositionLength > 1) {
          positionIncrement = restorePosIncr;
          storePosition(inputPositionLength, inputState);
        } else if (inputPositionLength == Integer.MIN_VALUE) {
          inputPositionLength = -1;
          restoreState(inputState);
          return false;
        } else {
          restoreState(inputState);
          payloadAtt.setPayload(encodePositionLength(inputPositionLength));
          return true;
        }
      }
    }
    boolean clearPosIncr;
    do {
      restoreState(inputState);
      if (!input.incrementToken()) {
        if (stored != null && !stored.isEmpty()) {
          if (stored.size() == 1) {
            ase = stored.remove(0);
          } else {
            stored.sort(ASE_COMPARATOR);
            ase = stored.get(storedIdx = 0);
          }
          inputPositionIncrement = Integer.MAX_VALUE;
          inputPositionLength = Integer.MIN_VALUE;
          inputState = captureState();
          restoreState(ase.state);
          if (positionIncrement > 0) {
            posIncAtt.setPositionIncrement(positionIncrement);
            positionIncrement = -1;
          }
          payloadAtt.setPayload(encodePositionLength(ase.positionLength));
          return true;
        }
        return false;
      }
      int posIncrement = posIncAtt.getPositionIncrement();
      posLength = posLengthAtt.getPositionLength();
      if (posIncrement <= 0) {
        clearPosIncr = false;
      } else {
        clearPosIncr = true;
        if (stored != null && !stored.isEmpty()) {
          if (stored.size() == 1) {
            ase = stored.remove(0);
          } else {
            stored.sort(ASE_COMPARATOR);
            ase = stored.get(storedIdx = 0);
          }
          inputPositionIncrement = posIncrement;
          inputPositionLength = posLength;
          if (posLength > 1) {
            posIncAtt.setPositionIncrement(0);
          }
          inputState = captureState();
          restoreState(ase.state);
          if (positionIncrement > 0) {
            posIncAtt.setPositionIncrement(positionIncrement);
            positionIncrement = -1;
          }
          payloadAtt.setPayload(encodePositionLength(ase.positionLength));
          return true;
        }
        positionIncrement = posIncrement;
      }
    } while (posLength > 1 && storePosition(posLength, clearPosIncr));
    if (positionIncrement > 0) {
      posIncAtt.setPositionIncrement(positionIncrement);
      positionIncrement = -1;
    }
    payloadAtt.setPayload(encodePositionLength(posLength));
    return true;
  }

  private boolean storePosition(int inputPositionLength, boolean clearPosIncr) {
    if (clearPosIncr) {
      posIncAtt.setPositionIncrement(0);
    }
    return storePosition(inputPositionLength, captureState());
  }

  private boolean storePosition(int inputPositionLength, State attributeState) {
    if (stored == null) {
      stored = new ArrayList<>(4);
    }
    stored.add(new AttributeStateEntry(inputPositionLength, attributeState));
    return true;
  }

  private static final Comparator<AttributeStateEntry> ASE_COMPARATOR = new Comparator<AttributeStateEntry>() {

    @Override
    public int compare(AttributeStateEntry o1, AttributeStateEntry o2) {
      return Integer.compare(o1.positionLength, o2.positionLength);
    }

  };

  private static class AttributeStateEntry {
    private final int positionLength;
    private final State state;

    public AttributeStateEntry(int positionLength, State state) {
      this.positionLength = positionLength;
      this.state = state;
    }
    
  }

}
