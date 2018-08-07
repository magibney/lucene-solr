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
package org.apache.lucene.search.spans;

import java.io.IOException;
import java.util.Arrays;
import java.util.ListIterator;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;

/**
 *
 */
public class TermSpansRepeatBuffer {

  private static final int DEFAULT_CAPACITY = 16; // power of 2
  private static final int MIN_CAPACITY = 2; // power of 2

  private final TermSpans backing;
  private final PostingsEnum backingPostings;
  private final int repeatCount;
  
  private final RepeatTermSpans[] repeatGroup;

  /**
   * Stores:
   * 1) refCount
   * 2) startPosition
   * 3) positionLength
   * 4) width
   * 5) startOffset
   * 6) endOffset
   */
  private int capacity;
  private int indexMask;

  private final boolean payloads;
  private final boolean offsets;
  private int[] startPositionBuffer;
  private int[] positionLengthBuffer;
  private BytesRef[] payloadBuffer;
  private int[] startOffsetBuffer;
  private int[] endOffsetBuffer;
  private final int[][] buffers;
  private int tail = 0;
  private int head = 0;
  private int nonNegativeHead = 0;
  private int maxPositionLength = IndexLookahead.UNINITIALIZED_AT_SPANS;
  
  public ListIterator<RepeatTermSpans> getRepeatTermSpans() {
    return Arrays.asList(repeatGroup).listIterator(repeatGroup.length);
  }

  public TermSpansRepeatBuffer(TermSpans backing, int count, boolean offsets, boolean payloads, TermSpansRepeatBuffer reuse) {
    this(backing, count, offsets, payloads, reuse, -1);
  }

  public TermSpansRepeatBuffer(TermSpans backing, int count, boolean offsets, boolean payloads, int capacity) {
    this(backing, count, offsets, payloads, null, capacity);
  }

  public TermSpansRepeatBuffer(TermSpans backing, int count, boolean offsets, boolean payloads) {
    this(backing, count, offsets, payloads, null, DEFAULT_CAPACITY);
  }

  public TermSpansRepeatBuffer(TermSpans backing, int count, boolean offsets, boolean payloads, TermSpansRepeatBuffer reuse, int capacityHint) {
    this.offsets = offsets;
    this.payloads = payloads;
    if (reuse != null) {
      this.capacity = reuse.capacity;
      indexMask = this.capacity - 1;
      this.startPositionBuffer = reuse.startPositionBuffer;
      this.positionLengthBuffer = reuse.positionLengthBuffer;
      this.payloadBuffer = reuse.payloadBuffer;
      this.startOffsetBuffer = reuse.startOffsetBuffer;
      this.endOffsetBuffer = reuse.endOffsetBuffer;
      this.buffers = reuse.buffers;
    } else {
      this.capacity = Math.max(MIN_CAPACITY, Integer.highestOneBit(capacityHint - 1) << 1); // power of 2
      indexMask = this.capacity - 1;
      startPositionBuffer = new int[capacity];
      positionLengthBuffer = new int[capacity];
      if (offsets) {
        startOffsetBuffer = new int[capacity];
        endOffsetBuffer = new int[capacity];
        buffers = new int[][]{startPositionBuffer, positionLengthBuffer, startOffsetBuffer, endOffsetBuffer};
      } else {
        startOffsetBuffer = null;
        endOffsetBuffer = null;
        buffers = new int[][]{startPositionBuffer, positionLengthBuffer};
      }
      payloadBuffer = payloads ? new BytesRef[capacity] : null;
    }
    this.backing = backing;
    this.backingPostings = backing.postings;
    this.repeatCount = count - 1;
    this.repeatGroup = new RepeatTermSpans[count];
    for (int i = 0; i < count; i++) {
      repeatGroup[i] = new RepeatTermSpans(this, i, offsets, payloads);
    }
  }
  
  private int nextStartPosition(int bufferIndex, int repeatIndex) throws IOException {
    if (bufferIndex < 0) {
      return advanceBacking(repeatIndex, backing.position >= 0);
    } else {
      if (repeatIndex == 0) {
        if ((tail & Integer.MAX_VALUE) != bufferIndex) {
          throw new AssertionError(tail +" != "+bufferIndex);
        }
        tail++;
      }
      int ret = ++bufferIndex & Integer.MAX_VALUE;
      if (ret == nonNegativeHead) {
        return ~backing.startPosition();
      } else {
        return ret;
      }
    }
  }
  
  private boolean assign(Object o1, Object o2) {
    return true;
  }

  private void increaseCapacity() {
    int srcHead = head & indexMask;
    int srcTail = tail & indexMask;
    int srcCapacity = capacity;
    capacity <<= 1; // double capacity
    indexMask = capacity - 1; // reset index mask
    int dstTail = tail & indexMask;
    int i;
    Object oldBuffer;
    Object newBuffer;
    if (payloads) {
      i = buffers.length;
      oldBuffer = payloadBuffer;
      newBuffer = new BytesRef[capacity];
    } else {
      i = buffers.length - 1;
      oldBuffer = buffers[i];
      newBuffer = buffers[i] = new int[capacity];
    }
    if (srcHead > srcTail) {
      int length = srcHead - srcTail;
      do {
        System.arraycopy(oldBuffer, srcTail, newBuffer, dstTail, length);
      } while (--i >= 0 && assign(oldBuffer = buffers[i], newBuffer = buffers[i] = new int[capacity]));
    } else {
      int tailChunkLength = srcCapacity - srcTail;
      int dstHeadChunkStart = (dstTail + tailChunkLength) & indexMask;
      do {
        System.arraycopy(oldBuffer, srcTail, newBuffer, dstTail, tailChunkLength);
        System.arraycopy(oldBuffer, 0, newBuffer, dstHeadChunkStart, srcHead);
      } while (--i >= 0 && assign(oldBuffer = buffers[i], newBuffer = buffers[i] = new int[capacity]));
    }
    startPositionBuffer = buffers[0];
    positionLengthBuffer = buffers[1];
    if (offsets) {
      startOffsetBuffer = buffers[2];
      endOffsetBuffer = buffers[3];
    }
  }
  
  private int advanceBacking(final int repeatIndex, final boolean initialized) throws IOException {
    if (head - tail >= capacity) {
      increaseCapacity();
      if (head - tail > capacity) {
        throw new AssertionError();
      }
    }
    int preserve = head;
    int setBufferIndex = head & Integer.MAX_VALUE;
    if (initialized) {
      int srcStartIndex = head & indexMask;
      startPositionBuffer[srcStartIndex] = backing.startPosition();
      positionLengthBuffer[srcStartIndex] = backing.getPositionLength(false);
      if (offsets) {
        startOffsetBuffer[srcStartIndex] = backingPostings.startOffset();
        endOffsetBuffer[srcStartIndex] = backingPostings.endOffset();
      }
      if (payloads) {
        BytesRef payload = backingPostings.getPayload();
        payloadBuffer[srcStartIndex] = payload == null ? null : BytesRef.deepCopyOf(payload);
      }
      nonNegativeHead = ++head & Integer.MAX_VALUE;
    }
    int ret = ~backing.nextStartPosition();
    if (!initialized) {
      maxPositionLength = backing.positionLengthCeiling();
    }
    int i = repeatCount;
    while (i > repeatIndex) {
      RepeatTermSpans rts = repeatGroup[i--];
      //System.err.println("1end run set "+rts.repeatIndex+" from "+rts.startPosition+" to "+~ret);
      assert i != -1 || rts.matchLimit != Integer.MAX_VALUE;
      if (--rts.matchLimit < 0) {
        return ~Spans.NO_MORE_POSITIONS;
      }
      rts.bufferIndex = ret;
    }
    if (i > 0) {
      switch (--i) {
        default:
          do {
            RepeatTermSpans rts = repeatGroup[i];
            if (rts.bufferIndex < 0) {
              //System.err.println("2preserve " + rts.repeatIndex + " at startPosition=" + startOffsetBuffer[(head - 1) & indexMask]);
              rts.bufferIndex = setBufferIndex;
            }
          } while (--i > 0);
        case 0:
          RepeatTermSpans rts = repeatGroup[0];
          if (rts.bufferIndex < 0) {
            //System.err.println("3preserve " + rts.repeatIndex + " at startPosition=" + startOffsetBuffer[(head - 1) & indexMask]);
            rts.bufferIndex = setBufferIndex;
            tail = preserve;
          }
      }
    }
    return ret;
  }

  private void clear() {
    tail = 0;
    head = 0;
    nonNegativeHead = 0;
    maxPositionLength = IndexLookahead.UNINITIALIZED_AT_DOC;
  }
  
  public static class RepeatTermSpans extends Spans implements IndexLookahead {

    private final TermSpansRepeatBuffer backing;
    final TermSpans backingSpans;
    private final boolean offsets;
    private final boolean payloads;
    private final PostingsEnum postings;
    private final RepeatPostingsEnum offsetPostings;
    private final ResettablePostings resettablePostings;
    private final Term term;
    private final int repeatIndex;
    private int bufferIndex = -1;
    
    private int startPosition = -1;
    private int endPosition = -1;

    private int matchLimit = Integer.MAX_VALUE;

    private RepeatTermSpans(TermSpansRepeatBuffer backing, int index, boolean offsets, boolean payloads) {
      this.backing = backing;
      this.backingSpans = backing.backing;
      this.term = backingSpans.term;
      this.offsets = offsets;
      this.payloads = payloads;
      if (offsets) {
        this.postings = this.offsetPostings = payloads ? new PayloadRepeatPostingsEnum(this) : new RepeatPostingsEnum(this);
        this.resettablePostings = this.offsetPostings;
      } else {
        this.offsetPostings = null;
        if (payloads) {
          PayloadNoOffsetsRepeatPostingsEnum payloadNoOffsetsPostings = new PayloadNoOffsetsRepeatPostingsEnum(this);
          this.resettablePostings = payloadNoOffsetsPostings;
          this.postings = payloadNoOffsetsPostings;
        } else {
          this.resettablePostings = null;
          this.postings = new NoOffsetsRepeatPostingsEnum(this);
        }
      }
      this.repeatIndex = index;
    }

    void setMatchLimit(int limit) {
      this.matchLimit = limit;
    }

    private void reset() {
      matchLimit = Integer.MAX_VALUE;
      startPosition = -1;
      endPosition = -1;
      if (offsets || payloads) {
        resettablePostings.reset();
      }
      this.bufferIndex = -1;
    }

    @Override
    public int positionLengthCeiling() {
      return backing.maxPositionLength;
    }

    @Override
    public int lookaheadNextStartPositionFloor() throws IOException {
      if (repeatIndex == 0 && matchLimit <= 0) {
        //System.err.println("here 1 matchLimit="+matchLimit+" term="+backing.backing.term.bytes().utf8ToString());
        return Spans.NO_MORE_POSITIONS;
      }
      if (bufferIndex >= 0) {
        final int incrementedBufferIndex = bufferIndex + 1;
        if ((incrementedBufferIndex & Integer.MAX_VALUE) == backing.nonNegativeHead) {
          return backing.backing.startPosition();
        } else {
          return backing.startPositionBuffer[incrementedBufferIndex & backing.indexMask];
        }
      } else if (startPosition < 0) {
        // truly advancing backing TermSpans... do something?
        int i = backing.repeatCount;
        while (i > repeatIndex) {
          RepeatTermSpans rts = backing.repeatGroup[i--];
          assert i != -1 || rts.matchLimit != Integer.MAX_VALUE;
          if (rts.matchLimit <= 0) {
            return Spans.NO_MORE_POSITIONS;
          }
        }
        int ret = backing.backing.lookaheadNextStartPositionFloor();
        return ret;
      } else if (bufferIndex < 0) {
        return ~bufferIndex;
      } else {
        return backing.startPositionBuffer[bufferIndex & backing.indexMask];
      }

    }

    @Override
    public int nextStartPosition() throws IOException {
      assert cachedDocId == backingSpans.doc : cachedDocId + " != " + backingSpans.doc;
      if (repeatIndex == 0) {
        assert matchLimit != Integer.MAX_VALUE;
        if (--matchLimit < 0) {
          startPosition = Spans.NO_MORE_POSITIONS;
          endPosition = Spans.NO_MORE_POSITIONS;
          return startPosition;
        }
      }
      if (bufferIndex >= 0 || startPosition < 0) {
        bufferIndex = backing.nextStartPosition(bufferIndex, repeatIndex);
      }
      endPosition = -1;
      if (offsets) {
        offsetPostings.reset();
      }
      if (bufferIndex < 0) {
        startPosition = -1;
        return ~bufferIndex;
      } else {
        return startPosition = backing.startPositionBuffer[bufferIndex & backing.indexMask];
      }
    }

    @Override
    public int startPosition() {
      assert cachedDocId == backingSpans.doc : cachedDocId+" != "+backingSpans.doc;
      return startPosition >= 0 ? startPosition
          : (bufferIndex < 0 ? backingSpans.startPosition() : (startPosition = backing.startPositionBuffer[bufferIndex & backing.indexMask]));
    }

    @Override
    public int endPosition() {
      assert cachedDocId == backingSpans.doc : cachedDocId+" != "+backingSpans.doc;
      if (endPosition >= 0) {
        return endPosition;
      } else if (bufferIndex < 0) {
        return backingSpans.endPosition();
      } else {
        return endPosition = startPosition() + backing.positionLengthBuffer[bufferIndex & backing.indexMask];
      }
    }

    @Override
    public int width() {
      return 0;
    }

    @Override
    public void collect(SpanCollector collector) throws IOException {
      assert cachedDocId == backingSpans.doc : cachedDocId+" != "+backingSpans.doc;
      collector.collectLeaf(postings, startPosition(), term);
    }

    @Override
    public float positionsCost() {
      return backingSpans.positionsCost();
    }

    
    private int cachedDocId = -1;
    @Override
    public int docID() {
      return cachedDocId;
    }

    @Override
    public int nextDoc() throws IOException {
      reset();
      int ret = backingSpans.docID();
      if (ret > cachedDocId) {
        return cachedDocId = ret;
      } else {
        backing.clear();
        //System.err.println(repeatIndex + ".nextDoc()=" + ret);
        return cachedDocId = backingSpans.nextDoc();
      }
    }

    @Override
    public int advance(int target) throws IOException {
      reset();
      int ret = backingSpans.docID();
      if (target > ret || ret == cachedDocId) {
        backing.clear();
        return cachedDocId = backingSpans.advance(target);
      } else {
        return cachedDocId = ret;
      }
    }

    @Override
    public long cost() {
      return backingSpans.cost();
    }
    
  }
  
  private static class NoOffsetsRepeatPostingsEnum extends PostingsEnum {
    
    private final TermSpans backingSpans;

    public NoOffsetsRepeatPostingsEnum(RepeatTermSpans backing) {
      this.backingSpans = backing.backingSpans;
    }
    
    @Override
    public int freq() throws IOException {
      return backingSpans.freq;
    }

    @Override
    public int nextPosition() throws IOException {
      throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public int startOffset() throws IOException {
      return -1;
    }

    @Override
    public int endOffset() throws IOException {
      return -1;
    }

    @Override
    public BytesRef getPayload() throws IOException {
      return null;
    }

    @Override
    public int docID() {
      return backingSpans.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public int advance(int target) throws IOException {
      throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public long cost() {
      return backingSpans.cost();
    }
    
  }
  
  private static final class PayloadNoOffsetsRepeatPostingsEnum extends NoOffsetsRepeatPostingsEnum implements ResettablePostings {

    private final RepeatTermSpans backing;
    private final TermSpansRepeatBuffer backingBuf;
    private final PostingsEnum backingPostings;
    private BytesRef payload;

    public PayloadNoOffsetsRepeatPostingsEnum(RepeatTermSpans backing) {
      super(backing);
      this.backing = backing;
      this.backingBuf = backing.backing;
      this.backingPostings = backing.backingSpans.postings;
    }

    @Override
    public BytesRef getPayload() throws IOException {
      return backing.bufferIndex < 0 ? backingPostings.getPayload()
          : (payload != null ? payload : (payload = backingBuf.payloadBuffer[backing.bufferIndex & backingBuf.indexMask]));
    }

    @Override
    public void reset() {
      payload = null;
    }

  }

  private static interface ResettablePostings {
    void reset();
  }

  private static final class PayloadRepeatPostingsEnum extends RepeatPostingsEnum {

    private BytesRef payload;

    public PayloadRepeatPostingsEnum(RepeatTermSpans backing) {
      super(backing);
    }

    @Override
    public BytesRef getPayload() throws IOException {
      return backing.bufferIndex < 0 ? backingPostings.getPayload()
          : (payload != null ? payload : (payload = backingBuf.payloadBuffer[backing.bufferIndex & backingBuf.indexMask]));
    }

    @Override
    public void reset() {
      super.reset();
      payload = null;
    }

  }

  private static class RepeatPostingsEnum extends PostingsEnum implements ResettablePostings {

    protected final RepeatTermSpans backing;
    protected final TermSpansRepeatBuffer backingBuf;
    private final TermSpans backingSpans;
    protected final PostingsEnum backingPostings;
    private int startOffset = -1;
    private int endOffset = -1;

    public RepeatPostingsEnum(RepeatTermSpans backing) {
      this.backing = backing;
      this.backingBuf = backing.backing;
      this.backingSpans = backing.backingSpans;
      this.backingPostings = backingSpans.postings;
    }
    
    public void reset() {
      startOffset = -1;
      endOffset = -1;
    }
    
    @Override
    public int freq() throws IOException {
      return backingSpans.freq;
    }

    @Override
    public int nextPosition() throws IOException {
      throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public int startOffset() throws IOException {
      return backing.bufferIndex < 0 ? backingPostings.startOffset()
          : (startOffset >= 0 ? startOffset : (startOffset = backingBuf.startOffsetBuffer[backing.bufferIndex & backingBuf.indexMask]));
    }

    @Override
    public int endOffset() throws IOException {
      return backing.bufferIndex < 0 ? backingPostings.endOffset()
          : (endOffset >= 0 ? endOffset : (endOffset = backingBuf.endOffsetBuffer[backing.bufferIndex & backingBuf.indexMask]));
    }

    @Override
    public BytesRef getPayload() throws IOException {
      return null;
    }

    @Override
    public int docID() {
      return backingSpans.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public int advance(int target) throws IOException {
      throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public long cost() {
      return backingSpans.cost();
    }
    
  }
}
