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


import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectScatterMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import java.io.IOException;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;

import org.apache.lucene.analysis.tokenattributes.TermFrequencyAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import static org.apache.lucene.search.spans.TermSpans.ENCODE_LOOKAHEAD;
import static org.apache.lucene.search.spans.TermSpans.MAGIC_NUMBER;
import static org.apache.lucene.search.spans.TermSpans.MAGIC_STABLE_LENGTH;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash.BytesStartArray;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IntBlockPool;

/**
 * This class stores streams of information per term without knowing
 * the size of the stream ahead of time. Each stream typically encodes one level
 * of information like term frequency per document or term proximity. Internally
 * this class allocates a linked list of slices that can be read by a {@link ByteSliceReader}
 * for each term. Terms are first deduplicated in a {@link BytesRefHash} once this is done
 * internal data-structures point to the current offset of each stream that can be written to.
 */
abstract class TermsHashPerField implements Comparable<TermsHashPerField> {

  private static final int HASH_INIT_SIZE = 4;

  private final TermsHashPerField nextPerField;
  private final IntBlockPool intPool;
  final ByteBlockPool bytePool;
  // for each term we store an integer per stream that points into the bytePool above
  // the address is updated once data is written to the stream to point to the next free offset
  // in the terms stream. The start address for the stream is stored in postingsArray.byteStarts[termId]
  // This is initialized in the #addTerm method, either to a brand new per term stream if the term is new or
  // to the addresses where the term stream was written to when we saw it the last time.
  private int[] termStreamAddressBuffer;
  private int streamAddressOffset;
  private final int streamCount;
  private final String fieldName;
  final IndexOptions indexOptions;
  /* This stores the actual term bytes for postings and offsets into the parent hash in the case that this
  * TermsHashPerField is hashing term vectors.*/
  private final BytesRefHash bytesHash;

  ParallelPostingsArray postingsArray;
  private int lastDocID; // only with assert

  /** streamCount: how many streams this field stores per term.
   * E.g. doc(+freq) is 1 stream, prox+offset is a second. */
  TermsHashPerField(int streamCount, IntBlockPool intPool, ByteBlockPool bytePool, ByteBlockPool termBytePool,
                    Counter bytesUsed, TermsHashPerField nextPerField, String fieldName, IndexOptions indexOptions) {
    this.intPool = intPool;
    this.bytePool = bytePool;
    this.streamCount = streamCount;
    this.fieldName = fieldName;
    this.nextPerField = nextPerField;
    assert indexOptions != IndexOptions.NONE;
    this.indexOptions = indexOptions;
    PostingsBytesStartArray byteStarts = new PostingsBytesStartArray(this, bytesUsed);
    bytesHash = new BytesRefHash(termBytePool, HASH_INIT_SIZE, byteStarts);
  }

  void reset() {
    bytesHash.clear(false);
    maxPosLen = 0;
    sortedTermIDs = null;
    if (nextPerField != null) {
      nextPerField.reset();
    }
  }

  final void initReader(ByteSliceReader reader, int termID, int stream) {
    assert stream < streamCount;
    int streamStartOffset = postingsArray.addressOffset[termID];
    final int[] streamAddressBuffer = intPool.buffers[streamStartOffset >> IntBlockPool.INT_BLOCK_SHIFT];
    final int offsetInAddressBuffer = streamStartOffset & IntBlockPool.INT_BLOCK_MASK;
    reader.init(bytePool,
                postingsArray.byteStarts[termID]+stream*ByteBlockPool.FIRST_LEVEL_SIZE,
                streamAddressBuffer[offsetInAddressBuffer+stream]);
  }

  private int[] sortedTermIDs;

  /** Collapse the hash table and sort in-place; also sets
   * this.sortedTermIDs to the results
   * This method must not be called twice unless {@link #reset()}
   * or {@link #reinitHash()} was called. */
  final void sortTerms() {
    assert sortedTermIDs == null;
    sortedTermIDs = bytesHash.sort();
  }

  /**
   * Returns the sorted term IDs. {@link #sortTerms()} must be called before
   */
  final int[] getSortedTermIDs() {
    assert sortedTermIDs != null;
    return sortedTermIDs;
  }

  final void reinitHash() {
    sortedTermIDs = null;
    bytesHash.reinit();
  }

  private boolean doNextCall;

  // Secondary entry point (for 2nd & subsequent TermsHash),
  // because token text has already been "interned" into
  // textStart, so we hash by textStart.  term vectors use
  // this API.
  private void add(int textStart, final int docID) throws IOException {
    int termID = bytesHash.addByPoolOffset(textStart);
    addInternal(termID, docID);
  }

  private int addInternal(int termID, final int docID) throws IOException {
    if (termID >= 0) {      // New posting
      // First time we are seeing this token since we last
      // flushed the hash.
      if (indexLookahead(fieldState.payloadAttribute)) {
        stateBuffer.put(termID, captureInvertFieldState.captureState(fieldState, true, null));
      } else {
        initStreamSlices(termID, docID);
        newTerm(termID, docID);
      }
    } else if (indexLookahead == IndexLookahead.TRUE) {
      final CapturedFieldInvertState.State previousState = stateBuffer.get(termID = ~termID);
      if (previousState != null
          && previousState.firstInSegment) {
        initStreamSlices(termID, docID);
        CapturedFieldInvertState.State newState = captureInvertFieldState.captureState(fieldState, false, previousState);
        stateBuffer.put(termID, newState);
        previousState.restoreState(fieldState, newState.position, false);
        newTerm(termID, docID);
        newState.restoreState(fieldState);

      } else {
        CapturedFieldInvertState.State newState = captureInvertFieldState.captureState(fieldState, false, previousState);
        stateBuffer.put(termID, newState);
        if (previousState != null) {
          positionStreamSlice(termID);
          previousState.restoreState(fieldState, newState.position, false);
          addTerm(termID, docID);
          newState.restoreState(fieldState);
        }
      }
    } else {
      termID = ~termID;
      positionStreamSlice(termID);
      addTerm(termID, docID);
    }
    return termID;
  }

  private final IntObjectHashMap<CapturedFieldInvertState.State> stateBuffer = new IntObjectScatterMap<>();
  private final CapturedFieldInvertState captureInvertFieldState = new CapturedFieldInvertState();
  private int maxPosLen = 0;

  protected static enum IndexLookahead { UNINITIALZED, TRUE, FALSE };
  protected IndexLookahead indexLookahead = IndexLookahead.UNINITIALZED;

  protected boolean indexLookahead(PayloadAttribute payloadAtt) {
    switch (indexLookahead) {
      case TRUE:
        return true;
      case FALSE:
        return false;
      default:
        if (payloadAtt == null) {
          indexLookahead = IndexLookahead.FALSE;
          return false;
        } else {
          BytesRef payload = payloadAtt.getPayload();
          if (payload == null || payload.length - payload.offset < MAGIC_NUMBER.length) {
            indexLookahead = IndexLookahead.FALSE;
            return false;
          } else {
            final byte[] payloadBytes = payload.bytes;
            int compareIdx = payload.offset;
            for (int i = 0; i < MAGIC_STABLE_LENGTH; i++) {
              if (MAGIC_NUMBER[i] != payloadBytes[compareIdx++]) {
                indexLookahead = IndexLookahead.FALSE;
                return false;
              }
            }
            switch (payloadBytes[MAGIC_STABLE_LENGTH]) {
              case ENCODE_LOOKAHEAD:
                indexLookahead = IndexLookahead.TRUE;
                return true;
              default:
                indexLookahead = IndexLookahead.FALSE;
                return false;
            }
          }
        }
    }
  }

  private void initStreamSlices(int termID) throws IOException {
    // Init stream slices
    // TODO: figure out why this is 2*streamCount here. streamCount should be enough?
    if ((2*streamCount) + intPool.intUpto > IntBlockPool.INT_BLOCK_SIZE) {
      // can we fit all the streams in the current buffer?
      intPool.nextBuffer();
    }

    if (ByteBlockPool.BYTE_BLOCK_SIZE - bytePool.byteUpto < (2*streamCount) * ByteBlockPool.FIRST_LEVEL_SIZE) {
      // can we fit at least one byte per stream in the current buffer, if not allocate a new one
      bytePool.nextBuffer();
    }

    termStreamAddressBuffer = intPool.buffer;
    streamAddressOffset = intPool.intUpto;
    intPool.intUpto += streamCount; // advance the pool to reserve the N streams for this term

    postingsArray.addressOffset[termID] = streamAddressOffset + intPool.intOffset;

    for (int i = 0; i < streamCount; i++) {
      // initialize each stream with a slice we start with ByteBlockPool.FIRST_LEVEL_SIZE)
      // and grow as we need more space. see ByteBlockPool.LEVEL_SIZE_ARRAY
      final int upto = bytePool.newSlice(ByteBlockPool.FIRST_LEVEL_SIZE);
      termStreamAddressBuffer[streamAddressOffset + i] = upto + bytePool.byteOffset;
    }
    postingsArray.byteStarts[termID] = termStreamAddressBuffer[streamAddressOffset];
  }

  private boolean assertDocId(int docId) {
    assert docId >= lastDocID : "docID must be >= " + lastDocID + " but was: " + docId;
    lastDocID = docId;
    return true;
  }

  /** Called once per inverted token.  This is the primary
   *  entry point (for first TermsHash); postings use this
   *  API. */
  void add(BytesRef termBytes, final int docID) throws IOException {
    assert assertDocId(docID);
    // We are first in the chain so we must "intern" the
    // term text into textStart address
    // Get the text & hash of this term.
    int termID = bytesHash.add(termBytes);
    //System.out.println("add term=" + termBytesRef.utf8ToString() + " doc=" + docState.docID + " termID=" + termID);

    termID = addInternal(termID);

    if (doNextCall) {
      nextPerField.add(postingsArray.textStarts[termID], docID);
    }
  }

  private int positionStreamSlice(int termID) throws IOException {
    int intStart = postingsArray.addressOffset[termID];
    termStreamAddressBuffer = intPool.buffers[intStart >> IntBlockPool.INT_BLOCK_SHIFT];
    streamAddressOffset = intStart & IntBlockPool.INT_BLOCK_MASK;
    return termID;
  }

  final void writeByte(int stream, byte b) {
    int streamAddress = streamAddressOffset + stream;
    int upto = termStreamAddressBuffer[streamAddress];
    byte[] bytes = bytePool.buffers[upto >> ByteBlockPool.BYTE_BLOCK_SHIFT];
    assert bytes != null;
    int offset = upto & ByteBlockPool.BYTE_BLOCK_MASK;
    if (bytes[offset] != 0) {
      // End of slice; allocate a new one
      offset = bytePool.allocSlice(bytes, offset);
      bytes = bytePool.buffer;
      termStreamAddressBuffer[streamAddress] = offset + bytePool.byteOffset;
    }
    bytes[offset] = b;
    (termStreamAddressBuffer[streamAddress])++;
  }

  final void writeBytes(int stream, byte[] b, int offset, int len) {
    // TODO: optimize
    final int end = offset + len;
    for(int i=offset;i<end;i++)
      writeByte(stream, b[i]);
  }

  final void writeVInt(int stream, int i) {
    assert stream < streamCount;
    while ((i & ~0x7F) != 0) {
      writeByte(stream, (byte)((i & 0x7f) | 0x80));
      i >>>= 7;
    }
    writeByte(stream, (byte) i);
  }

  final TermsHashPerField getNextPerField() {
    return nextPerField;
  }

  final String getFieldName() {
    return fieldName;
  }

  private static final class PostingsBytesStartArray extends BytesStartArray {

    private final TermsHashPerField perField;
    private final Counter bytesUsed;

    private PostingsBytesStartArray(
        TermsHashPerField perField, Counter bytesUsed) {
      this.perField = perField;
      this.bytesUsed = bytesUsed;
    }

    @Override
    public int[] init() {
      if (perField.postingsArray == null) {
        perField.postingsArray = perField.createPostingsArray(2);
        perField.newPostingsArray();
        bytesUsed.addAndGet(perField.postingsArray.size * perField.postingsArray.bytesPerPosting());
      }
      return perField.postingsArray.textStarts;
    }

    @Override
    public int[] grow() {
      ParallelPostingsArray postingsArray = perField.postingsArray;
      final int oldSize = perField.postingsArray.size;
      postingsArray = perField.postingsArray = postingsArray.grow();
      perField.newPostingsArray();
      bytesUsed.addAndGet((postingsArray.bytesPerPosting() * (postingsArray.size - oldSize)));
      return postingsArray.textStarts;
    }

    @Override
    public int[] clear() {
      if (perField.postingsArray != null) {
        bytesUsed.addAndGet(-(perField.postingsArray.size * perField.postingsArray.bytesPerPosting()));
        perField.postingsArray = null;
        perField.newPostingsArray();
      }
      return null;
    }

    @Override
    public Counter bytesUsed() {
      return bytesUsed;
    }
  }

  @Override
  public final int compareTo(TermsHashPerField other) {
    return fieldName.compareTo(other.fieldName);
  }

  protected final int OVERHEAD_TO_ENSURE_STABLE_ADDRESS = 3;

  void allocateMaxPositionLengthPlaceholder(int stream) {
    // we know we can atomically write 4 bytes and get a valid candidate position.
    byte testByte = 0;
    for (int i = 0; i < Integer.BYTES; i++) {
      writeByte(stream, ++testByte);
    }
    int candidateUpto = intUptos[intUptoStart + stream];
    final BytesRef firstSlice = fieldState.firstMaxPositionLengthSlice;
    firstSlice.bytes = bytePool.buffers[candidateUpto >> ByteBlockPool.BYTE_BLOCK_SHIFT];
    firstSlice.offset = (candidateUpto & ByteBlockPool.BYTE_BLOCK_MASK) - Integer.BYTES;

    // now we have to write 3 more bytes to in case the bytepool shifts the space we already allocated
    for (int i = 1; i <= OVERHEAD_TO_ENSURE_STABLE_ADDRESS; i++) {
      writeByte(stream, (byte)0);
      final int probeUpto = intUptos[intUptoStart + stream];
      if (probeUpto > candidateUpto + i) {
        // spread across 2 slices
        final BytesRef secondSlice = fieldState.secondMaxPositionLengthSlice;
        secondSlice.bytes = bytePool.buffers[probeUpto >> ByteBlockPool.BYTE_BLOCK_SHIFT];
        secondSlice.offset = (probeUpto & ByteBlockPool.BYTE_BLOCK_MASK) - Integer.BYTES;
        secondSlice.length = Integer.BYTES - i;
        firstSlice.length = i;
        intUptos[intUptoStart + stream] -= i;
        return;
      }
    }
    intUptos[intUptoStart + stream] -= OVERHEAD_TO_ENSURE_STABLE_ADDRESS;
    // stable all in one slice
    firstSlice.length = Integer.BYTES;
  }

  void flush() throws IOException {
    if (indexLookahead == IndexLookahead.FALSE) {
      return;
    }
    // flush buffer
    for (IntObjectCursor<CapturedFieldInvertState.State> entry : stateBuffer) {
      final int termID = entry.key;
      final CapturedFieldInvertState.State previousState = entry.value;
      if (previousState.firstInSegment) {
        initStreamSlices(termID);
        previousState.restoreState(fieldState, Integer.MAX_VALUE, true);
        newTerm(termID, docID);
      } else {
        positionStreamSlice(termID);
        previousState.restoreState(fieldState, Integer.MAX_VALUE, true);
        addTerm(termID);
      }
    }
    // reset
    captureInvertFieldState.resetPool();
    stateBuffer.clear();
    maxPosLen = 0;
    if (doNextCall) {
      nextPerField.flush();
    }
  }

  /** Finish adding all instances of this field to the
   *  current document. */
  void finish() throws IOException {
    if (nextPerField != null) {
      nextPerField.finish();
    }
  }

  final int getNumTerms() {
    return bytesHash.size();
  }

  /** Start adding a new field instance; first is true if
   *  this is the first time this field name was seen in the
   *  document. */
  boolean start(IndexableField field, boolean first) {
    if (nextPerField != null) {
      doNextCall = nextPerField.start(field, first);
    }
    return true;
  }

  /** Called when a term is seen for the first time. */
  abstract void newTerm(int termID, final int docID) throws IOException;

  /** Called when a previously seen term is seen again. */
  abstract void addTerm(int termID, final int docID) throws IOException;

  /** Called when the postings array is initialized or
   *  resized. */
  abstract void newPostingsArray();

  /** Creates a new postings array of the specified size. */
  abstract ParallelPostingsArray createPostingsArray(int size);
}
