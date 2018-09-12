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
import java.util.Objects;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.BytesRef;

/**
 * Expert:
 * Public for extension only.
 * This does not work correctly for terms that indexed at position Integer.MAX_VALUE.
 */
public class TermSpans extends Spans implements IndexLookahead {
  
  public static final byte[] MAGIC_NUMBER;
  public static final int MAGIC_STABLE_LENGTH;
  public static final byte ENCODE_LOOKAHEAD = -31;
  public static final byte NO_ENCODE_LOOKAHEAD = -30;
  static {
    MAGIC_NUMBER = new byte[]{-75, NO_ENCODE_LOOKAHEAD};
    MAGIC_STABLE_LENGTH = MAGIC_NUMBER.length - 1;
  }

  static boolean isMagicPayload(BytesRef br) {
    if (br.length < MAGIC_NUMBER.length) {
      return false;
    }
    final byte[] bytes = br.bytes;
    final int offset = br.offset;
    for (int i = 0; i < MAGIC_STABLE_LENGTH; i++) {
      if (MAGIC_NUMBER[i] != bytes[offset + i]) {
        return false;
      }
    }
    switch (bytes[offset + MAGIC_STABLE_LENGTH]) {
      case ENCODE_LOOKAHEAD:
      case NO_ENCODE_LOOKAHEAD:
        return true;
      default:
        return false;
    }
  }

  private static final int NO_PAYLOAD_POSITION_LENGTH = Integer.MIN_VALUE;
  private static final int PAYLOAD_POSITION_LENGTH_UNINITIALIZED = -1;
  private static final int FIELD_POSITION_LENGTH_UNINITIALIZED = -2;
  
  protected final PostingsEnum postings;
  private final ByteArrayDataInput positionLengthReader = new ByteArrayDataInput();
  protected final Term term;
  protected int doc;
  protected int freq;
  protected int count;
  protected int position;
  protected int positionLength = -1;
  protected boolean encodedLookahead = false;
  protected boolean readPayload;
  private final float positionsCost;

  public TermSpans(Similarity.SimScorer scorer,
                    PostingsEnum postings, Term term, float positionsCost, int postingsFlags) {
    this.postings = Objects.requireNonNull(postings);
    this.term = Objects.requireNonNull(term);
    this.doc = -1;
    this.position = -1;
    this.positionLength = FIELD_POSITION_LENGTH_UNINITIALIZED;
    assert positionsCost > 0; // otherwise the TermSpans should not be created.
    this.positionsCost = positionsCost;
  }

  @Override
  public int nextDoc() throws IOException {
    doc = postings.nextDoc();
    if (doc != DocIdSetIterator.NO_MORE_DOCS) {
      freq = postings.freq();
      assert freq >= 1;
      count = 0;
      resetLookahead();
    }
    position = -1;
    return doc;
  }

  @Override
  public int advance(int target) throws IOException {
    assert target > doc;
    doc = postings.advance(target);
    if (doc != DocIdSetIterator.NO_MORE_DOCS) {
      freq = postings.freq();
      assert freq >= 1;
      count = 0;
      resetLookahead();
    }
    position = -1;
    return doc;
  }

  private void resetLookahead() {
    if (lookaheadNextStartPositionFloor != UNKNOWN_AT_SPANS) {
      // set together; only need to check one.
      lookaheadNextStartPositionFloor = UNINITIALIZED_AT_DOC;
      maxPositionLength = UNINITIALIZED_AT_DOC;
    }
  }

  @Override
  public int docID() {
    return doc;
  }

  private int maxPositionLength = UNINITIALIZED_AT_SPANS;

  @Override
  public int positionLengthCeiling() {
    return maxPositionLength;
  }

  private int lookaheadNextStartPositionFloor = UNINITIALIZED_AT_SPANS;

  @Override
  public int lookaheadNextStartPositionFloor() throws IOException {
    boolean recurseOnce;
    pseudoRecurse:
    do {
      recurseOnce = false;
      if (position < 0) {
        return lookaheadNextStartPositionFloor;
      }
      switch (positionLength) {
        case NO_PAYLOAD_POSITION_LENGTH:
          return UNKNOWN_AT_DOC;
        default:
          return lookaheadNextStartPositionFloor;
        case FIELD_POSITION_LENGTH_UNINITIALIZED:
          recurseOnce = true;
        case PAYLOAD_POSITION_LENGTH_UNINITIALIZED:
          getPositionLength(false);
      }
    } while (recurseOnce);
    return lookaheadNextStartPositionFloor;
  }

  @Override
  public int endPositionDecreaseCeiling() throws IOException {
    final int lookahead = lookaheadNextStartPositionFloor();
    final int endPosition;
    if (lookahead < 0) {
      final int actualPositionLength = getPositionLength(false);
      return actualPositionLength < 3 ? 0 : actualPositionLength - 2;
    } else if (lookahead < (endPosition = endPosition())) {
      return endPosition - lookahead - 1;
    } else {
      return 0;
    }
  }

  @Override
  public int positionLengthFloor() {
    return 1;
  }

  @Override
  public int nextStartPosition() throws IOException {
    int predict = -1;
    if (position >= 0) {
      assert (predict = lookaheadNextStartPositionFloor()) > UNINITIALIZED_AT_DOC;
    }
    if (count == freq) {
      assert position != NO_MORE_POSITIONS;
      return position = NO_MORE_POSITIONS;
    }
    int prevPosition = position;
    position = postings.nextPosition();
    assert position >= prevPosition : "prevPosition="+prevPosition+" > position="+position;
    assert position != NO_MORE_POSITIONS; // int endPosition not possible
    count++;
    readPayload = false;
    switch (positionLength) {
      case NO_PAYLOAD_POSITION_LENGTH:
      case FIELD_POSITION_LENGTH_UNINITIALIZED:
        break;
      default:
        positionLength = PAYLOAD_POSITION_LENGTH_UNINITIALIZED;
    }
    if (count == 1) {
      // this is the first position for this term in this doc, initialize maxPositionLength
      getPositionLength(true);
    }
    assert predict < 0 || predict == position : predict + " != "+position;
    return position;
  }

  @Override
  public int startPosition() {
    return position;
  }

  private boolean validateMagicNumber(byte[] bytes, int offset) {
    for (int i = 0; i < MAGIC_STABLE_LENGTH; i++) {
      if (MAGIC_NUMBER[i] != bytes[offset + i]) {
        positionLength = NO_PAYLOAD_POSITION_LENGTH;
        return false;
      }
    }
    switch (bytes[offset + MAGIC_STABLE_LENGTH]) {
      case ENCODE_LOOKAHEAD:
        encodedLookahead = true;
        break;
      case NO_ENCODE_LOOKAHEAD:
        encodedLookahead = false;
        lookaheadNextStartPositionFloor = UNKNOWN_AT_SPANS;
        maxPositionLength = UNKNOWN_AT_SPANS;
        break;
      default:
        positionLength = NO_PAYLOAD_POSITION_LENGTH;
        return false;
    }
    return true;
  }

  int getPositionLength(boolean initMaxPositionLength) {
    final boolean validateMagicNumber;
    switch (positionLength) {
      case NO_PAYLOAD_POSITION_LENGTH:
        return 1;
      default:
        return positionLength;
      case FIELD_POSITION_LENGTH_UNINITIALIZED:
        validateMagicNumber = true;
        break;
      case PAYLOAD_POSITION_LENGTH_UNINITIALIZED:
        validateMagicNumber = false;
        break;
    }
    BytesRef payload;
    try {
      payload = postings.getPayload();
    } catch (IOException ex) {
      positionLength = NO_PAYLOAD_POSITION_LENGTH;
      return 1;
    }
    int length;
    if (payload == null || (length = payload.length) < MAGIC_NUMBER.length) {
      positionLength = NO_PAYLOAD_POSITION_LENGTH;
      return 1;
    }
    byte[] bytes = payload.bytes;
    int offset = payload.offset;
    if (validateMagicNumber && !validateMagicNumber(bytes, offset)) {
      return 1;
    }
    if (length == MAGIC_NUMBER.length) {
      return positionLength = 1;
    } else {
      positionLengthReader.reset(bytes, offset + MAGIC_NUMBER.length, length - MAGIC_NUMBER.length);
      positionLength = positionLengthReader.readVInt();
      if (encodedLookahead) {
        lookaheadNextStartPositionFloor = position + positionLengthReader.readVInt();
        if (initMaxPositionLength) {
          maxPositionLength = positionLengthReader.eof() ? 1 : positionLengthReader.readInt();
        } else {
          assert positionLengthReader.eof();
        }
      }
      return positionLength;
    }
  }

  @Override
  public int endPosition() {
    return (position == -1) ? -1
          : (position != NO_MORE_POSITIONS) ? position + getPositionLength(false)
          : NO_MORE_POSITIONS;
  }

  @Override
  public int width() {
    return 0;
  }

  @Override
  public long cost() {
    return postings.cost();
  }

  @Override
  public void collect(SpanCollector collector) throws IOException {
    collector.collectLeaf(postings, position, term);
  }

  @Override
  public float positionsCost() {
    return positionsCost;
  }

  @Override
  public String toString() {
    return "spans(" + term.toString() + ")@" +
            (doc == -1 ? "START" : (doc == NO_MORE_DOCS) ? "ENDDOC"
              : doc + " - " + (position == NO_MORE_POSITIONS ? "ENDPOS" : position));
  }

  public PostingsEnum getPostings() {
    return postings;
  }
}
