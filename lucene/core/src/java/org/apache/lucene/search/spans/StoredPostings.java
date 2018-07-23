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
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.spans.PositionDeque.Node;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;

/**
 *
 * @author magibney
 */
class StoredPostings extends PostingsEnum {
  private int freq;
  int startOffset;
  int endOffset;
  private BytesRef payload;
  int docID;
  int position;
  int positionLength;
  Term term;
  StoredPostings next;
  StoredPostings trackNextRef;
  
  StoredPostings addNext(StoredPostings next) {
    return this.next = next;
  }

  void clear() {
    this.next = null;
  }

  StoredPostings() {
    freq = -1;
    startOffset = -1;
    endOffset = -1;
    payload = null;
    position = -1;
    positionLength = -1;
    term = null;
    docID = -1;
  }
  
  private StoredPostings(boolean local) {
    // to distinguish the pooled, private accessible constructor
  }
  
  private static StoredPostings newInstance(Node n) {
    StoredPostings ret = n.dequePointer[0].pool.getStoredPostings();
    if (ret == null) {
      ret = new StoredPostings(true);
      n.pool.addStoredPostings(ret);
    } else {
      ret.clear();
    }
    return ret;
  }

  static StoredPostings newInstance(Node context, PostingsEnum toStore, int position, int positionLength, Term term, boolean offsets) throws IOException {
    return newInstance(context).init(toStore, position, positionLength, term, offsets);
  }

  static StoredPostings newInstance(Node context, PostingsEnum toStore, int position, Term term, boolean offsets) throws IOException {
    return newInstance(context).init(toStore, position, -1, term, offsets);
  }

  private StoredPostings init(PostingsEnum toStore, int position, int positionLength, Term term, boolean offsets) throws IOException {
    this.freq = toStore.freq();
    if (offsets) {
      this.startOffset = toStore.startOffset();
      this.endOffset = toStore.endOffset();
    } else {
      this.startOffset = -1;
      this.endOffset = -1;
    }
    BytesRef toStorePayload = toStore.getPayload();
    // only clone payload if not null; also ignore (for now) magic payloads currently used to carry lookahead and positionLength info
    this.payload = toStorePayload == null || TermSpans.isMagicPayload(toStorePayload) ? null : toStorePayload.clone();
    assert this.payload == null;
    this.docID = toStore.docID();
    assert toStore instanceof StoredPostings || !toStore.attributes().hasAttributes();
    this.position = position;
    this.positionLength = positionLength;
    this.term = term;
    return this;
  }

  @Override
  public AttributeSource attributes() {
    // couldn't find any examples of this being used. Vestigial?
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public int freq() throws IOException {
    return freq;
  }

  @Override
  public int nextPosition() throws IOException {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public int startOffset() throws IOException {
    return startOffset;
  }

  @Override
  public int endOffset() throws IOException {
    return endOffset;
  }

  @Override
  public BytesRef getPayload() throws IOException {
    return payload;
  }

  @Override
  public int docID() {
    return docID;
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
    return 1;
  }
    
}
