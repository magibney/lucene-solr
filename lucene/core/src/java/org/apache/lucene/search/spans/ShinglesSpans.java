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
import java.util.Collections;
import java.util.List;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.search.DocIdSetIterator;

/**
 *
 */
public class ShinglesSpans {

  public static List<Spans> pseudoSpansOver(PostingsEnum[] postings, int maxSlop) {
    if (postings.length == 1) {
      return Collections.singletonList(getOneShingleSpans(postings[0], maxSlop));
    } else {
      Spans[] oneShingleSpans = new Spans[postings.length];
      for (int i = postings.length - 1; i >= 0; i--) {
        oneShingleSpans[i] = getOneShingleSpans(postings[i], maxSlop);
      }
      return Arrays.asList(oneShingleSpans);
    }
  }

  private static TwoPhaseOnlySpans getOneShingleSpans(PostingsEnum shinglePostings, int maxSlop) {
    return new TwoPhaseOnlySpans(new OneShingleDISI(shinglePostings, maxSlop));
  }

  private static class OneShingleDISI extends NoMatchDISI {

    private final PostingsEnum postings;
    private final int maxSlopPlusOne;

    public OneShingleDISI(PostingsEnum postings, int maxSlop) {
      this.postings = postings;
      this.maxSlopPlusOne = maxSlop + 1; // to account for requirement that TermFrequency be > 0.
    }

    @Override
    public int advance(int target) throws IOException {
      int ret;
      if ((ret = postings.advance(target)) == Spans.NO_MORE_DOCS || postings.freq() <= maxSlopPlusOne) {
        return ret;
      } else {
        return nextDoc();
      }
    }

    @Override
    public int nextDoc() throws IOException {
      int ret;
      while ((ret = postings.nextDoc()) != Spans.NO_MORE_DOCS && postings.freq() > maxSlopPlusOne) {
        // continue advancing
      }
      return ret;
    }

    @Override
    public int docID() {
      return postings.docID();
    }

    @Override
    public long cost() {
      return postings.cost();
    }

  }

  private static final DocIdSetIterator NO_MATCH_DISI = new NoMatchDISI();
  public static final Spans NO_MATCH_SPANS = new TwoPhaseOnlySpans(NO_MATCH_DISI);

  private static class NoMatchDISI extends DocIdSetIterator {

    @Override
    public int docID() {
      return -1;
    }

    @Override
    public int nextDoc() throws IOException {
      return Spans.NO_MORE_DOCS;
    }

    @Override
    public int advance(int target) throws IOException {
      return Spans.NO_MORE_DOCS;
    }

    @Override
    public long cost() {
      return 1;
    }

  }
}
