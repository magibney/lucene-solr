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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;

/** Matches spans which are near one another.  One can specify <i>slop</i>, the
 * maximum number of intervening unmatched positions, as well as whether
 * matches are required to be in-order.
 */
public class SpanNearQuery extends SpanQuery implements Cloneable {

  /**
   * A builder for SpanNearQueries
   */
  public static class Builder {
    private final boolean ordered;
    private final ComboMode comboMode;
    private final int comboThreshold;
    private final boolean allowOverlap;
    private final boolean combineRepeatSpans;
    private final boolean supportVariableTermSpansLength;
    private final String field;
    private final List<SpanQuery> clauses = new LinkedList<>();
    private int slop;
    private String shingleFieldSuffix;
    private Set<BytesRef> shingles;

    /**
     * Construct a new builder
     * @param field the field to search in
     * @param ordered whether or not clauses must be in-order to match
     */
    public Builder(String field, boolean ordered) {
      this(field, ordered, DEFAULT_COMBO_MODE, DEFAULT_COMBO_THRESHOLD, DEFAULT_ALLOW_OVERLAP, DEFAULT_COMBINE_REPEAT_SPANS, DEFAULT_SUPPORT_VARIABLE_TERM_SPANS_LENGTH);
    }
    public Builder(String field, boolean ordered, ComboMode comboMode) {
      this(field, ordered, comboMode, DEFAULT_COMBO_THRESHOLD, DEFAULT_ALLOW_OVERLAP, DEFAULT_COMBINE_REPEAT_SPANS, DEFAULT_SUPPORT_VARIABLE_TERM_SPANS_LENGTH);
    }
    public Builder(String field, boolean ordered, ComboMode comboMode, int comboThreshold) {
      this(field, ordered, comboMode, comboThreshold, DEFAULT_ALLOW_OVERLAP, DEFAULT_COMBINE_REPEAT_SPANS, DEFAULT_SUPPORT_VARIABLE_TERM_SPANS_LENGTH);
    }
    public Builder(String field, boolean ordered, ComboMode comboMode, int comboThreshold, boolean allowOverlap) {
      this(field, ordered, comboMode, comboThreshold, allowOverlap, DEFAULT_COMBINE_REPEAT_SPANS, DEFAULT_SUPPORT_VARIABLE_TERM_SPANS_LENGTH);
    }
    public Builder(SpanNearQuery q) {
      this(q.field, q.inOrder, q.comboMode, q.comboThreshold, q.allowOverlap, q.combineRepeatSpans, q.supportVariableTermSpansLength);
      this.clauses.addAll(q.clauses);
    }
    public Builder(String field, boolean ordered, ComboMode comboMode, int comboThreshold, boolean allowOverlap,
        boolean combineRepeatSpans, boolean supportVariableTermSpansLength) {
      this.field = field;
      this.ordered = ordered;
      this.comboMode = comboMode;
      this.comboThreshold = comboThreshold;
      this.combineRepeatSpans = combineRepeatSpans;
      this.allowOverlap = allowOverlap;
      this.supportVariableTermSpansLength = supportVariableTermSpansLength;
    }

    /**
     * Add a new clause
     */
    public Builder addClause(SpanQuery clause) {
      if (Objects.equals(clause.getField(), field) == false)
        throw new IllegalArgumentException("Cannot add clause " + clause + " to SpanNearQuery for field " + field);
      this.clauses.add(clause);
      return this;
    }

    /**
     * Add a gap after the previous clause of a defined width
     */
    public Builder addGap(int width) {
      if (!ordered)
        throw new IllegalArgumentException("Gaps can only be added to ordered near queries");
      this.clauses.add(new SpanGapQuery(field, width));
      return this;
    }

    /**
     * Set the slop for this query
     */
    public Builder setSlop(int slop) {
      this.slop = slop;
      return this;
    }

    /**
     * Set shingles required for this query to match at specified slop
     * @param shingles
     * @return 
     */
    public Builder setShingles(String shingleFieldSuffix, Set<BytesRef> shingles) {
      if (!ordered) {
        throw new IllegalStateException("may not set shingles for unordered SpanNearQuery");
      }
      this.shingleFieldSuffix = shingleFieldSuffix;
      this.shingles = shingles;
      return this;
    }

    /**
     * Build the query
     */
    public SpanNearQuery build() {
      return new SpanNearQuery(clauses.toArray(new SpanQuery[clauses.size()]), slop, ordered, comboMode, comboThreshold, allowOverlap, combineRepeatSpans, supportVariableTermSpansLength, shingleFieldSuffix, shingles);
    }

  }

  /**
   * Returns a {@link Builder} for an ordered query on a particular field
   */
  public static Builder newOrderedNearQuery(String field) {
    return new Builder(field, true);
  }

  /**
   * Returns a {@link Builder} for an unordered query on a particular field
   */
  public static Builder newUnorderedNearQuery(String field) {
    return new Builder(field, false);
  }

  protected List<SpanQuery> clauses;
  protected int slop;
  protected final String shingleFieldName;
  protected final Term[] shingles;
  protected boolean inOrder;

  /**
   * Allow overlapping matches
   */
  protected boolean allowOverlap;
  protected boolean combineRepeatSpans;
  protected boolean supportVariableTermSpansLength;

  /**
   * threshold of combinations per (possibly overlapping) start position.
   * This is used to prevent exponential explosion of match possibilities for (very) sloppy SpanNearQueries over (very)
   * large numbers of (very) common terms, e.g., "the the the the the the the"~100
   */
  protected int comboThreshold;

  /**
   * Attempt to find, per (possibly overlapping) start position, returning immediately after success.
   * This attempt is subject to the constraint applied by comboThreshold. Estimate based on comboThreshold
   * guides the approach, but ultimately comboThreshold places a hard limit on the number of combos that 
   * will be attempted per start position.
   * 
   * GREEDY_END_POSITION: one match per start, greedy from first span to last
   * MIN_END_POSITION: the one match with minimum endPosition
   * MAX_END_POSITION: the one match with maximum endPosition
   * PER_END_POSITION: one match per valid endPosition (best for thorough matching in outer queries)
   * PER_POSITION: match attempted for each candidate position of each subSpan (best for thorough scoring)
   * FULL: match attempted for each possible combination of subSpan positions (this is crazy)
   */
  public static enum ComboMode {
    GREEDY_END_POSITION,
    MIN_END_POSITION,
    MAX_END_POSITION,
    PER_END_POSITION,
    PER_POSITION, // trackOutput
    PER_POSITION_PER_START_POSITION,
    FULL_DISTILLED_PER_POSITION, // trackOutput
    FULL_DISTILLED_PER_START_POSITION, // trackOutputPerPass
    FULL_DISTILLED,
    FULL
  }
  protected ComboMode comboMode;
  public static final ComboMode DEFAULT_COMBO_MODE = ComboMode.PER_END_POSITION;
  public static final int DEFAULT_COMBO_THRESHOLD = 1000;
  public static final boolean DEFAULT_ALLOW_OVERLAP = true;
  public static final boolean DEFAULT_COMBINE_REPEAT_SPANS = true;
  public static final boolean DEFAULT_SUPPORT_VARIABLE_TERM_SPANS_LENGTH = false;

  private static final String IMPOSSIBLE_FIELD_NAME = "\uFFFC\uFFFC\uFFFC\uFFFC"; // slightly different from edismax I_F_N
  protected final String field;

  /** Construct a SpanNearQuery.  Matches spans matching a span from each
   * clause, with up to <code>slop</code> total unmatched positions between
   * them.
   * <br>When <code>inOrder</code> is true, the spans from each clause
   * must be in the same order as in <code>clauses</code> and must be non-overlapping.
   * <br>When <code>inOrder</code> is false, the spans from each clause
   * need not be ordered and may overlap.
   * @param clausesIn the clauses to find near each other, in the same field, at least 2.
   * @param slop The slop value
   * @param inOrder true if order is important
   * @param comboMode
   * @param comboThreshold
   * @param allowOverlap
   */
  public SpanNearQuery(SpanQuery[] clausesIn, int slop, boolean inOrder, ComboMode comboMode, int comboThreshold, boolean allowOverlap,
      boolean combineRepeatSpans, boolean supportVariableTermSpansLength, String shingleFieldSuffix, Set<BytesRef> shingles) {
    this.clauses = new ArrayList<>(clausesIn.length);
    if (clausesIn.length < 1) {
      // workaround to avoid NPE
      this.field = IMPOSSIBLE_FIELD_NAME;
    } else {
      SpanQuery clause = clausesIn[0];
      this.field = clause.getField();
      this.clauses.add(clause);
      for (int i = 1; i < clausesIn.length; i++) {
        clause = clausesIn[i];
        if (!field.equals(clause.getField())) {
          throw new IllegalArgumentException("Clauses must have same field.");
        }
        this.clauses.add(clause);
      }
    }
    this.slop = slop;
    if (shingles == null || shingles.isEmpty()) {
      shingleFieldName = null;
      this.shingles = null;
    } else {
      shingleFieldName = this.field.concat(shingleFieldSuffix);
      this.shingles = new Term[shingles.size()];
      int i = 0;
      for (BytesRef br : shingles) {
        this.shingles[i++] = new Term(shingleFieldName, br);
      }
    }
    this.comboMode = comboMode;
    this.comboThreshold = comboThreshold;
    this.allowOverlap = allowOverlap;
    this.combineRepeatSpans = combineRepeatSpans;
    this.supportVariableTermSpansLength = supportVariableTermSpansLength;
    this.inOrder = inOrder;
  }

  public SpanNearQuery(SpanQuery[] clausesIn, int slop, boolean inOrder, ComboMode comboMode, int comboThreshold, boolean allowOverlap, boolean combineRepeatSpans) {
    this(clausesIn, slop, inOrder, comboMode, comboThreshold, allowOverlap, combineRepeatSpans, DEFAULT_SUPPORT_VARIABLE_TERM_SPANS_LENGTH, null, null);
  }

  public SpanNearQuery(SpanQuery[] clausesIn, int slop, boolean inOrder, ComboMode comboMode, int comboThreshold, boolean allowOverlap) {
    this(clausesIn, slop, inOrder, comboMode, comboThreshold, allowOverlap, DEFAULT_COMBINE_REPEAT_SPANS, DEFAULT_SUPPORT_VARIABLE_TERM_SPANS_LENGTH, null, null);
  }

  public SpanNearQuery(SpanQuery[] clausesIn, int slop, boolean inOrder, ComboMode comboMode, int comboThreshold) {
    this(clausesIn, slop, inOrder, comboMode, comboThreshold, DEFAULT_ALLOW_OVERLAP, DEFAULT_COMBINE_REPEAT_SPANS, DEFAULT_SUPPORT_VARIABLE_TERM_SPANS_LENGTH, null, null);
  }

  public SpanNearQuery(SpanQuery[] clausesIn, int slop, boolean inOrder, ComboMode comboMode) {
    this(clausesIn, slop, inOrder, comboMode, DEFAULT_COMBO_THRESHOLD, DEFAULT_ALLOW_OVERLAP, DEFAULT_COMBINE_REPEAT_SPANS, DEFAULT_SUPPORT_VARIABLE_TERM_SPANS_LENGTH, null, null);
  }

  public SpanNearQuery(SpanQuery[] clausesIn, int slop, boolean inOrder) {
    this(clausesIn, slop, inOrder, DEFAULT_COMBO_MODE, DEFAULT_COMBO_THRESHOLD, DEFAULT_ALLOW_OVERLAP, DEFAULT_COMBINE_REPEAT_SPANS, DEFAULT_SUPPORT_VARIABLE_TERM_SPANS_LENGTH, null, null);
  }

  /** Return the clauses whose spans are matched. */
  public SpanQuery[] getClauses() {
    return clauses.toArray(new SpanQuery[clauses.size()]);
  }

  /** Return the maximum number of intervening unmatched positions permitted.*/
  public int getSlop() { return slop; }

  /** Return true if matches are required to be in-order.*/
  public boolean isInOrder() { return inOrder; }

  @Override
  public String getField() { return field; }

  @Override
  public String toString(String field) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("spanNear([");
    Iterator<SpanQuery> i = clauses.iterator();
    while (i.hasNext()) {
      SpanQuery clause = i.next();
      buffer.append(clause.toString(field));
      if (i.hasNext()) {
        buffer.append(", ");
      }
    }
    buffer.append("], ");
    buffer.append(slop);
    buffer.append(", ");
    buffer.append(inOrder);
    buffer.append(")");
    return buffer.toString();
  }

  private TermsEnum[] getSharedTermEnums(IndexSearcher searcher) throws IOException {
    List<LeafReaderContext> leaves = searcher.getTopReaderContext().leaves();
    TermsEnum[] teShare = new TermsEnum[leaves.size()];
    for (LeafReaderContext lrc : leaves) {
      Terms terms = lrc.reader().terms(field);
      teShare[lrc.ord] = terms == null ? null : terms.iterator();
    }
    return teShare;
  }
  
  private TermsEnum[] sharedTermEnums;
  
  @Override
  public SpanWeight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
    List<SpanWeight> subWeights = new ArrayList<>();
    for (SpanQuery q : clauses) {
      if (q instanceof SpanTermQuery) {
        if (sharedTermEnums == null) {
          sharedTermEnums = getSharedTermEnums(searcher);
        }
        subWeights.add(((SpanTermQuery)q).createWeight(searcher, scoreMode, boost, sharedTermEnums));
      } else {
        subWeights.add(q.createWeight(searcher, scoreMode, boost));
      }
    }
    return new SpanNearWeight(subWeights, searcher, scoreMode.needsScores() ? getTermStates(subWeights) : null, boost);
  }

  public class SpanNearWeight extends SpanWeight {

    final List<SpanWeight> subWeights;
    private List<TermSpansRepeatBuffer> reuse;
    private List<PositionDeque> reuseDeque;
    private final ComboMode weightComboMode;

    public SpanNearWeight(List<SpanWeight> subWeights, IndexSearcher searcher, Map<Term, TermStates> terms, float boost) throws IOException {
      super(SpanNearQuery.this, searcher, terms, boost);
      this.weightComboMode = /*terms == null ? ComboMode.GREEDY_END_POSITION :*/ comboMode;
      this.subWeights = subWeights;
    }

    @Override
    public void extractTermStates(Map<Term, TermStates> contexts) {
      for (SpanWeight w : subWeights) {
        w.extractTermStates(contexts);
      }
    }

    @Override
    public Spans getSpans(final LeafReaderContext context, Postings requiredPostings) throws IOException {

      Terms terms = context.reader().terms(field);
      if (terms == null) {
        return null; // field does not exist
      }

      ArrayList<Spans> subSpans = new ArrayList<>(clauses.size());
      for (SpanWeight w : subWeights) {
        Spans subSpan = w.getSpans(context, requiredPostings);
        if (subSpan != null) {
          subSpans.add(subSpan);
        } else {
          return null; // all required
        }
      }

      final Iterator<TermSpansRepeatBuffer> reuseIter;
      if (reuse != null) {
        reuseIter = reuse.iterator();
        reuse = new ArrayList<>(reuse.size());
      } else {
        reuseIter = null;
        reuse = new ArrayList<>(subSpans.size());
      }

      final Iterator<PositionDeque> reuseDequeIter;
      if (reuseDeque != null) {
        reuseDequeIter = reuseDeque.iterator();
        reuseDeque = new ArrayList<>(reuseDeque.size());
      } else {
        reuseDequeIter = null;
        reuseDeque = new ArrayList<>(subSpans.size());
      }
      
      // all NearSpans require at least two subSpans
      if (!inOrder) {
        return new NearSpansUnordered(slop, subSpans);
      } else {
        final Terms shingleTerms;
        final List<Spans> shinglesSpans;
        getShinglesSpans:
        if (shingles == null) {
          // either slop is too great, or no shingles found for query.
          shinglesSpans = null;
        } else if ((shingleTerms = context.reader().terms(shingleFieldName)) == null) {
          // shingles field doesn't even exist for this segment
          shinglesSpans = NO_MATCH_SPANS_LIST;
        } else {
          final PostingsEnum[] shinglePostings = new PostingsEnum[shingles.length];
          int i = 0;
          TermsEnum te = shingleTerms.iterator();
          for (Term t : shingles) {
            if (te.seekExact(t.bytes())) {
              shinglePostings[i++] = te.postings(null, PostingsEnum.FREQS);
            } else {
              // can't match for this segment
              shinglesSpans = NO_MATCH_SPANS_LIST;
              break getShinglesSpans;
            }
          }
          shinglesSpans = ShinglesSpans.pseudoSpansOver(shinglePostings, slop);
        }
        return new NearSpansOrdered(slop, subSpans, weightComboMode, comboThreshold, allowOverlap,
            combineRepeatSpans, reuseIter, reuse, (requiredPostings.getRequiredPostings() & PostingsEnum.OFFSETS) != 0,
            supportVariableTermSpansLength, reuseDequeIter, reuseDeque, shinglesSpans);
      }
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      for (SpanWeight w : subWeights) {
        w.extractTerms(terms);
      }
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      for (Weight w : subWeights) {
        if (w.isCacheable(ctx) == false)
          return false;
      }
      return true;
    }

  }

  private static final List<Spans> NO_MATCH_SPANS_LIST = Collections.singletonList(ShinglesSpans.NO_MATCH_SPANS);

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    boolean actuallyRewritten = false;
    List<SpanQuery> rewrittenClauses = new ArrayList<>();
    for (int i = 0 ; i < clauses.size(); i++) {
      SpanQuery c = clauses.get(i);
      SpanQuery query = (SpanQuery) c.rewrite(reader);
      actuallyRewritten |= query != c;
      rewrittenClauses.add(query);
    }
    if (actuallyRewritten) {
      try {
        SpanNearQuery rewritten = (SpanNearQuery) clone();
        rewritten.clauses = rewrittenClauses;
        return rewritten;
      } catch (CloneNotSupportedException e) {
        throw new AssertionError(e);
      }
    }
    return super.rewrite(reader);
  }

  @Override
  public boolean equals(Object other) {
    return sameClassAs(other) &&
           equalsTo(getClass().cast(other));
  }
  
  private boolean equalsTo(SpanNearQuery other) {
    return inOrder == other.inOrder && 
           slop == other.slop &&
           clauses.equals(other.clauses);
  }

  @Override
  public int hashCode() {
    int result = classHash();
    result ^= clauses.hashCode();
    result += slop;
    int fac = 1 + (inOrder ? 8 : 4);
    return fac * result;
  }

  private static class SpanGapQuery extends SpanQuery {

    private final String field;
    private final int width;

    public SpanGapQuery(String field, int width) {
      this.field = field;
      this.width = width;
    }

    @Override
    public String getField() {
      return field;
    }

    @Override
    public String toString(String field) {
      return "SpanGap(" + field + ":" + width + ")";
    }

    @Override
    public SpanWeight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
      return new SpanGapWeight(searcher, boost);
    }

    private class SpanGapWeight extends SpanWeight {

      SpanGapWeight(IndexSearcher searcher, float boost) throws IOException {
        super(SpanGapQuery.this, searcher, null, boost);
      }

      @Override
      public void extractTermStates(Map<Term, TermStates> contexts) {

      }

      @Override
      public Spans getSpans(LeafReaderContext ctx, Postings requiredPostings) throws IOException {
        return new GapSpans(width);
      }

      @Override
      public void extractTerms(Set<Term> terms) {

      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return true;
      }

    }

    @Override
    public boolean equals(Object other) {
      return sameClassAs(other) &&
             equalsTo(getClass().cast(other));
    }
    
    private boolean equalsTo(SpanGapQuery other) {
      return width == other.width &&
             field.equals(other.field);
    }

    @Override
    public int hashCode() {
      int result = classHash();
      result -= 7 * width;
      return result * 15 - field.hashCode();
    }

  }

  static class GapSpans extends Spans {

    int doc = -1;
    int pos = -1;
    final int width;

    GapSpans(int width) {
      this.width = width;
    }

    @Override
    public int nextStartPosition() throws IOException {
      return ++pos;
    }

    public int skipToPosition(int position) throws IOException {
      return pos = position;
    }

    @Override
    public int startPosition() {
      return pos;
    }

    @Override
    public int endPosition() {
      return pos + width;
    }

    @Override
    public int width() {
      return width;
    }

    @Override
    public void collect(SpanCollector collector) throws IOException {

    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      pos = -1;
      return ++doc;
    }

    @Override
    public int advance(int target) throws IOException {
      pos = -1;
      return doc = target;
    }

    @Override
    public long cost() {
      return 0;
    }

    @Override
    public float positionsCost() {
      return 0;
    }
  }

}
