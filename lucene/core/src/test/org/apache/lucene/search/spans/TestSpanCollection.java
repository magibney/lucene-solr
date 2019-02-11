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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.FlattenGraphFilter;
import org.apache.lucene.analysis.core.PositionLengthOrderTokenFilter;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter;
import static org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter.CATENATE_WORDS;
import static org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter.GENERATE_NUMBER_PARTS;
import static org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter.GENERATE_WORD_PARTS;
import static org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter.PRESERVE_ORIGINAL;
import static org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter.STEM_ENGLISH_POSSESSIVE;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.QueryBuilder;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class TestSpanCollection extends LuceneTestCase {

  protected IndexSearcher searcher;
  protected Directory directory;
  protected IndexReader reader;

  public static final String FIELD = "field";

  public static FieldType OFFSETS = new FieldType(TextField.TYPE_STORED);
  static {
    OFFSETS.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    directory.close();
    super.tearDown();
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory,
        newIndexWriterConfig(new ExtensionAnalyzerWrapper(random())).setMergePolicy(newLogMergePolicy()));
    for (int i = 0; i < docFields.length; i++) {
      Document doc = new Document();
      doc.add(newField(FIELD, docFields[i], OFFSETS));
      writer.addDocument(doc);
    }
    writer.forceMerge(1);
    reader = writer.getReader();
    writer.close();
    searcher = newSearcher(getOnlyLeafReader(reader));
  }

  static final boolean ENCODE_LOOKAHEAD = true;

  private static class ExtensionAnalyzerWrapper extends Analyzer {

    private final MockAnalyzer backing;

    private ExtensionAnalyzerWrapper(Random random) {
      this.backing = new MockAnalyzer(random);
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
      TokenStreamComponents ret = backing.createComponents(fieldName);
      TokenStream result = new PositionLengthSetter(ret.getTokenStream());
      result = new PositionLengthOrderTokenFilter(result, ENCODE_LOOKAHEAD);
      return new TokenStreamComponents(ret.getTokenizer(), result);
    }

  }

  private static class PositionLengthSetter extends TokenFilter {

    private final CharTermAttribute termAtt;
    private final PositionLengthAttribute posLenAtt;

    public PositionLengthSetter(TokenStream input) {
      super(input);
      termAtt = getAttribute(CharTermAttribute.class);
      posLenAtt = addAttribute(PositionLengthAttribute.class);
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (!input.incrementToken()) {
        return false;
      } else {
        final int testNewLength = termAtt.length() - 1;
        if (termAtt.buffer()[testNewLength] == '~') {
          termAtt.setLength(testNewLength);
          posLenAtt.setPositionLength(2);
        } else {
          posLenAtt.setPositionLength(1);
        }
        return true;
      }
    }

  }

  private static class TermCollector implements SpanCollector {

    final Set<Term> terms = new HashSet<>();

    @Override
    public void collectLeaf(PostingsEnum postings, int position, Term term) throws IOException {
      terms.add(term);
    }

    @Override
    public void reset() {
      terms.clear();
    }

  }

  private static class TermPositionCollector implements SpanCollector {

    final Map<Integer, Term> terms = new TreeMap<>();

    @Override
    public void collectLeaf(PostingsEnum postings, int position, Term term) throws IOException {
      terms.put(position, term);
    }

    @Override
    public void reset() {
      terms.clear();
    }

  }

  private static final int[][] problem2 = new int[][] {
    {157, 159, 162, 165, 167, 170, 174, Integer.MAX_VALUE},
    {189, 236, 290, Integer.MAX_VALUE},
    {243, 246, 276, 325, Integer.MAX_VALUE},
    {349, Integer.MAX_VALUE}
  };
  private static final String problemCase2;
  private static final int[][] problem3 = new int[][] {
    {71,76,120,124,127,148,171,183,195,199,211,247,306,333,343,346,351,354,358,367,370,375,382,405,414, Integer.MAX_VALUE},
    {151,160,174,229,296,311,349,467,485, Integer.MAX_VALUE},
    {164,173,197,228,236,323,348,353,377,404,412,449,472,480,516, Integer.MAX_VALUE},
    {182,226,253,400,452,519, Integer.MAX_VALUE}
  };
  private static final int[][] problem3Solution = new int[][]{
    {120, 183, 59},
    {124, 183, 55},
    {124, 227, 99},
    {127, 183, 52},
    {127, 227, 96},
    {148, 183, 31},
    {148, 227, 75},
    {171, 227, 52},
    {171, 254, 79},
    {183, 254, 67},
    {195, 254, 55},
    {199, 254, 51},
    {211, 254, 39},
    {306, 401, 91},
    {333, 401, 64},
    {343, 401, 54},
    {346, 401, 51}
  };
  private static final String problemCase3;

  static {
    int a = 0;
    int b = 0;
    int c = 0;
    int d = 0;
    StringBuilder sb = new StringBuilder(400 * 3);
    for (int i = 0; i < 400; i++) {
      if (problem2[0][a] == i) {
        a++;
        sb.append("ax ");
      } else if (problem2[1][b] == i) {
        b++;
        sb.append("bx ");
      } else if (problem2[2][c] == i) {
        c++;
        sb.append("cx ");
      } else if (problem2[3][d] == i) {
        d++;
        sb.append("dx ");
      } else {
        sb.append("xx ");
      }
    }
    problemCase2 = sb.toString();
    sb.setLength(0);
    a = 0;
    b = 0;
    c = 0;
    d = 0;
    for (int i = 0; i < 600; i++) {
      if (problem3[0][a] == i) {
        a++;
        sb.append("ay ");
      } else if (problem3[1][b] == i) {
        b++;
        sb.append("by ");
      } else if (problem3[2][c] == i) {
        c++;
        sb.append("cy ");
      } else if (problem3[3][d] == i) {
        d++;
        sb.append("dy ");
      } else {
        sb.append("xy ");
      }
    }
    problemCase3 = sb.toString();
  }
  
  private static final int[] buildProblem4 = new int[] {-22, 87, 99, 103, 111, 117, 121, 124, 138, Integer.MAX_VALUE};
  private static final String problemCase4;
  private static final int[] buildProblem = new int[] {182, 240, 246, 253, 281, 284, 287, 314, 318, Integer.MAX_VALUE};
  private static final String problemCase;
  
  static {
    int j = 0;
    StringBuilder sb = new StringBuilder(330 * 2);
    for (int i = 0; i < 330; i++) {
      if (buildProblem[j] == i) {
        j++;
        sb.append("j ");
      } else {
        sb.append("i ");
      }
    }
    problemCase = sb.toString();
    int[] input = buildProblem4;
    sb.setLength(0);
    j = 0;
    for (int i = 0; i < 330; i++) {
      final int position = input[j];
      if (position == i) {
        j++;
        sb.append("j ");
      } else if (~position == i) {
        j++;
        sb.append("j~ ");
      } else {
        sb.append("i ");
      }
    }
    problemCase4 = sb.toString();
  }

  private static final String combinatorialCase;
  private static final int combinatorialTermCount = 8;
  private static final int combinatorialTermRepeat = 8;
  
  static {
    int edgeCount = combinatorialTermCount * combinatorialTermRepeat;
    int edgeTermMaxSize = (int) Math.log10(edgeCount) + 3; // 1 for adjust, 1 for "e", 1 for space delim
    StringBuilder sb = new StringBuilder(edgeTermMaxSize * edgeCount);
    for (int i = 1; i <= combinatorialTermCount; i++) {
      String term = " e".concat(Integer.toString(i));
      for (int j = 0; j < combinatorialTermRepeat; j++) {
        sb.append(term);
      }
    }
    combinatorialCase = sb.substring(1);
    System.err.println(combinatorialCase);
  }
  
  protected static final String[] docFields = {
      "w1 w2 w3 w4 w5", // 0
      "w1 w3 w2 w3 zz", // 1
      "w1 xx w2 yy w4", // 2
      "w1 w2 w1 w4 w2 w3", // 3
      "coordinate gene mapping research", // 4
      "coordinate gene research", // 5
      "the system of claim 16 further comprising a user location unit adapted to determine user location based on location information received from the user's device", // 6
      "alsdfkjf", // 7
      "aa bb fineness cc ee colority dd", // 8
      "a b c d e f g h i", // 9
      "t1 t2 t1 t2 t1 t2 t1 blah blah t2 blah", // 10
      combinatorialCase, // 11
      "a a a b b b c c c", // 12
      "a b a b a b a b a b a b a b a b a b a b a b a b a b a b a b a", // 13
      "a b b a b a b a b a b a b a b a b a b a b a b a b a b a b a b a", // 14
      "sa sb sx sc sd sx se sf sg sx sh si sj", // 15
      "a a a b b b c c c d x x d", // 16
      "x y x y x y x y x y x y y y y y y y y y", // 17
      problemCase, // 18
      "the the history the", // 19
      problemCase2, // 20
      problemCase3, // 21
      "q r q s q x r q x s r s q r s", // 22
      problemCase4 // 23
  };

  
  
  @Test 
  public void testSimple() throws IOException {
    if (NARROW) return;
    SpanTermQuery q1 = new SpanTermQuery(new Term(FIELD, "w1"));
    SpanTermQuery q2 = new SpanTermQuery(new Term(FIELD, "w2"));
    SpanTermQuery q3 = new SpanTermQuery(new Term(FIELD, "w3"));
    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{q1, q2, q3}, 1, true);
    Spans spans = q.createWeight(searcher, true, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    TermCollector collector = new TermCollector();
    int start;
    assertEquals(0, spans.advance(0));
    assertEquals(0, spans.nextStartPosition());
    assertEquals(3, spans.endPosition());
    assertEquals(0, spans.width());
    checkCollectedTerms(spans, collector, new Term(FIELD, "w1"), new Term(FIELD, "w2"), new Term(FIELD, "w3"));
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }
  @Test 
  public void testRejectedStartPosition() throws IOException {
    if (NARROW) return;
    SpanTermQuery q1 = new SpanTermQuery(new Term(FIELD, "q"));
    SpanTermQuery q2 = new SpanTermQuery(new Term(FIELD, "r"));
    SpanTermQuery q3 = new SpanTermQuery(new Term(FIELD, "s"));
    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{q1, q2, q3}, 2, true);
    Spans spans = q.createWeight(searcher, true, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    TermCollector collector = new TermCollector();
    int start;
    assertEquals(22, spans.advance(22));
    assertEquals(0, spans.nextStartPosition());
    assertEquals(4, spans.endPosition());
    assertEquals(1, spans.width());
    checkCollectedTerms(spans, collector, new Term(FIELD, "q"), new Term(FIELD, "r"), new Term(FIELD, "s"));
    assertEquals(7, spans.nextStartPosition());
    assertEquals(12, spans.endPosition());
    assertEquals(2, spans.width());
    checkCollectedTerms(spans, collector, new Term(FIELD, "q"), new Term(FIELD, "r"), new Term(FIELD, "s"));
    assertEquals(12, spans.nextStartPosition());
    assertEquals(15, spans.endPosition());
    assertEquals(0, spans.width());
    checkCollectedTerms(spans, collector, new Term(FIELD, "q"), new Term(FIELD, "r"), new Term(FIELD, "s"));
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }
  @Test 
  public void testProblem3() throws IOException {
    if (NARROW) return;
    SpanTermQuery q1 = new SpanTermQuery(new Term(FIELD, "ay"));
    SpanTermQuery q2 = new SpanTermQuery(new Term(FIELD, "by"));
    SpanTermQuery q3 = new SpanTermQuery(new Term(FIELD, "cy"));
    SpanTermQuery q4 = new SpanTermQuery(new Term(FIELD, "dy"));
    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{q1, q2, q3, q4}, 100, true,
        SpanNearQuery.ComboMode.FULL_DISTILLED_PER_START_POSITION, SpanNearQuery.DEFAULT_ALLOW_OVERLAP,
        SpanNearQuery.DEFAULT_COMBINE_REPEAT_GROUPS_THRESHOLD, true, null, null);
    Spans spans = q.createWeight(searcher, true, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    int start;
    assertEquals(21, spans.advance(20));
    for (int[] positionVals : problem3Solution) {
      assertEquals(positionVals[0], start = spans.nextStartPosition());
      assertEquals(positionVals[1], spans.endPosition());
      assertEquals(positionVals[2], spans.width());
      //System.err.println("hey "+start+"=>"+spans.endPosition()+" ("+spans.width()+")");
    }
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
    assertEquals(Spans.NO_MORE_DOCS, spans.nextDoc());
  }
  @Test 
  public void testOrLookahead() throws IOException {
    if (NARROW) return;
    SpanTermQuery q1 = new SpanTermQuery(new Term(FIELD, "ay"));
    SpanTermQuery q2 = new SpanTermQuery(new Term(FIELD, "by"));
    SpanTermQuery q3 = new SpanTermQuery(new Term(FIELD, "cy"));
    SpanTermQuery q4 = new SpanTermQuery(new Term(FIELD, "dy"));
    SpanOrQuery q = new SpanOrQuery(new SpanQuery[] {q1, q2, q3, q4});
    Spans spans = q.createWeight(searcher, true, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    ArrayList<Integer> allStartPositions = new ArrayList<>();
    for (int[] startPositions : problem3) {
      for (int i = 0; i < startPositions.length - 1; i++) {
        allStartPositions.add(startPositions[i]);
      }
    }
    Integer[] allStart = allStartPositions.toArray(new Integer[allStartPositions.size()]);
    Arrays.sort(allStart);
    int start;
    assertEquals(21, spans.advance(20));
    int expectedStart = allStart[0];
    for (int i = 1; i < allStart.length; i++) {
      int expectedLookaheadStart = allStart[i];
      assertEquals(expectedStart, start = spans.nextStartPosition());
      assertEquals(expectedLookaheadStart, ((IndexLookahead)spans).lookaheadNextStartPositionFloor());
      expectedStart = expectedLookaheadStart;
    }
    assertEquals(expectedStart, start = spans.nextStartPosition());
    assertEquals(Spans.NO_MORE_POSITIONS, ((IndexLookahead)spans).lookaheadNextStartPositionFloor());
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
    assertEquals(Spans.NO_MORE_DOCS, spans.nextDoc());
  }
  @Test 
  public void testProblem2() throws IOException {
    if (NARROW) return;
    SpanTermQuery q1 = new SpanTermQuery(new Term(FIELD, "ax"));
    SpanTermQuery q2 = new SpanTermQuery(new Term(FIELD, "bx"));
    SpanTermQuery q3 = new SpanTermQuery(new Term(FIELD, "cx"));
    SpanTermQuery q4 = new SpanTermQuery(new Term(FIELD, "dx"));
    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{q1, q2, q3, q4}, 100, true,
        SpanNearQuery.DEFAULT_COMBO_MODE, SpanNearQuery.DEFAULT_ALLOW_OVERLAP,
        SpanNearQuery.DEFAULT_COMBINE_REPEAT_GROUPS_THRESHOLD, true, null, null);
    Spans spans = q.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    assertEquals(Spans.NO_MORE_DOCS, spans.advance(20));
  }
  @Test 
  public void testKings() throws IOException {
    if (NARROW) return; // "the the history the", // 19
    SpanTermQuery q1 = new SpanTermQuery(new Term(FIELD, "the"));
    SpanTermQuery q2 = new SpanTermQuery(new Term(FIELD, "history"));
    SpanTermQuery q3 = new SpanTermQuery(new Term(FIELD, "the"));
    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{q1, q2, q3}, 0, true);
    Spans spans = q.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    TermCollector collector = new TermCollector();
    assertEquals(19, spans.advance(19));
    assertEquals(1, spans.nextStartPosition());
    assertEquals(4, spans.endPosition());
    assertEquals(0, spans.width());
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }
  @Test 
  public void testProblemCase() throws IOException {
    if (NARROW) return; //"x y x y x y x y x y y y y y y y y y y y" // 17
    SpanTermQuery q1 = new SpanTermQuery(new Term(FIELD, "j"));
    SpanTermQuery q2 = new SpanTermQuery(new Term(FIELD, "j"));
    SpanTermQuery q3 = new SpanTermQuery(new Term(FIELD, "j"));
    SpanTermQuery q4 = new SpanTermQuery(new Term(FIELD, "j"));
    SpanTermQuery q5 = new SpanTermQuery(new Term(FIELD, "j"));
    SpanTermQuery q6 = new SpanTermQuery(new Term(FIELD, "j"));
    SpanTermQuery q7 = new SpanTermQuery(new Term(FIELD, "j"));
    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{q1, q2, q3, q4, q5, q6, q7}, 100, true);
    Spans spans = q.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    TermCollector collector = new TermCollector();
    assertEquals(18, spans.advance(18));
    assertEquals(182, spans.nextStartPosition());
    assertEquals(288, spans.endPosition());
    assertEquals(99, spans.width());
    checkCollectedTerms(spans, collector, new Term(FIELD, "j"));
    assertEquals(240, spans.nextStartPosition());
    assertEquals(315, spans.endPosition());
    assertEquals(68, spans.width());
    checkCollectedTerms(spans, collector, new Term(FIELD, "j"));
    assertEquals(240, spans.nextStartPosition());
    assertEquals(319, spans.endPosition());
    assertEquals(72, spans.width());
    checkCollectedTerms(spans, collector, new Term(FIELD, "j"));
    assertEquals(246, spans.nextStartPosition());
    assertEquals(319, spans.endPosition());
    assertEquals(66, spans.width());
    checkCollectedTerms(spans, collector, new Term(FIELD, "j"));
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }
  @Test 
  public void testProblemCase4() throws IOException {
    if (NARROW) return;
    SpanTermQuery q1 = new SpanTermQuery(new Term(FIELD, "j"));
    SpanTermQuery q2 = new SpanTermQuery(new Term(FIELD, "j"));
    SpanTermQuery q3 = new SpanTermQuery(new Term(FIELD, "j"));
    SpanTermQuery q4 = new SpanTermQuery(new Term(FIELD, "j"));
    SpanTermQuery q5 = new SpanTermQuery(new Term(FIELD, "j"));
    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{q1, q2, q3, q4, q5}, 100, true, SpanNearQuery.ComboMode.GREEDY_END_POSITION);
    Spans spans = q.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    TermCollector collector = new TermCollector();
    assertEquals(23, spans.advance(23));
    assertEquals(21, spans.nextStartPosition());
    assertEquals(112, spans.endPosition());
    assertEquals(85, spans.width());
    checkCollectedTerms(spans, collector, new Term(FIELD, "j"));
    assertEquals(87, spans.nextStartPosition());
    assertEquals(118, spans.endPosition());
    assertEquals(26, spans.width());
    checkCollectedTerms(spans, collector, new Term(FIELD, "j"));
    assertEquals(99, spans.nextStartPosition());
    assertEquals(122, spans.endPosition());
    assertEquals(18, spans.width());
    checkCollectedTerms(spans, collector, new Term(FIELD, "j"));
    assertEquals(103, spans.nextStartPosition());
    assertEquals(125, spans.endPosition());
    assertEquals(17, spans.width());
    checkCollectedTerms(spans, collector, new Term(FIELD, "j"));
    assertEquals(111, spans.nextStartPosition());
    assertEquals(139, spans.endPosition());
    assertEquals(23, spans.width());
    checkCollectedTerms(spans, collector, new Term(FIELD, "j"));
    assertEquals(Spans.NO_MORE_DOCS, spans.nextDoc());
  }

  @Test
  public void testProblemCasePerPositionPerStartPosition() throws IOException {
    if (NARROW) return; //"x y x y x y x y x y y y y y y y y y y y" // 17
    Term j = new Term(FIELD, "j");
    SpanTermQuery q1 = new SpanTermQuery(j);
    SpanTermQuery q2 = new SpanTermQuery(j);
    SpanTermQuery q3 = new SpanTermQuery(j);
    SpanTermQuery q4 = new SpanTermQuery(j);
    SpanTermQuery q5 = new SpanTermQuery(j);
    SpanTermQuery q6 = new SpanTermQuery(j);
    SpanTermQuery q7 = new SpanTermQuery(j);
    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{q1, q2, q3, q4, q5, q6, q7}, 100, true, SpanNearQuery.ComboMode.PER_POSITION_PER_START_POSITION);
    Spans spans = q.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    TermPositionCollector collector = new TermPositionCollector();
    assertEquals(18, spans.advance(18));
    assertEquals(182, spans.nextStartPosition());
    assertEquals(288, spans.endPosition());
    assertEquals(99, spans.width());
    checkCollectedTerms(spans, collector, j, 182, 240, 246, 253, 281, 284, 287);
    assertEquals(240, spans.nextStartPosition());
    assertEquals(315, spans.endPosition());
    assertEquals(68, spans.width());
    checkCollectedTerms(spans, collector, j, 240, 246, 253, 281, 284, 287, 314);
    assertEquals(240, spans.nextStartPosition());
    assertEquals(319, spans.endPosition());
    assertEquals(72, spans.width());
    checkCollectedTerms(spans, collector, j, 253, 281, 284, 287, 314, 318);
    assertEquals(246, spans.nextStartPosition());
    assertEquals(319, spans.endPosition());
    assertEquals(66, spans.width());
    checkCollectedTerms(spans, collector, j, 246, 253, 281, 284, 287, 314, 318);
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }
  @Test 
  public void testProblemCasePerPositionFullDistilled() throws IOException {
    if (NARROW) return; //"x y x y x y x y x y y y y y y y y y y y" // 17
    Term j = new Term(FIELD, "j");
    SpanTermQuery q1 = new SpanTermQuery(j);
    SpanTermQuery q2 = new SpanTermQuery(j);
    SpanTermQuery q3 = new SpanTermQuery(j);
    SpanTermQuery q4 = new SpanTermQuery(j);
    SpanTermQuery q5 = new SpanTermQuery(j);
    SpanTermQuery q6 = new SpanTermQuery(j);
    SpanTermQuery q7 = new SpanTermQuery(j);
    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{q1, q2, q3, q4, q5, q6, q7}, 100, true, SpanNearQuery.ComboMode.FULL_DISTILLED);
    Spans spans = q.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    TermPositionCollector collector = new TermPositionCollector();
    assertEquals(18, spans.advance(18));
    assertEquals(182, spans.nextStartPosition());
    assertEquals(288, spans.endPosition());
    assertEquals(99, spans.width());
    checkCollectedTerms(spans, collector, j, 182, 240, 246, 253, 281, 284, 287);
    assertEquals(240, spans.nextStartPosition());
    assertEquals(315, spans.endPosition());
    assertEquals(68, spans.width());
    checkCollectedTerms(spans, collector, j, 240, 246, 253, 281, 284, 287, 314);
    assertEquals(240, spans.nextStartPosition());
    assertEquals(319, spans.endPosition());
    assertEquals(72, spans.width());
    checkCollectedTerms(spans, collector, j, 240, 246, 253, 281, 284, 287, 314, 318);
    assertEquals(246, spans.nextStartPosition());
    assertEquals(319, spans.endPosition());
    assertEquals(66, spans.width());
    checkCollectedTerms(spans, collector, j, 246, 253, 281, 284, 287, 314, 318);
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }
  @Test 
  public void testProblemCasePerPositionFullDistilledPerStartPosition() throws IOException {
    if (NARROW) return; //"x y x y x y x y x y y y y y y y y y y y" // 17
    Term j = new Term(FIELD, "j");
    SpanTermQuery q1 = new SpanTermQuery(j);
    SpanTermQuery q2 = new SpanTermQuery(j);
    SpanTermQuery q3 = new SpanTermQuery(j);
    SpanTermQuery q4 = new SpanTermQuery(j);
    SpanTermQuery q5 = new SpanTermQuery(j);
    SpanTermQuery q6 = new SpanTermQuery(j);
    SpanTermQuery q7 = new SpanTermQuery(j);
    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{q1, q2, q3, q4, q5, q6, q7}, 100, true, SpanNearQuery.ComboMode.FULL_DISTILLED_PER_START_POSITION);
    Spans spans = q.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    TermPositionCollector collector = new TermPositionCollector();
    assertEquals(18, spans.advance(18));
    assertEquals(182, spans.nextStartPosition());
    assertEquals(288, spans.endPosition());
    assertEquals(99, spans.width());
    checkCollectedTerms(spans, collector, j, 182, 240, 246, 253, 281, 284, 287);
    assertEquals(240, spans.nextStartPosition());
    assertEquals(315, spans.endPosition());
    assertEquals(68, spans.width());
    checkCollectedTerms(spans, collector, j, 240, 246, 253, 281, 284, 287, 314);
    assertEquals(240, spans.nextStartPosition());
    assertEquals(319, spans.endPosition());
    assertEquals(72, spans.width());
    checkCollectedTerms(spans, collector, j, 253, 281, 284, 287, 314, 318);
    assertEquals(246, spans.nextStartPosition());
    assertEquals(319, spans.endPosition());
    assertEquals(66, spans.width());
    checkCollectedTerms(spans, collector, j, 246, 253, 281, 284, 287, 314, 318);
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }
  @Test 
  public void testProblemCasePerPositionFullDistilledPerPosition() throws IOException {
    if (NARROW) return;
    Term j = new Term(FIELD, "j");
    SpanTermQuery q1 = new SpanTermQuery(j);
    SpanTermQuery q2 = new SpanTermQuery(j);
    SpanTermQuery q3 = new SpanTermQuery(j);
    SpanTermQuery q4 = new SpanTermQuery(j);
    SpanTermQuery q5 = new SpanTermQuery(j);
    SpanTermQuery q6 = new SpanTermQuery(j);
    SpanTermQuery q7 = new SpanTermQuery(j);
    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{q1, q2, q3, q4, q5, q6, q7}, 100, true, SpanNearQuery.ComboMode.FULL_DISTILLED_PER_POSITION);
    Spans spans = q.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    TermPositionCollector collector = new TermPositionCollector();
    assertEquals(18, spans.advance(18));
    assertEquals(182, spans.nextStartPosition());
    assertEquals(288, spans.endPosition());
    assertEquals(99, spans.width());
    checkCollectedTerms(spans, collector, j, 182, 240, 246, 253, 281, 284, 287);
    assertEquals(240, spans.nextStartPosition());
    assertEquals(315, spans.endPosition());
    assertEquals(68, spans.width());
    checkCollectedTerms(spans, collector, j, 240, 246, 253, 281, 284, 287, 314);
    assertEquals(240, spans.nextStartPosition());
    assertEquals(319, spans.endPosition());
    assertEquals(72, spans.width());
    checkCollectedTerms(spans, collector, j, 253, 281, 284, 287, 314, 318);
    assertEquals(246, spans.nextStartPosition());
    assertEquals(319, spans.endPosition());
    assertEquals(66, spans.width());
    checkCollectedTerms(spans, collector, j, 246);
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }
  @Test 
  public void testProblemCasePerPosition() throws IOException {
    if (NARROW) return;
    Term j = new Term(FIELD, "j");
    SpanTermQuery q1 = new SpanTermQuery(j);
    SpanTermQuery q2 = new SpanTermQuery(j);
    SpanTermQuery q3 = new SpanTermQuery(j);
    SpanTermQuery q4 = new SpanTermQuery(j);
    SpanTermQuery q5 = new SpanTermQuery(j);
    SpanTermQuery q6 = new SpanTermQuery(j);
    SpanTermQuery q7 = new SpanTermQuery(j);
    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{q1, q2, q3, q4, q5, q6, q7}, 100, true, SpanNearQuery.ComboMode.PER_POSITION);
    Spans spans = q.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    TermPositionCollector collector = new TermPositionCollector();
    assertEquals(18, spans.advance(18));
    assertEquals(182, spans.nextStartPosition());
    assertEquals(288, spans.endPosition());
    assertEquals(99, spans.width());
    checkCollectedTerms(spans, collector, j, 182, 240, 246, 253, 281, 284, 287);
    assertEquals(240, spans.nextStartPosition());
    assertEquals(315, spans.endPosition());
    assertEquals(68, spans.width());
    checkCollectedTerms(spans, collector, j, 240, 246, 253, 281, 284, 287, 314);
    assertEquals(240, spans.nextStartPosition());
    assertEquals(319, spans.endPosition());
    assertEquals(72, spans.width());
    checkCollectedTerms(spans, collector, j, 253, 281, 284, 287, 314, 318);
    assertEquals(246, spans.nextStartPosition());
    assertEquals(319, spans.endPosition());
    assertEquals(66, spans.width());
    checkCollectedTerms(spans, collector, j, 246);
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }
  @Test 
  public void testOverlap() throws IOException {
    if (NARROW) return; //"x y x y x y x y x y y y y y y y y y y y" // 17
    SpanTermQuery qa = new SpanTermQuery(new Term(FIELD, "x"));
    SpanTermQuery qb = new SpanTermQuery(new Term(FIELD, "x"));
    SpanTermQuery qc = new SpanTermQuery(new Term(FIELD, "x"));
    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{qa, qb, qc}, 6, true);
    Spans spans = q.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    TermCollector collector = new TermCollector();
    assertEquals(17, spans.advance(17));
    assertEquals(0, spans.nextStartPosition());
    assertEquals(5, spans.endPosition());
    assertEquals(2, spans.width());
    checkCollectedTerms(spans, collector, new Term(FIELD, "x"));
    assertEquals(0, spans.nextStartPosition());
    assertEquals(7, spans.endPosition());
    assertEquals(4, spans.width());
    checkCollectedTerms(spans, collector, new Term(FIELD, "x"));
    assertEquals(0, spans.nextStartPosition());
    assertEquals(9, spans.endPosition());
    assertEquals(6, spans.width());
    checkCollectedTerms(spans, collector, new Term(FIELD, "x"));
    assertEquals(2, spans.nextStartPosition());
    assertEquals(7, spans.endPosition());
    assertEquals(2, spans.width());
    checkCollectedTerms(spans, collector, new Term(FIELD, "x"));
    assertEquals(2, spans.nextStartPosition());
    assertEquals(9, spans.endPosition());
    assertEquals(4, spans.width());
    checkCollectedTerms(spans, collector, new Term(FIELD, "x"));
    assertEquals(2, spans.nextStartPosition());
    assertEquals(11, spans.endPosition());
    assertEquals(6, spans.width());
    checkCollectedTerms(spans, collector, new Term(FIELD, "x"));
    assertEquals(4, spans.nextStartPosition());
    assertEquals(9, spans.endPosition());
    assertEquals(2, spans.width());
    checkCollectedTerms(spans, collector, new Term(FIELD, "x"));
    assertEquals(4, spans.nextStartPosition());
    assertEquals(11, spans.endPosition());
    assertEquals(4, spans.width());
    checkCollectedTerms(spans, collector, new Term(FIELD, "x"));
    assertEquals(6, spans.nextStartPosition());
    assertEquals(11, spans.endPosition());
    assertEquals(2, spans.width());
    checkCollectedTerms(spans, collector, new Term(FIELD, "x"));
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }

  @Test 
  public void testPersistentSlopTracking() throws IOException {
    if (NARROW) return;
    SpanTermQuery qa = new SpanTermQuery(new Term(FIELD, "a"));
    SpanTermQuery qb = new SpanTermQuery(new Term(FIELD, "b"));
    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{qa, qb}, 2, true);
    Spans spans = q.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    TermCollector collector = new TermCollector();
    assertEquals(12, spans.advance(12));
    assertEquals(0, spans.nextStartPosition());
    assertEquals(4, spans.endPosition());
    assertEquals(2, spans.width());
    checkCollectedTerms(spans, collector, new Term(FIELD, "a"), new Term(FIELD, "b"));
    assertEquals(1, spans.nextStartPosition());
    assertEquals(4, spans.endPosition());
    assertEquals(1, spans.width());
    checkCollectedTerms(spans, collector, new Term(FIELD, "a"), new Term(FIELD, "b"));
    assertEquals(1, spans.nextStartPosition());
    assertEquals(5, spans.endPosition());
    assertEquals(2, spans.width());
    checkCollectedTerms(spans, collector, new Term(FIELD, "a"), new Term(FIELD, "b"));
    assertEquals(2, spans.nextStartPosition());
    assertEquals(4, spans.endPosition());
    assertEquals(0, spans.width());
    checkCollectedTerms(spans, collector, new Term(FIELD, "a"), new Term(FIELD, "b"));
    assertEquals(2, spans.nextStartPosition());
    assertEquals(5, spans.endPosition());
    assertEquals(1, spans.width());
    checkCollectedTerms(spans, collector, new Term(FIELD, "a"), new Term(FIELD, "b"));
    assertEquals(2, spans.nextStartPosition());
    assertEquals(6, spans.endPosition());
    assertEquals(2, spans.width());
    checkCollectedTerms(spans, collector, new Term(FIELD, "a"), new Term(FIELD, "b"));
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }

  @Test 
  public void testSkipAhead() throws IOException {
    if (NARROW) return; // "sa sb sx sc sd sx se sf sg sx sh si sj", // 15
    SpanTermQuery qx = new SpanTermQuery(new Term(FIELD, "sx"));
    SpanTermQuery qa = new SpanTermQuery(new Term(FIELD, "sa"));
    SpanTermQuery qb = new SpanTermQuery(new Term(FIELD, "sb"));
    SpanTermQuery qc = new SpanTermQuery(new Term(FIELD, "sc"));
    SpanTermQuery qd = new SpanTermQuery(new Term(FIELD, "sd"));
    SpanTermQuery qe = new SpanTermQuery(new Term(FIELD, "se"));
    SpanTermQuery qf = new SpanTermQuery(new Term(FIELD, "sf"));
    SpanOrQuery qbOrBx = new SpanOrQuery(qb, new SpanNearQuery(new SpanQuery[]{qb, qx}, 0, true));
    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{qa, qbOrBx, qc, qd, qe, qf}, 1, true);
    TermCollector collector = new TermCollector();
    Spans spans = q.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    assertEquals(15, spans.advance(15));
    assertEquals(0, spans.nextStartPosition());
    assertEquals(8, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "sa"), new Term(FIELD, "sb"), new Term(FIELD, "sx"), new Term(FIELD, "sc"), 
        new Term(FIELD, "sd"), new Term(FIELD, "se"), new Term(FIELD, "sf"));
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }
  
  @Test 
  public void testBlah2() throws IOException {
    if (NARROW) return; // "a b b a b a b a b a b a b a b a b a b a b a b a b a b a b a b a", // 14
    int[][] expected = new int[][]{
      {3, 18, 7},
      {5, 20, 7},
      {7, 22, 7},
      {9, 24, 7},
      {11, 26, 7},
      {13, 28, 7},
      {15, 30, 7},
      {17, 32, 7},
    };
    Term a = new Term(FIELD, "a");
    SpanTermQuery q1 = new SpanTermQuery(a);
    SpanTermQuery q2 = new SpanTermQuery(a);
    SpanTermQuery q3 = new SpanTermQuery(a);
    SpanTermQuery q4 = new SpanTermQuery(a);
    SpanTermQuery q5 = new SpanTermQuery(a);
    SpanTermQuery q6 = new SpanTermQuery(a);
    SpanTermQuery q7 = new SpanTermQuery(a);
    SpanTermQuery q8 = new SpanTermQuery(a);
    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{q1, q2, q3, q4, q5, q6, q7, q8}, 7, true);
    TermCollector collector = new TermCollector();
    Spans spans = q.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    assertEquals(14, spans.advance(14));
    int nextStart;
    for (int[] expectPosition : expected) {
      assertEquals(expectPosition[0], nextStart = spans.nextStartPosition());
      assertEquals(expectPosition[1], spans.endPosition());
      assertEquals(expectPosition[2], spans.width());
      //System.err.println(nextStart+" ?=> "+spans.endPosition()+" ("+spans.width()+")");
    }
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }

  @Test 
  public void testShortcircuitFalse() throws IOException {
    if (NARROW) return;
    Term a = new Term(FIELD, "a");
    SpanTermQuery q1 = new SpanTermQuery(a);
    SpanTermQuery q2 = new SpanTermQuery(a);
    SpanTermQuery q3 = new SpanTermQuery(a);
    SpanTermQuery q4 = new SpanTermQuery(a);
    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{q1, q2, q3, q4}, 5, true, SpanNearQuery.DEFAULT_COMBO_MODE, SpanNearQuery.DEFAULT_ALLOW_OVERLAP, Integer.MAX_VALUE);
    TermCollector collector = new TermCollector();
    Spans spans = q.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    assertEquals(13, spans.advance(13));
//    int nextStart;
//    while ((nextStart = spans.nextStartPosition()) != Spans.NO_MORE_POSITIONS) {
//      System.err.println(nextStart+" => "+spans.endPosition()+" ("+spans.width()+")");
//    }
//    if (true) return;
    for (int start = 0; start < 24; start += 2) {
      for (int end = start + 7; end <= start + 9; end += 2) {
        assertEquals(start, spans.nextStartPosition());
        assertEquals(end, spans.endPosition());
        checkCollectedTerms(spans, collector, new Term(FIELD, "a"));
      }
    }
    assertEquals(24, spans.nextStartPosition());
    assertEquals(31, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "a"));
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }

  @Test 
  public void testFallbackTrue() throws IOException {
    if (NARROW) return;
    Term a = new Term(FIELD, "a");
    SpanTermQuery q1 = new SpanTermQuery(a);
    SpanTermQuery q2 = new SpanTermQuery(a);
    SpanTermQuery q3 = new SpanTermQuery(a);
    SpanTermQuery q4 = new SpanTermQuery(a);
    SpanTermQuery q5 = new SpanTermQuery(a);
    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{q1, q2, q3, q4, q5}, 5, true, SpanNearQuery.ComboMode.FULL, SpanNearQuery.DEFAULT_ALLOW_OVERLAP, Integer.MAX_VALUE);
    TermCollector collector = new TermCollector();
    Spans spans = q.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    assertEquals(13, spans.advance(13));
    for (int start = 0; start < 24; start += 2) {
      assertEquals(start, spans.nextStartPosition());
      assertEquals(start + 9, spans.endPosition());
      checkCollectedTerms(spans, collector, new Term(FIELD, "a"));
    }
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }

  @Test 
  public void testAutoNoOverlap() throws IOException {
    // TODO this doesn't work properly yet.
    if (true || NARROW) return;
    Term a = new Term(FIELD, "a");
    SpanTermQuery q1 = new SpanTermQuery(a);
    SpanTermQuery q2 = new SpanTermQuery(a);
    SpanTermQuery q3 = new SpanTermQuery(a);
    SpanTermQuery q4 = new SpanTermQuery(a);
    SpanTermQuery q5 = new SpanTermQuery(a);
    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{q1, q2, q3, q4, q5}, 7, true, SpanNearQuery.ComboMode.MIN_END_POSITION, true);
    TermCollector collector = new TermCollector();
    Spans spans = q.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    assertEquals(13, spans.advance(13));
    int i;
    while ((i = spans.nextStartPosition()) != Spans.NO_MORE_POSITIONS) {
      System.err.println(i+" => "+spans.endPosition());
    }
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }

  private void checkCollectedTerms(Spans spans, TermCollector collector, Term... expectedTerms) throws IOException {
    collector.reset();
    spans.collect(collector);
    for (Term t : expectedTerms) {
      assertTrue("Missing term " + t, collector.terms.contains(t));
    }
    assertEquals("Unexpected terms found", expectedTerms.length, collector.terms.size());
  }

  private void checkCollectedTerms(Spans spans, TermPositionCollector collector, Term expectedTerm, int... expectedTermPositions) throws IOException {
    collector.reset();
    spans.collect(collector);
    for (int i : expectedTermPositions) {
      if (expectedTerm == null) {
        assertTrue("Missing position " + i, collector.terms.containsKey(i));
      } else {
        assertTrue("Missing term " + expectedTerm + " at position " + i, expectedTerm == collector.terms.get(i));
      }
    }
    assertEquals("Unexpected terms found; expected "+Arrays.toString(expectedTermPositions)+", found "+collector.terms.keySet(), expectedTermPositions.length, collector.terms.size());
  }

  @Test
  public void testCombinatorialCaseFull() throws IOException {
    if (NARROW) return;
    SpanQuery[] termQueries = new SpanQuery[combinatorialTermCount];
    for (int i = 0; i < combinatorialTermCount; i++) {
      termQueries[i] = new SpanTermQuery(new Term(FIELD, "e".concat(Integer.toString(i + 1))));
    }
    int slop = combinatorialTermCount * (combinatorialTermRepeat - 1);
    System.err.println("combinatorial slop = "+slop);
    SpanNearQuery nearQuery = new SpanNearQuery(termQueries, slop, true, SpanNearQuery.ComboMode.FULL);
    SpanWeight w = nearQuery.createWeight(searcher, true, 1f);
    LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
    Spans spans = w.getSpans(ctx, SpanWeight.Postings.POSITIONS);
    assertEquals(11, spans.advance(11));
    int widthStart = (combinatorialTermRepeat - 1) * (combinatorialTermCount - 1);
    int endStart = combinatorialTermRepeat * (combinatorialTermCount - 1) + 1;
    int endCeiling = combinatorialTermRepeat * combinatorialTermCount + 1;
    int repeatCombinations = (int) Math.pow(combinatorialTermRepeat, combinatorialTermCount - 2); // for fixed start/end terms
    for (int expectStart = 0; expectStart < combinatorialTermRepeat; expectStart++) {
      int expectWidth = widthStart;
      for (int expectEnd = endStart; expectEnd < endCeiling; expectEnd++) {
        for (int i = 0; i < repeatCombinations; i++) {
          assertEquals(expectStart, spans.nextStartPosition());
          assertEquals(expectEnd, spans.endPosition());
          assertEquals(expectWidth, spans.width());
        }
        expectWidth++;
      }
      widthStart--;
    }
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }

  @Test
  public void testCombinatorialCaseFullLimitSlop() throws IOException {
    if (NARROW) return;
    SpanQuery[] termQueries = new SpanQuery[combinatorialTermCount];
    for (int i = 0; i < combinatorialTermCount; i++) {
      termQueries[i] = new SpanTermQuery(new Term(FIELD, "e".concat(Integer.toString(i + 1))));
    }
    int slop = combinatorialTermCount * (combinatorialTermRepeat - 1) - 4;
    SpanNearQuery nearQuery = new SpanNearQuery(termQueries, slop, true, SpanNearQuery.ComboMode.FULL);
    SpanWeight w = nearQuery.createWeight(searcher, true, 1f);
    LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
    Spans spans = w.getSpans(ctx, SpanWeight.Postings.POSITIONS);
    assertEquals(11, spans.advance(11));
    int widthStart = (combinatorialTermRepeat - 1) * (combinatorialTermCount - 1);
    int endStart = combinatorialTermRepeat * (combinatorialTermCount - 1) + 1;
    int endCeiling = combinatorialTermRepeat * combinatorialTermCount + 1;
    int repeatCombinations = (int) Math.pow(combinatorialTermRepeat, combinatorialTermCount - 2); // for fixed start/end terms
    for (int expectStart = 0; expectStart < combinatorialTermRepeat; expectStart++) {
      int expectWidth = widthStart;
      for (int expectEnd = endStart; expectEnd < endCeiling; expectEnd++) {
        if (expectWidth <= slop) {
          for (int i = 0; i < repeatCombinations; i++) {
            assertEquals(expectStart, spans.nextStartPosition());
            assertEquals(expectEnd, spans.endPosition());
            assertEquals(expectWidth, spans.width());
          }
        }
        expectWidth++;
      }
      widthStart--;
    }
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }

  @Test
  public void testCombinatorialCasePerEndPosition() throws IOException {
    if (NARROW) return;
    SpanQuery[] termQueries = new SpanQuery[combinatorialTermCount];
    for (int i = 0; i < combinatorialTermCount; i++) {
      termQueries[i] = new SpanTermQuery(new Term(FIELD, "e".concat(Integer.toString(i + 1))));
    }
    int slop = combinatorialTermCount * (combinatorialTermRepeat - 1);
    System.err.println("combinatorial slop = "+slop);
    SpanNearQuery nearQuery = new SpanNearQuery(termQueries, slop, true);
    SpanWeight w = nearQuery.createWeight(searcher, true, 1f);
    LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
    Spans spans = w.getSpans(ctx, SpanWeight.Postings.POSITIONS);
    assertEquals(11, spans.advance(11));
    int widthStart = (combinatorialTermCount - 1) * (combinatorialTermRepeat - 1);
    int endStart = combinatorialTermRepeat * (combinatorialTermCount - 1) + 1;
    int endCeiling = combinatorialTermRepeat * combinatorialTermCount + 1;
    for (int expectStart = 0; expectStart < combinatorialTermRepeat; expectStart++) {
      for (int expectEnd = endStart; expectEnd < endCeiling; expectEnd++) {
        assertEquals(expectStart, spans.nextStartPosition());
        assertEquals(expectEnd, spans.endPosition());
      }
    }
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }
  @Test
  public void testCombinatorialCasePerPositionPerStartPosition() throws IOException {
    if (NARROW) return;
    SpanQuery[] termQueries = new SpanQuery[combinatorialTermCount];
    for (int i = 0; i < combinatorialTermCount; i++) {
      termQueries[i] = new SpanTermQuery(new Term(FIELD, "e".concat(Integer.toString(i + 1))));
    }
    int slop = combinatorialTermCount * (combinatorialTermRepeat - 1);
    System.err.println("combinatorial slop = "+slop);
    SpanNearQuery nearQuery = new SpanNearQuery(termQueries, slop, true, SpanNearQuery.ComboMode.PER_POSITION_PER_START_POSITION);
    SpanWeight w = nearQuery.createWeight(searcher, true, 1f);
    LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
    Spans spans = w.getSpans(ctx, SpanWeight.Postings.POSITIONS);
    assertEquals(11, spans.advance(11));
    TermCollector collector = new TermCollector();
    Term[] compareAll = new Term[combinatorialTermCount];
    int i = combinatorialTermCount;
    do {
      Term t = new Term(FIELD, "e".concat(Integer.toString(i)));
      compareAll[--i] = t;
    } while (i > 0);
    Term[] compareLastTerm = new Term[] {compareAll[combinatorialTermCount - 1]};
    int widthStart = (combinatorialTermRepeat - 1) * (combinatorialTermCount - 1);
    int endStart = combinatorialTermRepeat * (combinatorialTermCount - 1) + 1;
    int endCeiling = combinatorialTermRepeat * combinatorialTermCount + 1;
    for (int expectStart = 0; expectStart < combinatorialTermRepeat; expectStart++) {
      int expectWidth = widthStart;
      Term[] compare = compareAll;
      for (int expectEnd = endStart; expectEnd < endCeiling; expectEnd++) {
        assertEquals(expectStart, spans.nextStartPosition());
        assertEquals(expectEnd, spans.endPosition());
        assertEquals(expectWidth, spans.width());
        checkCollectedTerms(spans, collector, compare);
        compare = compareLastTerm;
        expectWidth++;
      }
      widthStart--;
    }
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }
  @Test
  public void testCombinatorialCasePerPosition() throws IOException {
    if (NARROW) return;
    SpanQuery[] termQueries = new SpanQuery[combinatorialTermCount];
    for (int i = 0; i < combinatorialTermCount; i++) {
      termQueries[i] = new SpanTermQuery(new Term(FIELD, "e".concat(Integer.toString(i + 1))));
    }
    int slop = combinatorialTermCount * (combinatorialTermRepeat - 1);
    System.err.println("combinatorial slop = "+slop);
    SpanNearQuery nearQuery = new SpanNearQuery(termQueries, slop, true, SpanNearQuery.ComboMode.PER_POSITION);
    SpanWeight w = nearQuery.createWeight(searcher, true, 1f);
    LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
    Spans spans = w.getSpans(ctx, SpanWeight.Postings.POSITIONS);
    assertEquals(11, spans.advance(11));
    TermPositionCollector collector = new TermPositionCollector();
    Term[] compareAll = new Term[combinatorialTermCount];
    int i = combinatorialTermCount;
    do {
      Term t = new Term(FIELD, "e".concat(Integer.toString(i)));
      compareAll[--i] = t;
    } while (i > 0);
    int[] initialCompare = new int[] {0, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56};
    int[] initialEndCompare = new int[] {57};
    Term[] compareLastTerm = new Term[] {compareAll[combinatorialTermCount - 1]};
    int widthStart = (combinatorialTermRepeat - 1) * (combinatorialTermCount - 1);
    int endStart = combinatorialTermRepeat * (combinatorialTermCount - 1) + 1;
    int endCeiling = combinatorialTermRepeat * combinatorialTermCount + 1;
    int[] compare = initialCompare;
    for (int expectStart = 0; expectStart < combinatorialTermRepeat; expectStart++) {
      int expectWidth = widthStart;
      for (int expectEnd = endStart; expectEnd < endCeiling; expectEnd++) {
        int startPosition = spans.nextStartPosition();
        assertEquals(expectStart, startPosition);
        assertEquals(expectEnd, spans.endPosition());
        assertEquals(expectWidth, spans.width());
        checkCollectedTerms(spans, collector, null, compare);
        if (compare == initialCompare) {
          compare = initialEndCompare;
        } else if (compare == initialEndCompare) {
          compare[0]++;
        } else {
          compare = new int[0];
        }
        expectWidth++;
      }
      compare = new int[] {expectStart + 1};
      widthStart--;
    }
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }

  @Test
  public void testCombinatorialCaseFullDistilled() throws IOException {
    if (NARROW) return;
    SpanQuery[] termQueries = new SpanQuery[combinatorialTermCount];
    for (int i = 0; i < combinatorialTermCount; i++) {
      termQueries[i] = new SpanTermQuery(new Term(FIELD, "e".concat(Integer.toString(i + 1))));
    }
    int slop = combinatorialTermCount * (combinatorialTermRepeat - 1);
    System.err.println("combinatorial slop = "+slop);
    SpanNearQuery nearQuery = new SpanNearQuery(termQueries, slop, true, SpanNearQuery.ComboMode.FULL_DISTILLED);
    SpanWeight w = nearQuery.createWeight(searcher, true, 1f);
    LeafReaderContext ctx = searcher.getIndexReader().leaves().get(0);
    Spans spans = w.getSpans(ctx, SpanWeight.Postings.POSITIONS);
    assertEquals(11, spans.advance(11));
    TermPositionCollector collector = new TermPositionCollector();
    Term[] compareAll = new Term[combinatorialTermCount];
    int i = combinatorialTermCount;
    do {
      Term t = new Term(FIELD, "e".concat(Integer.toString(i)));
      compareAll[--i] = t;
    } while (i > 0);
    int[] compare = new int[] {0, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56};
    final int lastCompareIndex = compare.length - 1;
    int widthStart = (combinatorialTermRepeat - 1) * (combinatorialTermCount - 1);
    int endStart = combinatorialTermRepeat * (combinatorialTermCount - 1) + 1;
    int endCeiling = combinatorialTermRepeat * combinatorialTermCount + 1;
    for (int expectStart = 0; expectStart < combinatorialTermRepeat; expectStart++) {
      int expectWidth = widthStart;
      compare[0] = expectStart;
      for (int expectEnd = endStart; expectEnd < endCeiling; expectEnd++) {
        compare[lastCompareIndex] = expectEnd - 1;
        int startPosition = spans.nextStartPosition();
        assertEquals(expectStart, startPosition);
        assertEquals(expectEnd, spans.endPosition());
        assertEquals(expectWidth, spans.width());
        checkCollectedTerms(spans, collector, null, compare);
        expectWidth++;
      }
      widthStart--;
    }
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }

  @Test 
  public void testSomethingHitFull() throws IOException {
    if (NARROW) return;
    SpanTermQuery qa = new SpanTermQuery(new Term(FIELD, "a"));
    SpanTermQuery qb = new SpanTermQuery(new Term(FIELD, "b"));
    SpanTermQuery qc = new SpanTermQuery(new Term(FIELD, "c"));
    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{qa, qb, qc}, 2, true, SpanNearQuery.ComboMode.FULL);
    TermCollector collector = new TermCollector();
    Spans spans = q.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    assertEquals(12, spans.advance(12));
    assertEquals(2, spans.nextStartPosition());
    assertEquals(7, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "a"), new Term(FIELD, "b"), new Term(FIELD, "c"));
    assertEquals(2, spans.nextStartPosition());
    assertEquals(7, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "a"), new Term(FIELD, "b"), new Term(FIELD, "c"));
    assertEquals(2, spans.nextStartPosition());
    assertEquals(7, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "a"), new Term(FIELD, "b"), new Term(FIELD, "c"));
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }
  
  @Test 
  public void testSomethingHitFullMulti() throws IOException {
    if (NARROW) return; // "a a a b b b c c c d x x d"
    SpanTermQuery qa = new SpanTermQuery(new Term(FIELD, "a"));
    SpanTermQuery qb = new SpanTermQuery(new Term(FIELD, "b"));
    SpanTermQuery qc = new SpanTermQuery(new Term(FIELD, "c"));
    SpanTermQuery qd = new SpanTermQuery(new Term(FIELD, "d"));
    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{qa, qb, qc, qd}, 5, true, SpanNearQuery.ComboMode.FULL);
    TermCollector collector = new TermCollector();
    Spans spans = q.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    assertEquals(16, spans.advance(16));
    int start;
//    while ((start = spans.nextStartPosition()) != Spans.NO_MORE_POSITIONS) {
//      System.err.println(start+"=>"+spans.endPosition()+" ("+spans.width()+")");
//    }
//    if (true) return;
    for (int i = 0; i < 9; i++) {
      assertEquals(1, spans.nextStartPosition());
      assertEquals(10, spans.endPosition());
      assertEquals(5, spans.width());
      checkCollectedTerms(spans, collector, new Term(FIELD, "a"), new Term(FIELD, "b"), new Term(FIELD, "c"), new Term(FIELD, "d"));
    }
    for (int i = 0; i < 9; i++) {
      assertEquals(2, spans.nextStartPosition());
      assertEquals(10, spans.endPosition());
      assertEquals(4, spans.width());
      checkCollectedTerms(spans, collector, new Term(FIELD, "a"), new Term(FIELD, "b"), new Term(FIELD, "c"), new Term(FIELD, "d"));
    }
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }
  @Test 
  public void testSomethingHitFullDistilled() throws IOException {
    if (NARROW) return; // "a a a b b b c c c d x x d"
    SpanTermQuery qa = new SpanTermQuery(new Term(FIELD, "a"));
    SpanTermQuery qb = new SpanTermQuery(new Term(FIELD, "b"));
    SpanTermQuery qc = new SpanTermQuery(new Term(FIELD, "c"));
    SpanTermQuery qd = new SpanTermQuery(new Term(FIELD, "d"));
    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{qa, qb, qc, qd}, 5, true, SpanNearQuery.ComboMode.FULL_DISTILLED_PER_START_POSITION);
    TermPositionCollector collector = new TermPositionCollector();
    Spans spans = q.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    assertEquals(16, spans.advance(16));
//    int start;
//    while ((start = spans.nextStartPosition()) != Spans.NO_MORE_POSITIONS) {
//      collector.reset();
//      spans.collect(collector);
//      System.err.println(start+"=>"+spans.endPosition()+" ("+spans.width()+") "+collector.terms);
//    }
//    if (true) return;
    assertEquals(1, spans.nextStartPosition());
    assertEquals(10, spans.endPosition());
    assertEquals(5, spans.width());
    checkCollectedTerms(spans, collector, null, 1, 3, 4, 5, 6, 7, 8, 9);
    assertEquals(2, spans.nextStartPosition());
    assertEquals(10, spans.endPosition());
    assertEquals(4, spans.width());
    checkCollectedTerms(spans, collector, null, 2, 3, 4, 5, 6, 7, 8, 9);
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }
  
  @Test 
  public void testSomethingHitPerEndPosition() throws IOException {
    if (NARROW) return;
    SpanTermQuery qa = new SpanTermQuery(new Term(FIELD, "a"));
    SpanTermQuery qb = new SpanTermQuery(new Term(FIELD, "b"));
    SpanTermQuery qc = new SpanTermQuery(new Term(FIELD, "c"));
    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{qa, qb, qc}, 2, true);
    TermCollector collector = new TermCollector();
    Spans spans = q.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    assertEquals(12, spans.advance(12));
    assertEquals(2, spans.nextStartPosition());
    assertEquals(7, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "a"), new Term(FIELD, "b"), new Term(FIELD, "c"));
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }
  
  @Test 
  public void testSomethingMiss() throws IOException {
    if (NARROW) return;
    SpanTermQuery qa = new SpanTermQuery(new Term(FIELD, "a"));
    SpanTermQuery qb = new SpanTermQuery(new Term(FIELD, "b"));
    SpanTermQuery qc = new SpanTermQuery(new Term(FIELD, "c"));
    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{qa, qb, qc}, 1, true);
    Spans spans = q.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    assertEquals(Spans.NO_MORE_DOCS, spans.advance(12));
  }
  
  @Test
  public void testSimpleOrQuery() throws IOException {
    if (NARROW) return;
    SpanTermQuery q1 = new SpanTermQuery(new Term(FIELD, "w1"));
    SpanTermQuery q2 = new SpanTermQuery(new Term(FIELD, "w2"));
    SpanOrQuery simpleOr = new SpanOrQuery(q1, q2);
    TermCollector collector = new TermCollector();
    Spans spans = simpleOr.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    assertEquals(3, spans.advance(3));
    assertEquals(0, spans.nextStartPosition());
    assertEquals(1, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "w1"));
    assertEquals(1, spans.nextStartPosition());
    assertEquals(2, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "w2"));
    assertEquals(2, spans.nextStartPosition());
    assertEquals(3, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "w1"));
    assertEquals(4, spans.nextStartPosition());
    assertEquals(5, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "w2"));
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }

  @Test
  public void testVariableLengthOrQuery() throws IOException {
    if (NARROW) return;
    SpanTermQuery q1 = new SpanTermQuery(new Term(FIELD, "t1"));
    SpanTermQuery q2 = new SpanTermQuery(new Term(FIELD, "t2"));
    SpanNearQuery q3 = new SpanNearQuery(new SpanQuery[]{q1, q2}, 0, true);
    SpanOrQuery variableLengthOr = new SpanOrQuery(q1, q3, q2);
    TermCollector collector = new TermCollector();
    Spans spans = variableLengthOr.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    assertEquals(10, spans.advance(10));
    assertEquals(0, spans.nextStartPosition());
    assertEquals(1, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "t1"));
    assertEquals(0, spans.nextStartPosition());
    assertEquals(2, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "t1"), new Term(FIELD, "t2"));
    assertEquals(1, spans.nextStartPosition());
    assertEquals(2, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "t2"));
    
    assertEquals(2, spans.nextStartPosition());
    assertEquals(3, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "t1"));
    assertEquals(2, spans.nextStartPosition());
    assertEquals(4, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "t1"), new Term(FIELD, "t2"));
    assertEquals(3, spans.nextStartPosition());
    assertEquals(4, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "t2"));
    
    assertEquals(4, spans.nextStartPosition());
    assertEquals(5, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "t1"));
    assertEquals(4, spans.nextStartPosition());
    assertEquals(6, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "t1"), new Term(FIELD, "t2"));
    assertEquals(5, spans.nextStartPosition());
    assertEquals(6, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "t2"));
    
    assertEquals(6, spans.nextStartPosition());
    assertEquals(7, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "t1"));
    assertEquals(9, spans.nextStartPosition());
    assertEquals(10, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "t2"));
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }

  @Test
  public void testSimpleNearQuery() throws IOException {
    if (NARROW) return;
    SpanTermQuery q1 = new SpanTermQuery(new Term(FIELD, "t1"));
    SpanTermQuery q2 = new SpanTermQuery(new Term(FIELD, "t2"));
    SpanNearQuery q3 = new SpanNearQuery(new SpanQuery[]{q1, q2}, 0, true);
    Spans spans = q3.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    assertEquals(10, spans.advance(10));
    TermCollector collector = new TermCollector();
    assertEquals(0, spans.nextStartPosition());
    assertEquals(2, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "t1"), new Term(FIELD, "t2"));
    assertEquals(2, spans.nextStartPosition());
    assertEquals(4, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "t1"), new Term(FIELD, "t2"));
    assertEquals(4, spans.nextStartPosition());
    assertEquals(6, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "t1"), new Term(FIELD, "t2"));
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }

  @Test
  public void testOverlappingNearQuery() throws IOException {
    if (NARROW) return;
    SpanTermQuery q1 = new SpanTermQuery(new Term(FIELD, "t1"));
    SpanTermQuery q2 = new SpanTermQuery(new Term(FIELD, "t2"));
    SpanNearQuery q3 = new SpanNearQuery(new SpanQuery[]{q1, q2}, 2, true);
    Spans spans = q3.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    assertEquals(10, spans.advance(10));
    TermCollector collector = new TermCollector();
    assertEquals(0, spans.nextStartPosition());
    assertEquals(2, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "t1"), new Term(FIELD, "t2"));
    assertEquals(0, spans.nextStartPosition());
    assertEquals(4, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "t1"), new Term(FIELD, "t2"));
    assertEquals(2, spans.nextStartPosition());
    assertEquals(4, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "t1"), new Term(FIELD, "t2"));
    assertEquals(2, spans.nextStartPosition());
    assertEquals(6, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "t1"), new Term(FIELD, "t2"));
    assertEquals(4, spans.nextStartPosition());
    assertEquals(6, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "t1"), new Term(FIELD, "t2"));
    assertEquals(6, spans.nextStartPosition());
    assertEquals(10, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "t1"), new Term(FIELD, "t2"));
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }

  @Test
  public void testLimitedOverlappingNearQuery() throws IOException {
    // "t1 t2 t1 t2 t1 t2 t1 blah blah t2 blah", // 10
    if (NARROW) return;
    SpanTermQuery q1 = new SpanTermQuery(new Term(FIELD, "t1"));
    SpanTermQuery q2 = new SpanTermQuery(new Term(FIELD, "t2"));
    SpanNearQuery q3 = new SpanNearQuery(new SpanQuery[]{q1, q2}, 2, true, SpanNearQuery.DEFAULT_COMBO_MODE, false);
    Spans spans = q3.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    assertEquals(10, spans.advance(10));
    TermCollector collector = new TermCollector();
    assertEquals(0, spans.nextStartPosition());
    assertEquals(2, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "t1"), new Term(FIELD, "t2"));
    assertEquals(2, spans.nextStartPosition());
    assertEquals(4, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "t1"), new Term(FIELD, "t2"));
    assertEquals(4, spans.nextStartPosition());
    assertEquals(6, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "t1"), new Term(FIELD, "t2"));
    assertEquals(6, spans.nextStartPosition());
    assertEquals(10, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "t1"), new Term(FIELD, "t2"));
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }

  @Test
  public void testSimpleQuery() throws IOException {
    if (NARROW) return;
    SpanTermQuery q1 = new SpanTermQuery(new Term(FIELD, "w1"));
    Spans spans = q1.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    assertEquals(0, spans.advance(0));
    TermCollector collector = new TermCollector();
    spans.nextStartPosition();
    checkCollectedTerms(spans, collector, new Term(FIELD, "w1"));
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }

  @Test
  public void testNestedNearShortenMatchLengthQuery() throws IOException {
    if (NARROW) return;
    // near(w1, or(w3, near(w2, w3, w4)), w4)

    SpanTermQuery q1 = new SpanTermQuery(new Term(FIELD, "w1"));
    SpanTermQuery q2 = new SpanTermQuery(new Term(FIELD, "w2"));
    SpanTermQuery q3 = new SpanTermQuery(new Term(FIELD, "w3"));
    SpanTermQuery q4 = new SpanTermQuery(new Term(FIELD, "w4"));
    SpanNearQuery q234 = new SpanNearQuery(new SpanQuery[]{q2, q3, q4}, 0, true);

    SpanOrQuery q5 = new SpanOrQuery(q234, q3);
    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{q1, q5, q4}, 1, true);

    TermCollector collector = new TermCollector();
    Spans spans;
    spans = q.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    //"w1 w2 w3 w4 w5", // 0
    assertEquals(0, spans.advance(0));
    assertEquals(0, spans.nextStartPosition());
    assertEquals(4, spans.endPosition());
    assertEquals(1, spans.width());
    checkCollectedTerms(spans, collector, new Term(FIELD, "w1"), new Term(FIELD, "w3"), new Term(FIELD, "w4"));
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }

  @Test
  public void testNestedNearQuery() throws IOException {
    if (NARROW) return;
    // near(w1, near(w2, or(w3, w4)))

    SpanTermQuery q1 = new SpanTermQuery(new Term(FIELD, "w1"));
    SpanTermQuery q2 = new SpanTermQuery(new Term(FIELD, "w2"));
    SpanTermQuery q3 = new SpanTermQuery(new Term(FIELD, "w3"));
    SpanTermQuery q4 = new SpanTermQuery(new Term(FIELD, "w4"));

    SpanOrQuery q5 = new SpanOrQuery(q4, q3);
    SpanNearQuery q6 = new SpanNearQuery(new SpanQuery[]{q2, q5}, 1, true);
    SpanNearQuery q7 = new SpanNearQuery(new SpanQuery[]{q1, q6}, 1, true);

    TermCollector collector = new TermCollector();
    Spans spans;
    spans = q7.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    //"w1 w2 w3 w4 w5", // 0
    assertEquals(0, spans.advance(0));
    spans.nextStartPosition();
    checkCollectedTerms(spans, collector, new Term(FIELD, "w1"), new Term(FIELD, "w2"), new Term(FIELD, "w3"));
    spans.nextStartPosition();
    checkCollectedTerms(spans, collector, new Term(FIELD, "w1"), new Term(FIELD, "w2"), new Term(FIELD, "w4"));
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
    System.err.println("OK 1");

    // "w1 w2 w1 w4 w2 w3", // 3
    assertEquals(3, spans.advance(3));
    assertEquals(0, spans.nextStartPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "w1"), new Term(FIELD, "w2"), new Term(FIELD, "w4"));
    assertEquals(2, spans.nextStartPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "w1"), new Term(FIELD, "w2"), new Term(FIELD, "w3"));
    try {
      assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
    } catch (AssertionError er) {
      System.err.println("QUERY: "+q7);
      System.err.println("INPUT: "+docFields[3]);
      System.err.println("ERROR: "+spans);
      throw er;
    }
    System.err.println("OK 2");
    
    SpanNearQuery q23 = new SpanNearQuery(new SpanQuery[]{q2, q3}, 0, true);
    SpanOrQuery q223 = new SpanOrQuery(q2, q23);

    SpanNearQuery q1q2q3q4 = new SpanNearQuery(new SpanQuery[]{q1, q2, q3, q4}, 0, true);
    SpanNearQuery q1q23q4 = new SpanNearQuery(new SpanQuery[]{q1, q23, q4}, 0, true);
    SpanNearQuery q1q223q4 = new SpanNearQuery(new SpanQuery[]{q1, q223, q4}, 0, true);
    SpanNearQuery q223q4 = new SpanNearQuery(new SpanQuery[]{q223, q4}, 0, true);
    SpanNearQuery q1q223 = new SpanNearQuery(new SpanQuery[]{q1, q223}, 0, true);

    spans = q1q2q3q4.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    assertEquals(0, spans.advance(0));
    spans.nextStartPosition();
    checkCollectedTerms(spans, collector, new Term(FIELD, "w1"), new Term(FIELD, "w2"), new Term(FIELD, "w3"), new Term(FIELD, "w4"));
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
    System.err.println("OK 3");

    spans = q1q23q4.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    assertEquals(0, spans.advance(0));
    spans.nextStartPosition();
    checkCollectedTerms(spans, collector, new Term(FIELD, "w1"), new Term(FIELD, "w2"), new Term(FIELD, "w3"), new Term(FIELD, "w4"));
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
    System.err.println("OK 4");

    spans = q223q4.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    assertEquals(0, spans.advance(0));
    spans.nextStartPosition();
    checkCollectedTerms(spans, collector, new Term(FIELD, "w2"), new Term(FIELD, "w3"), new Term(FIELD, "w4"));
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
    System.err.println("OK 5");

    spans =q1q223.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    assertEquals(0, spans.advance(0));
    spans.nextStartPosition();
    checkCollectedTerms(spans, collector, new Term(FIELD, "w1"), new Term(FIELD, "w2"));
    spans.nextStartPosition();
    checkCollectedTerms(spans, collector, new Term(FIELD, "w1"), new Term(FIELD, "w2"), new Term(FIELD, "w3"));
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
    System.err.println("OK 6");

    spans =q1q223q4.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    assertEquals(0, spans.advance(0));
    assertEquals(0, spans.nextStartPosition());
    assertEquals(4, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD, "w1"), new Term(FIELD, "w2"), new Term(FIELD, "w3"), new Term(FIELD, "w4"));
    System.err.println("OK 7");

  }

  public void testLucene7848() throws Exception {
    if (NARROW) return;
    CharArraySet protWords = new CharArraySet(Collections.emptySet(), false);

    Analyzer searchAnalyzer = new Analyzer() {
      @Override
      public Analyzer.TokenStreamComponents createComponents(String field) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        return new Analyzer.TokenStreamComponents(tokenizer,
            new WordDelimiterGraphFilter(tokenizer, GENERATE_WORD_PARTS | PRESERVE_ORIGINAL | GENERATE_NUMBER_PARTS | STEM_ENGLISH_POSSESSIVE, protWords));
      }
    };

    Analyzer indexingAnalyzer = new Analyzer() {
      @Override
      public Analyzer.TokenStreamComponents createComponents(String field) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        TokenStream filter = new WordDelimiterGraphFilter(tokenizer, GENERATE_WORD_PARTS | PRESERVE_ORIGINAL | GENERATE_NUMBER_PARTS | STEM_ENGLISH_POSSESSIVE, protWords);
        filter = new FlattenGraphFilter(filter);
        return new Analyzer.TokenStreamComponents(tokenizer, filter);
      }
    };

    String input = "SPECIAL PROJECTS - xxx,SPECIAL PROJECTS -- yyy";
    String field = "field";
    IndexWriterConfig config = new IndexWriterConfig(indexingAnalyzer);
    try (IndexWriter w = new IndexWriter(new ByteBuffersDirectory(), config)) {
      Document doc = new Document();
      doc.add(new TextField(field, input, Field.Store.YES));
      w.addDocument(doc);
      w.commit();

      try (DirectoryReader reader = DirectoryReader.open(w, true, true)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        Query q = new QueryBuilder(searchAnalyzer).setUseSpansForGraphQueries(true).createPhraseQuery(field, input);
        System.err.println("QUERY: "+q);
        TopDocs topDocs = searcher.search(q, 10);
        try {
          assertEquals(1, topDocs.totalHits);
        } catch (AssertionError er) {
          throw er;
        }
      }
    }
  }

  /**
   * here we test the case where the token "ill" is indexed with
   * varying position lengths, and can make certain non-matching queries
   * become matching, iff supportVariableTermSpansLength == true
   * @throws Exception
   */
  public void testVariableTermSpanLength() throws Exception {
    if (NARROW) return;
    CharArraySet protWords = new CharArraySet(Collections.emptySet(), false);

    Analyzer indexingAnalyzer = new Analyzer() {
      @Override
      public Analyzer.TokenStreamComponents createComponents(String field) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        TokenStream filter = new WordDelimiterGraphFilter(tokenizer, GENERATE_WORD_PARTS | PRESERVE_ORIGINAL | GENERATE_NUMBER_PARTS | STEM_ENGLISH_POSSESSIVE | CATENATE_WORDS, protWords);
        filter = new FlattenGraphFilter(filter);
        filter = new PositionLengthOrderTokenFilter(filter, true);
        return new Analyzer.TokenStreamComponents(tokenizer, filter);
      }
    };

    String input = "i'm so ill i'll be staying home";
    String field = "field";
    IndexWriterConfig config = new IndexWriterConfig(indexingAnalyzer);
    try (IndexWriter w = new IndexWriter(new ByteBuffersDirectory(), config)) {
      Document doc = new Document();
      doc.add(new TextField(field, input, Field.Store.YES));
      w.addDocument(doc);
      w.commit();

      try (DirectoryReader reader = DirectoryReader.open(w, true, true)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        SpanTermQuery q1 = new SpanTermQuery(new Term(FIELD, "so"));
        SpanTermQuery q2 = new SpanTermQuery(new Term(FIELD, "ill"));
        SpanTermQuery q3 = new SpanTermQuery(new Term(FIELD, "be"));
        SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{q1, q2, q3}, 1, true,
            SpanNearQuery.DEFAULT_COMBO_MODE, SpanNearQuery.DEFAULT_ALLOW_OVERLAP,
            SpanNearQuery.DEFAULT_COMBINE_REPEAT_GROUPS_THRESHOLD, true, null, null);
        TopDocs topDocs = searcher.search(q, 10);
        assertEquals(1, topDocs.totalHits);
        q = new SpanNearQuery(new SpanQuery[]{q1, q2, q3}, 1, true,
            SpanNearQuery.DEFAULT_COMBO_MODE, SpanNearQuery.DEFAULT_ALLOW_OVERLAP,
            SpanNearQuery.DEFAULT_COMBINE_REPEAT_GROUPS_THRESHOLD, false, null, null);
        topDocs = searcher.search(q, 10);
        assertEquals(1, topDocs.totalHits);
      }
    }
  }

  private final boolean NARROW = false;
  @Test
  public void testNestedOrQuery() throws IOException {
    if (NARROW) return;
    SpanNearQuery snq = new SpanNearQuery.Builder(FIELD, true)
        .addClause(new SpanTermQuery(new Term(FIELD, "coordinate")))
        .addClause(new SpanOrQuery(
            new SpanTermQuery(new Term(FIELD, "gene")),
            SpanNearQuery.newOrderedNearQuery(FIELD)
                .addClause(new SpanTermQuery(new Term(FIELD, "gene")))
                .addClause(new SpanTermQuery(new Term(FIELD, "mapping")))
                .build()
        ))
        .addClause(new SpanTermQuery(new Term(FIELD, "research")))
        .build();

    Spans spans = snq.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    assertEquals(4, spans.advance(4));
    assertEquals(5, spans.nextDoc());
  }

  @Test
  public void testNestedOrQuerySlop() throws IOException {
    if (NARROW) return;
    SpanNearQuery snq = new SpanNearQuery.Builder(FIELD, true)
        .setSlop(1)
        .addClause(new SpanTermQuery(new Term(FIELD, "w1")))
        .addClause(new SpanOrQuery(
            new SpanTermQuery(new Term(FIELD, "w2")),
            SpanNearQuery.newOrderedNearQuery(FIELD)
                .addClause(new SpanTermQuery(new Term(FIELD, "w3")))
                .addClause(new SpanTermQuery(new Term(FIELD, "w4")))
                .build()
        ))
        .addClause(new SpanTermQuery(new Term(FIELD, "w5")))
        .build();

    Spans spans = snq.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    assertEquals(0, spans.advance(0));
    assertEquals(Spans.NO_MORE_DOCS, spans.nextDoc());
  }

  @Test
  public void testNestedOrQuery3() throws IOException {
    if (NARROW) return;
    SpanNearQuery snq = new SpanNearQuery.Builder(FIELD, true)
        .addClause(
            new SpanNearQuery.Builder(FIELD, true)
                .addClause(new SpanTermQuery(new Term(FIELD, "aa")))
                .addClause(new SpanOrQuery(
                    new SpanTermQuery(new Term(FIELD, "bb")),
                    new SpanNearQuery.Builder(FIELD, true)
                        .addClause(new SpanTermQuery(new Term(FIELD, "cc")))
                        .addClause(new SpanTermQuery(new Term(FIELD, "ee")))
                        .setSlop(2)
                        .build()
                ))
                .setSlop(2)
                .build()
        )
        .addClause(new SpanTermQuery(new Term(FIELD, "dd")))
        .setSlop(2)
        .build();

    Spans spans = snq.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    assertEquals(8, spans.advance(8));
  }

  @Test
  public void testNestedOrQuery4() throws IOException {
    if (NARROW) return;
    SpanNearQuery snq = new SpanNearQuery.Builder(FIELD, true)
        .addClause(new SpanTermQuery(new Term(FIELD, "aa")))
        .addClause(new SpanOrQuery(
            new SpanTermQuery(new Term(FIELD, "bb")),
            SpanNearQuery.newOrderedNearQuery(FIELD)
                .addClause(new SpanTermQuery(new Term(FIELD, "cc")))
                .addClause(new SpanTermQuery(new Term(FIELD, "ee")))
                .setSlop(2)
                .build()
        ))
        .addClause(new SpanTermQuery(new Term(FIELD, "dd")))
        .setSlop(3)
        .build();

    Spans spans = snq.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    assertEquals(8, spans.advance(8));
  }
  
  @Test
  public void testNestedOrQuery2() throws IOException {
    if (NARROW) return;
    SpanNearQuery snq = new SpanNearQuery.Builder(FIELD, true)
        .addClause(
            new SpanNearQuery.Builder(FIELD, true)
                .addClause(new SpanTermQuery(new Term(FIELD, "coordinate")))
                .addClause(new SpanOrQuery(
                    new SpanTermQuery(new Term(FIELD, "gene")),
                    new SpanNearQuery.Builder(FIELD, true)
                        .addClause(new SpanTermQuery(new Term(FIELD, "gene")))
                        .addClause(new SpanTermQuery(new Term(FIELD, "mapping")))
                        .build()
                ))
                .build()
        )
        .addClause(new SpanTermQuery(new Term(FIELD, "research")))
        .build();

    Spans spans = snq.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    assertEquals(4, spans.advance(4));
    assertEquals(5, spans.nextDoc());
  }
  
  @Test
  public void testNestedOrQueryLookAhead() throws IOException {
    if (NARROW) return;
    SpanNearQuery snq = new SpanNearQuery.Builder(FIELD, true)
        .addClause(new SpanOrQuery(
            new SpanTermQuery(new Term(FIELD, "user")),
            new SpanTermQuery(new Term(FIELD, "ue"))
        ))
        .addClause(new SpanNearQuery.Builder(FIELD, true)
            .setSlop(3)
            .addClause(new SpanTermQuery(new Term(FIELD, "location")))
            .addClause(new SpanTermQuery(new Term(FIELD, "information")))
            .build()
        )
        .build();

    System.err.println(docFields[6]);
    Spans spans = snq.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    assertEquals(6, spans.advance(0));
    assertEquals(Spans.NO_MORE_DOCS, spans.nextDoc());
  }
  
  @Test
  public void testBlah() throws IOException {
    if (NARROW) return;
    SpanNearQuery snq = new SpanNearQuery.Builder(FIELD, true)
        .setSlop(2)
        .addClause(new SpanOrQuery(
            new SpanTermQuery(new Term(FIELD, "a")),
            new SpanTermQuery(new Term(FIELD, "b")),
            new SpanNearQuery.Builder(FIELD, true)
                .addClause(new SpanTermQuery(new Term(FIELD, "a")))
                .addClause(new SpanTermQuery(new Term(FIELD, "b")))
                .build(),
            new SpanNearQuery.Builder(FIELD, true)
                .addClause(new SpanTermQuery(new Term(FIELD, "a")))
                .addClause(new SpanTermQuery(new Term(FIELD, "b")))
                .addClause(new SpanTermQuery(new Term(FIELD, "c")))
                .build()
        ))
        .addClause(new SpanOrQuery(
            new SpanTermQuery(new Term(FIELD, "c")),
            new SpanTermQuery(new Term(FIELD, "d")),
            new SpanNearQuery.Builder(FIELD, true)
                .addClause(new SpanTermQuery(new Term(FIELD, "d")))
                .addClause(new SpanTermQuery(new Term(FIELD, "e")))
                .build(),
            new SpanNearQuery.Builder(FIELD, true)
                .addClause(new SpanTermQuery(new Term(FIELD, "d")))
                .addClause(new SpanTermQuery(new Term(FIELD, "e")))
                .addClause(new SpanTermQuery(new Term(FIELD, "f")))
                .build()
        ))
        .addClause(new SpanOrQuery(
            new SpanTermQuery(new Term(FIELD, "e")),
            new SpanTermQuery(new Term(FIELD, "f")),
            new SpanNearQuery.Builder(FIELD, true)
                .addClause(new SpanTermQuery(new Term(FIELD, "g")))
                .addClause(new SpanTermQuery(new Term(FIELD, "h")))
                .build(),
            new SpanNearQuery.Builder(FIELD, true)
                .addClause(new SpanTermQuery(new Term(FIELD, "g")))
                .addClause(new SpanTermQuery(new Term(FIELD, "h")))
                .addClause(new SpanTermQuery(new Term(FIELD, "i")))
                .build()
        ))
        .build();

    TermCollector collector = new TermCollector();
    Spans spans = snq.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);
    assertEquals(9, spans.advance(9));
    int i = 0;
    while (spans.nextStartPosition() != Spans.NO_MORE_POSITIONS) {
      collector.reset();
      spans.collect(collector);
      i++;
    }
//    spans.nextStartPosition();
//    checkCollectedTerms(spans, collector, new Term(FIELD, "a"), new Term(FIELD, "d"), new Term(FIELD, "g"));
//    assertEquals(Spans.NO_MORE_DOCS, spans.nextDoc());
  }
  
  @Test
  public void testOrQuery() throws IOException {
    if (NARROW) return;
    SpanTermQuery q2 = new SpanTermQuery(new Term(FIELD, "w2"));
    SpanTermQuery q3 = new SpanTermQuery(new Term(FIELD, "w3"));
    SpanOrQuery orQuery = new SpanOrQuery(q2, q3);

    TermCollector collector = new TermCollector();
    Spans spans = orQuery.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);

    assertEquals(1, spans.advance(1));
    spans.nextStartPosition();
    checkCollectedTerms(spans, collector, new Term(FIELD, "w3"));
    spans.nextStartPosition();
    checkCollectedTerms(spans, collector, new Term(FIELD, "w2"));
    spans.nextStartPosition();
    checkCollectedTerms(spans, collector, new Term(FIELD, "w3"));

    assertEquals(3, spans.advance(3));
    spans.nextStartPosition();
    checkCollectedTerms(spans, collector, new Term(FIELD, "w2"));
    spans.nextStartPosition();
    checkCollectedTerms(spans, collector, new Term(FIELD, "w2"));
    spans.nextStartPosition();
    checkCollectedTerms(spans, collector, new Term(FIELD, "w3"));
  }

  @Test
  public void testSpanNotQuery() throws IOException {
    if (NARROW) return;

    SpanTermQuery q1 = new SpanTermQuery(new Term(FIELD, "w1"));
    SpanTermQuery q2 = new SpanTermQuery(new Term(FIELD, "w2"));
    SpanTermQuery q3 = new SpanTermQuery(new Term(FIELD, "w3"));

    SpanNearQuery nq = new SpanNearQuery(new SpanQuery[]{q1, q2}, 2, true);
    SpanNotQuery notq = new SpanNotQuery(nq, q3);

    TermCollector collector = new TermCollector();
    Spans spans = notq.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.POSITIONS);

    assertEquals(2, spans.advance(2));
    spans.nextStartPosition();
    checkCollectedTerms(spans, collector, new Term(FIELD, "w1"), new Term(FIELD, "w2"));

  }

}

