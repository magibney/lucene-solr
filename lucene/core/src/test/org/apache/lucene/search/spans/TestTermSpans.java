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
import java.io.StringReader;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.FlattenGraphFilterFactory;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import static org.apache.lucene.search.spans.TestSpanCollection.ENCODE_LOOKAHEAD;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.analysis.ReversedWildcardFilterFactory;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class TestTermSpans extends LuceneTestCase {

  protected IndexSearcher searcher;
  protected Directory directory;
  protected IndexReader reader;

  public static final String FIELD = "field";
  public static final String FIELD2 = "other";

  public static final FieldType OFFSETS = new FieldType(TextField.TYPE_STORED);
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
        newIndexWriterConfig(new ExpandingAnalyzer()).setMergePolicy(newLogMergePolicy()));
    for (int i = 0; i < docFields.length; i++) {
      Document doc = new Document();
      doc.add(newField(FIELD, docFields[i], OFFSETS));
      doc.add(newField(FIELD2, docFields[i], OFFSETS));
      writer.addDocument(doc);
    }
    writer.forceMerge(1);
    reader = writer.getReader();
    writer.close();
    searcher = newSearcher(getOnlyLeafReader(reader));
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

  protected static final String[] docFields = {
      "a a a",
      "b b b",
      "itzak perlman's greatest hits"
  };

  private void checkCollectedTerms(Spans spans, TermCollector collector, Term... expectedTerms) throws IOException {
    collector.reset();
    spans.collect(collector);
    for (Term t : expectedTerms) {
      assertTrue("Missing term " + t, collector.terms.contains(t));
    }
    assertEquals("Unexpected terms found", expectedTerms.length, collector.terms.size());
  }

  @Test 
  public void testSomethingHit() throws IOException {
    SpanTermQuery q = new SpanTermQuery(new Term(FIELD, "a"));
    TermCollector collector = new TermCollector();
    Spans spans = q.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.PAYLOADS);
    assertEquals(0, spans.advance(0));
    for (int i = 0; i < 3; i++) {
      int expectEnd = i + 1;
      for (int j = 0; j < 3; j++) {
        assertEquals(i, spans.nextStartPosition());
        assertEquals(expectEnd, spans.endPosition());
        checkCollectedTerms(spans, collector, new Term(FIELD, "a"));
      }
      int endCeiling = expectEnd + 4;
      while (++expectEnd < endCeiling) {
        assertEquals(i, spans.nextStartPosition());
        assertEquals(expectEnd, spans.endPosition());
        checkCollectedTerms(spans, collector, new Term(FIELD, "a"));
      }
    }
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }
  
  private static class ExpandingAnalyzer extends AnalyzerWrapper {

    public ExpandingAnalyzer() {
      super(Analyzer.PER_FIELD_REUSE_STRATEGY);
    }

    @Override
    protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
      TokenStream filter;
      switch (fieldName) {
        case FIELD:
          filter = new ExpandingTokenFilter(components.getTokenStream());
          filter = new PositionLengthOrderTokenFilterFactory.PositionLengthOrderTokenFilter(filter, ENCODE_LOOKAHEAD);
          return new TokenStreamComponents(components.getTokenizer(), filter);
        case FIELD2:
          filter = new WordDelimiterGraphFilter(components.getTokenStream(),
              WordDelimiterGraphFilter.GENERATE_WORD_PARTS
              | WordDelimiterGraphFilter.PRESERVE_ORIGINAL
              | WordDelimiterGraphFilter.CATENATE_WORDS,
              CharArraySet.EMPTY_SET);
          filter = new FlattenGraphFilterFactory(Collections.EMPTY_MAP).create(filter);
          filter = new ReversedWildcardFilterFactory(Collections.EMPTY_MAP).create(filter);
          filter = new PositionLengthOrderTokenFilterFactory.PositionLengthOrderTokenFilter(filter, ENCODE_LOOKAHEAD);
          return new TokenStreamComponents(components.getTokenizer(), filter);
        default:
          throw new AssertionError();
      }
    }

    @Override
    protected Analyzer getWrappedAnalyzer(String fieldName) {
      return new MockAnalyzer(random());
    }
    
  }
  
  private static final int REPEAT_COUNT = 6;
  
  private static class ExpandingTokenFilter extends TokenFilter {

    
    private final int repeatCount = REPEAT_COUNT + 2;
    private int repeat = 0;
    private final PositionIncrementAttribute posIncAtt = addAttribute(PositionIncrementAttribute.class);
    private final PositionLengthAttribute posLengthAtt = addAttribute(PositionLengthAttribute.class);
    
    public ExpandingTokenFilter(TokenStream input) {
      super(input);
    }
    
    @Override
    public boolean incrementToken() throws IOException {
      if (--repeat > 2) {
        posIncAtt.setPositionIncrement(0);
      } else {
        if (!input.incrementToken()) {
          return false;
        }
        posIncAtt.setPositionIncrement(1);
        repeat = repeatCount;
      }
      int posLen;
      if (repeat % 2 == 0) {
        posLen = repeat / 2;
      } else {
        posLen = 1;
      }
      posLengthAtt.setPositionLength(posLen);
      return true;
    }
    
  }
  
  @Test 
  public void testQuery() throws IOException {
    Spans spans;
    SpanTermQuery qa = new SpanTermQuery(new Term(FIELD2, "itzak"));
    SpanTermQuery qb = new SpanTermQuery(new Term(FIELD2, "perlmans"));
    SpanTermQuery qc = new SpanTermQuery(new Term(FIELD2, "greatest"));
    SpanTermQuery qd = new SpanTermQuery(new Term(FIELD2, "hits"));
    SpanNearQuery q = new SpanNearQuery(new SpanQuery[]{qa, qb, qc, qd}, 0, true);
    TermCollector collector = new TermCollector();
    spans = q.createWeight(searcher, false, 1f).getSpans(searcher.getIndexReader().leaves().get(0), SpanWeight.Postings.PAYLOADS);
    assertEquals(2, spans.advance(2));
    assertEquals(0, spans.nextStartPosition());
    assertEquals(5, spans.endPosition());
    checkCollectedTerms(spans, collector, new Term(FIELD2, "itzak"), new Term(FIELD2, "perlmans"), new Term(FIELD2, "greatest"), new Term(FIELD2, "hits"));
    assertEquals(Spans.NO_MORE_POSITIONS, spans.nextStartPosition());
  }
  
  @Test 
  public void testExplicit() throws IOException {
    Tokenizer t = new MockTokenizer();
    TokenFilter tf = new WordDelimiterGraphFilter(t, 
        WordDelimiterGraphFilter.GENERATE_WORD_PARTS | 
        WordDelimiterGraphFilter.PRESERVE_ORIGINAL | 
        WordDelimiterGraphFilter.CATENATE_WORDS, 
        CharArraySet.EMPTY_SET);
    tf = new PositionLengthOrderTokenFilterFactory.PositionLengthOrderTokenFilter(tf, ENCODE_LOOKAHEAD);
    CharTermAttribute charTermAtt = tf.addAttribute(CharTermAttribute.class);
    PositionIncrementAttribute posIncrAtt = tf.addAttribute(PositionIncrementAttribute.class);
    PositionLengthAttribute posLenAtt = tf.addAttribute(PositionLengthAttribute.class);
    t.setReader(new StringReader("itzak perlman's hits"));
    t.reset();
    assertTrue(tf.incrementToken());
    assertEquals("itzak", charTermAtt.toString());
    assertEquals(1, posIncrAtt.getPositionIncrement());
    assertEquals(1, posLenAtt.getPositionLength());
    assertTrue(tf.incrementToken());
    assertEquals("perlman", charTermAtt.toString());
    assertEquals(1, posIncrAtt.getPositionIncrement());
    assertEquals(1, posLenAtt.getPositionLength());
    assertTrue(tf.incrementToken());
    assertEquals("perlmans", charTermAtt.toString());
    assertEquals(0, posIncrAtt.getPositionIncrement());
    assertEquals(2, posLenAtt.getPositionLength());
    assertTrue(tf.incrementToken());
    assertEquals("perlman's", charTermAtt.toString());
    assertEquals(0, posIncrAtt.getPositionIncrement());
    assertEquals(2, posLenAtt.getPositionLength());
    assertTrue(tf.incrementToken());
    assertEquals("s", charTermAtt.toString());
    assertEquals(1, posIncrAtt.getPositionIncrement());
    assertEquals(1, posLenAtt.getPositionLength());
    assertTrue(tf.incrementToken());
    assertEquals("hits", charTermAtt.toString());
    assertEquals(1, posIncrAtt.getPositionIncrement());
    assertEquals(1, posLenAtt.getPositionLength());
    assertFalse(tf.incrementToken());
    t.end();
    t.close();
  }
  
}

