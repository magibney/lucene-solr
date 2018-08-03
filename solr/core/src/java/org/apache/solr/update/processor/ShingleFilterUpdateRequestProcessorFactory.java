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
package org.apache.solr.update.processor;

import com.carrotsearch.hppc.ObjectIntMap;
import com.carrotsearch.hppc.ObjectIntScatterMap;
import com.carrotsearch.hppc.cursors.ObjectIntCursor;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.solr.analysis.ShingleWords;
import org.apache.solr.analysis.ShingleWords.Words;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TermFrequencyAttribute;
import org.apache.lucene.document.Field;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.JsonPreAnalyzedParser;
import org.apache.solr.schema.PreAnalyzedField;
import org.apache.solr.update.AddUpdateCommand;

/**
 *
 */
public class ShingleFilterUpdateRequestProcessorFactory extends UpdateRequestProcessorFactory {

  private static final JsonPreAnalyzedParser parser = new JsonPreAnalyzedParser();

  private NamedList<Object> shingleWordsSpec;
  private ShingleWords shingleWords;

  @Override
  public void init(NamedList args) {
    super.init(args);
    shingleWordsSpec = args;
  }

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    if (shingleWordsSpec != null) {
      NamedList<Object> tmp = shingleWordsSpec;
      shingleWordsSpec = null;
      try {
        shingleWords = ShingleWords.newInstance(tmp, req.getCore().getResourceLoader());
      } catch (IOException ex) {
        ex.printStackTrace(System.err);
        shingleWords = null;
      }
    }
    return new ShingleFilterUpdateRequestProcessor(next, shingleWords);
  }

  private static class ShingleFilterUpdateRequestProcessor extends UpdateRequestProcessor {

    private final ShingleWords shingleWords;
    private final Map<String, FieldInputStruct> trackFields;

    public ShingleFilterUpdateRequestProcessor(UpdateRequestProcessor next, ShingleWords shingleWords) {
      super(next);
      this.shingleWords = shingleWords;
      this.trackFields = shingleWords == null ? null : new HashMap<>();
    }

    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
      SolrInputDocument doc = cmd.solrDoc;
      trackFields.clear();
      for (SolrInputField sif : doc) {
        final List<Entry<String, Words<String>>> destFields = shingleWords.getShingleStringsForField(sif.getName(), cmd);
        if (destFields != null) {
          for (Entry<String, Words<String>> e : destFields) {
            String fieldName = e.getKey();
            Words<String> shingleWordsForField = e.getValue();
            final FieldInputStruct fis;
            if (!trackFields.containsKey(fieldName)) {
              trackFields.put(fieldName, new FieldInputStruct(shingleWordsForField, sif));
            } else if ((fis = trackFields.get(fieldName)) != null) {
              fis.inputFields.add(sif);
            }
          }
        }
      }
      if (!trackFields.isEmpty()) {
        IndexSchema schema = cmd.getReq().getSchema();
        Analyzer a = schema.getIndexAnalyzer();
        ShinglesTokenStream sts = new ShinglesTokenStream(); // can this be a single final instance var (i.e., is it thread-safe)?
        for (Entry<String, FieldInputStruct> e : trackFields.entrySet()) {
          FieldInputStruct fis = e.getValue();
          if (fis != null) {
            ObjectIntMap<String> shingles = new ObjectIntScatterMap<>(fis.shingleWordsForField.size());
            for (SolrInputField sif : fis.inputFields) {
              for (Object val : sif) {
                analyzeTokenStream(fis.maxSlop, fis.shingleWordsForField, shingles, a.tokenStream(e.getKey(), (String)val));
              }
            }
            if (!shingles.isEmpty()) {
              String shinglesFieldName = e.getKey().concat(fis.shingleFieldSuffix);
              sts.setShingles(shingles);
              String s = parser.toFormattedString(new Field(shinglesFieldName, sts, PreAnalyzedField.createFieldType(schema.getField(shinglesFieldName))));
              doc.setField(shinglesFieldName, s);
            }
          }
        }
      }
      super.processAdd(cmd);
    }

    private void analyzeTokenStream(int maxSlop, Set<String> shingleWordsForField, ObjectIntMap<String> shingles, TokenStream ts) throws IOException {
      ts.reset();
      if (ts.incrementToken()) {
        final CharTermAttribute termAtt = ts.getAttribute(CharTermAttribute.class);
        final PositionIncrementAttribute posIncAtt = ts.getAttribute(PositionIncrementAttribute.class);
        final PositionLengthAttribute posLenAtt = ts.getAttribute(PositionLengthAttribute.class);
        final ArrayDeque<TermEndStruct> deque = new ArrayDeque<>();
        final ArrayDeque<TermEndStruct> shingleTermDeque = new ArrayDeque<>();
        int position = 0;
        StringBuilder sb = new StringBuilder();
        do {
          position += posIncAtt.getPositionIncrement();
          String term = termAtt.toString();
          TermEndStruct termEndStruct = new TermEndStruct(term, position + posLenAtt.getPositionLength());
          if (shingleWordsForField.contains(term)) {
            shingleTermDeque.add(termEndStruct);
            if (!deque.isEmpty()) {
              buildShingles(position, deque.iterator(), term, sb, maxSlop, shingles);
            }
          } else if (!shingleTermDeque.isEmpty()) {
            buildShingles(position, shingleTermDeque.iterator(), term, sb, maxSlop, shingles);
          }
          deque.add(termEndStruct);
        } while (ts.incrementToken());
      }
      ts.end();
      ts.close();
    }

    private void buildShingles(int position, Iterator<TermEndStruct> precedingIter, String term, StringBuilder sb, int maxSlop, ObjectIntMap<String> shingles) {
      while (precedingIter.hasNext()) {
        TermEndStruct preceding = precedingIter.next();
        int slop = position - preceding.endPosition;
        if (slop < 0) {
          // overlap, not a valid shingle, do nothing
        } else if (slop > maxSlop) {
          precedingIter.remove();
        } else {
          sb.setLength(0);
          sb.append(preceding.term).append('\0').append(term);
          String shingle = sb.toString();
          if (!shingles.containsKey(shingle)) {
            shingles.put(shingle, slop);
          } else {
            int extantMinSlop = shingles.get(shingle);
            if (slop < extantMinSlop) {
              shingles.put(shingle, slop);
            }
          }
        }
      }
    }

  }

  private static class ShinglesTokenStream extends TokenStream {

    private Iterator<ObjectIntCursor<String>> iter;
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final TermFrequencyAttribute tfAtt = addAttribute(TermFrequencyAttribute.class);

    public void setShingles(ObjectIntMap<String> shingles) {
      this.iter = shingles.iterator();
    }

    @Override
    public boolean incrementToken() throws IOException {
      if (!iter.hasNext()) {
        return false;
      }
      ObjectIntCursor<String> nextShingle = iter.next();
      termAtt.setLength(0).append(nextShingle.key);
      tfAtt.setTermFrequency(nextShingle.value + 1);
      return true;
    }

  }

  private static class TermEndStruct {
    private final String term;
    private final int endPosition;

    public TermEndStruct(String term, int endPosition) {
      this.term = term;
      this.endPosition = endPosition;
    }
  }

  private static class FieldInputStruct {
    private final int maxSlop;
    private final String shingleFieldSuffix;
    private final Set<String> shingleWordsForField;
    private final List<SolrInputField> inputFields = new ArrayList<>();
    public FieldInputStruct(Words<String> words, SolrInputField initial) {
      this.maxSlop = words.maxSlop;
      this.shingleFieldSuffix = words.shingleFieldSuffix;
      this.shingleWordsForField = words.words;
      inputFields.add(initial);
    }
  }

}
