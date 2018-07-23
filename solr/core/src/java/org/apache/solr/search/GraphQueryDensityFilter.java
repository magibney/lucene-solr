/*
 * Copyright 2017 The Trustees of the University of Pennsylvania
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.lucene.analysis.miscellaneous.ShingleWords;
import org.apache.lucene.analysis.miscellaneous.ShingleWords.Words;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanNearQuery.ComboMode;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.ExtendedDismaxQParser.GraphQueryFilter;

/**
 *
 * @author magibney
 */
public class GraphQueryDensityFilter implements GraphQueryFilter {

  private static final int DEFAULT_DENSE_CLAUSE_NO_OVERLAP_THRESHOLD = Integer.MAX_VALUE; //5?
  private static final int DEFAULT_MAX_DENSE_CLAUSES = Integer.MAX_VALUE; //10?
  private static final int DEFAULT_MAX_CLAUSES = Integer.MAX_VALUE; //1000?
  private static final int DEFAULT_MAX_SUPPORTED_SLOP = Integer.MAX_VALUE; //100?
  private boolean initializedFieldDensities;
  private NamedList<Integer> denseFields;
  private int maxDenseClauses;
  private int maxClauses;
  private int maxSupportedSlop;
  private ShingleWords shingleWords;
  private boolean combineRepeatSpans;
  private ComboMode comboMode;
  private boolean allowOverlap;
  private int denseClauseNoOverlapThreshold;
  private int comboThreshold;
  private boolean supportVariableTermSpansLength;
  private Map<String, Map<BytesRef, DensityEntry>> densityLookup;

  @Override
  public void init(NamedList args, ResourceLoader loader) {
    denseFields = (NamedList<Integer>)args.remove("denseFields");
    Object tmp = args.remove("maxDenseClauses");
    this.maxDenseClauses = tmp == null ? DEFAULT_MAX_DENSE_CLAUSES : (Integer)tmp;
    tmp = args.remove("denseClauseNoOverlapThreshold");
    this.denseClauseNoOverlapThreshold = tmp == null ? DEFAULT_DENSE_CLAUSE_NO_OVERLAP_THRESHOLD : (Integer)tmp;
    tmp = args.remove("maxClauses");
    this.maxClauses = tmp == null ? DEFAULT_MAX_CLAUSES : (Integer)tmp;
    tmp = args.remove("maxSupportedSlop");
    this.maxSupportedSlop = tmp == null ? DEFAULT_MAX_SUPPORTED_SLOP : (Integer)tmp;
    Boolean tmpBool = args.removeBooleanArg("combineRepeatSpans");
    this.combineRepeatSpans = tmpBool == null ? SpanNearQuery.DEFAULT_COMBINE_REPEAT_SPANS : tmpBool;
    tmp = args.remove("comboMode");
    this.comboMode = tmp == null ? SpanNearQuery.DEFAULT_COMBO_MODE : ComboMode.valueOf((String)tmp);
    tmpBool = args.removeBooleanArg("allowOverlap");
    this.allowOverlap = tmpBool == null ? SpanNearQuery.DEFAULT_ALLOW_OVERLAP : tmpBool;
    tmpBool = args.removeBooleanArg("supportVariableTermSpansLength");
    this.supportVariableTermSpansLength = tmpBool == null ? SpanNearQuery.DEFAULT_SUPPORT_VARIABLE_TERM_SPANS_LENGTH : tmpBool;
    tmp = args.remove("comboThreshold");
    this.comboThreshold = tmp == null ? SpanNearQuery.DEFAULT_COMBO_THRESHOLD : (Integer)tmp;
    tmp = args.remove("shingleWords");
    try {
      this.shingleWords = tmp == null ? null : ShingleWords.newInstance((NamedList<Object>)tmp, loader);
    } catch (IOException ex) {
      ex.printStackTrace(System.err);
      this.shingleWords = null;
    }
  }

  @Override
  public void requestInit(SolrQueryRequest req) {
    if (!initializedFieldDensities) {
      initializedFieldDensities = true;
      if (denseFields != null && denseFields.size() > 0) {
        densityLookup = new HashMap<>(denseFields.size());
        SolrIndexSearcher searcher = req.getSearcher();
        for (Entry<String, Integer> fieldEntry : denseFields) {
          String field = fieldEntry.getKey();
          Map<BytesRef, DensityEntry> densities = initializeFieldDensities(searcher, field, fieldEntry.getValue());
          if (!densities.isEmpty()) {
            densityLookup.put(field, densities);
          }
        }
        if (densityLookup.isEmpty()) {
          densityLookup = null;
        }
      }
    }
  }

  private static Map<BytesRef, DensityEntry> initializeFieldDensities(SolrIndexSearcher searcher, String field, int size) {
    Map<BytesRef, DensityEntry> ret = new HashMap<>();
    int paddedSize = size * 2; // for better accuracy
    DensityEntry[] entries = new DensityEntry[paddedSize];
    int entryCount = 0;
    double minScore = Double.MAX_VALUE;
    for (LeafReaderContext ctx : searcher.getLeafContexts()) {
      LeafReader r = ctx.reader();
      int maxDoc = r.maxDoc();
      Terms terms;
      try {
        terms = r.terms(field);
        if (terms != null) {
          TermsEnum iter = terms.iterator();
          BytesRef br;
          while ((br = iter.next()) != null) {
            int docFreq = iter.docFreq();
            long totalTermFreq = iter.totalTermFreq();
            double appendDensity = (double)totalTermFreq / docFreq;
            double appendScore = getScore(appendDensity, docFreq, maxDoc);
            if (appendScore > minScore || entryCount < paddedSize) {
              DensityEntry existing = ret.get(br);
              double updatedScore;
              if (existing == null) {
                br = BytesRef.deepCopyOf(br);
                existing = new DensityEntry(br, appendDensity, docFreq, maxDoc);
                if (entryCount < paddedSize) {
                  entries[entryCount++] = existing;
                  if (appendScore < minScore) {
                    minScore = appendScore;
                  }
                } else {
                  Arrays.sort(entries, DEC);
                  BytesRef toRemove = entries[0].br;
                  entries[0] = existing;
                  minScore = Math.min(appendScore, entries[1].score);
                  ret.remove(toRemove);
                }
                ret.put(br, existing);
              } else if ((updatedScore = existing.updateDensity(appendDensity, docFreq, maxDoc)) < minScore) {
                minScore = updatedScore;
              }
            }
          }
        }
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
    List<DensityEntry> finalEntries = new ArrayList<>(ret.values());
    finalEntries.sort(DEC);
    ListIterator<DensityEntry> iter = finalEntries.listIterator(finalEntries.size());
    int i = 0;
    while (iter.hasPrevious()) {
      DensityEntry d = iter.previous();
      if (i++ < size) {
        System.err.println("DENSE_TERM " + i + ": " + field + " term=" + d.br.utf8ToString() + ", density=" + d.density + ", docCount=" + d.docCount + ", score=" + d.score);
      } else {
        ret.remove(d.br);
      }
    }
    return ret;
  }

  private static class DensityEntryComparator implements Comparator<DensityEntry> {

    @Override
    public int compare(DensityEntry o1, DensityEntry o2) {
      if (o1 == null) {
        return o2 == null ? 0 : 1;
      } else if (o2 == null) {
        return -1;
      } else {
        return Double.compare(o1.score, o2.score);
      }
    }

  }

  private static final DensityEntryComparator DEC = new DensityEntryComparator();

  private static class DensityEntry {

    private double score;
    private double density;
    private long docCount;
    private long maxDoc;
    private final BytesRef br;

    public DensityEntry(BytesRef br, double density, int docCount, int maxDoc) {
      this.density = density;
      this.docCount = docCount;
      this.maxDoc = maxDoc;
      this.br = br;
      this.score = GraphQueryDensityFilter.getScore(density, docCount, maxDoc);
    }

    public double updateDensity(double appendDensity, int appendDocCount, int appendMaxDoc) {
      this.maxDoc += appendMaxDoc;
      density += (appendDensity - density) / (docCount += appendDocCount);
      return score = GraphQueryDensityFilter.getScore(density, docCount, maxDoc);
    }

  }

  private static double getScore(double density, long docCount, long maxDoc) {
    return (density * docCount) / ((maxDoc + 1000) / 1000);
  }

  private SpanOrQuery filter(SpanOrQuery q) throws SyntaxError {
    final SpanQuery[] clauses = q.getClauses();
    boolean modified = false;
    for (int i = clauses.length - 1; i >= 0; i--) {
      final SpanQuery subQ = clauses[i];
      final SpanQuery newSubQ;
      if (subQ instanceof SpanOrQuery) {
        newSubQ = filter((SpanOrQuery)subQ);
      } else if (subQ instanceof SpanNearQuery) {
        SpanNearQuery snq = (SpanNearQuery) subQ;
        newSubQ = filter(snq, snq.getSlop(), 0);
      } else {
        continue;
      }
      if (newSubQ == null) {
        return null;
      } else if (newSubQ != subQ) {
        modified = true;
        clauses[i] = newSubQ;
      }
    }
    return modified ? new SpanOrQuery(clauses) : q;
  }

  private static final byte NULL_BYTE = (byte)0;
  @Override
  public SpanQuery filter(SpanNearQuery snq, int slop, int minClauseSize) throws SyntaxError {
    SpanQuery[] clauses = snq.getClauses();
    if (minClauseSize > 1 && clauses.length < minClauseSize) {
      return null;
    }
    if (clauses.length > maxClauses) {
      return null;
    }
    if (slop <= 0 && shingleWords == null) {
      return snq;
    } else {
      if (slop > maxSupportedSlop) {
        return null;
      }
      if (clauses.length <= 0 || !snq.isInOrder()) {
        return snq;
      } else {
        final boolean explicitPhraseQuery = minClauseSize == 0; // we might want to do something based on this.
        final String field = snq.getField();
        final Map<BytesRef, DensityEntry> densityEntry = densityLookup == null ? null : densityLookup.get(field);
        SpanNearQuery.Builder builder = new SpanNearQuery.Builder(field, snq.isInOrder(), comboMode,
            SpanNearQuery.DEFAULT_COMBO_THRESHOLD, allowOverlap, combineRepeatSpans, supportVariableTermSpansLength);
        final BytesRefBuilder brb = new BytesRefBuilder();
        int denseTermCount = 0;
        final Words<BytesRef> tmp;
        final Set<BytesRef> shingleWordsForField;
        final Set<BytesRef> shingles;
        final String shingleFieldSuffix;
        if (shingleWords == null || slop > shingleWords.maxShingleSlop) {
          shingleWordsForField = null;
          shingles = null;
          shingleFieldSuffix = null;
        } else if ((tmp = shingleWords.getShingleBytesRefsForField(field)) != null && slop <= tmp.maxSlop) {
          shingleWordsForField = tmp.words;
          shingles = new HashSet<>();
          shingleFieldSuffix = tmp.shingleFieldSuffix;
        } else {
          shingleWordsForField = null;
          shingles = null;
          shingleFieldSuffix = null;
        }
        boolean lastTermShingle = false;
        BytesRef lastTerm = null;
        for (int i = 0; i < clauses.length; i++) {
          SpanQuery sq = clauses[i];
          boolean termShingle = false;
          BytesRef term = null;
          if (sq instanceof SpanTermQuery) {
            term = ((SpanTermQuery)sq).getTerm().bytes();
            if (densityEntry != null) {
              if (densityEntry.containsKey(term)) {
                denseTermCount++;
              }
            }
            if (shingleWordsForField != null && ((termShingle = shingleWordsForField.contains(term)) ? lastTerm != null : lastTermShingle)) {
              brb.clear();
              brb.append(lastTerm);
              brb.append(NULL_BYTE);
              brb.append(term);
              shingles.add(brb.toBytesRef());
            }
          } else if (sq instanceof SpanNearQuery) {
            SpanNearQuery subSnq = (SpanNearQuery) sq;
            sq = filter(subSnq, subSnq.getSlop(), 0);
            if (sq == null) {
              return null;
            }
          } else if (sq instanceof SpanOrQuery) {
            sq = filter((SpanOrQuery)sq);
            if (sq == null) {
              return null;
            }
          }
          builder.addClause(sq);
          lastTerm = term;
          lastTermShingle = termShingle;
        }
        if (denseTermCount == clauses.length) {
          if (denseTermCount > maxDenseClauses) {
            return null;
          }
          builder = new SpanNearQuery.Builder(field, snq.isInOrder(), ComboMode.GREEDY_END_POSITION,
              comboThreshold, denseTermCount > denseClauseNoOverlapThreshold ? false : allowOverlap,
              combineRepeatSpans, supportVariableTermSpansLength);
          for (int i = 0; i < clauses.length; i++) {
            builder.addClause(clauses[i]);
          }
        }
        builder.setSlop(slop);
        if (shingles != null) {
          builder.setShingles(shingleFieldSuffix, shingles);
        }
        return builder.build();
      }
    }

  }
}
