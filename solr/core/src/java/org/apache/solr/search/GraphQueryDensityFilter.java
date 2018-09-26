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
import org.apache.solr.analysis.ShingleWords;
import org.apache.solr.analysis.ShingleWords.Words;
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
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.ExtendedDismaxQParser.GraphQueryFilter;

/**
 *
 */
public class GraphQueryDensityFilter implements GraphQueryFilter {

  private static final String SHINGLE_WORDS_ARGNAME = "shingleWords";
  private static final String MAX_SUPPORTED_SLOP_ARGNAME = "maxSupportedSlop";
  private static final String DENSE_FIELDS_ARGNAME = "denseFields";
  private static final String ALLOW_OVERLAP_ARGNAME = "allowOverlap";
  private static final String COMBINE_REPEAT_GROUPS_THRESHOLD_ARGNAME = "combineRepeatGroupsThreshold";
  private static final String SUPPORT_VARIABLE_TERM_SPANS_LENGTH_ARGNAME = "supportVariableTermSpansLength";
  private static final String COMBO_MODE_ARGNAME = "comboMode";
  private static final String DENSE_CLAUSE_NO_OVERLAP_THRESHOLD_ARGNAME = "denseClauseNoOverlapThreshold";
  private static final int DEFAULT_DENSE_CLAUSE_NO_OVERLAP_THRESHOLD = Integer.MAX_VALUE; //5?
  public static final String MAX_DENSE_CLAUSES_ARGNAME = "maxDenseClauses";
  private static final int DEFAULT_MAX_DENSE_CLAUSES = Integer.MAX_VALUE; //10?
  public static final String MAX_CLAUSES_ARGNAME = "maxClauses";
  private static final int DEFAULT_MAX_CLAUSES = Integer.MAX_VALUE; //1000?
  private static final int DEFAULT_MAX_SUPPORTED_SLOP = Integer.MAX_VALUE; //100?
  private static final String USE_LEGACY_IMPLEMENTATION_ARGNAME = "useLegacyImplementation";
  private static final boolean DEFAULT_USE_LEGACY_IMPLEMENTATION = false;
  private static final String USE_LOOP_IMPLEMENTATION_ARGNAME = "useLoopImplementation";
  private static final boolean DEFAULT_USE_LOOP_IMPLEMENTATION = true;
  public static final String MAKE_OUTER_GREEDY_ARGNAME = "makeOuterGreedy";
  private static final boolean DEFAULT_MAKE_OUTER_GREEDY = false;
  private boolean useLegacyImplementation;
  private boolean useLoopImplementation;
  private boolean initializedFieldDensities;
  private NamedList<Integer> denseFields;
  private int maxDenseClauses;
  private int maxClauses;
  private int maxSupportedSlop;
  private ShingleWords shingleWords;
  private int combineRepeatGroupsThreshold;
  private ComboMode comboMode;
  private boolean makeOuterGreedy;
  private boolean allowOverlap;
  private int denseClauseNoOverlapThreshold;
  private boolean supportVariableTermSpansLength;
  private Map<String, Map<BytesRef, DensityEntry>> densityLookup;

  @Override
  public void init(NamedList args, ResourceLoader loader) {
    denseFields = (NamedList<Integer>)args.remove(DENSE_FIELDS_ARGNAME);
    Object tmp = args.remove(MAX_DENSE_CLAUSES_ARGNAME);
    this.maxDenseClauses = tmp == null ? DEFAULT_MAX_DENSE_CLAUSES : (Integer)tmp;
    tmp = args.remove(DENSE_CLAUSE_NO_OVERLAP_THRESHOLD_ARGNAME);
    this.denseClauseNoOverlapThreshold = tmp == null ? DEFAULT_DENSE_CLAUSE_NO_OVERLAP_THRESHOLD : (Integer)tmp;
    tmp = args.remove(MAX_CLAUSES_ARGNAME);
    this.maxClauses = tmp == null ? DEFAULT_MAX_CLAUSES : (Integer)tmp;
    tmp = args.remove(MAX_SUPPORTED_SLOP_ARGNAME);
    this.maxSupportedSlop = tmp == null ? DEFAULT_MAX_SUPPORTED_SLOP : (Integer)tmp;
    Boolean tmpBool = args.removeBooleanArg(COMBINE_REPEAT_GROUPS_THRESHOLD_ARGNAME);
    this.combineRepeatGroupsThreshold = tmp == null ? SpanNearQuery.DEFAULT_COMBINE_REPEAT_GROUPS_THRESHOLD : (Integer)tmp;
    tmp = args.remove(COMBO_MODE_ARGNAME);
    this.comboMode = tmp == null ? SpanNearQuery.DEFAULT_COMBO_MODE : ComboMode.valueOf((String)tmp);
    tmpBool = args.removeBooleanArg(ALLOW_OVERLAP_ARGNAME);
    this.allowOverlap = tmpBool == null ? SpanNearQuery.DEFAULT_ALLOW_OVERLAP : tmpBool;
    tmpBool = args.removeBooleanArg(SUPPORT_VARIABLE_TERM_SPANS_LENGTH_ARGNAME);
    this.supportVariableTermSpansLength = tmpBool == null ? SpanNearQuery.DEFAULT_SUPPORT_VARIABLE_TERM_SPANS_LENGTH : tmpBool;
    tmp = args.remove(SHINGLE_WORDS_ARGNAME);
    try {
      this.shingleWords = tmp == null ? null : ShingleWords.newInstance((NamedList<Object>)tmp, loader);
    } catch (IOException ex) {
      ex.printStackTrace(System.err);
      this.shingleWords = null;
    }
    useLegacyImplementation = (tmpBool = args.removeBooleanArg(USE_LEGACY_IMPLEMENTATION_ARGNAME)) == null ? DEFAULT_USE_LEGACY_IMPLEMENTATION : tmpBool;
    useLoopImplementation = (tmpBool = args.removeBooleanArg(USE_LOOP_IMPLEMENTATION_ARGNAME)) == null ? DEFAULT_USE_LOOP_IMPLEMENTATION : tmpBool;
    makeOuterGreedy = (tmpBool = args.removeBooleanArg(MAKE_OUTER_GREEDY_ARGNAME)) == null ? DEFAULT_MAKE_OUTER_GREEDY : tmpBool;
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

  private static SpanOrQuery filter(SpanOrQuery q, int depth, FilterConfig c) throws SyntaxError {
    final SpanQuery[] clauses = q.getClauses();
    boolean modified = false;
    for (int i = clauses.length - 1; i >= 0; i--) {
      final SpanQuery subQ = clauses[i];
      final SpanQuery newSubQ;
      if (subQ instanceof SpanOrQuery) {
        newSubQ = filter((SpanOrQuery)subQ, depth + 1, c);
      } else if (subQ instanceof SpanNearQuery) {
        SpanNearQuery snq = (SpanNearQuery) subQ;
        newSubQ = filter(snq, snq.getSlop(), 0, depth + 1, c);
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
  public SpanQuery filter(SpanNearQuery snq, int slop, int minClauseSize, SolrParams params) throws SyntaxError {
    return filter(snq, slop, minClauseSize, 0, new FilterConfig(params));
  }

  private final class FilterConfig {
    private Boolean skipDensityCheck = null;
    private final Map<String, Map<BytesRef, DensityEntry>> densityLookup;
    private Boolean skipShingleWords = null;
    private final ShingleWords shingleWords;
    private final int maxSupportedSlop;
    private final SolrParams params;
    private int maxClauses = Integer.MIN_VALUE;
    private int maxDenseClauses = Integer.MIN_VALUE;
    private int denseClauseNoOverlapThreshold = Integer.MIN_VALUE;
    private Boolean makeOuterGreedy = null;
    private Boolean allowOverlap = null;
    private int combineRepeatGroupsThreshold = Integer.MIN_VALUE;
    private Boolean supportVariableTermSpansLength = null;
    private Boolean useLegacyImplementation = null;
    private Boolean useLoopImplementation = null;
    private ComboMode comboMode = null;
    private FilterConfig(SolrParams params) {
      this.params = params;
      this.shingleWords = GraphQueryDensityFilter.this.shingleWords;
      this.maxSupportedSlop = GraphQueryDensityFilter.this.maxSupportedSlop;
      this.densityLookup = GraphQueryDensityFilter.this.densityLookup;
    }
    private Map<String, Map<BytesRef, DensityEntry>> densityLookup() {
      if (densityLookup == null) {
        return null;
      } else if (skipDensityCheck == null) {
        String tmp = params.get(DENSE_FIELDS_ARGNAME);
        if (tmp != null && tmp.isEmpty()) {
          skipDensityCheck = true;
          return null;
        } else {
          skipDensityCheck = false;
          return densityLookup;
        }
      } else if (skipDensityCheck) {
        return null;
      } else {
        return densityLookup;
      }
    }
    private ShingleWords shingleWords() {
      if (shingleWords == null) {
        return null;
      } else if (skipShingleWords == null) {
        String tmp = params.get(SHINGLE_WORDS_ARGNAME);
        if (tmp != null && tmp.isEmpty()) {
          skipShingleWords = true;
          return null;
        } else {
          skipShingleWords = false;
          return shingleWords;
        }
      } else if (skipShingleWords) {
        return null;
      } else {
        return shingleWords;
      }
    }
    private int maxClauses() {
      return maxClauses == Integer.MIN_VALUE ? (maxClauses = params.getInt(MAX_CLAUSES_ARGNAME, GraphQueryDensityFilter.this.maxClauses)) : maxClauses;
    }
    private boolean makeOuterGreedy() {
      return makeOuterGreedy == null ? (makeOuterGreedy = params.getBool(MAKE_OUTER_GREEDY_ARGNAME, GraphQueryDensityFilter.this.makeOuterGreedy)) : makeOuterGreedy;
    }
    private ComboMode comboMode() {
      if (comboMode != null) {
        return comboMode;
      } else {
        String tmp = params.get(COMBO_MODE_ARGNAME);
        return comboMode = ((tmp == null || tmp.isEmpty()) ? GraphQueryDensityFilter.this.comboMode : ComboMode.valueOf(tmp));
      }
    }
    private int maxDenseClauses() {
      return maxDenseClauses == Integer.MIN_VALUE ? (maxDenseClauses = params.getInt(MAX_DENSE_CLAUSES_ARGNAME, GraphQueryDensityFilter.this.maxDenseClauses)) : maxDenseClauses;
    }
    private boolean allowOverlap() {
      return allowOverlap == null ? (allowOverlap = params.getBool(ALLOW_OVERLAP_ARGNAME, GraphQueryDensityFilter.this.allowOverlap)) : allowOverlap;
    }
    private int combineRepeatSpans() {
      return combineRepeatGroupsThreshold == Integer.MIN_VALUE ? (combineRepeatGroupsThreshold = params.getInt(COMBINE_REPEAT_GROUPS_THRESHOLD_ARGNAME, GraphQueryDensityFilter.this.combineRepeatGroupsThreshold)) : combineRepeatGroupsThreshold;
    }
    private boolean supportVariableTermSpansLength() {
      return supportVariableTermSpansLength == null ? (supportVariableTermSpansLength = params.getBool(SUPPORT_VARIABLE_TERM_SPANS_LENGTH_ARGNAME, GraphQueryDensityFilter.this.supportVariableTermSpansLength)) : supportVariableTermSpansLength;
    }
    private boolean useLegacyImplementation() {
      return useLegacyImplementation == null ? (useLegacyImplementation = params.getBool(USE_LEGACY_IMPLEMENTATION_ARGNAME, GraphQueryDensityFilter.this.useLegacyImplementation)) : useLegacyImplementation;
    }
     private boolean useLoopImplementation() {
      return useLoopImplementation == null ? (useLoopImplementation = params.getBool(USE_LOOP_IMPLEMENTATION_ARGNAME, GraphQueryDensityFilter.this.useLoopImplementation)) : useLoopImplementation;
    }
    private int denseClauseNoOverlapThreshold() {
      return denseClauseNoOverlapThreshold == Integer.MIN_VALUE ? (denseClauseNoOverlapThreshold = params.getInt(DENSE_CLAUSE_NO_OVERLAP_THRESHOLD_ARGNAME, GraphQueryDensityFilter.this.denseClauseNoOverlapThreshold)) : denseClauseNoOverlapThreshold;
    }
}
  private static SpanQuery filter(SpanNearQuery snq, int slop, int minClauseSize, int depth, FilterConfig c) throws SyntaxError {
    SpanQuery[] clauses = snq.getClauses();
    if (minClauseSize > 1 && clauses.length < minClauseSize) {
      return null;
    }
    if (clauses.length > c.maxClauses()) {
      return null;
    }
    if (slop <= 0 && c.shingleWords() == null) {
      return snq;
    } else {
      if (slop > c.maxSupportedSlop) {
        return null;
      }
      if (clauses.length <= 0 || !snq.isInOrder()) {
        return snq;
      } else {
        final boolean explicitPhraseQuery = minClauseSize == 0; // we might want to do something based on this.
        final String field = snq.getField();
        final ComboMode effectiveComboMode = c.makeOuterGreedy() && depth == 0 ? ComboMode.GREEDY_END_POSITION : c.comboMode();
        final Map<String, Map<BytesRef, DensityEntry>> densityLookup = c.densityLookup();
        final Map<BytesRef, DensityEntry> densityEntry = densityLookup == null ? null : densityLookup.get(field);
        SpanNearQuery.Builder builder = new SpanNearQuery.Builder(field, snq.isInOrder(), effectiveComboMode,
            c.allowOverlap(), c.combineRepeatSpans(), c.supportVariableTermSpansLength());
        final BytesRefBuilder brb = new BytesRefBuilder();
        int denseTermCount = 0;
        final Words<BytesRef> tmp;
        final Set<BytesRef> shingleWordsForField;
        final Set<BytesRef> shingles;
        final String shingleFieldSuffix;
        final ShingleWords shingleWords = c.shingleWords();
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
            sq = filter(subSnq, subSnq.getSlop(), 0, depth + 1, c);
            if (sq == null) {
              return null;
            }
          } else if (sq instanceof SpanOrQuery) {
            sq = filter((SpanOrQuery)sq, depth + 1, c);
            if (sq == null) {
              return null;
            }
          }
          builder.addClause(sq);
          lastTerm = term;
          lastTermShingle = termShingle;
        }
        if (denseTermCount == clauses.length) {
          if (denseTermCount > c.maxDenseClauses()) {
            return null;
          }
          builder = new SpanNearQuery.Builder(field, snq.isInOrder(), ComboMode.GREEDY_END_POSITION,
              denseTermCount > c.denseClauseNoOverlapThreshold() ? false : c.allowOverlap(),
              c.combineRepeatSpans(), c.supportVariableTermSpansLength());
          for (int i = 0; i < clauses.length; i++) {
            builder.addClause(clauses[i]);
          }
        }
        builder.setSlop(slop);
        builder.setLegacyImplementation(c.useLegacyImplementation());
        builder.setLoopImplementation(c.useLoopImplementation());
        if (shingles != null) {
          builder.setShingles(shingleFieldSuffix, shingles);
        }
        return builder.build();
      }
    }

  }
}
