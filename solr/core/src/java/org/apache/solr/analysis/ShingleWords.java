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
package org.apache.solr.analysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.schema.CopyField;
import org.apache.solr.update.AddUpdateCommand;

/**
 *
 */
public class ShingleWords {

  public static final String DEFAULT_SHINGLE_FIELD_SUFFIX = "_shingles";

  private static final int DEFAULT_MAXSLOP = 0;

  public static void main(String[] args) {
    String input;
    BytesRef br;
    System.err.println('"'+(input = "\\")+"\" => "+(br = processLine(input, null))+"/"+(br == null ? null : '"'+br.utf8ToString()+'"'));
    System.err.println('"'+(input = "")+"\" => "+(br = processLine(input, null))+"/"+(br == null ? null : '"'+br.utf8ToString()+'"'));
    System.err.println('"'+(input = "# comment")+"\" => "+(br = processLine(input, null))+"/"+(br == null ? null : '"'+br.utf8ToString()+'"'));
    System.err.println('"'+(input = "    #other comment")+"\" => "+(br = processLine(input, null))+"/"+(br == null ? null : '"'+br.utf8ToString()+'"'));
    System.err.println('"'+(input = "\\ # single space comment")+"\" => "+(br = processLine(input, null))+"/"+(br == null ? null : '"'+br.utf8ToString()+'"'));
    System.err.println('"'+(input = "word     # word")+"\" => "+(br = processLine(input, null))+"/"+(br == null ? null : '"'+br.utf8ToString()+'"'));
    System.err.println('"'+(input = "other word\\#  \\\\# and a comment!")+"\" => "+(br = processLine(input, null))+"/"+(br == null ? null : '"'+br.utf8ToString()+'"'));
    System.err.println('"'+(input = "superfluous\\#  \\")+"\" => "+(br = processLine(input, null))+"/"+(br == null ? null : '"'+br.utf8ToString()+'"'));
  }

  /**
   * Comments are "#" character through end of line. To include a literal "#" character in the shingled word,
   * escape as "\#". Leading and trailing whitespace characters are trimmed, unless escaped
   * with a preceding "\", e.g., "\ ". Literal backslash must be escaped as "\\".
   * As a *very* special case, a single line consisting only of the escape character (i.e., "\") may
   * be specified to include the *empty* string as a shingled token. Weird, but include for completeness.
   */
  private static final Pattern COMMENT = Pattern.compile("(^|[^\\\\](\\\\\\\\)*)#");
  private static final Pattern TRIM = Pattern.compile("(^\\s*)|(^|[^\\\\])((\\\\((\\\\\\\\)*\\s))?\\s*$)");
  private static final Pattern TRAILING_ESCAPE_CHAR = Pattern.compile("(^|[^\\\\])\\\\(\\\\\\\\)*$");
  private static final Pattern REPLACE_ESCAPE_CHARS = Pattern.compile("\\\\(.)");

  public static ShingleWords newInstance(NamedList<Object> tmp, ResourceLoader loader) throws IOException {
    Map<String, Entry<Words<String>, Words<BytesRef>>> shingleWordsPerField = new HashMap<>();
    int overallMaxShingleSlop = -1;
    for (Entry<String, Object> fieldConfig : tmp) {
      String fieldName = fieldConfig.getKey();
      Object val = fieldConfig.getValue();
      int maxSlop;
      String wordsResourceName;
      InputStream wordsStream;
      String shingleFieldSuffix;
      if (val instanceof String) {
        maxSlop = DEFAULT_MAXSLOP;
        wordsResourceName = (String)val;
        wordsStream = loader.openResource(wordsResourceName);
        shingleFieldSuffix = DEFAULT_SHINGLE_FIELD_SUFFIX;
      } else if (val instanceof NamedList) {
        NamedList<Object> config = (NamedList<Object>)val;
        wordsResourceName = (String)config.get("words");
        wordsStream = loader.openResource(wordsResourceName);
        val = config.get("maxSlop");
        if (val == null) {
          maxSlop = DEFAULT_MAXSLOP;
        } else if (val instanceof Integer) {
          maxSlop = (Integer) val;
        } else if (val instanceof String) {
          maxSlop = Integer.parseInt((String)val);
        } else {
          throw new RuntimeException("unexpected value for field \"" + fieldName + "\"/words: " + val);
        }
        if (maxSlop < 0) {
          throw new IllegalArgumentException("illegal negative maxslop for field spec: \""+fieldName+"\"");
        }
        val = config.get("shingleFieldSuffix");
        shingleFieldSuffix = val == null ? DEFAULT_SHINGLE_FIELD_SUFFIX : (String)val;
      } else {
        throw new RuntimeException("unexpected value for field \""+fieldName+"\": "+val);
      }
      if (buildShingleWords(fieldName, maxSlop, shingleFieldSuffix, wordsResourceName, wordsStream, shingleWordsPerField) != null && maxSlop > overallMaxShingleSlop) {
        overallMaxShingleSlop = maxSlop;
      }
    }
    if (overallMaxShingleSlop < 0) {
      return null;
    } else {
      return new ShingleWords(shingleWordsPerField, overallMaxShingleSlop);
    }
  }

  private final Map<String, List<Entry<String, Words<String>>>> copyFieldCache = new HashMap<>(); 
  private final Map<String, Entry<Words<String>, Words<BytesRef>>> dynamicFieldCache = new HashMap<>(); 
  private final Map<String, Entry<Words<String>, Words<BytesRef>>> shingleWordsPerField; 
  private final Entry<String, String>[] matchSuffixes;
  public final int maxShingleSlop;

  @Override
  public String toString() {
    return "ShingleWords{" + ", words=" + shingleWordsPerField + ", maxShingleSlop=" + maxShingleSlop + '}';
  }

  private ShingleWords(Map<String, Entry<Words<String>, Words<BytesRef>>> shingleWordsPerField, int maxShingleSlop) {
    this.maxShingleSlop = maxShingleSlop;
    this.shingleWordsPerField = shingleWordsPerField;
    if (shingleWordsPerField == null || shingleWordsPerField.isEmpty()) {
      matchSuffixes = null;
    } else {
      final int size = shingleWordsPerField.size();
      List<Entry<String, String>> suffixPatterns = new ArrayList<>(size);
      for (String fieldPattern : shingleWordsPerField.keySet()) {
        if (fieldPattern.startsWith("*")) {
          suffixPatterns.add(new SimpleImmutableEntry(fieldPattern.substring(1), fieldPattern));
        }
      }
      if (suffixPatterns.isEmpty()) {
        matchSuffixes = null;
      } else {
        suffixPatterns.sort(KEY_LENGTH_COMPARATOR);
        matchSuffixes = suffixPatterns.toArray(new Entry[suffixPatterns.size()]);
      }
    }
  }

  public List<Entry<String, Words<String>>> getShingleStringsForField(String fieldName, AddUpdateCommand cmd) {
    if (copyFieldCache.containsKey(fieldName)) {
      synchronized (copyFieldCache) {
        return copyFieldCache.get(fieldName);
      }
    }
    List<CopyField> copyFields = cmd.getReq().getSchema().getCopyFieldsList(fieldName);
    Entry<Words<String>, Words<BytesRef>> tmp = getShingleWordsForField(fieldName);
    List<Entry<String, Words<String>>> ret;
    if (copyFields.isEmpty()) {
      ret = tmp == null ? null : Collections.singletonList(new SimpleImmutableEntry<>(fieldName, tmp.getKey()));
    } else {
      if (tmp == null) {
        ret = new ArrayList<>(copyFields.size());
      } else {
        ret = new ArrayList<>(copyFields.size() + 1);
        ret.add(new SimpleImmutableEntry<>(fieldName, tmp.getKey()));
      }
      Iterator<CopyField> iter = copyFields.iterator();
      do {
        String copyFieldName = iter.next().getDestination().getName();
        tmp = getShingleWordsForField(copyFieldName);
        if (tmp != null) {
          ret.add(new SimpleImmutableEntry<>(copyFieldName, tmp.getKey()));
        }
      } while (iter.hasNext());
      if (ret.isEmpty()) {
        ret = null;
      }
    }
    synchronized (copyFieldCache) {
      copyFieldCache.put(fieldName, ret);
    }
    return ret;
  }

  public Words<BytesRef> getShingleBytesRefsForField(String fieldName) {
    Entry<Words<String>, Words<BytesRef>> tmp = getShingleWordsForField(fieldName);
    return tmp == null ? null : tmp.getValue();
  }

  private Entry<Words<String>, Words<BytesRef>> getShingleWordsForField(String fieldName) {
    if (shingleWordsPerField.containsKey(fieldName)) {
      return shingleWordsPerField.get(fieldName);
    } else if (dynamicFieldCache.containsKey(fieldName)) {
      synchronized (dynamicFieldCache) {
        return dynamicFieldCache.get(fieldName);
      }
    } else if (matchSuffixes != null) {
      for (int i = matchSuffixes.length - 1; i >= 0; i--) {
        Entry<String, String> e = matchSuffixes[i];
        if (fieldName.endsWith(e.getKey())) {
          Entry<Words<String>, Words<BytesRef>> ret = shingleWordsPerField.get(e.getValue());
          synchronized (dynamicFieldCache) {
            dynamicFieldCache.put(fieldName, ret);
          }
          return ret;
        }
      }
    }
    return null;
  }

  private static final Comparator<Entry<String, String>> KEY_LENGTH_COMPARATOR = new Comparator<Entry<String, String>>() {

    @Override
    public int compare(Entry<String, String> o1, Entry<String, String> o2) {
      return Integer.compare(o1.getKey().length(), o2.getKey().length());
    }

  };

  public static final class Words<T> {
    public final int maxSlop;
    public final String shingleFieldSuffix;
    public final Set<T> words;

    public Words(int maxSlop, String shingleFieldSuffix, Set<T> words) {
      this.maxSlop = maxSlop;
      this.shingleFieldSuffix = shingleFieldSuffix;
      this.words = words;
    }
  }

  public static Entry<Words<String>, Words<BytesRef>> buildShingleWords(String fieldName,
      int maxSlop,
      String shingleFieldSuffix,
      String wordsResourceName,
      InputStream f,
      Map<String, Entry<Words<String>, Words<BytesRef>>> shingleWordsPerField) {
    BufferedReader br = null;
    try {
      br = new BufferedReader(new InputStreamReader(f, StandardCharsets.UTF_8));
      Set<String> s = new HashSet<>();
      Set<BytesRef> b = new HashSet<>();
      String line;
      while ((line = br.readLine()) != null) {
        processLine(line, wordsResourceName, s, b);
      }
      if (s.isEmpty()) {
        return null;
      } else {
        Entry<Words<String>, Words<BytesRef>> ret = new SimpleImmutableEntry<>(new Words<>(maxSlop, shingleFieldSuffix, s), new Words<>(maxSlop, shingleFieldSuffix, b));
        shingleWordsPerField.put(fieldName, ret);
        return ret;
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException ex) {
          throw new RuntimeException(ex);
        }
      }
    }
  }

  private static final BytesRef EMPTY_BYTES_REF = new BytesRef(BytesRef.EMPTY_BYTES);

  private static final Set<Object> DEV_NULL_SET = new AbstractSet<Object>() {

    @Override
    public boolean add(Object e) {
      return false;
    }

    @Override
    public Iterator<Object> iterator() {
      return Collections.emptyIterator();
    }

    @Override
    public int size() {
      return 0;
    }
  };

  private static BytesRef processLine(String line, File f) {
    return (BytesRef) processLine(line, f == null ? null : f.toString(), DEV_NULL_SET, DEV_NULL_SET);
  }

  private static BytesRef processLine(String line, String wordsResourceName, Set<? super String> shingleStrings, Set<? super BytesRef> shingleBytesRefs) {
    final String orig = line;
    Matcher m = COMMENT.matcher(line);
    if (m.find()) {
      line = line.substring(0, m.end() - 1);
    }
    line = TRIM.matcher(line).replaceAll("$2$5");
    if (TRAILING_ESCAPE_CHAR.matcher(line).find()) {
      if (line.length() == 1) {
        shingleStrings.add("");
        shingleBytesRefs.add(EMPTY_BYTES_REF);
        return EMPTY_BYTES_REF;
      } else {
        System.err.println("superfluous trailing escape character in line \""+orig+"\" (file \""+wordsResourceName+"\")");
        line = line.substring(0, line.length() - 1); // ignore superfluous character and continue
      }
    }
    line = REPLACE_ESCAPE_CHARS.matcher(line).replaceAll("$1");
    if (line.isEmpty()) {
      return null;
    } else {
      shingleStrings.add(line);
      BytesRef ret = new BytesRef(line);
      shingleBytesRefs.add(ret);
      return ret;
    }
  }

}
