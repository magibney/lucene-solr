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
package org.apache.solr.search;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.ExtendedDismaxQParser.GraphQueryFilter;

/**
 * An advanced multi-field query parser based on the DisMax parser.
 * See Wiki page http://wiki.apache.org/solr/ExtendedDisMax
 */
public class ExtendedDismaxQParserPlugin extends QParserPlugin {
  public static final String NAME = "edismax";

  private static final String PROJECT_PREFIX = "solr.";
  public static final String GRAPH_QUERY_FILTER_ARGNAME = "graphQueryFilter";
  public static final String PHRASE_AS_GRAPH_QUERY_ARGNAME = "phraseAsGraphQuery";
  public static final String EXPLICIT_PHRASE_AS_GRAPH_QUERY_ARGNAME = "explicitPhraseAsGraphQuery";
  public static final String MULTIPHRASE_AS_GRAPH_QUERY_ARGNAME = "multiphraseAsGraphQuery";
  public static final String EXPLICIT_MULTIPHRASE_AS_GRAPH_QUERY_ARGNAME = "explicitMultiphraseAsGraphQuery";
  public static final boolean DEFAULT_PHRASE_AS_GRAPH_QUERY = false;
  public static final boolean DEFAULT_EXPLICIT_PHRASE_AS_GRAPH_QUERY = false;
  public static final boolean DEFAULT_MULTIPHRASE_AS_GRAPH_QUERY = false;
  public static final boolean DEFAULT_EXPLICIT_MULTIPHRASE_AS_GRAPH_QUERY = false;
  public static final boolean DEFAULT_USE_LEGACY_IMPLEMENTATION = false;
  
  private NamedList graphQueryFilterSpec;
  private GraphQueryFilter graphQueryFilter;
  private boolean phraseAsGraphQuery;
  private boolean explicitPhraseAsGraphQuery;
  private boolean multiphraseAsGraphQuery;
  private boolean explicitMultiphraseAsGraphQuery;
  
  @Override
  public void init(NamedList args) {
    graphQueryFilterSpec = (NamedList)args.remove(GRAPH_QUERY_FILTER_ARGNAME);
    Boolean tmp;
    phraseAsGraphQuery = (tmp = args.removeBooleanArg(PHRASE_AS_GRAPH_QUERY_ARGNAME)) == null ? DEFAULT_PHRASE_AS_GRAPH_QUERY : tmp;
    explicitPhraseAsGraphQuery = (tmp = args.removeBooleanArg(EXPLICIT_PHRASE_AS_GRAPH_QUERY_ARGNAME)) == null ? DEFAULT_EXPLICIT_PHRASE_AS_GRAPH_QUERY : tmp;
    multiphraseAsGraphQuery = (tmp = args.removeBooleanArg(MULTIPHRASE_AS_GRAPH_QUERY_ARGNAME)) == null ? DEFAULT_MULTIPHRASE_AS_GRAPH_QUERY : tmp;
    explicitMultiphraseAsGraphQuery = (tmp = args.removeBooleanArg(EXPLICIT_MULTIPHRASE_AS_GRAPH_QUERY_ARGNAME)) == null ? DEFAULT_EXPLICIT_MULTIPHRASE_AS_GRAPH_QUERY : tmp;
    super.init(args);
  }

  private GraphQueryFilter initGraphQueryFilter(NamedList args, ResourceLoader loader) {
    List classNameArgs = args.removeAll("class");
    if (classNameArgs == null || classNameArgs.size() != 1) {
      throw new IllegalArgumentException("must specify a valid class for "+GRAPH_QUERY_FILTER_ARGNAME+"; found: "+classNameArgs);
    } else {
      String className = (String)classNameArgs.get(0);
      ClassLoader cl = ExtendedDismaxQParserPlugin.class.getClassLoader();
      try {
        try {
          Class<?> backingClass = cl.loadClass(className);
          Constructor<?> constructor = backingClass.getConstructor();
          GraphQueryFilter ret = (GraphQueryFilter)constructor.newInstance();
          ret.init(args, loader);
          return ret;
        } catch (ClassNotFoundException ex) {
          if (!className.startsWith(PROJECT_PREFIX)) {
            throw ex;
          } else {
            String tryClassName = ExtendedDismaxQParserPlugin.class.getPackage().getName().concat(className.substring(PROJECT_PREFIX.length() - 1));
            Class<?> backingClass = cl.loadClass(tryClassName);
            Constructor<?> constructor = backingClass.getConstructor();
            GraphQueryFilter ret = (GraphQueryFilter)constructor.newInstance();
            ret.init(args, loader);
            return ret;
          }
        }
      } catch (ClassNotFoundException |
          NoSuchMethodException |
          SecurityException |
          InstantiationException |
          IllegalAccessException |
          IllegalArgumentException |
          InvocationTargetException ex) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error loading class '" + className + "'", ex);
      }
    }
  }
  
  @Override
  public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    if (graphQueryFilterSpec != null) {
      NamedList tmp = graphQueryFilterSpec;
      graphQueryFilterSpec = null;
      graphQueryFilter = initGraphQueryFilter(tmp, req.getCore().getResourceLoader());
    }
    return new ExtendedDismaxQParser(qstr, localParams, params, req, graphQueryFilter,
        phraseAsGraphQuery, explicitPhraseAsGraphQuery, multiphraseAsGraphQuery, explicitMultiphraseAsGraphQuery);
  }
}