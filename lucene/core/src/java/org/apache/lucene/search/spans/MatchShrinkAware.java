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

/**
 *
 * @author magibney
 */
public interface MatchShrinkAware {

  /**
   * Returns the minimum position length that could possibly be associated with any instance of
   * this term in this document. This will generally be 1 for TermSpans, n for NearSpansOrdered of n clauses,
   * and min of subSpans.positionLengthFloor() for SpanOrQuery.
   * @return minimum position length that could possibly be associated with any instance of this term in this document
   */
  int positionLengthFloor() throws IOException;

  /**
   * Returns the max amount by which endPosition may decrease on subsequent invocations of nextStartPosition().
   * return value may be specific to the current state/position of the Spans.
   * @return max amount by which endPosition may decrease on subsequent invocations of nextStartPosition()
   */
  int endPositionDecreaseCeiling() throws IOException;
}
