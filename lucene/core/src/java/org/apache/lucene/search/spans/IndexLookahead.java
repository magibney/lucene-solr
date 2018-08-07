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
 */
interface IndexLookahead {

  static final int UNKNOWN_AT_POSITION = Integer.MIN_VALUE + 6;
  static final int UNKNOWN_AT_DOC = Integer.MIN_VALUE + 5;
  static final int UNKNOWN_AT_SPANS = Integer.MIN_VALUE + 4;
  static final int UNINITIALIZED_AT_POSITION = Integer.MIN_VALUE + 3;
  static final int UNINITIALIZED_AT_DOC = Integer.MIN_VALUE + 2;
  static final int UNINITIALIZED_AT_SPANS = Integer.MIN_VALUE + 1;
  static final int NO_INDEX_LOOKAHEAD_IMPLEMENTED = Integer.MIN_VALUE;

  static String valueToString(int value) {
    switch (value) {
      case UNKNOWN_AT_POSITION:
        return "UNKNOWN_AT_POSITION";
      case UNKNOWN_AT_DOC:
        return "UNKNOWN_AT_DOC";
      case UNKNOWN_AT_SPANS:
        return "UNKNOWN_AT_SPANS";
      case UNINITIALIZED_AT_POSITION:
        return "UNINITIALIZED_AT_POSITION";
      case UNINITIALIZED_AT_DOC:
        return "UNINITIALIZED_AT_DOC";
      case UNINITIALIZED_AT_SPANS:
        return "UNINITIALIZED_AT_SPANS";
      case NO_INDEX_LOOKAHEAD_IMPLEMENTED:
        return "NO_INDEX_LOOKAHEAD_IMPLEMENTED";
      default:
        return Integer.toString(value);
    }
  }

  static final int MAX_SPECIAL_VALUE = UNKNOWN_AT_POSITION;
  static final int MIN_POSITION_LENGTH_CEILING_WITH_POSSIBLE_ENDPOSITION_DECREASE = 3;

  /**
   * Returns the minimum possible position that might be returned by a call to nextStartPosition()
   * If negative, must be one of the special values listed above, indicating that lookahead is
   * not possible. The different variants of special value negative return values are used to distinguish
   * different properties/scope that affect downstream caching options.
   * @return
   * @throws IOException
   */
  int lookaheadNextStartPositionFloor() throws IOException;

  /**
   * Returns the maximum position length that could possibly be associated with any instance of
   * this term in this document. Special return values indicating "no known positionLengthCeiling" are same as
   * special return values for lookaheadNextStartPositionFloor(). Negative values (aside from the special values)
   * indicate that there is the potential for endPosition to *decrease* between subsequent calls to
   * nextStartPosition() -- a case which requires special handling to reliably support complete combinatoric matching.
   * @return
   */
  int positionLengthCeiling();
}
