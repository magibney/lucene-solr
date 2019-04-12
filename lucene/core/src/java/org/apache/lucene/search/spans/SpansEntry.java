/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.apache.lucene.search.spans;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import org.apache.lucene.search.spans.PositionDeque.LocalNodeArrayList;
import org.apache.lucene.search.spans.PositionDeque.Node;
import static org.apache.lucene.search.spans.PositionDeque.PHRASE_ORDER_COMPARATOR;
import org.apache.lucene.search.spans.SpanCollector;
import org.apache.lucene.search.spans.SpanNearQuery.ComboMode;
import org.apache.lucene.search.spans.Spans;

/**
 *
 * @author magibney
 */
final class SpansEntry implements Comparable<SpansEntry> {
  private static enum Type {TYPE_NO_MORE_SPANS, TYPE_BASE, PER_POSITION};
  private final Type type;
  protected int width;
  
  /* For TYPE_BASE */
  protected final Spans[] subSpans;
  protected final int lastIndex;

  /* For TYPE_PER_POSITION */
  int startPosition;
  int endPosition;
  LocalNodeArrayList backing;
  final ComboMode comboMode;
  
  public SpansEntry() {
    type = Type.TYPE_NO_MORE_SPANS;
    subSpans = null;
    lastIndex = -1;
    comboMode = null;
  }
  
  public SpansEntry(Spans[] subSpans, int lastIndex) {
    type = Type.TYPE_BASE;
    this.subSpans = subSpans;
    this.lastIndex = lastIndex;
    this.comboMode = null;
  }
  
  public SpansEntry(ComboMode comboMode) {
    type = Type.PER_POSITION;
    subSpans = null;
    lastIndex = -1;
    this.comboMode = comboMode;
  }
  
  protected SpansEntry init(int width) {
    this.width = width;
    return this;
  }
  
  public SpansEntry init(int startPosition, int endPosition, LocalNodeArrayList backing, int width) {
    this.width = width;
    this.startPosition = startPosition;
    this.endPosition = endPosition;
    this.backing = backing;
    if (backing != null && backing.size > 1) {
      backing.sort(PHRASE_ORDER_COMPARATOR);
    }
    return this;
  }

  public int width() {
    return width;
  }

  public int startPosition() {
    switch (type) {
      case TYPE_BASE:
        return subSpans[0].startPosition();
      case PER_POSITION:
        return startPosition;
    }
    return Spans.NO_MORE_POSITIONS;
  }

  public int endPosition() {
    switch (type) {
      case TYPE_BASE:
        return subSpans[lastIndex].endPosition();
      case PER_POSITION:
        return endPosition;
    }
    return Spans.NO_MORE_POSITIONS;
  }

  public void collect(SpanCollector collector) throws IOException {
    switch (type) {
      case TYPE_BASE:
        for (int i = 0; i < subSpans.length; i++) {
          subSpans[i].collect(collector);
        }
        return;
      case TYPE_NO_MORE_SPANS:
        throw new IllegalStateException("spans.collect() called when positioned past end of content for document");
    }
    if (backing != null) {
      switch (comboMode) {
        case FULL_DISTILLED_PER_START_POSITION:
// we already know output.passId == passId
        case PER_POSITION:
        case FULL_DISTILLED_PER_POSITION:
          for (final Node n : backing) {
            n.output++;
            n.collect(collector);
          }
          break;
        default:
          for (final Node n : backing) {
            n.collect(collector);
          }
      }
    }
  }

  @Override
  public int compareTo(SpansEntry o) {
    int ret = Integer.compare(startPosition(), o.startPosition());
    if (ret != 0) {
      return ret;
    }
    ret = Integer.compare(endPosition(), o.endPosition());
    if (ret != 0) {
      return ret;
    }
    return Integer.compare(width, o.width);
  }
    
}
