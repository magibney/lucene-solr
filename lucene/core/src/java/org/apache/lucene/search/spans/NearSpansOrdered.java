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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.spans.SpanNearQuery.ComboMode;
import static org.apache.lucene.search.spans.Spans.NO_MORE_POSITIONS;
import org.apache.lucene.search.spans.TermSpansRepeatBuffer.RepeatTermSpans;
import org.apache.lucene.util.BytesRef;

/**
 * A Spans that is formed from the ordered subspans of a SpanNearQuery
 * where the subspans do not overlap and have a maximum slop between them.
 * <p>
 * The formed spans only contains minimum slop matches.<br>
 * The matching slop is computed from the distance(s) between
 * the non overlapping matching Spans.<br>
 * Successive matches are always formed from the successive Spans
 * of the SpanNearQuery.
 * <p>
 * The formed spans may contain overlaps when the slop is at least 1.
 * For example, when querying using
 * <pre>t1 t2 t3</pre>
 * with slop at least 1, the fragment:
 * <pre>t1 t2 t1 t3 t2 t3</pre>
 * matches twice:
 * <pre>t1 t2 .. t3      </pre>
 * <pre>      t1 .. t2 t3</pre>
 *
 * Expert:
 * Only public for subclassing.  Most implementations should not need this class
 */
public class NearSpansOrdered extends ConjunctionSpans implements IndexLookahead {

  private static boolean assign(int val, boolean ret) {
    return ret;
  }

  private static boolean assign(Object obj, boolean ret) {
    return ret;
  }

  /**
   * marked final for possible performance  optimization
   */
  private static final class TPIWrapper extends TwoPhaseIterator {

    private final TwoPhaseIterator backing;

    public TPIWrapper(TwoPhaseIterator backing, RecordingPushbackSpans toClear) {
      super(new ApproximationWrapper(backing.approximation(), toClear));
      this.backing = backing;
    }

    @Override
    public boolean matches() throws IOException {
      return backing.matches();
    }

    @Override
    public float matchCost() {
      return backing.matchCost();
    }

  }

  /**
   * marked final for possible performance optimization
   */
  private static final class ApproximationWrapper extends DocIdSetIterator {

    private final DocIdSetIterator backing;
    private final RecordingPushbackSpans toClear;

    public ApproximationWrapper(DocIdSetIterator backing, RecordingPushbackSpans toClear) {
      this.backing = backing;
      this.toClear = toClear;
    }

    @Override
    public int docID() {
      return backing.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      toClear.stored.clear(true);
      int ret = backing.nextDoc();
      toClear.clear(true, ret);
      return ret;
    }

    @Override
    public int advance(int target) throws IOException {
      toClear.stored.clear(true);
      int ret = backing.advance(target);
      toClear.clear(true, ret);
      return ret;
    }

    @Override
    public long cost() {
      return backing.cost();
    }

  }

  static final class RecordingPushbackSpans extends Spans {

    private static final boolean DEFAULT_OFFSETS = true;
    private final boolean offsets;
    final int index; // used mostly for debugging
    private final RecordingPushbackSpans nextRepeat;
    private final RecordingPushbackSpans previousVariablePositionLength;
    final RecordingPushbackSpans previous;
    final RecordingPushbackSpans next;
    final Spans backing;
    final IndexLookahead lookaheadBacking;
    private final RepeatTermSpans repeatTermBacking;
    private final boolean checkMatchLimit;
    final boolean variablePositionLength;
    final int allowedSlop;
    private final PositionDeque stored;
    private Iterator<Spans> storedIter;
    private Spans replayStored;
    private Spans replayBacking;
    private Spans active;

    private int minStart;
    private int minEnd;
    private int maxEnd;

    int getMinEnd() {
      return minEnd;
    }

    int getMinStart() {
      return minStart;
    }

    private int matchLimit = Integer.MAX_VALUE;

    private void setMatchLimit(int limit) {
      assert checkMatchLimit : "no check at index=="+index;
      this.matchLimit = limit;
    }

    private void setRepeatTermMatchLimit(int limit) {
      this.repeatTermBacking.setMatchLimit(limit);
    }

    public RecordingPushbackSpans(List<Spans> backing, int allowedSlop, List<List<RecordingPushbackSpans>> repeatGroups, List<RecordingPushbackSpans> noRepeat, 
        boolean combineRepeatGroups, Iterator<TermSpansRepeatBuffer> reuseInput, List<TermSpansRepeatBuffer> reuseOutput, boolean offsets, boolean supportVariableTermSpansLength,
        ComboMode comboMode, Iterator<PositionDeque> reuseDequeInput, List<PositionDeque> reuseDequeOutput) {
      this(null, null, backing.iterator(), allowedSlop, 0, new HashMap<>(backing.size()), repeatGroups, noRepeat, backing.size(), combineRepeatGroups,
          reuseInput, reuseOutput, offsets, supportVariableTermSpansLength, comboMode, reuseDequeInput, reuseDequeOutput);
      Iterator<List<RecordingPushbackSpans>> iter = repeatGroups.iterator();
      while (iter.hasNext()) {
        List<RecordingPushbackSpans> next = iter.next();
        if (next.size() < 2) {
          iter.remove();
          noRepeat.add(next.get(0));
        }
      }
    }

    private static final class RepeatGroupEntry {
      private final ArrayList<RecordingPushbackSpans> instances;
      private ListIterator<RepeatTermSpans> wrappedTermSpansIter;

      public RepeatGroupEntry(ArrayList<RecordingPushbackSpans> instances) {
        this.instances = instances;
      }
      
    }
    
    private RecordingPushbackSpans(RecordingPushbackSpans previous, RecordingPushbackSpans previousVariablePositionLength, Iterator<Spans> backing, int allowedSlop, int index,
        Map<BytesRef, RepeatGroupEntry> repeatGroupsLookup, List<List<RecordingPushbackSpans>> repeatGroups, List<RecordingPushbackSpans> noRepeat, int size, boolean combineRepeatGroups,
        Iterator<TermSpansRepeatBuffer> reuseInput, List<TermSpansRepeatBuffer> reuseOutput, boolean offsets, boolean supportVariableTermSpansLength, ComboMode comboMode,
        Iterator<PositionDeque> reuseDequeInput, List<PositionDeque> reuseDequeOutput) {
      this.index = index;
      this.allowedSlop = allowedSlop;
      this.previousVariablePositionLength = previousVariablePositionLength;
      this.previous = previous;
      this.offsets = offsets;
      Spans tmpBacking = backing.next();
      final int repeatIndex;
      final RepeatGroupEntry instanceEntry;
      final ArrayList<RecordingPushbackSpans> instances;
      final TermSpans ts;
      boolean matchLimitCheck = false;
      if (tmpBacking instanceof TermSpans) {
        this.variablePositionLength = false;
        ts = (TermSpans) tmpBacking;
        BytesRef termBytes = ts.term.bytes();
        if (repeatGroupsLookup.containsKey(termBytes)) {
          instanceEntry = repeatGroupsLookup.get(termBytes);
          instances = instanceEntry.instances;
        } else {
          instances = new ArrayList<>(size - index);
          instanceEntry = new RepeatGroupEntry(instances);
          repeatGroups.add(instances);
          repeatGroupsLookup.put(termBytes, instanceEntry);
        }
        instances.add(this);
        repeatIndex = instances.size();
      } else {
        ts = null;
        instanceEntry = null;
        instances = null;
        repeatIndex = -1;
        this.variablePositionLength = true;
        noRepeat.add(this);
        previousVariablePositionLength = index == 0 ? null : this;
      }
      if (backing.hasNext()) {
        this.next = new RecordingPushbackSpans(this, previousVariablePositionLength, backing, allowedSlop, index + 1, repeatGroupsLookup, repeatGroups, noRepeat, size, combineRepeatGroups, reuseInput, reuseOutput, offsets, supportVariableTermSpansLength, comboMode, reuseDequeInput, reuseDequeOutput);
        this.stored = this.next.stored.prev;
        if (instances == null) {
          this.nextRepeat = null;
          this.repeatTermBacking = null;
          this.backing = tmpBacking;
          this.lookaheadBacking = (tmpBacking instanceof IndexLookahead) ? (IndexLookahead)tmpBacking : null;
        } else {
          int repeatCount = instances.size();
          if (repeatCount == 1) {
            this.nextRepeat = null;
            this.repeatTermBacking = null;
            this.backing = tmpBacking;
            this.lookaheadBacking = (IndexLookahead)tmpBacking; // we know it's a TermSpans
          } else {
            final ListIterator<RepeatTermSpans> iter;
            if (repeatIndex >= repeatCount) {
              this.nextRepeat = null;
              if (!combineRepeatGroups) {
                this.repeatTermBacking = null;
                this.backing = tmpBacking;
                this.lookaheadBacking = (IndexLookahead)tmpBacking; // we know it's a TermSpans
              } else {
                TermSpansRepeatBuffer tsrb = new TermSpansRepeatBuffer(ts, repeatCount, offsets, reuseInput == null ? null : reuseInput.next(), allowedSlop + size);
                reuseOutput.add(tsrb);
                iter = tsrb.getRepeatTermSpans();
                instanceEntry.wrappedTermSpansIter = iter;
                this.repeatTermBacking = iter.previous();
                this.backing = this.repeatTermBacking;
                this.lookaheadBacking = this.repeatTermBacking;
              }
            } else {
              this.nextRepeat = instances.get(repeatIndex);
              if (!combineRepeatGroups) {
                if (repeatIndex == 1) {
                  matchLimitCheck = true;
                }
                this.repeatTermBacking = null;
                this.backing = tmpBacking;
                this.lookaheadBacking = (IndexLookahead)tmpBacking; // we know it's a TermSpans
              } else {
                iter = instanceEntry.wrappedTermSpansIter;
                this.repeatTermBacking = iter.previous();
                this.backing = this.repeatTermBacking;
                this.lookaheadBacking = this.repeatTermBacking;
              }
            }
          }
        }
      } else {
        this.stored = new PositionDeque(size, offsets, reuseDequeInput, reuseDequeOutput, this, supportVariableTermSpansLength, comboMode);
        this.next = null;
        this.nextRepeat = null;
        int repeatCount;
        if (instances == null || !combineRepeatGroups || (repeatCount = instances.size()) == 1) {
          this.repeatTermBacking = null;
          this.backing = tmpBacking;
          this.lookaheadBacking = (tmpBacking instanceof IndexLookahead) ? (IndexLookahead)tmpBacking : null;
        } else {
          TermSpansRepeatBuffer tsrb = new TermSpansRepeatBuffer(ts, repeatCount, offsets, reuseInput == null ? null : reuseInput.next(), allowedSlop + size);
          reuseOutput.add(tsrb);
          final ListIterator<RepeatTermSpans> iter = tsrb.getRepeatTermSpans();
          instanceEntry.wrappedTermSpansIter = iter;
          this.repeatTermBacking = iter.previous();
          this.backing = this.repeatTermBacking;
          this.lookaheadBacking = this.repeatTermBacking;
        }
      }
      this.checkMatchLimit = matchLimitCheck;
      this.stored.init();
    }

    @Override
    public TwoPhaseIterator asTwoPhaseIterator() {
      TwoPhaseIterator backingTPI = backing.asTwoPhaseIterator();
      return backingTPI == null ? null : new TPIWrapper(backingTPI, this);
    }
    
    public void clear(boolean hard, int nextDocId) {
      //stored.clear();
      if (hard) {
        stored.init(nextDocId);
        replayBacking = null;
      }
      if (checkMatchLimit) {
        matchLimit = Integer.MAX_VALUE;
      }
      replayStored = null;
      minStart = backing.startPosition();
      maxEnd = backing.endPosition();
      minEnd = Math.min(maxEnd, minStart + 2);
      active = backing;
      backingRegistered = RegistrationStatus.NONE;
    }
    
    public RecordingPushbackSpans reset(int hardMinStart, int softMinStart) {
      if (stored.isEmpty()) {
        replayBacking = backing;
        storedIter = null;
      } else {
        replayStored = null;
        storedIter = stored.iteratorMinStart(hardMinStart, softMinStart);
      }
      return this;
    }
    public RecordingPushbackSpans reset(int restoreKey, int hardMinStart, int softMinStart) {
      if (stored.isEmpty()) {
        replayBacking = backing;
        storedIter = null;
      } else {
        replayStored = null;
        storedIter = stored.iteratorMinStart(restoreKey, hardMinStart, softMinStart);
      }
      return this;
    }

    public Spans reset(int restoreKey) {
      if (stored.isEmpty()) {
        throw new AssertionError();
      } else {
        replayStored = null;
        storedIter = stored.iterator(restoreKey);
        active = storedIter.next();
        storedIter.hasNext();
        return active;
      }
    }

    private static final class PositionEntry {
      private final int position;
      private final int[] count;
      private PositionEntry(int position, int[] count) {
        this.position = position;
        this.count = count;
      }
    }
    
    private static enum RegistrationStatus { NONE, PROVISIONAL, REGISTERED, STORED };
    
    private RegistrationStatus backingRegistered = RegistrationStatus.NONE;
    
    private int registerProvisionalPositions(int startPosition, int endPosition) {
      backingRegistered = RegistrationStatus.PROVISIONAL;
      if (stored.isEmpty() && backingRegistered != RegistrationStatus.REGISTERED) {
        minStart = startPosition;
        minEnd = Math.min(endPosition, startPosition + 2);
        maxEnd = endPosition;
      }
      return startPosition;
    }

    private int registeredStart;
    private int registeredEnd;

    private int registerStartAndEndPositions(int startPosition, int endPosition) {
      backingRegistered = RegistrationStatus.REGISTERED;
      registeredStart = startPosition;
      registeredEnd = endPosition;
      if (stored.isEmpty()) {
        minStart = startPosition;
        minEnd = endPosition;
        maxEnd = endPosition;
      } else if (endPosition < minEnd) {
        minEnd = endPosition;
      } else if (endPosition > maxEnd) {
        maxEnd = endPosition;
      }
      return startPosition;
    }

    private void storeSpan() throws IOException {
      backingRegistered = RegistrationStatus.STORED;
      int start = backing.startPosition();
      int end = backing.endPosition();
      backing.collect(stored.getCollector(start, end, backing.width()));
    }

    @Override
    public int nextStartPosition() throws IOException {
      throw new UnsupportedOperationException();
    }
    
    private int lookaheadBacking() throws IOException {
      if (lookaheadBacking != null) {
        return lookaheadBacking.lookaheadNextStartPositionFloor();
      } else {
        return UNKNOWN_AT_SPANS;
      }
    }

    private int advanceBacking(int startCeiling) throws IOException {
      assert startCeiling >= 0;
      if (lookaheadBacking != null) {
        final int lookahead = lookaheadBacking.lookaheadNextStartPositionFloor();
        if (lookahead >= startCeiling) {
          return ~lookahead;
        }
      }
      return advanceBacking();
    }

    private int advanceBacking() throws IOException {
      if (!checkMatchLimit || matchLimit-- > 0) {
        this.stored.initProvisional();
        return backing.nextStartPosition();
      } else {
        return NO_MORE_POSITIONS;
      }
    }

    private static final class DescendingListIteratorWrapper<V> implements Iterator<V> {
      private final ListIterator<V> backing;

      public DescendingListIteratorWrapper(ListIterator<V> backing) {
        this.backing = backing;
      }

      @Override
      public boolean hasNext() {
        return backing.hasPrevious();
      }

      @Override
      public V next() {
        return backing.previous();
      }
      
    }
    
    private boolean checkUnregister(int startPosition, int endPosition) {
      switch (backingRegistered) {
        case REGISTERED:
          assert startPosition == registeredStart && endPosition == registeredEnd; // this may not be a valid (nor relevant) assertion
          if (!stored.isEmpty()) {
            minEnd = stored.minEnd();
            maxEnd = stored.maxEnd();
          }
          break;
        case STORED:
          throw new IllegalStateException("checkUnregister should never be called in state STORED");//updatePositionRange(startPosition, endPosition);
      }
      return false;
    }
    
    private void purgeStored(int hardMinStart, int minEnd) {
      if (!stored.isEmpty() && (minStart < hardMinStart || this.minEnd < minEnd)) {
        Iterator<Spans> iter = stored.iterator();
        do {
          Spans next = iter.next();
          if (next.startPosition() < hardMinStart) {
            iter.remove();
          } else if (this.minEnd < minEnd) {
            do {
              if (next.endPosition() < minEnd) {
                iter.remove();
              }
            } while (this.minEnd < minEnd && iter.hasNext() && assign(next = iter.next(), true));
            return;
          }
        } while (iter.hasNext());
      }
      stored.purgeProvisional(hardMinStart);
    }

    /**
     * 
     * @param hardMinStart forever discard spans with start &lt; this parameter
     * @param softMinStart skip (for purpose of returning nextMatch, but do not discard) spans with start &lt; this parameter
     * @param startCeiling disregard (for purpose of returning nextMatch, but do not discard) spans with start &gt;= this parameter
     * @param minEnd when non-negative, defines a minimum threshold for span endPositions. Spans with endPosition &lt; 
     * this value should be discarded forever.
     * @return 
     */
    public int nextMatch(int hardMinStart, int softMinStart, int startCeiling, int minEnd) throws IOException {
      Spans pending = null;
      int pendingStart = -1;
      int pendingEnd;
      if (replayStored != null) {
        pending = replayStored;
        replayStored = null;
        pendingStart = pending.startPosition();
        
        if (pendingStart < hardMinStart || (pendingEnd = pending.endPosition()) < minEnd) {
          // discard forever.
          storedIter.remove();
        } else if (pendingStart >= softMinStart) {
          if (pendingStart >= startCeiling) {
            replayStored = pending;
            //active = null;
            return ~pendingStart; // negative number indicates end
          } else {
            active = pending;
            return pendingStart;
          }
        }
        pending = null;
      }
      if (storedIter != null) {
        while (storedIter.hasNext() && assign(pending = storedIter.next(), true)
            && (pendingStart = pending.startPosition()) < hardMinStart) {
          storedIter.remove();
          pending = null;
        }
        if (pending != null) {
          // we still have candidate(s) from stored
          if (pendingStart < softMinStart) {
            do {
              pending = null; // skip
            } while (storedIter.hasNext() && assign(pending = storedIter.next(), true)
                && (pendingStart = pending.startPosition()) < softMinStart);
          }
          if (pending != null) {
            // return based on this state
            if (pendingStart >= startCeiling) {
              replayStored = pending;
              //active = null;
              return ~pendingStart; // negative number indicates end
            } else {
              active = pending;
              return pendingStart;
            }
          }
        }
        storedIter = null;
        replayBacking = backing;
        int ret = nextMatch(hardMinStart, softMinStart, startCeiling, minEnd);
        return ret;
      }
      purgeStored(hardMinStart, minEnd);
      if (replayBacking != null) {
        pending = replayBacking;
        replayBacking = null;
        pendingStart = checkMatchLimit && matchLimit < 0 ? NO_MORE_POSITIONS : pending.startPosition();
        
        if (pendingStart < hardMinStart || (pendingEnd = pending.endPosition()) < minEnd) {
          // implicitly discard forever
        } else if (pendingStart >= softMinStart) {
          if (pendingStart >= startCeiling) {
            replayBacking = pending;
            //active = null;
            return ~pendingStart; // negative number indicates end
          } else {
            active = pending;
            return registerStartAndEndPositions(pendingStart, pendingEnd);
          }
        }
        pending = null;
      }
      pending = backing;
      final int lookahead = lookaheadBacking();
      if (lookahead >= startCeiling) {
        active = pending;
        return ~lookahead;
      }
      if ((pendingStart = backing.startPosition()) >= hardMinStart || checkUnregister(pendingStart, backing.endPosition())) {
        if ((pendingEnd = backing.endPosition()) >= minEnd || checkUnregister(pendingStart, pendingEnd)) {
          // record for future use, otherwise (implicitly) discard forever
          switch (backingRegistered) {
            case STORED:
              throw new IllegalStateException("should never be STORED here");
            case NONE:
            case PROVISIONAL:
              registerStartAndEndPositions(pendingStart, pendingEnd);
            default:
              storeSpan();
          }
        }
        backingRegistered = RegistrationStatus.NONE;
        pendingStart = advanceBacking(); // guaranteed to also be >= hardMinStart
      } else {
        backingRegistered = RegistrationStatus.NONE;
        if ((pendingStart = advanceBacking()) < hardMinStart) {
          do {
            // discard forever
            if ((pendingStart = advanceBacking(startCeiling)) < 0) {
              active = pending;
              return pendingStart;
            }
          } while (pendingStart < hardMinStart);
        }
      }
      if (pendingStart < softMinStart) {
        do {
          if ((pendingEnd = backing.endPosition()) >= minEnd) {
            // record for future use, otherwise (implicitly) discard forever
            registerStartAndEndPositions(pendingStart, pendingEnd);
            storeSpan();
          }
          backingRegistered = RegistrationStatus.NONE;
          if ((pendingStart = advanceBacking(startCeiling)) < 0) {
            active = pending;
            return pendingStart;
          }
        } while (pendingStart < softMinStart);
      }
      if (pendingStart >= startCeiling) {
        replayBacking = backing;
        if (stored.isEmpty()) {
          active = null;
        } else {
          reset(stored.getLastIndex());
        }
        registerProvisionalPositions(pendingStart, backing.endPosition());
        return ~pendingStart;
      } else if ((pendingEnd = backing.endPosition()) >= minEnd) {
        active = pending;
        return registerStartAndEndPositions(pendingStart, pendingEnd);
      } else {
        backingRegistered = RegistrationStatus.NONE;
        do {
          // discard forever
          if ((pendingStart = advanceBacking(startCeiling)) < 0) {
            active = pending;
            return pendingStart;
          }
        } while (backing.endPosition() < minEnd);
        if (pendingStart >= startCeiling) {
          replayBacking = backing;
          if (stored.isEmpty()) {
            active = null;
          } else {
            reset(stored.getLastIndex());
          }
          registerProvisionalPositions(pendingStart, backing.endPosition());
          return ~pendingStart;
        } else {
          active = pending;
          return registerStartAndEndPositions(pendingStart, pendingEnd);
        }
      }
    }

    @Override
    public int startPosition() {
      return active == null ? -1 : active.startPosition();
    }

    @Override
    public int endPosition() {
      return active == null ? -1 : active.endPosition();
    }

    @Override
    public int width() {
      return active.width();
    }

    @Override
    public void collect(SpanCollector collector) throws IOException {
      active.collect(collector);
    }

    @Override
    public float positionsCost() {
      return backing.positionsCost();
    }

    @Override
    public int docID() {
      return backing.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      stored.clear(true);
      int ret = backing.nextDoc();
      clear(true, ret);
//      if (ret == Spans.NO_MORE_DOCS) {
//        System.err.println("Done["+index+"]: "+stored.size()+": "+stored+", "+stored.printMemoryInfo());
//      }
      return ret;
    }

    @Override
    public int advance(int target) throws IOException {
      stored.clear(true);
      int ret = backing.advance(target);
      clear(true, ret);
//      if (ret == Spans.NO_MORE_DOCS) {
//        System.err.println("Done["+index+"]: "+stored.size()+": "+stored+", "+stored.printMemoryInfo());
//      }
      return ret;
    }

    @Override
    public long cost() {
      return backing.cost();
    }
    
  }

  private static RecordingPushbackSpans[] wrapSpans(List<Spans> subSpans, int allowedSlop, List<List<RecordingPushbackSpans>> repeatGroups, List<RecordingPushbackSpans> noRepeat,
      boolean combineRepeatGroups, Iterator<TermSpansRepeatBuffer> reuseInput, List<TermSpansRepeatBuffer> reuseOutput, boolean offsets, boolean supportVariableTermSpansLength,
      ComboMode comboMode, Iterator<PositionDeque> reuseDequeInput, List<PositionDeque> reuseDequeOutput) {
    RecordingPushbackSpans[] ret = new RecordingPushbackSpans[subSpans.size()];
    RecordingPushbackSpans next = new RecordingPushbackSpans(subSpans, allowedSlop, repeatGroups, noRepeat,
        combineRepeatGroups, reuseInput, reuseOutput, offsets, supportVariableTermSpansLength, comboMode, reuseDequeInput, reuseDequeOutput);
    int i = 0;
    do {
      ret[i++] = next;
    } while ((next = next.next) != null);
    return ret;
  }
  
  private final RecordingPushbackSpans[] resettableSpans;
  private final IndexLookahead[] lookaheadSpans;
  private final int lastIndex;
  private final int allowedSlop;
  private SpansEntry spansHead = null;

  private static final SpansEntry NO_MORE_POSITIONS_ENTRY = new NoMorePositionsSpansEntry();
  
  private final ComboMode requestedComboMode;
  private ComboMode comboMode;
  private final boolean combineRepeatGroups;
  private final boolean requestedAllowOverlap;
  private boolean allowOverlap;
  private int lastEnd;
  
  private final RecordingPushbackSpans[][] repeatGroups;
  private final RecordingPushbackSpans[] noRepeat;
  private boolean initializedRepeatGroups;

  public NearSpansOrdered(int allowedSlop, List<Spans> subSpans, ComboMode comboMode, int comboThreshold, boolean allowOverlap,
      boolean combineRepeatSpans, Iterator<TermSpansRepeatBuffer> reuseInput, List<TermSpansRepeatBuffer> reuseOutput, boolean offsets, boolean supportVariableTermSpansLength,
      Iterator<PositionDeque> reuseDequeInput, List<PositionDeque> reuseDequeOutput, List<Spans> shingles) throws IOException {
    this(allowedSlop, subSpans, comboMode, comboThreshold, allowOverlap, new LinkedList<>(), new LinkedList<>(),
        combineRepeatSpans, reuseInput, reuseOutput, offsets, supportVariableTermSpansLength, reuseDequeInput, reuseDequeOutput, shingles);
  }

  private NearSpansOrdered(int allowedSlop, List<Spans> subSpans, ComboMode comboMode, int comboThreshold, boolean allowOverlap, List<List<RecordingPushbackSpans>> repeatGroups, List<RecordingPushbackSpans> noRepeat,
      boolean combineRepeatSpans, Iterator<TermSpansRepeatBuffer> reuseInput, List<TermSpansRepeatBuffer> reuseOutput, boolean offsets, boolean supportVariableTermSpansLength,
      Iterator<PositionDeque> reuseDequeInput, List<PositionDeque> reuseDequeOutput, List<Spans> shingles) throws IOException {
    this(allowedSlop, wrapSpans(subSpans, allowedSlop, repeatGroups, noRepeat, combineRepeatSpans, reuseInput, reuseOutput, offsets, supportVariableTermSpansLength, comboMode,
        reuseDequeInput, reuseDequeOutput), comboMode, comboThreshold, allowOverlap, repeatGroups, noRepeat,
        combineRepeatSpans, supportVariableTermSpansLength, shingles);
  }

  private static final Comparator<RecordingPushbackSpans[]> ARRAY_SIZE_COMPARATOR = new Comparator<RecordingPushbackSpans[]>() {

    @Override
    public int compare(RecordingPushbackSpans[] o1, RecordingPushbackSpans[] o2) {
      return Integer.compare(o2.length, o1.length);
    }
  };

  private NearSpansOrdered(int allowedSlop, RecordingPushbackSpans[] resettableSpans, ComboMode comboMode, int comboThreshold, boolean allowOverlap, List<List<RecordingPushbackSpans>> repeatGroups, List<RecordingPushbackSpans> noRepeat,
      boolean combineRepeatGroups, boolean supportVariableTermSpansLength, List<Spans> shingles) throws IOException {
    super(Arrays.asList(resettableSpans), shingles);
    if (repeatGroups.isEmpty()) {
      this.initializedRepeatGroups = true;
      this.repeatGroups = null;
    } else {
      this.initializedRepeatGroups = false;
      this.repeatGroups = new RecordingPushbackSpans[repeatGroups.size()][];
      int i = 0;
      Iterator<List<RecordingPushbackSpans>> iter = repeatGroups.iterator();
      do {
        List<RecordingPushbackSpans> next = iter.next();
        this.repeatGroups[i] = next.toArray(new RecordingPushbackSpans[next.size()]);
      } while (iter.hasNext() && assign(i++, true));
      Arrays.sort(this.repeatGroups, ARRAY_SIZE_COMPARATOR);
    }
    this.noRepeat = noRepeat.isEmpty() ? null : noRepeat.toArray(new RecordingPushbackSpans[noRepeat.size()]);
    this.resettableSpans = resettableSpans;
    this.lastIndex = this.resettableSpans.length - 1;
    RecordingPushbackSpans lastSpans;
    if (comboMode != ComboMode.GREEDY_END_POSITION && allowedSlop == 0 && !supportVariableTermSpansLength
        && (lastSpans = resettableSpans[lastIndex]).previousVariablePositionLength == null
        && !lastSpans.variablePositionLength
        && !resettableSpans[0].variablePositionLength) {
      // with slop==0 and all fixed positionLength-per-startPosition spans, we lose no accuracy, but
      // gain significantly in speed, by performing greedy matching.
      // this assumes that for a given term/startPosition, the endPosition will not change.
      // n.b. that this still works even allowing the same term to have different positionLengths at *different* startPositions
      comboMode = ComboMode.GREEDY_END_POSITION;
    }
    this.requestedComboMode = comboMode;
    this.comboMode = comboMode;
    this.requestedAllowOverlap = allowOverlap;
    this.allowOverlap = allowOverlap;
    this.allowedSlop = allowedSlop;
    this.combineRepeatGroups = combineRepeatGroups;
    final IndexLookahead[] builder = new IndexLookahead[this.resettableSpans.length];
    for (int i = 0; i < this.resettableSpans.length; i++) {
      if ((builder[i] = this.resettableSpans[i].lookaheadBacking) == null) {
        this.lookaheadSpans = null;
        return;
      }
    }
    this.lookaheadSpans = builder;
  }

  private static final boolean PER_DOC_COMBO_MODE_RESET = true;

  @Override
  public int lookaheadNextStartPositionFloor() throws IOException {
    if (atFirstInCurrentDoc) {
      return startPosition();
    } else if (spansIter != null && spansIter.hasNext()) {
      // all vals of the same iter instance have the same startPosition
      return spansHead.startPosition();
    } else {
      return resettableSpans[0].lookaheadBacking();
    }
  }

  private int positionLengthCeiling = UNINITIALIZED_AT_DOC;

  @Override
  public int positionLengthCeiling() {
    if (lookaheadSpans == null) {
      return UNKNOWN_AT_SPANS;
    } else if (positionLengthCeiling != UNINITIALIZED_AT_DOC) {
      return positionLengthCeiling;
    } else {
      int sumMaxPositionLengths = allowedSlop;
      int endPositionMayDecrease = allowedSlop;
      for (int i = lookaheadSpans.length - 1; i >= 0; i--) {
        final int subclausePositionLengthCeiling = lookaheadSpans[i].positionLengthCeiling();
        if (subclausePositionLengthCeiling <= MAX_SPECIAL_VALUE) {
          return positionLengthCeiling = UNKNOWN_AT_DOC;
        }
        final int addMaxPositionLength;
        if (subclausePositionLengthCeiling < 0) {
          addMaxPositionLength = ~subclausePositionLengthCeiling;
          if (endPositionMayDecrease < 2) {
            endPositionMayDecrease += (addMaxPositionLength - 1);
          }
        } else {
          addMaxPositionLength = subclausePositionLengthCeiling;
        }
        sumMaxPositionLengths += addMaxPositionLength;
      }
      return positionLengthCeiling = endPositionMayDecrease >= 2 ? ~sumMaxPositionLengths : sumMaxPositionLengths;
    }
  }

  private void reset() {
    spansIter = null;
    spansHead = null;
    initializedRepeatGroups = repeatGroups == null;
    lastEnd = 0;
    atFirstInCurrentDoc = false;
    if (positionLengthCeiling != UNKNOWN_AT_SPANS) {
      positionLengthCeiling = UNINITIALIZED_AT_DOC;
    }
    if (PER_DOC_COMBO_MODE_RESET) {
      allowOverlap = requestedAllowOverlap;
      comboMode = requestedComboMode;
    } else if (comboMode == null) {
      // only (partially) reset if we've fully shortcircuited processing of previous doc
      comboMode = ComboMode.GREEDY_END_POSITION;
    }
  }
  
  @Override
  boolean twoPhaseCurrentDocMatches() throws IOException {
    assert unpositioned();
    reset();
    if (nextStartPosition() != NO_MORE_POSITIONS) {
      return atFirstInCurrentDoc = true;
    }
    return false;
  }

  /**
   * Advances subSpans to next complete match group, combines and orders results, first result goes in spansHead, 
   * subsequent results in spansIter
   * @return 
   */
  private int initNextSpansGroup() throws IOException {
    if (!initializedRepeatGroups) {
      for (final RecordingPushbackSpans[] repeatGroup : repeatGroups) {
        final RecordingPushbackSpans rps = repeatGroup[0];
        int freq = combineRepeatGroups ? ((RepeatTermSpans)rps.backing).backingSpans.freq : ((TermSpans)rps.backing).freq;
        if (freq < repeatGroup.length) {
          spansHead = NO_MORE_POSITIONS_ENTRY;
          return NO_MORE_POSITIONS;
        }
        final int firstPhraseInstanceMatchLimit = freq - repeatGroup.length + 1;
        if (combineRepeatGroups) {
          rps.setRepeatTermMatchLimit(firstPhraseInstanceMatchLimit);
        } else {
          rps.setMatchLimit(firstPhraseInstanceMatchLimit);
        }
      }
      initializedRepeatGroups = true;
    }
    RecordingPushbackSpans first = resettableSpans[0];
    PositionDeque.DLLReturnNode blah;
    int startPosition = first.startPosition();
    do {
      int minStart = allowOverlap ? startPosition + 1 : lastEnd;
      if ((startPosition = first.nextMatch(minStart, minStart, Integer.MAX_VALUE, -1)) < 0) {
        spansHead = NO_MORE_POSITIONS_ENTRY;
        return NO_MORE_POSITIONS;
      }
      blah = resettableSpans[0].stored.buildLattice(startPosition, allowedSlop, comboMode);
    } while (blah.isEmpty());
    startPosition = blah.getCurrentStart();
//    PositionDeque1.DLLReturnNode drn;
//    if (((drn = blah.next) != null)) {
//      System.err.println("start endSpanPosition "+aslfdkj);
//      do {
//        Spans s = drn.node;
//        System.err.println("got endSpanPosition: " + s.startPosition() + "=>" + s.endPosition());
//        //PositionDeque1.printSpans("", s);
//      } while ((drn = drn.next) != null);
//    } else {
//      System.err.println("got no endSpanPositions for start "+aslfdkj+", docId="+docID());
//    }
    switch (comboMode) {
      case PER_END_POSITION:
        spansIter = resettableSpans[0].stored.perEndPosition(blah);
        break;
      case PER_POSITION:
      case PER_POSITION_PER_START_POSITION:
      case FULL_DISTILLED_PER_POSITION:
      case FULL_DISTILLED_PER_START_POSITION:
      case FULL_DISTILLED:
      case GREEDY_END_POSITION:
        spansIter = resettableSpans[0].stored.perPosition(blah, allowedSlop << 1, startPosition, comboMode);
        break;
      case FULL:
        spansIter = resettableSpans[0].stored.fullPositions(blah);
        break;
      //case GREEDY_END_POSITION:
      default:
        throw new UnsupportedOperationException("for comboMode "+comboMode);
    }
    if (!spansIter.hasNext()) {
      throw new AssertionError();
    } else {
      spansHead = spansIter.next();
      if (!allowOverlap) {
        lastEnd = spansHead.endPosition();
        spansIter = null;
      } else if (comboMode == ComboMode.GREEDY_END_POSITION) {
        spansIter = null;
      }
      return spansHead.startPosition();
    }
  }

  static abstract class SpansEntry implements Comparable<SpansEntry> {

    protected int width;

    SpansEntry init(int width) {
      this.width = width;
      return this;
    }

    public abstract int startPosition();

    public abstract int endPosition();

    public abstract void collect(SpanCollector collector) throws IOException;

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

  static class SpansEntryBase extends SpansEntry {

    protected final Spans[] subSpans;
    protected final int lastIndex;

    public SpansEntryBase(Spans[] subSpans, int lastIndex) {
      this.subSpans = subSpans;
      this.lastIndex = lastIndex;
    }

    public SpansEntryBase init(int width) {
      super.init(width);
      return this;
    }
    @Override
    public int startPosition() {
      return subSpans[0].startPosition();
    }

    @Override
    public int endPosition() {
      try {
        return subSpans[lastIndex].endPosition();
      } catch (NullPointerException ex) {
        throw new NullPointerException("for "+subSpans);
      }
    }
    @Override
    public void collect(SpanCollector collector) throws IOException {
      for (int i = 0; i < subSpans.length; i++) {
        subSpans[i].collect(collector);
      }
    }
  }

  private static final class NoMorePositionsSpansEntry extends SpansEntry {

    @Override
    public int startPosition() {
      return NO_MORE_POSITIONS;
    }

    @Override
    public int endPosition() {
      return NO_MORE_POSITIONS;
    }

    @Override
    public void collect(SpanCollector collector) throws IOException {
      throw new IllegalStateException("spans.collect() called when positioned past end of content for document");
    }

  }

  private Iterator<SpansEntry> spansIter = null;
  
  @Override
  public int nextStartPosition() throws IOException {
    if (atFirstInCurrentDoc) {
      atFirstInCurrentDoc = false;
      return startPosition();
    }
    if (spansIter != null && spansIter.hasNext()) {
      spansHead = spansIter.next();
      lastEnd = spansHead.endPosition();
      return spansHead.startPosition();
    } else {
      int nextStartPosition;
      if ((nextStartPosition = initNextSpansGroup()) >= 0) {
        return nextStartPosition;
      } else {
        // no subsequent positions from previous unbuffered output
        return initNextSpansGroup();
      }
    }
  }

  private boolean unpositioned() {
    for (final Spans span : subSpans) {
      if (span.startPosition() != -1)
        return false;
    }
    return true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    Class<? extends Spans> clazz = getClass();
    sb.append(clazz.isAnonymousClass() ? clazz.getName() : clazz.getSimpleName());
    sb.append("(doc=").append(docID());
    sb.append(",subSpans=[");
    for (int i = 0; i < subSpans.length; i++) {
      sb.append(subSpans[i]).append(", ");
    }
    sb.setLength(sb.length() - 2);
    sb.append("])");
    return sb.toString();
  }

  @Override
  public int startPosition() {
    return atFirstInCurrentDoc ? -1 : spansHead.startPosition();
  }

  @Override
  public int endPosition() {
    return atFirstInCurrentDoc ? -1 : spansHead.endPosition();
  }

  @Override
  public int width() {
    return spansHead.width;
  }

  @Override
  public void collect(SpanCollector collector) throws IOException {
    spansHead.collect(collector);
  }

}

