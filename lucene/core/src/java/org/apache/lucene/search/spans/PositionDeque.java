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

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectScatterMap;
import com.carrotsearch.hppc.LongObjectHashMap;
import com.carrotsearch.hppc.LongObjectScatterMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.spans.NearSpansOrdered.RecordingPushbackSpans;
import org.apache.lucene.search.spans.SpanNearQuery.ComboMode;

/**
 * This class implements a backtracking-capable wrapper for advancing through a backing Spans. In its most basic use,
 * it allows to store and replay cursor states of its backing Spans. This class is basically a queue implemented as a
 * circular buffer over a resizable backing array, with support for efficient random-access seek, iteration, and
 * arbitrary element removal, and with stable int offset references (with respect to backing array, mod capacity of
 * backing array).
 * 
 * This class also encapsulates much of the logic for *using* the queue, because each Node in the queue represents
 * the state of the backing Spans at a given cursor position, but also potentially represents a component of valid
 * phrase path(s) across different Spans/phrase-positions. The nodes are really nodes in a two-dimensional queue, with
 * node removal driven either with respect to the Spans-aligned or Spans-transverse dimensions. This behavior justifies
 * the special-purpose data structure, and explains why the data structure is relatively tightly coupled with logic for
 * its own construction and traversal.
 * 
 * Having a linked data structure like this in the hot path of user query execution results in the creation of a
 * significant amount of objects (Nodes and linking nodes to link them to each other). To avoid excessive GC, this
 * implementation by default pools these objects per-Queue, and recycles them using an efficient queue-based per-Node
 * reuse strategy. In case of bugs, object pooling can be statically disabled using the POOL_NODES boolean class
 * variable. This disables only the actual reuse of objects, but the overhead of maintaining dead-end reuse queues is
 * quite low, and should have minimal impact on performance.
 */
public class PositionDeque implements Iterable<Spans> {

  private static final int DEFAULT_CAPACITY = 16; // power of 2
  private static final int MIN_CAPACITY = 2; // power of 2

  private int capacity;
  private int indexMask;

  private final PositionDeque[] dequePointer;
  private final RecordingPushbackSpans driver;
  private final DLLReturnNode returned;
  private final RevisitNode revisit;
  private final ComboMode comboMode;
  private final boolean greedyReturn;
  private final int allowedSlop;
  final PositionDeque prev;
  private final PositionDeque next;
  private final PositionDeque last;
  final int phraseIndex;
  private final int phraseSize;
  private final boolean offsets;
  private Node[] nodeBuffer;
  private int tail = 0;
  private int head = 0;
  private boolean uninitializedForDoc = true;

  void initializeForDoc() {
    uninitializedForDoc = false;
  }
  
  // allows provisional nodes to resuse same offset id/index, while preserving a unique phraseScopeId.
  private final long repeatProvisionalIncrement;

  public PositionDeque(int size, boolean offsets, Iterator<PositionDeque> reuseInput, List<PositionDeque> reuseOutput, RecordingPushbackSpans driver, boolean supporVariableTermSpansLength, ComboMode comboMode) {
    this(size, size - 1, driver, null, null, offsets, reuseInput, reuseOutput, DEFAULT_CAPACITY, supporVariableTermSpansLength, comboMode);
  }

  private PositionDeque(int size, int phraseIndex, RecordingPushbackSpans driver, PositionDeque next, PositionDeque last, boolean offsets, Iterator<PositionDeque> reuseInput, List<PositionDeque> reuseOutput, int capacityHint, boolean supportVariableTermSpansLength, ComboMode comboMode) {
    this.supportVariableTermSpansLength = supportVariableTermSpansLength;
    this.phraseIndex = phraseIndex;
    this.phraseSize = size;
    this.driver = driver;
    this.comboMode = comboMode;
    this.greedyReturn = ENABLE_GREEDY_RETURN && comboMode == ComboMode.GREEDY_END_POSITION;
    this.allowedSlop = driver.allowedSlop;
    this.next = next;
    this.offsets = offsets;
    if (reuseInput != null) {
      reuseOutput.add(this);
      PositionDeque reuse = reuseInput.next();
      this.capacity = reuse.capacity;
      indexMask = this.capacity - 1;
      this.nodeBuffer = reuse.nodeBuffer;

      // the following references preserve the node pool across segments
      reuse.clear(true, true);
      reuse.returned.clear();
      this.dequePointer = reuse.dequePointer;
      this.pool = reuse.pool;
      this.nodePoolHead = reuse.nodePoolHead;
      this.nodePoolTail = reuse.nodePoolTail;
      enqueueForPoolReturn(reuse.provisional);
      returnNodesToPool();
      this.returned = reuse.returned;
    } else {
      if (reuseOutput != null) {
        reuseOutput.add(this);
      }
      this.pool = new LinkPool();
      this.capacity = Math.max(MIN_CAPACITY, Integer.highestOneBit(capacityHint - 1) << 1); // power of 2
      indexMask = this.capacity - 1;
      this.nodeBuffer = new Node[capacity];
      this.returned = new DLLReturnNode();
      this.dequePointer = new PositionDeque[1];
    }
    this.dequePointer[0] = this;
    if (--phraseIndex >= 0) {
      if (next == null) {
        this.last = this;
      } else {
        this.last = last;
      }
      this.revisit = new RevisitNode();
      this.prev = new PositionDeque(size, phraseIndex, driver.previous, this, this.last, offsets, reuseInput, reuseOutput, capacityHint, supportVariableTermSpansLength, comboMode);
      this.snStack = null;
      this.blStack = null;
      this.dbStack = null;
      this.perEndPositionIter = null;
      this.fullPositionsIter = null;
      this.perPositionIter = null;
    } else {
      this.revisit = null;
      this.last = last == null ? this : last;
      this.prev = null;
      this.snStack = new SLLNode[size];
      this.blStack = new StackFrame[size];
      this.dbStack = new DeleteBacklinksStackFrame[size];
      PositionDeque d = this;
      for (int i = 0; i < size; i++) {
        this.dbStack[i] = new DeleteBacklinksStackFrame();
        this.blStack[i] = new StackFrame(d, greedyReturn);
        d = d.next;
      }
      switch (comboMode) {
        case PER_END_POSITION:
          this.perEndPositionIter = new PerEndPositionIter(size, allowedSlop, root, last.returned.anchor);
          this.fullPositionsIter = null;
          this.perPositionIter = null;
          break;
        case PER_POSITION:
        case PER_POSITION_PER_START_POSITION:
        case FULL_DISTILLED_PER_POSITION:
        case FULL_DISTILLED_PER_START_POSITION:
        case FULL_DISTILLED:
          //case GREEDY_END_POSITION:
          this.perEndPositionIter = null;
          this.fullPositionsIter = null;
          this.perPositionIter = new PerPositionIter(allowedSlop, comboMode);
          break;
        case FULL:
          this.perEndPositionIter = null;
          this.fullPositionsIter = new FullPositionsIter(size, allowedSlop, last.returned.anchor);
          this.perPositionIter = null;
          break;
        case GREEDY_END_POSITION:
          this.perEndPositionIter = null;
          this.fullPositionsIter = null;
          this.perPositionIter = null;
          break;
        default:
          throw new UnsupportedOperationException("for comboMode " + comboMode);
      }
    }
    this.repeatProvisionalIncrement = (long)Integer.highestOneBit(size) << (Integer.SIZE + 1);
  }
  
  void init() {
    provisional = getPooledNode(driver.backing);
  }

  void initProvisional(boolean newDoc) {
    if (initProvisional) {
      if (greedyReturn || newDoc) {
        provisional.clear();
        provisional.initProvisionalGreedy(driver.backing);
      } else {
        if (provisional.validity == PROVISIONAL) {
          if (provisional.initialzedReverseLinks != null) {
            provisional.validity = 0;
          } else {
            provisional.trackNextRefProvisional = null;
            if (provisional.startPosition < 0) {
              // we're not actually saving it -- that would have been handled in storeSpan() --
              // we're just signaling that it should no longer refer to backing spans,
              // and more importantly indicating when it's safe to purge from the purge queue!
              provisional.startPosition = provisional.backing.startPosition();
            }
            if (provisionalPurgeQueueTail == null) {
              provisionalPurgeQueueHead = provisional;
              provisionalPurgeQueueTail = provisional;
            } else {
              provisionalPurgeQueueTail.trackNextRefProvisional = provisional;
              provisionalPurgeQueueTail = provisional;
            }
          }
        }
        provisional = getPooledNode(driver.backing);
      }
      initProvisional = false;
    }
  }

  private Node provisionalPurgeQueueHead;
  private Node provisionalPurgeQueueTail;

  void purgeProvisional(int hardMinStart) {
    if (provisionalPurgeQueueHead != null) {
      while (provisionalPurgeQueueHead.startPosition < hardMinStart &&
          enqueueForPoolReturn(provisionalPurgeQueueHead)) {
        if ((provisionalPurgeQueueHead = provisionalPurgeQueueHead.trackNextRefProvisional) == null) {
          provisionalPurgeQueueTail = null;
          break;
        }
        // continue purging;
      }
    }
  }

  private void purgeProvisional() {
    int i = 0;
    if (provisionalPurgeQueueHead != null) {
      do {
        enqueueForPoolReturn(provisionalPurgeQueueHead);
      } while ((provisionalPurgeQueueHead = provisionalPurgeQueueHead.trackNextRefProvisional) != null);
      provisionalPurgeQueueTail = null;
    }
  }

  private int docId = -1;
  
  public int getLastIndex() {
    return (iterNode != null && iterNode.prev != null) ? iterNode.prev.index : headNode.index;
  }
  
  int lookaheadNextStartPosition() {
    final Node lookaheadStored;
    if (iterNode == null) {
      return -2;
    } else if ((lookaheadStored = iterNode.next) == null) {
      return -1;
    } else {
      return lookaheadStored.startPosition;
    }
  }

  public Node getLastNode() {
    final Node ret;
    if (iterNode != null) {
      ret = iterNode;
    } else {
      ret = provisional;
      initProvisional = true; // released a reference; must be stable
    }
    return ret;
  }
  
  public boolean isEmpty() {
    return head == tail;
  }
  
  public int size() {
    return head - tail;
  }

  static final boolean TRACK_POOLING = false;

  public String printMemoryInfo() {
    StringBuilder sb = new StringBuilder();
    sb.append("created ").append(createNodeCount).append(" nodes; reused ").append(reusedNodeCount).append(" times");
    if (dllEndNodeLinkCtR != 0 || dllEndNodeLinkCt != 0) {
      sb.append(", dllEndNodeLink=").append(dllEndNodeLinkCt).append("/").append(dllEndNodeLinkCtR);
    }
    if (fullEndNodeLinkCtR != 0 || fullEndNodeLinkCt != 0) {
      sb.append(", fullEndNodeLink=").append(fullEndNodeLinkCt).append("/").append(fullEndNodeLinkCtR);
    }
    if (dllNodeCtR != 0 || dllNodeCt != 0) {
      sb.append(", dllNode=").append(dllNodeCt).append("/").append(dllNodeCtR);
    }
    if (sllNodeCtR != 0 || sllNodeCt != 0) {
      sb.append(", sllNode=").append(sllNodeCt).append("/").append(sllNodeCtR);
    }
    if (dllReturnNodeCtR != 0 || dllReturnNodeCt != 0) {
      sb.append(", dllReturnNode=").append(dllReturnNodeCt).append("/").append(dllReturnNodeCtR);
    }
    if (revisitNodeCtR != 0 || revisitNodeCt != 0) {
      sb.append(", revisitNode=").append(revisitNodeCt).append("/").append(revisitNodeCtR);
    }
    if (storedPostingsCtR != 0 || storedPostingsCt != 0) {
      sb.append(", storedPostings=").append(storedPostingsCt).append("/").append(storedPostingsCtR);
    }
    return sb.toString();
  }

  public SpanCollector getCollector(int startPosition, int endPosition, int width) {
    SpanCollector ret = collector.init(provisional, startPosition, endPosition, width);
    this.provisional = getPooledNode(driver.backing);
    this.initProvisional = false;
    return ret;
  }
  
  private boolean active = true;
  boolean isActive() {
    return active && driver.docID() != Spans.NO_MORE_DOCS;
  }

  void release() {
    active = false;
  }

  static final class LocalNodeArrayList extends LocalArrayList<Node> {

    LocalNodeArrayList trackNextRef;

    public LocalNodeArrayList(int initialCapacityHint) {
      super(initialCapacityHint);
    }

    @Override
    protected Node[] newArray(int arrayCapacity) {
      return new Node[arrayCapacity];
    }

  }

  private static final class LocalDRNArrayList extends LocalArrayList<DLLReturnNode> {

    public LocalDRNArrayList(int initialCapacityHint) {
      super(initialCapacityHint);
    }

    @Override
    protected DLLReturnNode[] newArray(int arrayCapacity) {
      return new DLLReturnNode[arrayCapacity];
    }

  }

  private static abstract class LocalArrayList<E> implements Iterator<E>, Collection<E> {

    private int iterIdx = -1;
    protected E[] data;
    private int capacity;
    protected int size = 0;

    void sort(Comparator<? super E> comparator) {
      Arrays.sort(data, 0, size, comparator);
    }

    protected LocalArrayList(int initialCapacityHint) {
      this.capacity = Math.max(PositionDeque.MIN_CAPACITY, Integer.highestOneBit(initialCapacityHint - 1) << 1);
      this.data = newArray(capacity);
    }

    public void reset(int requiredCapacity) {
      if (requiredCapacity > capacity) {
        final int newCapacity = Integer.highestOneBit(requiredCapacity - 1) << 1;
        data = newArray(newCapacity);
      }
      size = 0;
    }

    protected abstract E[] newArray(int arrayCapacity);

    @Override
    public int size() {
      return size;
    }

    @Override
    public boolean isEmpty() {
      return size == 0;
    }

    @Override
    public boolean contains(Object o) {
      throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public Iterator<E> iterator() {
      iterIdx = size - 1;
      return this;
    }

    @Override
    public Object[] toArray() {
      throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public <T> T[] toArray(T[] a) {
      throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public boolean add(E e) {
      if (size == capacity) {
        E[] nextData = newArray(capacity <<= 1);
        System.arraycopy(data, 0, nextData, 0, size);
        data = nextData;
      }
      data[size++] = e;
      return true;
    }

    @Override
    public boolean remove(Object o) {
      throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public boolean containsAll(Collection<?> c) {
      throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
      throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public void clear() {
      size = 0;
    }

    @Override
    public boolean hasNext() {
      return iterIdx >= 0;
    }

    @Override
    public E next() {
      return data[iterIdx--];
    }

  }

  private boolean initProvisional = false;
  private Node provisional;
  private final PositionDequeCollector collector = new PositionDequeCollector(this);
  
  private static final class PositionDequeCollector implements SpanCollector {

    private final PositionDeque deque;
    private Node n;

    public PositionDequeCollector(PositionDeque deque) {
      this.deque = deque;
    }

    public PositionDequeCollector init(Node n, int startPosition, int endPosition, int width) {
      deque.add(n, startPosition, endPosition, width);
      this.n = n;
      return this;
    }

    @Override
    public void collectLeaf(PostingsEnum postings, int position, Term term) throws IOException {
      n.collectLeaf(postings, position, term);
    }

    @Override
    public void reset() {
      // do nothing?
    }
  }

  public void init(int docId) {
    returned.clear();
    this.docId = docId;
  }
  
  @Override
  public PositionDequeIterator iterator() {
    iterator.resetIterator(tailNode, true);
    return iterator;
  }

  public PositionDequeIterator descendingIterator() {
    iterator.resetIterator(headNode, false);
    return iterator;
  }

  public PositionDequeIterator iterator(int startKey) {
    Node n = validateStartKeyWithRangeCheck(startKey);
    iterator.resetIterator(n, true);
    return iterator;
  }

  public PositionDequeIterator iteratorMinStart(int startKey, int hardMinStart, int softMinStart) {
    return iteratorInternal(startKey, hardMinStart, softMinStart, true);
  }
  
  private Node validateStartKeyWithRangeCheck(int startKey) {
    if (startKey - tail < 0) {
      startKey = tail;
    } else if (head - startKey < 0) {
      startKey = head;
    }
    return validateStartKey(startKey);
  }
  
  private Node validateStartKey(int startKey) {
    Node n = nodeBuffer[startKey & indexMask];
    if (n != null) {
      while ((n.validity & (VALID | VALID_MAXQ | VALID_MINQ)) == 0 && (n = n.next) != null) {
        // advance until active or head is reached
      }
    }
    return n;
  }

  public PositionDequeIterator iteratorMinStart(int hardMinStart, int softMinStart) {
    return iteratorInternal(tail, hardMinStart, softMinStart, true);
  }

  /**
   * 
   * @param hardMinStart remove all nodes with start &lt; this param
   * @return true if deque is cleared as a result of this operation, otherwise false
   */
  private boolean truncate(int hardMinStart) {
    int idx = binarySearch(nodeBuffer, tail, head, hardMinStart, indexMask);
    Node newTail;
    if (idx == head || (newTail = validateStartKey(idx)) == null) {
      clear(true, false);
      return true;
    } else {
      newTail.truncatePreceding();
      return false;
    }
  }
  
  public PositionDequeIterator iteratorInternal(int fromIdx, int hardMinStart, int softMinStart, boolean ascending) {
    Node n;
    if (tailNode.startPosition < hardMinStart) {
      if (truncate(hardMinStart)) {
        iterator.resetIterator(null, ascending);
        return iterator;
      }
    }
    if (tailNode.startPosition >= softMinStart) {
      //System.err.println("noskip");
      n = tailNode;
    } else {
      int startIdx = binarySearch(nodeBuffer, fromIdx, head, softMinStart, indexMask);
      n = startIdx == head ? null : validateStartKey(startIdx);
      int skippedCt = 0;
      if (n != null) {
        Node tmp = n;
        while ((tmp = tmp.prev) != null) {
          skippedCt++;
        }
        
      }
      //System.err.println("skippedCount=" + skippedCt);
      if (skippedCt == 0) {
        //System.err.println("return "+n+", "+tailNode.startPosition+", minStart="+softMinStart);
      }
    }
    iterator.resetIterator(n, ascending);
    return iterator;
  }

  private static int binarySearch(Node[] a, int fromIndex, int toIndex, int key, int indexMask) {
    int low = fromIndex;
    int high = toIndex - 1;

    while (high - low >= 0) {
      int mid = ((low >>> 1) + (high >>> 1)) + (low & high & 1);
      int midVal = a[mid & indexMask].startPosition;

      if (midVal < key) {
        low = mid + 1;
      } else if (midVal > key) {
        high = mid - 1;
      } else {
        // key found
        if (mid == low) {
          return mid;
        } else {
          while (--mid - low >= 0 && a[mid & indexMask].startPosition == key) {
            // keep backing up
          }
          return mid + 1;
        }
      }
    }
    return low;  // key not found.
  }

  private final PositionDequeIterator iterator = new PositionDequeIterator();

  private Node iterNode = null;

  public boolean firstNodeActive() {
    return iterNode != null && iterNode == tailNode;
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (head - tail == 0) {
      return "[]";
    } else {
      sb.append("[");
      Node n = tailNode;
      do {
        sb.append(n.startPosition);
      } while ((n = n.next) != null && sb.append(", ") != null);
      return sb.append(']').toString();
    }
  }

  final class PositionDequeIterator implements Iterator<Spans> {

    private final Node initNode = new Node();
    private boolean ascending;

    private void resetIterator(Node n, boolean ascending) {
      if (n == null) {
        iterNode = null;
      } else {
        iterNode = initNode;
        if (this.ascending = ascending) {
          initNode.next = n;
        } else {
          initNode.prev = n;
        }
      }
    }

    @Override
    public boolean hasNext() {
      if (iterNode == null) {
        return false;
      } else {
        final boolean ret = (ascending ? iterNode.next : iterNode.prev) != null;
        if (!ret) {
          iterNode = null;
        }
        return ret;
      }
    }

    @Override
    public Spans next() {
      return iterNode = (ascending ? iterNode.next : iterNode.prev);
    }

    @Override
    public void remove() {
      iterNode.remove();
    }

  }

  private static final class PerEndPositionIter implements Iterator<SpansEntry> {

    private final int lastIndex;
    private final int allowedSlop;
    private final Node root;
    private Node[] spans;
    private final Node[][] backingSpans;
    private final DLLReturnNode anchor;
    private int width;
    private DLLReturnNode drn;
    private SpansEntry spansEntry;
    private boolean hasNextInitialized = false;
    private int nextEntry = 0;
    private final SpansEntry[] spansEntries;

    public PerEndPositionIter(int length, int allowedSlop, Node root, DLLReturnNode endPositionReturnNodeAnchor) {
      this.lastIndex = length - 1;
      this.backingSpans = new Node[][] {spans = new Node[length], new Node[length]};
      this.allowedSlop = allowedSlop;
      this.root = root;
      this.spansEntries = new SpansEntry[]{spansEntry = new SpansEntry(spans, lastIndex),
        new SpansEntry(backingSpans[1], lastIndex)};
      this.anchor = endPositionReturnNodeAnchor;
    }

    public PerEndPositionIter init(DLLReturnNode drn) {
      this.drn = drn.prev;
      return this;
    }
    
    @Override
    public boolean hasNext() {
      if (hasNextInitialized) {
        return true;
      } else if (drn == anchor) {
        return false;
      } else {
        Node n = drn.node;
        width = allowedSlop - n.maxSlopRemainingToStart;
        int i = lastIndex;
        do {
          spans[i--] = n;
        } while ((n = n.maxSlopRemainingPhrasePrev) != root);
        return hasNextInitialized = true;
      }
    }

    @Override
    public SpansEntry next() {
      drn = drn.prev;
      SpansEntry ret = spansEntry.init(width);
      spansEntry = spansEntries[nextEntry ^= 1];
      spans = backingSpans[nextEntry];
      hasNextInitialized = false;
      return ret;
    }

  }
  
  private final PerEndPositionIter perEndPositionIter;

  public Iterator<SpansEntry> perEndPosition(DLLReturnNode drn) {
    return perEndPositionIter.init(drn);
  }
  
  private static final Comparator<DLLReturnNode> END_POS_COMPARATOR = new Comparator<DLLReturnNode>() {

    @Override
    public int compare(DLLReturnNode o1, DLLReturnNode o2) {
      return Integer.compare(o1.node.endPosition(), o2.node.endPosition());
    }
  };
  
  private static final class PerPositionIter implements Iterator<SpansEntry> {

    private int startPosition;
    private final int allowedSlop;
    private final ComboMode comboMode;
    private int passId;
    private int i = 0;
    private DLLReturnNode[] drns;
    private int drnsSize;
    private boolean backingInitialized = false;
    private final WidthAtStartEndIter backing;
    private final LocalDRNArrayList drnsBuilder = new LocalDRNArrayList(2);
    private final IntObjectHashMap<LocalNodeArrayList> bySlopRemaining;

    private final LocalNodeArrayList pooledNodeListAnchor = new LocalNodeArrayList(2);
    private LocalNodeArrayList pooledNodeListHead = pooledNodeListAnchor;
    private LocalNodeArrayList pooledNodeListLastHead = null;

    private LocalNodeArrayList getPooledNodeList() {
      LocalNodeArrayList ret = pooledNodeListHead;
      if (ret == null) {
        ret = new LocalNodeArrayList(2);
        pooledNodeListLastHead.trackNextRef = ret;
      } else {
        ret.clear();
        pooledNodeListHead = ret.trackNextRef;
      }
      pooledNodeListLastHead = ret;
      return ret;
    }

    private void resetNodeListPool() {
      pooledNodeListHead = pooledNodeListAnchor;
      pooledNodeListLastHead = null;
    }

    public PerPositionIter(int allowedSlop, ComboMode comboMode) {
      this.allowedSlop = allowedSlop;
      this.comboMode = comboMode;
      this.bySlopRemaining = new IntObjectScatterMap<>();
      this.backing = new WidthAtStartEndIter(allowedSlop << 1, comboMode);
    }
    
    public PerPositionIter init(DLLReturnNode drn, int startPosition, int passId) {
      this.backingInitialized = false;
      this.i = 0;
      this.startPosition = startPosition;
      this.passId = passId;
      final DLLReturnNode anchor = drn.anchor;
      drnsBuilder.clear();
      drn = drn.prev;
      do {
        drnsBuilder.add(drn);
      } while ((drn = drn.prev) != anchor);
      this.drns = drnsBuilder.data;
      this.drnsSize = drnsBuilder.size;
      if (drnsSize > 1) {
        Arrays.sort(this.drns, 0, this.drnsSize, END_POS_COMPARATOR); //TODO can we avoid doing this every time?
      }
      return this;
    }
    
    @Override
    public boolean hasNext() {
      if (backingInitialized) {
        if (backing.hasNext()) {
          return true;
        }
        backingInitialized = false;
      }
      return initBacking();
    }

    private boolean initBacking() {
      if (i >= drnsSize) {
        bySlopRemaining.clear();
        resetNodeListPool();
        return false;
      } else {
        DLLReturnNode drn = drns[i++];
        bySlopRemaining.clear();
        resetNodeListPool();
        Node endNode = drn.node;
        switch (comboMode) {
          case FULL_DISTILLED:
          case FULL_DISTILLED_PER_POSITION:
          case FULL_DISTILLED_PER_START_POSITION:
            populateBySlopRemainingFullDistilled(endNode);
            break;
          default:
            populateBySlopRemainingPerPosition(endNode);
        }
        backing.init(startPosition, endNode.endPosition(), bySlopRemaining.iterator());
        return true;
      }
    }

    private void addToBySlopRemaining(final IntObjectHashMap<LocalNodeArrayList> bySlopRemaining, final Node node, final int totalSlopRemaining) {
        LocalNodeArrayList forSlopRemaining = bySlopRemaining.get(totalSlopRemaining);
        if (forSlopRemaining == null) {
          forSlopRemaining = getPooledNodeList();
          bySlopRemaining.put(totalSlopRemaining, forSlopRemaining);
        }
        forSlopRemaining.add(node);
    }
    
    private void conditionalAddToBySlopRemaining(final Node node, final int maxSlopRemainingToStart, final int slopRemainingToEnd) {
      switch (comboMode) {
        case FULL_DISTILLED_PER_START_POSITION:
          if (node.outputPassId != passId) {
            node.outputPassId = passId;
            node.output = 0;
            break;
          }
        case PER_POSITION:
        case FULL_DISTILLED_PER_POSITION:
          if (node.output > 0) {
            final int totalSlopRemaining = maxSlopRemainingToStart + slopRemainingToEnd;
            int idx = bySlopRemaining.indexOf(totalSlopRemaining);
            if (idx < 0) {
              bySlopRemaining.put(totalSlopRemaining, null);
            }
            return;
          }
      }
      addToBySlopRemaining(bySlopRemaining, node, maxSlopRemainingToStart + slopRemainingToEnd);
    }

    private void populateBySlopRemainingFullDistilled(Node endNode) {
      conditionalAddToBySlopRemaining(endNode, endNode.maxSlopRemainingToStart, allowedSlop);
      for (final ObjectCursor<FullEndNodeLink> val : endNode.associatedNodes.values()) {
        FullEndNodeLink nodeLink = val.value;
        Node node = nodeLink.node;
        conditionalAddToBySlopRemaining(node, node.maxSlopRemainingToStart, nodeLink.remainingSlopToEnd);
      }
    }

    private void populateBySlopRemainingPerPosition(Node endNode) {
      DLLEndNodeLink nodeLink = endNode.maxSlopRemainingEndNodeLink;
      do {
        Node node = nodeLink.node;
        conditionalAddToBySlopRemaining(node, node.maxSlopRemainingToStart, node.maxSlopRemainingToEnd);
      } while ((nodeLink = nodeLink.next) != null);
    }

    @Override
    public SpansEntry next() {
      return backing.next();
    }

  }
  
  private static final class WidthAtStartEndIter implements Iterator<SpansEntry> {

    private int startPosition;
    private int endPosition;
    private final int twiceAllowedSlop;
    private Iterator<IntObjectCursor<LocalNodeArrayList>> entries;
    private final ComboMode comboMode;
    private final SpansEntry spansEntry;

    public WidthAtStartEndIter(int twiceAllowedSlop, ComboMode comboMode) {
      this.twiceAllowedSlop = twiceAllowedSlop;
      this.comboMode = comboMode;
      this.spansEntry = new SpansEntry(comboMode);
    }

    public WidthAtStartEndIter init(int startPosition, int endPosition, Iterator<IntObjectCursor<LocalNodeArrayList>> entries) {
      this.startPosition = startPosition;
      this.endPosition = endPosition;
      this.entries = entries;
      return this;
    }

    @Override
    public boolean hasNext() {
      return entries.hasNext();
    }

    @Override
    public SpansEntry next() {
      IntObjectCursor<LocalNodeArrayList> e = entries.next();
      return spansEntry.init(startPosition, endPosition, e.value, twiceAllowedSlop - e.key);
    }
    
  }
  
  static final Comparator<Node> PHRASE_ORDER_COMPARATOR = new Comparator<Node>() {

    @Override
    public int compare(Node o1, Node o2) {
      return Integer.compare(o2.phraseIndex, o1.phraseIndex);
    }
  };

  private final PerPositionIter perPositionIter;
  public Iterator<SpansEntry> perPosition(DLLReturnNode drn, final int twiceAllowedSlop, int startPosition, ComboMode comboMode) {
    return perPositionIter.init(drn, startPosition, passId);
  }
  
  private static final class FullPositionsIter implements Iterator<SpansEntry> {

    private final int length;
    private final int allowedSlop;
    private final int lastIndex;
    private final Node[] spansBuilder;
    private final DLLNode[] state;
    private int i;
    private final DLLReturnNode anchor;
    private DLLReturnNode drn;
    private Node n;
    private final DLLNode initialDn;
    private DLLNode dn;
    private int remainingSlop;
    private boolean nextInitialized = false;
    private final SpansEntry spansEntry;

    public FullPositionsIter(int length, int allowedSlop, DLLReturnNode endPositionReturnNodeAnchor) {
      this.length = length;
      this.allowedSlop = allowedSlop;
      this.remainingSlop = allowedSlop;
      this.lastIndex = length - 1;
      this.spansBuilder = new Node[length];
      this.state = new DLLNode[lastIndex];
      this.anchor = endPositionReturnNodeAnchor;
      this.initialDn = new DLLNode();
      this.spansEntry = new SpansEntry(this.spansBuilder, this.lastIndex);
    }

    public FullPositionsIter init(DLLReturnNode drn) {
      this.nextInitialized = false;
      this.remainingSlop = allowedSlop;
      this.i = lastIndex;
      this.drn = drn.prev;
      this.n = this.drn.node;
      this.dn = initialDn.init(n, n, n.phrasePrev, -1);
      return this;
    }
    
    @Override
    public boolean hasNext() {
      return nextInitialized || (nextInitialized = hasNextInternal());
    }

    private boolean hasNextInternal() {
      do {
        Node prev;
        shiftRight:
        do {
          int slopAdjust;
          sameLevel:
          while ((dn = dn.next) != null && assign(true, prev = n, n = dn.phraseNext)) {
            if ((slopAdjust = prev.endPosition() - n.endPosition()) > remainingSlop) {
              continue sameLevel;
            } else {
              remainingSlop -= slopAdjust;
            }
            shiftLeft:
            do {
              //System.err.println("spansBuilder["+i+"]="+n+", "+Arrays.toString(state));
              spansBuilder[i] = n;
              if (i <= 0) {
                break;
              } else {
                prev = n;
                n = dn.phrasePrev;
                if ((slopAdjust = prev.startPosition() - n.endPosition()) > remainingSlop) {
                  n = prev;
                  continue sameLevel;
                } else {
                  state[--i] = dn;
                  dn = n.phrasePrev;
                  remainingSlop -= slopAdjust;
                }
              }
            } while (true);
            //System.err.println("ret=" + Arrays.toString(spansBuilder) + ", width=" + (allowedSlop - remainingSlop));
            return true;
          }
        } while (i < lastIndex && assign(true, dn = state[i++], prev = dn.phraseNext, remainingSlop += prev.startPosition() - n.endPosition(), n = prev));
        remainingSlop = allowedSlop;
      } while ((drn = drn.prev) != anchor && assign(true, n = drn.node, dn = DLLNode.newInstance(n, n, n.phrasePrev, -1)));
      return false;
    }

    @Override
    public SpansEntry next() {
      this.nextInitialized = false;
      return spansEntry.init(allowedSlop - remainingSlop);
    }

  }
  
  public Iterator<SpansEntry> fullPositions(DLLReturnNode drn) {
    return this.fullPositionsIter.init(drn);
  }

  private final FullPositionsIter fullPositionsIter;
  
  private final StackFrame[] blStack;
  private final DeleteBacklinksStackFrame[] dbStack;

  private static final class DeleteBacklinksStackFrame {
    Node n;
    SLLNode pn;
    boolean pseudoReturn = false;
    void preRecurse(Node n, SLLNode pn) {
      this.n = n;
      this.pn = pn;
    }
    SLLNode init(ComboMode comboMode) {
      if (pseudoReturn) {
        pseudoReturn = false;
        return pn.next;
      } else {
        n.initialzedReverseLinks.remove();
        n.initialzedReverseLinks = null;
        if (n.phraseNext == null) {
          return null;
        } else {
          switch (comboMode) {
            case FULL_DISTILLED:
            case FULL_DISTILLED_PER_POSITION:
            case FULL_DISTILLED_PER_START_POSITION:
              final long phraseScopeId = n.phraseScopeId;
              final FullEndNodeLink anchor = n.endNodeLinks;
              FullEndNodeLink endNodeLink = anchor.next;
              do {
                endNodeLink.endNode.associatedNodes.remove(phraseScopeId);
              } while ((endNodeLink = endNodeLink.next) != anchor);
              anchor.clear();
              break;
          }
          n.maxSlopRemainingEndNodeLink.remove();
          n.maxSlopRemainingEndNodeLink = null;
          return n.phraseNext;
        }
      }
    }
  }

  /**
   * In preparation for the deletion of the specified Node n, delete links from subsequent phrase positions that refer
   * back to the specified Node. Recursively prune any subsequent node that *only* refers back to the specified Node.
   * @param n 
   */
  private void deleteBacklinks(final int initialPhraseIndex, final DeleteBacklinksStackFrame[] frames) {
    //System.err.println("delete backlinks to node "+n+"["+n.deque.phraseIndex+"]");
    int localPhraseIndex = initialPhraseIndex;
    DeleteBacklinksStackFrame f = frames[localPhraseIndex];
    pseudoRecurse:
    do {
      Node n = f.n;
      SLLNode pn = f.init(comboMode);
      if (pn != null) {
        do {
          DLLNode dn = pn.node;
          DLLNode ret = dn.remove();
          if (ret == DLLNode.EMPTY_LIST) {
            Node toRemove = dn.phraseNext;
            toRemove.phrasePrev = null;
            f.pn = pn;
            f = frames[++localPhraseIndex];
            f.preRecurse(toRemove, toRemove.phraseNext);
            continue pseudoRecurse;
          } else {
            if (ret != null) {
              dn.phraseNext.phrasePrev = ret;
            }
          }
        } while ((pn = pn.next) != null);
      }
      if (n.validity == 0) {
        n.dequePointer[0].enqueueForPoolReturn(n);
      }
      if (localPhraseIndex == initialPhraseIndex) {
        return;
      } else {
        f = frames[--localPhraseIndex];
        f.pseudoReturn = true;
      }
    } while (true); // pseudoRecurse
  }

  private int passId = -1;
  private final boolean[] reuseableHasNext = new boolean[1];
  
  /**
   * Public entrypoint for building a "lattice" of Spans-transverse phrase links for a given startPosition.
   */
  public DLLReturnNode buildLattice(int minStart, int remainingSlopToCaller, ComboMode comboMode, boolean loop) throws IOException {
    final DLLReturnNode anchor = returned.anchor;
    DLLReturnNode drn = returned.next;
    int drnStartPos = drn.node == null ? -1 : drn.node.startPosition();
    do {
      //driver.reset(-1, minStart);
//    if (last.returned != null) {
//      printDLLReturnNode("A", last.returned);
//    }
//    long start=System.currentTimeMillis();
      root.maxSlopToCaller = -1;
      int minEnd = Math.min(1, driver.next.getMinStart() - allowedSlop);
      final int ret;
      if (loop) {
        blStack[0].initArgs(root, minStart, minStart, minStart + 1, minEnd, remainingSlopToCaller);
        ret = buildLatticeLoop(allowedSlop, comboMode, greedyReturn, ++passId, blStack, root);
      } else {
        reuseableHasNext[0] = false;
        ret = buildLattice(root, minStart, minStart, minStart + 1, minEnd, remainingSlopToCaller, comboMode, ++passId, reuseableHasNext);
      }
      int startPosition;
      if (ret == Integer.MAX_VALUE) {
        if (greedyReturn) {
          last.returned.setGreedyWidth(Integer.MAX_VALUE);
        }
        last.returned.setCurrentStart(Spans.NO_MORE_POSITIONS);
        return last.returned;
      } else if (greedyReturn ? last.returned.setGreedyWidth(ret) : drn != returned.next) {
        break;
      } else {
        //revisitPruneBacklinks();
        PositionDeque pd = this;
        do {
          pd.returnNodesToPool();
        } while ((pd = pd.next) != null);
        minStart++;
        if ((startPosition = driver.nextMatch(minStart, minStart, Integer.MAX_VALUE, -1)) < 0) {
          minStart = Spans.NO_MORE_POSITIONS;
          break;
        } else {
          minStart = startPosition;
        }
      }
    } while (true);
//    System.err.println("buildLattice="+(System.currentTimeMillis() - start));
//    start=System.currentTimeMillis();
//    printDLLReturnNode("B", last.returned);
    if (!greedyReturn) {
      if (drn != anchor) {
        drn.prev.next = returned.anchor; // truncate
        returned.anchor.prev = drn.prev;
        do {
          Node n = drn.node;
          assert comboMode == this.comboMode;
          dbStack[0].preRecurse(n, n.phraseNext);
          deleteBacklinks(0, dbStack);
        } while ((drn = drn.next) != anchor);
      }
//    System.err.println("deleteBacklinks="+(System.currentTimeMillis() - start));
//    start=System.currentTimeMillis();
//    printDLLReturnNode("C", last.returned);
      revisitPruneBacklinks(dbStack);
//    System.err.println("revisit="+(System.currentTimeMillis() - start));
//    printDLLReturnNode("D", last.returned);
      PositionDeque pd = this;
      do {
        pd.returnNodesToPool();
      } while ((pd = pd.next) != null);
    }
    last.returned.setCurrentStart(minStart);
    return last.returned;
  }

  private final SLLNode[] snStack;

  private void updateMaxSlopRemainingToStart(Node caller, Node n, int remainingSlopToStart) {
    n.maxSlopRemainingToStart = remainingSlopToStart;
    n.maxSlopRemainingPhrasePrev = caller;
    SLLNode sn = n.phraseNext;
    if (sn != null) {
      int i = 0;
      outer:
      do {
        atLevel:
        do {
          DLLNode dn = sn.node;
          Node child = dn.phraseNext;
          final int gapToChild = child.startPosition() - n.endPosition();
          switch (comboMode) {
            case FULL_DISTILLED:
            case FULL_DISTILLED_PER_POSITION:
            case FULL_DISTILLED_PER_START_POSITION:
              if (child.associatedNodes != null) {
                if (remainingSlopToStart - gapToChild <= allowedSlop) {
                  associateWithEndNode(n, child, allowedSlop - gapToChild);
                }
              } else {
                FullEndNodeLink endNodeLink;
                if (sn.revisitEndNode != null) {
                  endNodeLink = sn.revisitEndNode;
                } else {
                  endNodeLink = child.endNodeLinks.prev;
                }
                FullEndNodeLink revisitFromEndNode = null;
                final FullEndNodeLink anchor = endNodeLink.anchor;
                do {
                  int remainingSlopCandidate = endNodeLink.remainingSlopToEnd - gapToChild;
                  if (remainingSlopToStart + remainingSlopCandidate >= allowedSlop) {
                    LongObjectHashMap<FullEndNodeLink> associated = endNodeLink.endNode.associatedNodes;
                    FullEndNodeLink extantEndNodeLink = associated.get(n.phraseScopeId);
                    if (extantEndNodeLink == null) {
                      FullEndNodeLink newLink = n.endNodeLinks.add(endNodeLink.endNode, remainingSlopCandidate);
                      associated.put(n.phraseScopeId, newLink);
                    } else if (remainingSlopCandidate < extantEndNodeLink.remainingSlopToEnd) {
                      extantEndNodeLink.remove();
                      FullEndNodeLink newLink = n.endNodeLinks.add(endNodeLink.endNode, remainingSlopCandidate);
                      associated.put(n.phraseScopeId, newLink);
                    }
                  } else if (revisitFromEndNode == null && remainingSlopCandidate >= 0) {
                    revisitFromEndNode = endNodeLink;
                  }
                } while ((endNodeLink = endNodeLink.prev) != anchor);
                sn.revisitEndNode = revisitFromEndNode;
              }
          }
          int slopToChild = remainingSlopToStart - gapToChild;
          if (slopToChild > child.maxSlopRemainingToStart) {
            //updateMaxSlopRemainingToStart(n, child, slopToChild);
            child.maxSlopRemainingToStart = slopToChild;
            child.maxSlopRemainingPhrasePrev = n;
            SLLNode childSn = child.phraseNext;
            if (childSn != null) {
              assert n == sn.node.phrasePrev;
              snStack[i++] = sn;
              sn = childSn;
              n = child;
              remainingSlopToStart = slopToChild;
              continue outer;
            }
          } else if (sn.lastPassId == passId && child.revisitRefCount[0]-- == 1) {
            downstreamNoRevisitNecessary(child, i + 1);
          }
        } while ((sn = sn.next) != null);
        if (--i < 0) {
          break;
        }
        sn = snStack[i];
        remainingSlopToStart += n.startPosition() - (n = sn.node.phrasePrev).endPosition();
      } while (true);
    }
  }

  private void downstreamNoRevisitNecessary(Node n, int i) {
    SLLNode sn = n.phraseNext;
    if (sn != null) {
      final int floor = i;
      outer:
      do {
        atLevel:
        do {
          if (sn.lastPassId == passId) {
            sn.revisitNode.remove();
            DLLNode dn = sn.node;
            Node child = dn.phraseNext;
            SLLNode childSn;
            if (child.revisitRefCount[0]-- == 1 && (childSn = child.phraseNext) != null) {
              //downstreamNoRevisitNecessary(child);
              snStack[i++] = sn;
              sn = childSn;
              continue outer;
            }
          }
        } while ((sn = sn.next) != null);
        if (--i < floor) {
          break;
        }
        sn = snStack[i];
      } while (true);
    }
  }

  /**
   * Recursively adjust preliminary cached slop-to-start/end based on complete/current path information;
   * recursively prune nodes that no longer have a valid path to start/end.
   */
  private void revisitPruneBacklinks(final DeleteBacklinksStackFrame[] frames) {
    PositionDeque pd = this.next;
    do {
      RevisitNode r = pd.revisit;
      if (r.next != null) {
        do {
          RevisitNode srn = r.next;
          r.next = null; // unlink
          do {
            Node n1 = srn.phrasePrev;
            Node n2 = srn.phraseNext;
            //System.err.println("revisit connection " + n1+"["+n1.deque.phraseIndex+"]" + "=>" + n2+"["+n2.deque.phraseIndex+"]");
            int remainingSlop = n1.maxSlopRemainingToStart - (n2.startPosition() - n1.endPosition());
            assert remainingSlop + n2.maxSlopRemainingToEnd >= allowedSlop : remainingSlop+", "+n2.maxSlopRemainingToEnd+", "+allowedSlop;// because it's in the revisit queue
            final boolean revisitsComplete = n2.revisitRefCount[0]-- == 1;
            SLLNode pn = srn.updateFromOnRevisit;
            do {
              DLLNode dn = pn.node;
              Node nextNode = dn.phraseNext;
              int remainingSlopToNext = remainingSlop - (nextNode.startPosition() - n2.endPosition());
              if (remainingSlopToNext + nextNode.maxSlopRemainingToEnd < allowedSlop) {
                //System.err.println("rstn=" + remainingSlopToNext + ", msrte=" + nextNode.maxSlopRemainingToEnd + ", " + n1 + "=>" + n2 + "=>" + nextNode);
                DLLNode ret = dn.remove();
                if (ret == DLLNode.EMPTY_LIST) {
                  nextNode.phrasePrev = null;
                  final int localPhraseIndex = nextNode.phraseIndex;
                  frames[localPhraseIndex].preRecurse(nextNode, nextNode.phraseNext);
                  deleteBacklinks(localPhraseIndex, frames);
                } else {
                  if (ret != null) {
                    nextNode.phrasePrev = ret;
                  }
                }
                //System.err.println("\tremove node");
              } else if (remainingSlopToNext > nextNode.maxSlopRemainingToStart) {
                //System.err.println("\tupdate slopToStart");
                updateMaxSlopRemainingToStart(n2, nextNode, remainingSlopToNext);
              } else if (revisitsComplete && pn.lastPassId == passId && nextNode.revisitRefCount[0]-- == 1) {
                //System.err.println("\tmarking downstream as no revisit necessary");
                downstreamNoRevisitNecessary(nextNode, 0);
              } else {
                //System.err.println("\tno action necessary");
              }
            } while ((pn = pn.next) != null);
          } while ((srn = srn.next) != null);
        } while ((r = r.next) != null);
      }
    } while ((pd = pd.next) != null);
  }

  public static void printDLLReturnNode(String prefix, PositionDeque.DLLReturnNode drn) {
    final DLLReturnNode anchor = drn.anchor;
    if (((drn = drn.next) != anchor)) {
      do {
        Spans s = drn.node;
        //System.out.println(prefix+"got endSpanPosition: " + s.startPosition() + "=>" + s.endPosition());
        PositionDeque.printSpans(prefix, s, -1);
      } while ((drn = drn.next) != anchor);
    } else {
      //System.out.println(prefix+"got no endSpanPositions");
    }
  }
  
  public static void printSpans(String prefix, Spans s, int dnLink) {
    Node n = (Node) s;
    //System.out.println(prefix + n.startPosition()+"=>"+n.endPosition()+" ("+n.maxSlopRemainingToStart+"<-/->"+n.maxSlopRemainingToEnd+") -- "+dnLink);
    if (n.phrasePrev != null) {
      DLLNode dn = n.phrasePrev;
      do {
        Node prev = dn.phrasePrev;
        printSpans(prefix.concat("  "), prev, System.identityHashCode(dn));
      } while ((dn = dn.next) != null);
    }
  }
  
  private final Node root = new Node();
  static final boolean ENABLE_GREEDY_RETURN = true;
  
  private final boolean supportVariableTermSpansLength;
  
  /**
   * Recursive (phrase-position-per-level) method for building Spans-transverse links for slop-valid phrase paths.
   * This performs a depth-first traversal of the graph, caching paths where possible to avoid re-traversal of
   * sub-graphs (to end) that have been full explored (for the given input slop available).
   * 
   * @param caller
   * @param hardMinStart
   * @param softMinStart
   * @param startCeiling
   * @param minEnd
   * @param remainingSlopToCaller
   * @param comboMode
   * @param passId
   * @return
   * @throws IOException 
   */
  private int buildLattice(final Node caller, final int hardMinStart, final int softMinStart, final int startCeiling, final int minEnd, final int remainingSlopToCaller, final ComboMode comboMode, final int passId, final boolean[] hasMatch) throws IOException {
    //System.err.println("here["+phraseIndex+"]!! "+caller+", "+softMinStart+", "+startCeiling+", "+remainingSlopToCaller+" ("+lstToString()+")");
    final int previousMaxSlopToCaller = caller.maxSlopToCaller;
    if (remainingSlopToCaller > caller.maxSlopToCaller) {
      caller.maxSlopToCaller = remainingSlopToCaller;
    }
    int start;
    final int initialStart = driver.startPosition();
    final boolean hasInitialMatch;
    if (phraseIndex == 0 || (initialStart >= softMinStart && initialStart < startCeiling && driver.endPosition() >= minEnd)) {
      start = initialStart;
      hasInitialMatch = true;
    } else {
      start = driver.nextMatch(hardMinStart, softMinStart, startCeiling, minEnd);
      hasInitialMatch = start >= 0;
    }
    if (hasInitialMatch) {
      Node leastSloppyPathToPhraseEnd = null;
      Node maxSlopRemainingEndNode = null;
      int maxSlopRemainingToPhraseEnd = Integer.MIN_VALUE;
      Node nextNode = getLastNode();
      int updateSealedToThreshold = Integer.MIN_VALUE;
      int updateSealedTo = nextNode.index;
      boolean keepUpdatingSealedTo = !greedyReturn;
      loopAtLevel:
      do {
        final int remainingSlop = remainingSlopToCaller - (start - softMinStart);
        //System.err.println("start[" + phraseIndex + "]: " + start + " (?= " + nextNode.startPosition+"(=>" + nextNode.endPosition+", remainingSlop="+remainingSlop+"))");
        int maxSlopRemainingCandidate;
        final int remainingSlopToEnd;
        final SLLNode revisitFrom;
        if (remainingSlop < 0) {
          // we can't get *to* nextNode
          if (allowedSlop + remainingSlop < 0) {
            if (keepUpdatingSealedTo) {
              updateSealedTo = nextNode.index;
            }
          } else if (remainingSlop > updateSealedToThreshold) {
            if (keepUpdatingSealedTo) {
              keepUpdatingSealedTo = false; // we will need to inspect this again.
              updateSealedTo = nextNode.index;
            }
            updateSealedToThreshold = remainingSlop;
            //System.err.println("set1 updateSealedToThreshold="+updateSealedToThreshold+", "+updateSealedTo+", "+nextNode.index+", "+nextNode);
          }
          break;
        } else if (next == null) {
          if (greedyReturn) {
            return remainingSlop;
          }
          if (keepUpdatingSealedTo) {
            updateSealedTo = nextNode.index;
            updateSealedToThreshold = 0;
            //System.err.println("set4 updateSealedToThreshold="+updateSealedToThreshold+", "+updateSealedTo+", "+nextNode.index+", "+nextNode);
          }
          revisitFrom = null;
          //System.err.println("got 1 for " + nextNode+" initialize?="+(nextNode.initialzedReverseLinks == null));
          if (nextNode.initialzedReverseLinks == null) {
            nextNode.maxSlopRemainingToEnd = allowedSlop;
            nextNode.maxSlopRemainingEndNodeLink = DLLEndNodeLink.newInstance(nextNode);
            nextNode.initialzedReverseLinks = returned.add(nextNode);
          }
          remainingSlopToEnd = allowedSlop;
          // the *only* path
        } else {
          switch (nextNode.sealed) {
            case SEALED:
              //System.err.println("got 2 at " + nextNode+"["+nextNode.deque.phraseIndex+"]");
              // nothing will change
              revisitFrom = nextNode.phraseNext;
              remainingSlopToEnd = nextNode.maxSlopRemainingToEnd;
              break;
            case INITIALIZED:
              //System.err.println("\t remainingSlop="+remainingSlop+", sealedThreshold="+nextNode.sealedThreshold+" maxSlopToEnd="+nextNode.maxSlopRemainingToEnd);
              if (remainingSlop < nextNode.sealedThreshold) {
                // nothing will change
                revisitFrom = nextNode.phraseNext;
                remainingSlopToEnd = nextNode.maxSlopRemainingToEnd;
                //System.err.println("got 3 at " + nextNode+"["+nextNode.deque.phraseIndex+"] ("+remainingSlopToEnd+")");
              } else {
                revisitFrom = nextNode.phraseNext;
                int nextHardMinStart = Math.min(driver.getMinEnd(), driver.getMinStart() + 2);
                int nextSoftMinStart = nextNode.endPosition();
                int nextMinEnd = next.next == null ? -1 : Math.min(nextHardMinStart + 1, next.next.driver.getMinStart());
                next.driver.reset(nextNode.sealedTo, -1, nextSoftMinStart);
                //System.err.println("inspecting from "+nextNode+"["+nextNode.deque.phraseIndex+"] "+nextSoftMinStart+", "+next.driver.startPosition()+", !"+nextNode.sealedThreshold+">=<"+remainingSlop);
                int ret = next.buildLattice(nextNode, nextHardMinStart, nextSoftMinStart, nextSoftMinStart + remainingSlop + 1, nextMinEnd, remainingSlop, comboMode, passId, hasMatch);
                if (ENABLE_GREEDY_RETURN && ret > Integer.MIN_VALUE + 1) {
                  return ret;
                }
                remainingSlopToEnd = nextNode.maxSlopRemainingToEnd;
                //System.err.println("rste="+remainingSlopToEnd+ " for "+nextNode+"["+nextNode.deque.phraseIndex+"]");
              }
              break;
            case NONE:
              revisitFrom = null;
              int nextHardMinStart = Math.min(driver.getMinEnd(), driver.getMinStart() + 2);
              int nextSoftMinStart = nextNode.endPosition();
              int nextMinEnd = next.next == null ? -1 : Math.min(nextHardMinStart + 1, next.next.driver.getMinStart());
              next.driver.reset(-1, nextSoftMinStart);
              //System.err.println("not cached; inspecting from " + nextNode + "[" + nextNode.deque.phraseIndex + "] " + softMinStart + ", " + next.driver.startPosition());
              int ret = next.buildLattice(nextNode, nextHardMinStart, nextSoftMinStart, nextSoftMinStart + remainingSlop + 1, nextMinEnd, remainingSlop, comboMode, passId, hasMatch);
              if (ENABLE_GREEDY_RETURN && ret > Integer.MIN_VALUE + 1) {
                return ret;
              }
              remainingSlopToEnd = nextNode.maxSlopRemainingToEnd;
              //System.err.println("rste=" + remainingSlopToEnd + " for " + nextNode + "[" + nextNode.deque.phraseIndex + "]");
              break;
            default:
              throw new AssertionError();
          }
        }
        if (remainingSlopToEnd >= 0) {
          // we *can* get to phrase end via this route
          if (!hasMatch[0]) {
            hasMatch[0] = true;
          }
          if (keepUpdatingSealedTo) {
            updateSealedTo = nextNode.index;
            if (nextNode.sealed == Node.SlopStatus.SEALED) {
              updateSealedToThreshold = 0;
            } else {
              // although we can currently get to end via this node, with less slop more paths may become available
              updateSealedToThreshold = -1; // even slightest bit more slop could result in more matches.
              keepUpdatingSealedTo = false;
            }
            //System.err.println("set3 updateSealedToThreshold=" + updateSealedToThreshold + ", " + updateSealedTo + ", " + nextNode.index + ", " + nextNode);
          }
          if (remainingSlop > nextNode.maxSlopRemainingToStart) {
            nextNode.maxSlopRemainingToStart = remainingSlop;
            nextNode.maxSlopRemainingPhrasePrev = caller;
          }
          final int gapFromCallerToNext = start - softMinStart;
          maxSlopRemainingCandidate = remainingSlopToEnd - gapFromCallerToNext;
          SLLNode linkFromCaller = null;
          if (previousMaxSlopToCaller + maxSlopRemainingCandidate < allowedSlop) {
            linkFromCaller = initNodeLinks(caller, nextNode, returned, remainingSlop, passId, comboMode);
          } else {
            // we have probably already created a link between caller and nextNode
            switch (comboMode) {
              default:
                if (revisitFrom == null) {
                  break;
                }
              case FULL_DISTILLED:
              case FULL_DISTILLED_PER_POSITION:
              case FULL_DISTILLED_PER_START_POSITION:
                linkFromCaller = caller.phraseNext;
                while (linkFromCaller.node.phraseNext != nextNode) {
                  linkFromCaller = linkFromCaller.next;
                  if (linkFromCaller == null) {
                    throw new AssertionError("this shouldn't happen, but uncomment below 2 lines if it does");
//                    linkFromCaller = initNodeLinks(caller, nextNode, returned, remainingSlop, passId, comboMode);
//                    break;
                  }
                }
            }
          }
          if (revisitFrom != null) {
            revisit.add(linkFromCaller, revisitFrom, passId);
          } else if (nextNode.initialzedReverseLinks == null) {
            nextNode.initialzedReverseLinks = returned.add(nextNode);
          }
          if (caller != root) {
            switch (comboMode) {
              case FULL_DISTILLED:
              case FULL_DISTILLED_PER_POSITION:
              case FULL_DISTILLED_PER_START_POSITION:
                if (nextNode.associatedNodes != null) {
                  if (remainingSlopToCaller - gapFromCallerToNext <= allowedSlop) {
                    associateWithEndNode(caller, nextNode, allowedSlop - gapFromCallerToNext);
                  }
                } else {
                  FullEndNodeLink endNodeLink;
                  if (linkFromCaller.revisitEndNode != null) {
                    endNodeLink = linkFromCaller.revisitEndNode;
                  } else {
                    endNodeLink = nextNode.endNodeLinks.prev;
                  }
                  FullEndNodeLink revisitFromEndNode = null;
                  final FullEndNodeLink anchor = endNodeLink.anchor;
                  do {
                    int remainingSlopCandidate = endNodeLink.remainingSlopToEnd - gapFromCallerToNext;
                    if (remainingSlopToCaller + remainingSlopCandidate >= allowedSlop) {
                      associateWithEndNode(caller, endNodeLink.endNode, remainingSlopCandidate);
                    } else if (revisitFromEndNode == null && remainingSlopCandidate >= 0) {
                      revisitFromEndNode = endNodeLink;
                    }
                  } while ((endNodeLink = endNodeLink.prev) != anchor);
                  linkFromCaller.revisitEndNode = revisitFromEndNode;
                }
            }
          }
          if (leastSloppyPathToPhraseEnd == null || maxSlopRemainingCandidate > maxSlopRemainingToPhraseEnd) {
            leastSloppyPathToPhraseEnd = nextNode;
            maxSlopRemainingToPhraseEnd = maxSlopRemainingCandidate;
            maxSlopRemainingEndNode = nextNode.maxSlopRemainingEndNodeLink.endNode;
          }
        } else if (keepUpdatingSealedTo) {
          updateSealedTo = nextNode.index;
          if (nextNode.sealed == Node.SlopStatus.SEALED) {
            // we will never be able to get to the end via this node, and may cache through this point
            updateSealedToThreshold = 0;
          } else {
          // we cannot currently get to the end via this node, but might be able to later, and can cache up to this point
            updateSealedToThreshold = -1; // even slightest bit more slop could result in a match.
            keepUpdatingSealedTo = false;
          }
          //System.err.println("set2 updateSealedToThreshold="+updateSealedToThreshold+", "+updateSealedTo+", "+nextNode.index+", "+nextNode);
        }
        if (comboMode == ComboMode.GREEDY_END_POSITION && // only want one match
            ((next == null && (remainingSlop >= 0 || assign(start = ~start))) || // reached end irrespective of slop constraints
            remainingSlop + maxSlopRemainingToPhraseEnd >= allowedSlop)) { // have cached path to end within slop constraints
          break loopAtLevel;
        } else {
          if (prev != null || comboMode == ComboMode.GREEDY_END_POSITION) {
            switch (comboMode) {
              case FULL:
              case FULL_DISTILLED:
              case FULL_DISTILLED_PER_POSITION:
              case FULL_DISTILLED_PER_START_POSITION:
              case PER_POSITION_PER_START_POSITION:
                break;
              default:
                final boolean perPosition = comboMode == ComboMode.PER_POSITION;
                if (next == null) {
                  // progress until you find position of different end
                  final int extantEnd = nextNode.endPosition();
                  while ((start = driver.nextMatch(hardMinStart, softMinStart, startCeiling, minEnd)) >= 0 && assign(0, nextNode = getLastNode(), true)) {
                    if ((perPosition && nextNode.output == 0) || nextNode.endPosition() != extantEnd) {
                      continue loopAtLevel;
                    }
                  }
                } else {
                  final int extantEnd = nextNode.endPosition();
                  final int extantWidth = extantEnd - nextNode.startPosition();
                  int checkFor = 0;
                  if (!perPosition && (checkFor = checkForIncreasedMatchLength(extantWidth, leastSloppyPathToPhraseEnd == null, startCeiling - softMinStart > 1)) == 0) {
                    start = ~start;
                  } else {
                    final int decreasedEndStartCeiling = extantEnd - 1;
                    // progress until you find position of decreased end or different width
                    checkLoop:
                    while ((start = driver.nextMatch(hardMinStart, softMinStart, startCeiling, minEnd)) >= 0 && PositionDeque.assign(0, nextNode = getLastNode(), true)) {
                      if (perPosition) {
                        if (nextNode.output == 0) {
                          continue loopAtLevel;
                        }
                      } else {
                        switch (checkFor) {
                          default:
                            throw new AssertionError("this should never happen");
                          case PositionDeque.DECREASED_END_POSITION:
                            if (start >= decreasedEndStartCeiling) {
                              break checkLoop;
                            } else if (nextNode.endPosition() < extantEnd) {
                              continue loopAtLevel;
                            }
                            break;
                          case PositionDeque.INCREASED_WIDTH:
                            if (nextNode.endPosition() - start > extantWidth) {
                              continue loopAtLevel;
                            }
                            break;
                          case PositionDeque.DECREASED_END_POSITION | PositionDeque.INCREASED_WIDTH:
                            final int nextEnd = nextNode.endPosition();
                            if (nextEnd < extantEnd || nextEnd - start > extantWidth) {
                              continue loopAtLevel;
                            } else if (start >= decreasedEndStartCeiling) {
                              checkFor ^= PositionDeque.DECREASED_END_POSITION;
                            }
                            break;
                        }
                      }
                    }
                  }
                }
                break loopAtLevel;
            }
          }
          if ((start = driver.nextMatch(hardMinStart, softMinStart, startCeiling, minEnd)) < 0) {
            // exhasted further matches, for now
            break loopAtLevel;
          } else {
            nextNode = getLastNode();
          }
        }
      } while (true);
      if (maxSlopRemainingToPhraseEnd > caller.maxSlopRemainingToEnd) {
        caller.maxSlopRemainingToEnd = maxSlopRemainingToPhraseEnd;
        caller.maxSlopRemainingPhraseNext = leastSloppyPathToPhraseEnd;
        if (caller != root) {
          DLLEndNodeLink extantEndNodeLink = caller.maxSlopRemainingEndNodeLink;
          if (extantEndNodeLink == null) {
            caller.maxSlopRemainingEndNodeLink = maxSlopRemainingEndNode.maxSlopRemainingEndNodeLink.add(caller);
          } else if (extantEndNodeLink.endNode != maxSlopRemainingEndNode) {
            extantEndNodeLink.remove();
            caller.maxSlopRemainingEndNodeLink = maxSlopRemainingEndNode.maxSlopRemainingEndNodeLink.add(caller);
          }
        }
      }
      if (caller != root) {
        int overshot;
        if (updateSealedToThreshold < 0) {
          caller.sealedTo = updateSealedTo;
          caller.sealed = Node.SlopStatus.INITIALIZED;
          assert updateSealedToThreshold != Integer.MIN_VALUE;
          caller.sealedThreshold = updateSealedToThreshold == Integer.MIN_VALUE ? remainingSlopToCaller : remainingSlopToCaller - updateSealedToThreshold;
          //System.err.println("initialize sealedThreshold1 "+caller+"["+caller.deque.phraseIndex+"]="+caller.sealedThreshold+", "+caller.maxSlopRemainingToEnd);
        } else if ((overshot = ~start - softMinStart) > allowedSlop) {
          caller.sealed = Node.SlopStatus.SEALED; // final slop-to-end has been calculated for the calling node.
          //System.err.println("initialize sealedThreshold3 "+caller+"["+caller.deque.phraseIndex+"], "+caller.maxSlopRemainingToEnd);
        } else {
          caller.sealedTo = nextNode.index + 1;
          caller.sealed = Node.SlopStatus.INITIALIZED;
          caller.sealedThreshold = remainingSlopToCaller + overshot;
          //System.err.println("initialize sealedThreshold2 "+caller+"["+caller.deque.phraseIndex+"]="+caller.sealedThreshold+", "+caller.maxSlopRemainingToEnd);
        }
      }
      return Integer.MIN_VALUE;
    } else if (start == Integer.MIN_VALUE && !hasMatch[0]) {
        // we have to check d.isEmpty because stored spans could still be used for other matches,
      // and we have to check d.driver.startPosition() because f.start could have been set by lookahead,
      // without actually advancing the backing spans.
      if ((isEmpty() && driver.startPosition() == Spans.NO_MORE_POSITIONS) || !mayStillMatchByShrink(driver)) {
        return Integer.MAX_VALUE;
      }
    }
    //System.err.println("RETURN "+maxSlopRemainingToPhraseEnd+"(/"+caller.maxSlopRemainingToEnd+") for "+caller+" via "+leastSloppyPathToPhraseEnd+" (phraseIndex="+(caller.deque == null ? null : caller.deque.phraseIndex)+")");
    return Integer.MIN_VALUE;
  }

  private static boolean assign(int val) {
    return true;
  }

  private static int buildLatticeLoop(final int allowedSlop, final ComboMode comboMode, final boolean greedyReturn, final int passId, final StackFrame[] frames, final Node root) throws IOException {
    int phraseIndex = 0;
    boolean hasMatch = false;
    StackFrame f = frames[phraseIndex];
    pseudoRecurse:
    do {
      PositionDeque d = f.d;
      if (f.initAlways()) {
        f.init();
        loopAtLevel:
        do {
          if (f.notPseudoReturn()) {
            f.remainingSlop = f.remainingSlopToCaller - (f.start - f.softMinStart);
            //System.err.println("start[" + phraseIndex + "]: " + start + " (?= " + nextNode.startPosition+"(=>" + nextNode.endPosition+", remainingSlop="+remainingSlop+"))");
            if (f.remainingSlop < 0) {
              // we can't get *to* nextNode
              if (allowedSlop + f.remainingSlop < 0) {
                if (f.keepUpdatingSealedTo) {
                  f.updateSealedTo = f.nextNode.index;
                }
              } else if (f.remainingSlop > f.updateSealedToThreshold) {
                if (f.keepUpdatingSealedTo) {
                  f.keepUpdatingSealedTo = false; // we will need to inspect this again.
                  f.updateSealedTo = f.nextNode.index;
                }
                f.updateSealedToThreshold = f.remainingSlop;
                //System.err.println("set1 updateSealedToThreshold="+updateSealedToThreshold+", "+updateSealedTo+", "+nextNode.index+", "+nextNode);
              }
              break;
            } else if (d.next == null) {
              if (greedyReturn) {
                return f.remainingSlop;
              }
              if (f.keepUpdatingSealedTo) {
                f.updateSealedTo = f.nextNode.index;
                f.updateSealedToThreshold = 0;
                //System.err.println("set4 updateSealedToThreshold="+updateSealedToThreshold+", "+updateSealedTo+", "+nextNode.index+", "+nextNode);
              }
              f.revisitFrom = null;
              //System.err.println("got 1 for " + nextNode+" initialize?="+(nextNode.initialzedReverseLinks == null));
              if (f.nextNode.initialzedReverseLinks == null) {
                f.nextNode.maxSlopRemainingToEnd = allowedSlop;
                f.nextNode.maxSlopRemainingEndNodeLink = DLLEndNodeLink.newInstance(f.nextNode);
                f.nextNode.initialzedReverseLinks = d.returned.add(f.nextNode);
              }
              f.remainingSlopToEnd = allowedSlop;
              // the *only* path
            } else {
              switch (f.nextNode.sealed) {
                case SEALED:
                  //System.err.println("got 2 at " + nextNode+"["+nextNode.deque.phraseIndex+"]");
                  // nothing will change
                  f.revisitFrom = f.nextNode.phraseNext;
                  f.remainingSlopToEnd = f.nextNode.maxSlopRemainingToEnd;
                  break;
                case INITIALIZED:
                  //System.err.println("\t remainingSlop="+remainingSlop+", sealedThreshold="+nextNode.sealedThreshold+" maxSlopToEnd="+nextNode.maxSlopRemainingToEnd);
                  if (f.remainingSlop < f.nextNode.sealedThreshold) {
                    // nothing will change
                    f.revisitFrom = f.nextNode.phraseNext;
                    f.remainingSlopToEnd = f.nextNode.maxSlopRemainingToEnd;
                    //System.err.println("got 3 at " + nextNode+"["+nextNode.deque.phraseIndex+"] ("+remainingSlopToEnd+")");
                  } else {
                    f.revisitFrom = f.nextNode.phraseNext;
                    int nextHardMinStart = Math.min(d.driver.getMinEnd(), d.driver.getMinStart() + 2);
                    int nextSoftMinStart = f.nextNode.endPosition();
                    int nextMinEnd = d.next.next == null ? -1 : Math.min(nextHardMinStart + 1, d.next.next.driver.getMinStart());
                    d.next.driver.reset(f.nextNode.sealedTo, -1, nextSoftMinStart);
                    //System.err.println("inspecting from "+nextNode+"["+nextNode.deque.phraseIndex+"] "+nextSoftMinStart+", "+next.driver.startPosition()+", !"+nextNode.sealedThreshold+">=<"+remainingSlop);
                    StackFrame rf = frames[++phraseIndex];
                    rf.initArgs(f.nextNode, nextHardMinStart, nextSoftMinStart, nextSoftMinStart + f.remainingSlop + 1, nextMinEnd, f.remainingSlop);
                    f = rf;
                    continue pseudoRecurse;
                    //System.err.println("rste="+remainingSlopToEnd+ " for "+nextNode+"["+nextNode.deque.phraseIndex+"]");
                  }
                  break;
                case NONE:
                  f.revisitFrom = null;
                  int nextHardMinStart = Math.min(d.driver.getMinEnd(), d.driver.getMinStart() + 2);
                  int nextSoftMinStart = f.nextNode.endPosition();
                  int nextMinEnd = d.next.next == null ? -1 : Math.min(nextHardMinStart + 1, d.next.next.driver.getMinStart());
                  d.next.driver.reset(-1, nextSoftMinStart);
                  //System.err.println("not cached; inspecting from " + nextNode + "[" + nextNode.deque.phraseIndex + "] " + softMinStart + ", " + next.driver.startPosition());
                  StackFrame rf = frames[++phraseIndex];
                  rf.initArgs(f.nextNode, nextHardMinStart, nextSoftMinStart, nextSoftMinStart + f.remainingSlop + 1, nextMinEnd, f.remainingSlop);
                  f = rf;
                  continue pseudoRecurse;
                //System.err.println("rste=" + remainingSlopToEnd + " for " + nextNode + "[" + nextNode.deque.phraseIndex + "]");
                default:
                  throw new AssertionError();
              }
            }
          }
          if (f.remainingSlopToEnd >= 0) {
            // we *can* get to phrase end via this route
            if (!hasMatch) {
              hasMatch = true;
            }
            if (f.keepUpdatingSealedTo) {
              f.updateSealedTo = f.nextNode.index;
              if (f.nextNode.sealed == Node.SlopStatus.SEALED) {
                f.updateSealedToThreshold = 0;
              } else {
                // although we can currently get to end via this node, with less slop more paths may become available
                f.updateSealedToThreshold = -1; // even slightest bit more slop could result in more matches.
                f.keepUpdatingSealedTo = false;
              }
              //System.err.println("set3 updateSealedToThreshold=" + updateSealedToThreshold + ", " + updateSealedTo + ", " + nextNode.index + ", " + nextNode);
            }
            if (f.remainingSlop > f.nextNode.maxSlopRemainingToStart) {
              f.nextNode.maxSlopRemainingToStart = f.remainingSlop;
              f.nextNode.maxSlopRemainingPhrasePrev = f.caller;
            }
            final int gapFromCallerToNext = f.start - f.softMinStart;
            f.maxSlopRemainingCandidate = f.remainingSlopToEnd - gapFromCallerToNext;
            SLLNode linkFromCaller = null;
            if (f.previousMaxSlopToCaller + f.maxSlopRemainingCandidate < allowedSlop) {
              linkFromCaller = initNodeLinks(f.caller, f.nextNode, d.returned, f.remainingSlop, passId, comboMode);
            } else {
              // we have probably already created a link between caller and nextNode
              switch (comboMode) {
                default:
                  if (f.revisitFrom == null) {
                    break;
                  }
                case FULL_DISTILLED:
                case FULL_DISTILLED_PER_POSITION:
                case FULL_DISTILLED_PER_START_POSITION:
                  linkFromCaller = f.caller.phraseNext;
                  while (linkFromCaller.node.phraseNext != f.nextNode) {
                    linkFromCaller = linkFromCaller.next;
                    if (linkFromCaller == null) {
                      throw new AssertionError("this shouldn't happen, but uncomment below 2 lines if it does");
                      //                    linkFromCaller = initNodeLinks(caller, nextNode, returned, remainingSlop, passId, comboMode);
                      //                    break;
                    }
                  }
              }
            }
            if (f.revisitFrom != null) {
              d.revisit.add(linkFromCaller, f.revisitFrom, passId);
            } else if (f.nextNode.initialzedReverseLinks == null) {
              f.nextNode.initialzedReverseLinks = d.returned.add(f.nextNode);
            }
            if (f.caller != root) {
              switch (comboMode) {
                case FULL_DISTILLED:
                case FULL_DISTILLED_PER_POSITION:
                case FULL_DISTILLED_PER_START_POSITION:
                  if (f.nextNode.associatedNodes != null) {
                    if (f.remainingSlopToCaller - gapFromCallerToNext <= allowedSlop) {
                      d.associateWithEndNode(f.caller, f.nextNode, allowedSlop - gapFromCallerToNext);
                    }
                  } else {
                    FullEndNodeLink endNodeLink;
                    if (linkFromCaller.revisitEndNode != null) {
                      endNodeLink = linkFromCaller.revisitEndNode;
                    } else {
                      endNodeLink = f.nextNode.endNodeLinks.prev;
                    }
                    FullEndNodeLink revisitFromEndNode = null;
                    final FullEndNodeLink anchor = endNodeLink.anchor;
                    do {
                      int remainingSlopCandidate = endNodeLink.remainingSlopToEnd - gapFromCallerToNext;
                      if (f.remainingSlopToCaller + remainingSlopCandidate >= allowedSlop) {
                        d.associateWithEndNode(f.caller, endNodeLink.endNode, remainingSlopCandidate);
                      } else if (revisitFromEndNode == null && remainingSlopCandidate >= 0) {
                        revisitFromEndNode = endNodeLink;
                      }
                    } while ((endNodeLink = endNodeLink.prev) != anchor);
                    linkFromCaller.revisitEndNode = revisitFromEndNode;
                  }
              }
            }
            if (f.leastSloppyPathToPhraseEnd == null || f.maxSlopRemainingCandidate > f.maxSlopRemainingToPhraseEnd) {
              f.leastSloppyPathToPhraseEnd = f.nextNode;
              f.maxSlopRemainingToPhraseEnd = f.maxSlopRemainingCandidate;
              f.maxSlopRemainingEndNode = f.nextNode.maxSlopRemainingEndNodeLink.endNode;
            }
          } else if (f.keepUpdatingSealedTo) {
            f.updateSealedTo = f.nextNode.index;
            if (f.nextNode.sealed == Node.SlopStatus.SEALED) {
              // we will never be able to get to the end via this node, and may cache through this point
              f.updateSealedToThreshold = 0;
            } else {
              // we cannot currently get to the end via this node, but might be able to later, and can cache up to this point
              f.updateSealedToThreshold = -1; // even slightest bit more slop could result in a match.
              f.keepUpdatingSealedTo = false;
            }
            //System.err.println("set2 updateSealedToThreshold="+updateSealedToThreshold+", "+updateSealedTo+", "+nextNode.index+", "+nextNode);
          }
          if (comboMode == ComboMode.GREEDY_END_POSITION && // only want one match
              ((d.next == null && (f.remainingSlop >= 0 || assign(f.start = ~f.start))) || // reached end irrespective of slop constraints
              f.remainingSlop + f.maxSlopRemainingToPhraseEnd >= allowedSlop)) { // have cached path to end within slop constraints
            break loopAtLevel;
          } else {
            if (d.prev != null || comboMode == ComboMode.GREEDY_END_POSITION) {
              switch (comboMode) {
                case FULL:
                case FULL_DISTILLED:
                case FULL_DISTILLED_PER_POSITION:
                case FULL_DISTILLED_PER_START_POSITION:
                case PER_POSITION_PER_START_POSITION:
                  break;
                default:
                  final boolean perPosition = comboMode == ComboMode.PER_POSITION;
                  if (d.next == null) {
                    // progress until you find position of different end
                    final int extantEnd = f.nextNode.endPosition();
                    while ((f.start = d.driver.nextMatch(f.hardMinStart, f.softMinStart, f.startCeiling, f.minEnd)) >= 0 && assign(0, f.nextNode = d.getLastNode(), true)) {
                      if ((perPosition && f.nextNode.output == 0) || f.nextNode.endPosition() != extantEnd) {
                        continue loopAtLevel;
                      }
                    }
                  } else {
                    final int extantEnd = f.nextNode.endPosition();
                    final int extantWidth = extantEnd - f.nextNode.startPosition();
                    int checkFor = 0;
                    if (!perPosition && (checkFor = d.checkForIncreasedMatchLength(extantWidth, f.leastSloppyPathToPhraseEnd == null, f.startCeiling - f.softMinStart > 1)) == 0) {
                      f.start = ~f.start;
                    } else {
                      final int decreasedEndStartCeiling = extantEnd - 1;
                      // progress until you find position of decreased end or different width
                      checkLoop:
                      while ((f.start = d.driver.nextMatch(f.hardMinStart, f.softMinStart, f.startCeiling, f.minEnd)) >= 0 && PositionDeque.assign(0, f.nextNode = d.getLastNode(), true)) {
                        if (perPosition) {
                          if (f.nextNode.output == 0) {
                            continue loopAtLevel;
                          }
                        } else {
                          switch (checkFor) {
                            default:
                              throw new AssertionError("this should never happen");
                            case PositionDeque.DECREASED_END_POSITION:
                              if (f.start >= decreasedEndStartCeiling) {
                                break checkLoop;
                              } else if (f.nextNode.endPosition() < extantEnd) {
                                continue loopAtLevel;
                              }
                              break;
                            case PositionDeque.INCREASED_WIDTH:
                              if (f.nextNode.endPosition() - f.start > extantWidth) {
                                continue loopAtLevel;
                              }
                              break;
                            case PositionDeque.DECREASED_END_POSITION | PositionDeque.INCREASED_WIDTH:
                              final int nextEnd = f.nextNode.endPosition();
                              if (nextEnd < extantEnd || nextEnd - f.start > extantWidth) {
                                continue loopAtLevel;
                              } else if (f.start >= decreasedEndStartCeiling) {
                                checkFor ^= PositionDeque.DECREASED_END_POSITION;
                              }
                              break;
                          }
                        }
                      }
                    }
                  }
                  break loopAtLevel;
              }
            }
            if ((f.start = d.driver.nextMatch(f.hardMinStart, f.softMinStart, f.startCeiling, f.minEnd)) < 0) {
              // exhasted further matches, for now
              break loopAtLevel;
            } else {
              f.nextNode = d.getLastNode();
            }
          }
        } while (true);
        if (f.maxSlopRemainingToPhraseEnd > f.caller.maxSlopRemainingToEnd) {
          f.caller.maxSlopRemainingToEnd = f.maxSlopRemainingToPhraseEnd;
          f.caller.maxSlopRemainingPhraseNext = f.leastSloppyPathToPhraseEnd;
          if (f.caller != root) {
            DLLEndNodeLink extantEndNodeLink = f.caller.maxSlopRemainingEndNodeLink;
            if (extantEndNodeLink == null) {
              f.caller.maxSlopRemainingEndNodeLink = f.maxSlopRemainingEndNode.maxSlopRemainingEndNodeLink.add(f.caller);
            } else if (extantEndNodeLink.endNode != f.maxSlopRemainingEndNode) {
              extantEndNodeLink.remove();
              f.caller.maxSlopRemainingEndNodeLink = f.maxSlopRemainingEndNode.maxSlopRemainingEndNodeLink.add(f.caller);
            }
          }
        }
        if (f.caller != root) {
          int overshot;
          if (f.updateSealedToThreshold < 0) {
            f.caller.sealedTo = f.updateSealedTo;
            f.caller.sealed = Node.SlopStatus.INITIALIZED;
            assert f.updateSealedToThreshold != Integer.MIN_VALUE;
            f.caller.sealedThreshold = f.updateSealedToThreshold == Integer.MIN_VALUE ? f.remainingSlopToCaller : f.remainingSlopToCaller - f.updateSealedToThreshold;
            //System.err.println("initialize sealedThreshold1 "+caller+"["+caller.deque.phraseIndex+"]="+caller.sealedThreshold+", "+caller.maxSlopRemainingToEnd);
          } else if ((overshot = ~f.start - f.softMinStart) > allowedSlop) {
            f.caller.sealed = Node.SlopStatus.SEALED; // final slop-to-end has been calculated for the calling node.
            //System.err.println("initialize sealedThreshold3 "+caller+"["+caller.deque.phraseIndex+"], "+caller.maxSlopRemainingToEnd);
          } else {
            f.caller.sealedTo = f.nextNode.index + 1;
            f.caller.sealed = Node.SlopStatus.INITIALIZED;
            f.caller.sealedThreshold = f.remainingSlopToCaller + overshot;
            //System.err.println("initialize sealedThreshold2 "+caller+"["+caller.deque.phraseIndex+"]="+caller.sealedThreshold+", "+caller.maxSlopRemainingToEnd);
          }
        }
      } else if (f.start == Integer.MIN_VALUE && !hasMatch) {
        // we have to check d.isEmpty because stored spans could still be used for other matches,
        // and we have to check d.driver.startPosition() because f.start could have been set by lookahead,
        // without actually advancing the backing spans.
        if ((d.isEmpty() && d.driver.startPosition() == Spans.NO_MORE_POSITIONS) || !mayStillMatchByShrink(phraseIndex, frames)) {
          return Integer.MAX_VALUE;
        }
      }
      //System.err.println("RETURN "+maxSlopRemainingToPhraseEnd+"(/"+caller.maxSlopRemainingToEnd+") for "+caller+" via "+leastSloppyPathToPhraseEnd+" (phraseIndex="+(caller.deque == null ? null : caller.deque.phraseIndex)+")");
      if (phraseIndex == 0) {
        return comboMode != ComboMode.GREEDY_END_POSITION ? -1 : f.remainingSlopToEnd;
      } else {
        f = frames[--phraseIndex];
        f.pseudoReturn = true;
        continue pseudoRecurse;
      }
    } while (true); // pseudoRecurse
  }
  private static final int DECREASED_END_POSITION = 1, INCREASED_WIDTH = 1 << 1;

  private static boolean mayStillMatchByShrink(RecordingPushbackSpans rps) throws IOException {
    while ((rps = rps.previous) != null) {
      if (rps.endPositionDecreaseCeiling() > 0) {
        return true;
      }
    }
    return false;
  }

  private static boolean mayStillMatchByShrink(int i, StackFrame[] frames) throws IOException {
    while (i-- > 0) {
      if (frames[i].d.driver.endPositionDecreaseCeiling() > 0) {
        return true;
      }
    }
    return false;
  }
  /**
   * Based on indexed data (if available), determine whether it is possible for subsequent positions to expose new
   * match possibilities (and thus, whether to continue advancing in search of a(nother) match). Determine based on
   * state (whether a match has been found) and configuration whether to continue searching for a(nother) match.
   * 
   * @param extantWidth
   * @param noMatchYet
   * @return 
   */
  private int checkForIncreasedMatchLength(int extantWidth, boolean noMatchYet, boolean checkForDecreasedEnd) throws IOException {
    if (comboMode == ComboMode.GREEDY_END_POSITION && !noMatchYet) {
      return 0;
    } else if (driver.lookaheadBacking == null) {
      // positionLengthCeiling lookahead not supported, fallback to default
      if (driver.variablePositionLength || supportVariableTermSpansLength || noMatchYet) {
        return checkForDecreasedEnd ? DECREASED_END_POSITION | INCREASED_WIDTH : INCREASED_WIDTH;
      } else {
        return 0;
      }
    } else {
      final int positionLengthCeiling = driver.lookaheadBacking.positionLengthCeiling();
      if (positionLengthCeiling > extantWidth) {
        // we know there are longer matches than this.
        return INCREASED_WIDTH;
      } else if (positionLengthCeiling <= IndexLookahead.MAX_SPECIAL_VALUE) {
        // positionLengthCeiling lookahead not supported in index, fallback to default
        if (driver.variablePositionLength || supportVariableTermSpansLength || noMatchYet) {
          return checkForDecreasedEnd ? DECREASED_END_POSITION | INCREASED_WIDTH : INCREASED_WIDTH;
        } else {
          return 0;
        }
      } else if (positionLengthCeiling < 0) {
        // we know that positionLength may decrease enough to expose new downstream matches
        if (~positionLengthCeiling > extantWidth) {
          return checkForDecreasedEnd ? DECREASED_END_POSITION | INCREASED_WIDTH : INCREASED_WIDTH;
        } else if (checkForDecreasedEnd) {
          return DECREASED_END_POSITION;
        } else {
          return 0;
        }
      } else {
        // we know there are no longer matches than current
        return 0;
      }
    }
  }

  private void associateWithEndNode(Node caller, Node endNode, int remainingSlopCandidate) {
    LongObjectHashMap<FullEndNodeLink> associated = endNode.associatedNodes;
    int idx = associated.indexOf(caller.phraseScopeId);
    FullEndNodeLink extantEndNodeLink;
    if (idx < 0) {
      FullEndNodeLink newLink = caller.endNodeLinks.add(endNode, remainingSlopCandidate);
      associated.put(caller.phraseScopeId, newLink);
    } else if (remainingSlopCandidate < (extantEndNodeLink = associated.indexGet(idx)).remainingSlopToEnd) {
      extantEndNodeLink.remove();
      FullEndNodeLink newLink = caller.endNodeLinks.add(endNode, remainingSlopCandidate);
      associated.indexReplace(idx, newLink);
    }
  }

  private static boolean greedyInitNodeLinks(Node caller, Node nextNode, int passId) {
    final DLLNode linkNode;
    DLLNode extantPhrasePrev = nextNode.phrasePrev;
    if (extantPhrasePrev == null) {
      linkNode = DLLNode.add(caller, nextNode, passId);
    } else {
      if (extantPhrasePrev.passId != passId) {
        nextNode.resetBacklinks();
      }
      linkNode = DLLNode.add(caller, nextNode, extantPhrasePrev, passId);
    }
    nextNode.phrasePrev = linkNode;
    SLLNode linkFromCaller = SLLNode.add(nextNode, linkNode, caller.phraseNext);
    caller.phraseNext = linkFromCaller;
    return true;
  }
  private static SLLNode initNodeLinks(Node caller, Node nextNode, DLLReturnNode returned, int slopRemainingToNext, int passId, ComboMode comboMode) {
//    // This is if only first and last returned nodes are tracked
//    if (caller.deque == null) {
//      returned.add(nextNode);
//    }
    //System.err.println("initNodeLinks " + caller + " <-> " + nextNode);
    final DLLNode linkNode;
    DLLNode extantPhrasePrev = nextNode.phrasePrev;
    if (extantPhrasePrev == null) {
      linkNode = DLLNode.add(caller, nextNode, passId);
    } else {
      if (extantPhrasePrev.passId != passId) {
        nextNode.resetBacklinks();
      }
      linkNode = DLLNode.add(caller, nextNode, extantPhrasePrev, passId);
    }
    nextNode.phrasePrev = linkNode;
    SLLNode linkFromCaller = SLLNode.add(nextNode, linkNode, caller.phraseNext);
    caller.phraseNext = linkFromCaller;
    if (slopRemainingToNext > nextNode.maxSlopRemainingToStart) {
      nextNode.maxSlopRemainingToStart = slopRemainingToNext;
      nextNode.maxSlopRemainingPhrasePrev = caller;
    }
    //System.err.println("add link "+caller+"["+(caller.deque == null ? null : caller.deque.phraseIndex)+"]=>"+nextNode+"["+nextNode.deque.phraseIndex+"]");
    return linkFromCaller;
  }

  private static boolean assign(int val, Object obj, boolean ret) {
    return ret;
  }

  private static boolean assign(boolean ret, Object obj1, Object obj2) {
    return ret;
  }

  private static boolean assign(boolean ret, Object obj1, Object obj2, int val, Object obj4) {
    return ret;
  }

  void popLast() {
    if (headNode != null) headNode.remove();
  }
  
  private Node getActiveNode() {
    return null;//(iterNode != null) ? iterNode : 
  }

  private static final short VALID = 1, VALID_MAXQ = 1 << 1, IN_MAXQ = 1 << 2, VALID_MINQ = 1 << 3, IN_MINQ = 1 << 4, ACTIVE = 1 << 5, PROVISIONAL = 1 << 6;

  final LinkPool pool;

  static final class LinkPool {
    private final LinkTypeGroup h = new LinkTypeGroup(); // heads
    private final LinkTypeGroup t = new LinkTypeGroup(); // tails
    public void addDLLEndNodeLink(RefLinkTracker<DLLEndNodeLink> v) {
      final RefLinkTracker<DLLEndNodeLink> extantHead = h.dllEndNodeLink();
      if (extantHead != null) {
        v.next = extantHead;
        h.dllEndNodeLink(v);
      } else {
        h.dllEndNodeLink(v);
        t.dllEndNodeLink(v);
      }
    }
    public void addDLLNode(RefLinkTracker<DLLNode> v) {
      final RefLinkTracker<DLLNode> extantHead = h.dllNode();
      if (extantHead != null) {
        v.next = extantHead;
        h.dllNode(v);
      } else {
        h.dllNode(v);
        t.dllNode(v);
      }
    }
    public void addDLLReturnNode(RefLinkTracker<DLLReturnNode> v) {
      final RefLinkTracker<DLLReturnNode> extantHead = h.dllReturnNode();
      if (extantHead != null) {
        v.next = extantHead;
        h.dllReturnNode(v);
      } else {
        h.dllReturnNode(v);
        t.dllReturnNode(v);
      }
    }
    public void addFullEndNodeLink(RefLinkTracker<FullEndNodeLink> v) {
      final RefLinkTracker<FullEndNodeLink> extantHead = h.fullEndNodeLink();
      if (extantHead != null) {
        v.next = extantHead;
        h.fullEndNodeLink(v);
      } else {
        h.fullEndNodeLink(v);
        t.fullEndNodeLink(v);
      }
    }
    public void addRevisitNode(RefLinkTracker<RevisitNode> v) {
      final RefLinkTracker<RevisitNode> extantHead = h.revisitNode();
      if (extantHead != null) {
        v.next = extantHead;
        h.revisitNode(v);
      } else {
        h.revisitNode(v);
        t.revisitNode(v);
      }
    }
    public void addSLLNode(RefLinkTracker<SLLNode> v) {
      final RefLinkTracker<SLLNode> extantHead = h.sllNode();
      if (extantHead != null) {
        v.next = extantHead;
        h.sllNode(v);
      } else {
        h.sllNode(v);
        t.sllNode(v);
      }
    }
    public void addStoredPostings(RefLinkTracker<StoredPostings> v) {
      final RefLinkTracker<StoredPostings> extantHead = h.storedPostings();
      if (extantHead != null) {
        v.next = extantHead;
        h.storedPostings(v);
      } else {
        h.storedPostings(v);
        t.storedPostings(v);
      }
    }
    public DLLEndNodeLink getDLLEndNodeLink() {
      final RefLinkTracker<DLLEndNodeLink> ret = h.dllEndNodeLink();
      if (ret == null) {
        return null;
      } else if (ret == t.dllEndNodeLink()) {
        h.dllEndNodeLink(null);
        t.dllEndNodeLink(null);
      } else {
        h.dllEndNodeLink(ret.next);
      }
      return ret.ref;
    }
    public DLLNode getDLLNode() {
      final RefLinkTracker<DLLNode> ret = h.dllNode();
      if (ret == null) {
        return null;
      } else if (ret == t.dllNode()) {
        h.dllNode(null);
        t.dllNode(null);
      } else {
        h.dllNode(ret.next);
      }
      return ret.ref;
    }
    public DLLReturnNode getDLLReturnNode() {
      final RefLinkTracker<DLLReturnNode> ret = h.dllReturnNode();
      if (ret == null) {
        return null;
      } else if (ret == t.dllReturnNode()) {
        h.dllReturnNode(null);
        t.dllReturnNode(null);
      } else {
        h.dllReturnNode(ret.next);
      }
      return ret.ref;
    }
    public FullEndNodeLink getFullEndNodeLink() {
      final RefLinkTracker<FullEndNodeLink> ret = h.fullEndNodeLink();
      if (ret == null) {
        return null;
      } else if (ret == t.fullEndNodeLink()) {
        h.fullEndNodeLink(null);
        t.fullEndNodeLink(null);
      } else {
        h.fullEndNodeLink(ret.next);
      }
      return ret.ref;
    }
    public RevisitNode getRevisitNode() {
      final RefLinkTracker<RevisitNode> ret = h.revisitNode();
      if (ret == null) {
        return null;
      } else if (ret == t.revisitNode()) {
        h.revisitNode(null);
        t.revisitNode(null);
      } else {
        h.revisitNode(ret.next);
      }
      return ret.ref;
    }
    public SLLNode getSLLNode() {
      final RefLinkTracker<SLLNode> ret = h.sllNode();
      if (ret == null) {
        return null;
      } else if (ret == t.sllNode()) {
        h.sllNode(null);
        t.sllNode(null);
      } else {
        h.sllNode(ret.next);
      }
      return ret.ref;
    }
    public StoredPostings getStoredPostings() {
      final RefLinkTracker<StoredPostings> ret = h.storedPostings();
      if (ret == null) {
        return null;
      } else if (ret == t.storedPostings()) {
        h.storedPostings(null);
        t.storedPostings(null);
      } else {
        h.storedPostings(ret.next);
      }
      return ret.ref;
    }

    public void returnLinksToPool(final LinkPool r) {
      final LinkTypeGroup rh = r.h;
      final LinkTypeGroup rt = r.t;
      for (int i = LinkTypeGroup.LINK_TYPE_GROUP_SIZE - 1; i >= 0; i--) {
        if (rh.links[i] != null) {
          if (h.links[i] == null) {
            h.links[i] = rh.links[i];
            t.links[i] = rt.links[i];
          } else {
            rt.links[i].next = h.links[i];
            h.links[i] = rh.links[i];
          }
          rh.links[i] = null;
          rt.links[i] = null;
        }
      }
    }
  }

  static final class RefLinkTracker<T> {
    private final T ref;
    private RefLinkTracker<T> next;
    public RefLinkTracker(T ref) {
      this.ref = ref;
    }
  }

  private static final class LinkTypeGroup {

    private static final int LINK_TYPE_GROUP_SIZE = 7;
    private final RefLinkTracker[] links = new RefLinkTracker[LINK_TYPE_GROUP_SIZE];
    private RefLinkTracker<DLLEndNodeLink> dllEndNodeLink() {
      return links[0];
    }
    private RefLinkTracker<DLLNode> dllNode() {
      return links[1];
    }
    private RefLinkTracker<DLLReturnNode> dllReturnNode() {
      return links[2];
    }
    private RefLinkTracker<FullEndNodeLink> fullEndNodeLink() {
      return links[3];
    }
    private RefLinkTracker<RevisitNode> revisitNode() {
      return links[4];
    }
    private RefLinkTracker<SLLNode> sllNode() {
      return links[5];
    }
    private RefLinkTracker<StoredPostings> storedPostings() {
      return links[6];
    }
    private void dllEndNodeLink(RefLinkTracker<DLLEndNodeLink> rlt) {
      links[0] = rlt;
    }
    private void dllNode(RefLinkTracker<DLLNode> rlt) {
      links[1] = rlt;
    }
    private void dllReturnNode(RefLinkTracker<DLLReturnNode> rlt) {
      links[2] = rlt;
    }
    private void fullEndNodeLink(RefLinkTracker<FullEndNodeLink> rlt) {
      links[3] = rlt;
    }
    private void revisitNode(RefLinkTracker<RevisitNode> rlt) {
      links[4] = rlt;
    }
    private void sllNode(RefLinkTracker<SLLNode> rlt) {
      links[5] = rlt;
    }
    private void storedPostings(RefLinkTracker<StoredPostings> rlt) {
      links[6] = rlt;
    }
  }

  public static final class DLLReturnNode {

    private final DLLReturnNode anchor;
    public Node node;
    DLLReturnNode prev;
    DLLReturnNode next;
    private int currentStart;
    private final RefLinkTracker<DLLReturnNode> trackNextRef = new RefLinkTracker<>(this);
    private int greedySlopRemaining = Integer.MIN_VALUE;

    int getGreedySlopRemaining() {
      return ENABLE_GREEDY_RETURN ? greedySlopRemaining : next.node.maxSlopRemainingEndNodeLink.endNode.maxSlopRemainingToStart;
    }

    public DLLReturnNode() {
      this.anchor = this;
      this.node = null;
      this.next = anchor;
      this.prev = anchor;
    }
    
    private void setCurrentStart(int startPosition) {
      this.currentStart = startPosition;
    }

    private boolean setGreedyWidth(int slopRemaining) {
      this.greedySlopRemaining = slopRemaining;
      return slopRemaining >= 0;
    }

    int getCurrentStart() {
      return currentStart;
    }

    private DLLReturnNode(DLLReturnNode anchor) {
      this.anchor = anchor;
    }

    public boolean isEmpty() {
      return prev == anchor;
    }

    private static DLLReturnNode newInstance(DLLReturnNode anchor, Node n) {
      final PositionDeque d = n.dequePointer[0];
      DLLReturnNode ret = d.pool.getDLLReturnNode();
      if (ret == null) {
        ret = new DLLReturnNode(anchor);
        if (TRACK_POOLING) {
          d.dllReturnNodeCt++;
        }
      } else if (TRACK_POOLING) {
        d.dllReturnNodeCtR++;
      }
      n.pool.addDLLReturnNode(ret.trackNextRef);
      return ret;
    }

    private void init(Node node, DLLReturnNode next) {
      this.prev = anchor;
      this.node = node;
      this.next = next;
    }

    public DLLReturnNode add(Node node) {
      DLLReturnNode extant = anchor.next;
      DLLReturnNode ret = newInstance(anchor, node);
      ret.init(node, extant);
      anchor.next = ret;
      extant.prev = ret;
      return ret;
    }
    
    public void clear() {
      assert this == anchor;
      prev = anchor;
      next = anchor;
    }

    /**
     *
     * @return true if list is empty as result of this call
     */
    public boolean remove() {
      prev.next = next;
      next.prev = prev;
      return next == prev; // this will also incidentally be == anchor
    }
    
  }
  
  /**
   * Used to track Nodes that have been included in results based on potentially incomplete cached information. A second
   * pass is necessary to revisit these nodes and potentially update them with current slop information.
   */
  static final class RevisitNode {

    private final RevisitNode anchor;
    public Node phrasePrev;
    public Node phraseNext;
    public SLLNode updateFromOnRevisit;
    private int[] refCount;
    private RevisitNode prev;
    private RevisitNode next;
    private final RefLinkTracker<RevisitNode> trackNextRef = new RefLinkTracker<>(this);

    public RevisitNode() {
      this.phrasePrev = null;
      this.phraseNext = null;
      this.updateFromOnRevisit = null;
      this.refCount = null;
      this.anchor = this;
    }

    private RevisitNode(RevisitNode anchor) {
      this.anchor = anchor;
    }

    private static RevisitNode newInstance(RevisitNode anchor, Node n) {
      final PositionDeque d = n.dequePointer[0];
      RevisitNode ret = d.pool.getRevisitNode();
      if (ret == null) {
        ret = new RevisitNode(anchor);
        if (TRACK_POOLING) {
          d.revisitNodeCt++;
        }
      } else if (TRACK_POOLING) {
        d.revisitNodeCtR++;
      }
      n.pool.addRevisitNode(ret.trackNextRef);
      return ret;
    }

    private RevisitNode init(Node phrasePrev, Node phraseNext, SLLNode updateFromOnRevisit, RevisitNode anchor, RevisitNode next, int[] refCount) {
      this.phrasePrev = phrasePrev;
      this.phraseNext = phraseNext;
      this.updateFromOnRevisit = updateFromOnRevisit;
      this.prev = anchor;
      this.next = next;
      this.refCount = refCount;
      return this;
    }

    public RevisitNode add(SLLNode linkNode, SLLNode updateFromOnRevisit, final int passId) {
      RevisitNode ret = linkNode.getRevisitNode(updateFromOnRevisit, this, this.next, passId);
      if (this.next != null) {
        this.next.prev = ret;
      }
      this.next = ret;
      return ret;
    }
    
    public void remove() {
      prev.next = next;
      if (next != null) {
        next.prev = prev;
      } else if (prev == anchor) {
        // was the last entry
      }
    }
    
    public void clear() {
      this.next = null;
    }

  }

  private static final class DLLEndNodeLink {
    private Node node;
    private Node endNode;
    private DLLEndNodeLink prev;
    private DLLEndNodeLink next;
    private final RefLinkTracker<DLLEndNodeLink> trackNextRef = new RefLinkTracker<>(this);

    private static DLLEndNodeLink newPooledInstance(Node n) {
      final PositionDeque d = n.dequePointer[0];
      DLLEndNodeLink ret = d.pool.getDLLEndNodeLink();
      if (ret == null) {
        ret = new DLLEndNodeLink();
        if (TRACK_POOLING) {
          d.dllEndNodeLinkCt++;
        }
      } else if (TRACK_POOLING) {
        d.dllEndNodeLinkCtR++;
      }
      n.pool.addDLLEndNodeLink(ret.trackNextRef);
      return ret;
    }

    public static DLLEndNodeLink newInstance(Node node) {
      DLLEndNodeLink ret = newPooledInstance(node);
      ret.init(node, node, null, null);
      return ret;
    }

    private DLLEndNodeLink init(Node node, Node endNode, DLLEndNodeLink prev, DLLEndNodeLink next) {
      this.node = node;
      this.endNode = endNode;
      this.prev = prev;
      this.next = next;
      if (next != null) {
        next.prev = this;
      }
      return this;
    }
    
    public DLLEndNodeLink add(Node node) {
      DLLEndNodeLink ret = newPooledInstance(node);
      ret.init(node, endNode, this, next);
      next = ret;
      return ret;
    }

    public void remove() {
      prev.next = next;
      if (next != null) {
        next.prev = prev;
      }
    }
  }
  
  private static final class FullEndNodeLink {
    private FullEndNodeLink anchor;
    private Node node;
    private Node endNode;
    private FullEndNodeLink prev;
    private FullEndNodeLink next;
    private int remainingSlopToEnd;
    private final RefLinkTracker<FullEndNodeLink> trackNextRef = new RefLinkTracker<>(this);

    public FullEndNodeLink initBootstrap(Node node) {
      this.remainingSlopToEnd = -1;
      this.node = node;
      this.endNode = null;
      this.next = this;
      this.prev = this;
      this.anchor = this;
      return this;
    }

    private static FullEndNodeLink newInstance(Node n) {
      final PositionDeque d = n.dequePointer[0];
      FullEndNodeLink ret = d.pool.getFullEndNodeLink();
      if (ret == null) {
        ret = new FullEndNodeLink();
        if (TRACK_POOLING) {
          d.fullEndNodeLinkCt++;
        }
      } else if (TRACK_POOLING) {
        d.fullEndNodeLinkCtR++;
      }
      n.pool.addFullEndNodeLink(ret.trackNextRef);
      return ret.initBootstrap(n);
    }

    private static FullEndNodeLink newInstance(FullEndNodeLink anchor, Node n) {
      final PositionDeque d = n.dequePointer[0];
      FullEndNodeLink ret = d.pool.getFullEndNodeLink();
      if (ret == null) {
        ret = new FullEndNodeLink();
        if (TRACK_POOLING) {
          d.fullEndNodeLinkCt++;
        }
      } else if (TRACK_POOLING) {
        d.fullEndNodeLinkCtR++;
      }
      n.pool.addFullEndNodeLink(ret.trackNextRef);
      ret.anchor = anchor;
      return ret;
    }

    private FullEndNodeLink init(Node node, Node endNode, FullEndNodeLink prev, FullEndNodeLink next, int remainingSlopToEnd) {
      this.node = node;
      this.endNode = endNode;
      this.prev = prev;
      this.next = next;
      next.prev = this;
      this.remainingSlopToEnd = remainingSlopToEnd;
      return this;
    }
    
    public FullEndNodeLink add(Node endNode, int remainingSlopToEnd) {
      FullEndNodeLink ret = newInstance(anchor, node).init(node, endNode, this, next, remainingSlopToEnd);
      next = ret;
      return ret;
    }

    public void clear() {
      assert this == anchor;
      this.next = this;
      this.prev = this;
    }
    
    public void remove() {
      prev.next = next;
      next.prev = prev;
    }
  }
  
  private static final class DLLNode {

    public static final DLLNode EMPTY_LIST = new DLLNode();
    public Node phrasePrev;
    public Node phraseNext;
    private DLLNode prev;
    private DLLNode next;
    private int passId;
    private final RefLinkTracker<DLLNode> trackNextRef = new RefLinkTracker<>(this);

    @Override
    public String toString() {
      return phrasePrev + "=>" + phraseNext+"["+(phrasePrev.dequePointer == null ? null : phrasePrev.dequePointer[0].phraseIndex)+"]";
    }

    private DLLNode init(Node phrasePrev, Node phraseNext, DLLNode next, int passId) {
      this.phrasePrev = phrasePrev;
      this.phraseNext = phraseNext;
      this.next = next;
      this.passId = passId;
      this.prev = null;
      return this;
    }

    private static DLLNode newInstance(Node n) {
      final PositionDeque d = n.dequePointer[0];
      DLLNode ret = d.pool.getDLLNode();
      if (ret == null) {
        ret = new DLLNode();
        if (TRACK_POOLING) {
          d.dllNodeCt++;
        }
      } else if (TRACK_POOLING) {
        d.dllNodeCtR++;
      }
      n.pool.addDLLNode(ret.trackNextRef);
      return ret;
    }

    public static DLLNode newInstance(Node phrasePrev, Node phraseNext, DLLNode next, int passId) {
      DLLNode ret = newInstance(phraseNext);
      ret.init(phrasePrev, phraseNext, next, passId);
      return ret;
    }

    public static DLLNode add(Node phrasePrev, Node phraseNext, int passId) {
      return newInstance(phraseNext).init(phrasePrev, phraseNext, null, passId);
    }

    public static DLLNode add(Node phrasePrev, Node phraseNext, DLLNode extant, int passId) {
      DLLNode ret = newInstance(phraseNext).init(phrasePrev, phraseNext, extant, passId);
      extant.prev = ret;
      return ret;
    }

    /**
     * 
     * @return null if head of list not changed as result of this call; EMPTY_LIST sentinel value
     * if list is empty as a result of this call; otherwise the new head of this list
     */
    public DLLNode remove() {
      if (next == null) {
        if (prev == null) {
          return EMPTY_LIST;
        } else {
          prev.next = null;
          return null;
        }
      } else if (prev == null) {
        next.prev = null;
        return next;
      } else {
        prev.next = next;
        next.prev = prev;
        return null;
      }
    }
  }
  
  public static final class SLLNode {
    public DLLNode node;
    public SLLNode next;
    private RevisitNode revisitNode;
    private FullEndNodeLink revisitEndNode;
    private int lastPassId = -1;
    private final RefLinkTracker<SLLNode> trackNextRef = new RefLinkTracker<>(this);

    private SLLNode init(DLLNode node, SLLNode next) {
      this.node = node;
      this.next = next;
      return this;
    }

    private void clear() {
      revisitNode = null;
      revisitEndNode = null;
      lastPassId = -1;
    }

    private static SLLNode newInstance(Node n) {
      final PositionDeque d = n.dequePointer[0];
      SLLNode ret = d.pool.getSLLNode();//.getSLLNode();
      if (ret != null) {
        if (TRACK_POOLING) {
          d.sllNodeCtR++;
        }
        ret.clear();
      } else {
        ret = new SLLNode();
        if (TRACK_POOLING) {
          d.sllNodeCt++;
        }
      }
      n.pool.addSLLNode(ret.trackNextRef);
      return ret;
    }

    private static SLLNode add(Node n, DLLNode linkNode, SLLNode extant) {
      return SLLNode.newInstance(n).init(linkNode, extant);
    }

    public RevisitNode getRevisitNode(SLLNode updateFromOnRevisit, RevisitNode prev, RevisitNode next, final int passId) {
      if (passId == lastPassId) {
        revisitNode.updateFromOnRevisit = updateFromOnRevisit;
        return revisitNode;
      } else {
        lastPassId = passId;
        Node phraseNext = node.phraseNext;
        Node phrasePrev = node.phrasePrev;
        return revisitNode = RevisitNode.newInstance(prev, phrasePrev).init(phrasePrev, phraseNext, updateFromOnRevisit, prev, next, phraseNext.incrementRefForPassId(passId));
      }
    }
  }

  /**
   * The Node class is the main node used in the "two-dimensional Queue" consisting of parallel PositionDeques.
   */
  static final class Node extends Spans implements SpanCollector {

    private int outputPassId = -1;
    int output = 0;
    private DLLReturnNode initialzedReverseLinks = null;
    final PositionDeque[] dequePointer;
    private final int phraseIndex;
    private final long phraseIndexShifted;
    private int index;
    private long phraseScopeId;
    private int docId;
    private short validity;
    private Node prev;
    private Node next;
    private Spans backing;
    private int startPosition = -1;
    private int endPosition = -1;
    private int width = -1;
    private final StoredPostings postingsHead;
    private StoredPostings postings;
    private int maxSlopToCaller = -1;
    final LinkPool pool = new LinkPool();

    private int refCountAtPassId = -1;
    private final int[] revisitRefCount = new int[1];
    private Node trackNextRef;
    private Node trackNextRefProvisional;

    public int[] incrementRefForPassId(int passId) {
      if (refCountAtPassId == passId) {
        revisitRefCount[0]++;
      } else {
        refCountAtPassId = passId;
        revisitRefCount[0] = 1;
      }
      return revisitRefCount;
    }

    /**
     * From this node, the least sloppy path to phrase start/end, respectively.
     */
    private int maxSlopRemainingToStart = -1;
    private Node maxSlopRemainingPhrasePrev = null;
    
    void resetBacklinks() {
      maxSlopRemainingToStart = -1;
      maxSlopRemainingPhrasePrev = null;
    }
    
    private static enum SlopStatus { NONE, INITIALIZED, SEALED};
    
    private SlopStatus sealed = SlopStatus.NONE;
    private int sealedTo = -1;
    private int sealedThreshold = -1;

    private int maxSlopRemainingToEnd = -1;
    private DLLEndNodeLink maxSlopRemainingEndNodeLink = null;
    private final LongObjectHashMap<FullEndNodeLink> associatedNodes;
    private FullEndNodeLink endNodeLinks;
    private Node maxSlopRemainingPhraseNext = null;

    /**
     * The following references are head/tail (inclusive limits) defining a lists that include nodes at 
     * next/prev phrase positions that *may* be reachable (within slop constraints) from this node.
     * 
     * The head and tail of each list is guaranteed to indicate a slop-valid path to phrase start/end, 
     * but no such guarantee exists for intermediate nodes in the defined list -- the list must be scanned,
     * and each node inspected individually to determine if it indicates a slop-valid path from this node.
     * 
     * The building of the resulting lattice is driven from phrase start, and the defined lists will grow
     * as the lattice is built.
     */
    private SLLNode phraseNext = null;
    private DLLNode phrasePrev = null;


    public Node(PositionDeque[] dequePointer, int phraseIndex, int index, long provisionalRepeat, Spans backing) {
      this.dequePointer = dequePointer;
      final PositionDeque deque = dequePointer[0];
      this.phraseIndex = phraseIndex;
      this.phraseIndexShifted = ((long)phraseIndex) << Integer.SIZE;
      this.postingsHead = this.postings = new StoredPostings();
      if (deque.next != null) {
        this.associatedNodes = null;
      } else {
      switch (deque.comboMode) {
        case FULL_DISTILLED:
        case FULL_DISTILLED_PER_POSITION:
        case FULL_DISTILLED_PER_START_POSITION:
          this.associatedNodes = new LongObjectScatterMap<>();
          break;
        default:
          this.associatedNodes = null;
      }
      }
      this.backing = backing;
      this.validity = PROVISIONAL;
      this.docId = deque.docId;
      this.index = index;
      switch (deque.comboMode) {
        case FULL_DISTILLED:
        case FULL_DISTILLED_PER_POSITION:
        case FULL_DISTILLED_PER_START_POSITION:
          this.endNodeLinks = FullEndNodeLink.newInstance(this);
          this.phraseScopeId = phraseIndexShifted | provisionalRepeat | index;
          break;
        default:
          this.endNodeLinks = null;
          this.phraseScopeId = -1;
      }
    }

    public void initProvisional(int index, long provisionalRepeat, Spans backing) {
      this.backing = backing;
      this.validity = PROVISIONAL;
      final PositionDeque deque = dequePointer[0];
      this.docId = deque.docId;
      this.index = index;
      switch (deque.comboMode) {
        case FULL_DISTILLED:
        case FULL_DISTILLED_PER_POSITION:
        case FULL_DISTILLED_PER_START_POSITION:
          this.endNodeLinks = FullEndNodeLink.newInstance(this);
          this.phraseScopeId = phraseIndexShifted | provisionalRepeat | index;
          break;
        default:
          this.endNodeLinks = null;
          this.phraseScopeId = -1;
      }
    }

    public void initProvisionalGreedy(Spans backing) {
      this.backing = backing;
      this.validity = PROVISIONAL;
      final PositionDeque deque = dequePointer[0];
      this.docId = deque.docId;
    }

    public void clear() {
      if (this.associatedNodes != null) {
        this.associatedNodes.clear();
      }
      this.initialzedReverseLinks = null;
      this.maxSlopRemainingEndNodeLink = null;
      this.maxSlopRemainingPhraseNext = null;
      this.maxSlopRemainingPhrasePrev = null;
      this.maxSlopRemainingToEnd = -1;
      this.maxSlopRemainingToStart = -1;
      this.maxSlopToCaller = -1;
      this.output = 0;
      this.outputPassId = -1;
      this.phraseNext = null;
      this.phrasePrev = null;
      this.postingsHead.clear();
      this.postings = postingsHead;
      this.refCountAtPassId = -1;
      this.sealed = SlopStatus.NONE;
      this.sealedThreshold = -1;
      this.sealedTo = -1;
      this.startPosition = -1;
      this.endPosition = -1;
      this.width = -1;
    }

    public void init(int startPosition, int endPosition, int width) {
      this.startPosition = startPosition;
      this.endPosition = endPosition;
      this.width = width;
    }

    private Node() {
      this.dequePointer = null;
      this.phraseIndex = -1;
      this.phraseIndexShifted = -1;
      this.index = -1;
      this.phraseScopeId = -1;
      this.docId = -1;
      this.startPosition = -1;
      this.endPosition = 0;
      this.width = 0;
      this.postingsHead = null;
      this.associatedNodes = null;
      this.endNodeLinks = null;
    }

    @Override
    public String toString() {
      if (dequePointer == null) {
        return "x";
      }
      final PositionDeque deque = dequePointer[0];
      String prefix;
      if (index == deque.tail) {
        prefix = "S";
      } else if (index + 1 == deque.head) {
        prefix = "E";
      } else {
        prefix = "";
      }
      return prefix + ((validity & (VALID | VALID_MAXQ | VALID_MINQ)) != 0 ? Integer.toString(startPosition()) : Integer.toString(startPosition())+'x');
    }
    
    protected void remove() {
      final PositionDeque deque = dequePointer[0];
      if (prev != null) {
        if (next == null) {
          prev.next = null;
          if ((validity & VALID_MINQ) == VALID_MINQ) {
            prev.validity &= ~VALID_MAXQ;
          }
          if (this == deque.headNode) {
            deque.headNode = prev;
            deque.head = prev.index + 1;
          }
        } else {
          next.prev = prev;
          prev.next = next;
          switch (validity & (VALID_MINQ | VALID_MAXQ)) {
            case VALID_MAXQ:
              prev.validity &= ~VALID_MINQ;
              break;
            case VALID_MINQ:
              next.validity &= ~VALID_MAXQ;
              break;
            case VALID_MAXQ | VALID_MINQ:
              if (prev.endPosition < next.endPosition) {
                prev.validity &= ~VALID_MINQ;
                next.validity &= ~VALID_MAXQ;
              }
              break;
          }
        }
      } else if (next != null) {
        next.prev = null;
        if ((validity & VALID_MAXQ) == VALID_MAXQ) {
          next.validity &= ~VALID_MINQ;
        }
        if (this == deque.tailNode) {
          deque.tailNode = next;
          deque.tail = next.index;
        }
      } else {
        // this is the only node; head should == tail
        deque.clear(false, false);
      }
      validity = 0;
      if (initialzedReverseLinks == null) {
        deque.enqueueForPoolReturn(this);
      }
    }
    
    protected void truncatePreceding() {
      if ((validity & VALID_MINQ) == VALID_MINQ) {
        validity &= ~VALID_MINQ;
      }
      Node n = this;
      while ((n = n.prev) != null) {
        n.validity = 0;
      }
      final PositionDeque deque = dequePointer[0];
      deque.tailNode = this;
      deque.tail = this.index;
    }

    // SPANS
    @Override
    public int nextStartPosition() throws IOException {
      throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public final int startPosition() {
      return startPosition == -1 ? backing.startPosition() : startPosition;
    }

    @Override
    public final int endPosition() {
      return startPosition == -1 ? backing.endPosition() : endPosition;
    }

    @Override
    public int width() {
      return startPosition == -1 ? backing.width() : width;
    }

    @Override
    public void collect(SpanCollector collector) throws IOException {
      if (startPosition == -1) {
        backing.collect(collector);
      } else {
        StoredPostings sp = postingsHead;
        while ((sp = sp.next) != null) {
          collector.collectLeaf(sp, sp.position, sp.term);
        }
      }
    }

    @Override
    public float positionsCost() {
      return 0;
    }

    @Override
    public int docID() {
      return docId;
    }

    @Override
    public int nextDoc() throws IOException {
      throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public int advance(int target) throws IOException {
      throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public long cost() {
      return 1;
    }

    // SPANCOLLECTOR
    @Override
    public void collectLeaf(PostingsEnum postings, int position, Term term) throws IOException {
      this.postings = this.postings.addNext(StoredPostings.newInstance(this, postings, position, term, dequePointer[0].offsets));
    }

    @Override
    public void reset() {
      (postings = postingsHead).next = null;
    }
    
  }

  private static final boolean POOL_NODES = true; // provide a way to statically disable object pooling
  
  private Node nodePoolHead = null;
  private Node nodePoolTail = null;
  private Node nodePoolReturnQueueHead = null;
  private Node nodePoolReturnQueueTail = null;

  private boolean enqueueForPoolReturn(Node n) {
    if (nodePoolReturnQueueHead != null) {
      n.trackNextRef = nodePoolReturnQueueHead;
      nodePoolReturnQueueHead = n;
    } else {
      nodePoolReturnQueueHead = n;
      nodePoolReturnQueueTail = n;
    }
    return true;
  }

  private static boolean assign(Object obj) {
    return true;
  }
  private void returnNodesToPool() {
    if (nodePoolReturnQueueHead != null) {
      if (POOL_NODES) {
        final Node limit = nodePoolReturnQueueTail;
        Node n = nodePoolReturnQueueHead;
        do {
          pool.returnLinksToPool(n.pool);
          if (n.endNodeLinks != null) {
            n.endNodeLinks.clear();
          }
        } while (n != limit && assign(n = n.trackNextRef));
        if (nodePoolHead != null) {
          nodePoolReturnQueueTail.trackNextRef = nodePoolHead;
          nodePoolHead = nodePoolReturnQueueHead;
        } else {
          nodePoolHead = nodePoolReturnQueueHead;
          nodePoolTail = nodePoolReturnQueueTail;
        }
      }
      nodePoolReturnQueueHead = null;
      nodePoolReturnQueueTail = null;
    }
  }

  private int lastProvisionalIndex = -1;
  private long provisionalRepeat = 0;
  private int reusedNodeCount = 0;
  private int createNodeCount = 0;
  private int dllEndNodeLinkCt = 0;
  private int dllEndNodeLinkCtR = 0;
  private int fullEndNodeLinkCt = 0;
  private int fullEndNodeLinkCtR = 0;
  private int dllNodeCt = 0;
  private int dllNodeCtR = 0;
  private int sllNodeCt = 0;
  private int sllNodeCtR = 0;
  private int dllReturnNodeCt = 0;
  private int dllReturnNodeCtR = 0;
  private int revisitNodeCt = 0;
  private int revisitNodeCtR = 0;
  int storedPostingsCt = 0;
  int storedPostingsCtR = 0;
  private Node getPooledNode(Spans backing) {
    switch (comboMode) {
      case FULL_DISTILLED:
      case FULL_DISTILLED_PER_POSITION:
      case FULL_DISTILLED_PER_START_POSITION:
        if (head == lastProvisionalIndex) {
          provisionalRepeat += repeatProvisionalIncrement;
        } else {
          provisionalRepeat = 0;
          lastProvisionalIndex = head;
        }
        break;
    }
    final Node ret;
    if (nodePoolHead != null) {
      ret = nodePoolHead;
      if (ret != nodePoolTail) {
        nodePoolHead = ret.trackNextRef;
      } else {
        nodePoolHead = null;
        nodePoolTail = null;
      }
      if (TRACK_POOLING) {
        reusedNodeCount++;
      }
      ret.clear();
      ret.initProvisional(head, provisionalRepeat, backing);
    } else {
      if (TRACK_POOLING) {
        createNodeCount++;
      }
      ret = new Node(dequePointer, phraseIndex, head, provisionalRepeat, backing);
    }
    return ret;
  }

  private void increaseCapacity() {
    final int srcHead = head & indexMask;
    final int srcTail = tail & indexMask;
    final int srcCapacity = capacity;
    capacity <<= 1; // double capacity
    indexMask = capacity - 1; // reset index mask
    int dstTail = tail & indexMask;
    if (srcHead > srcTail) {
      final int length = srcHead - srcTail;
      final Node[] oldBuffer = nodeBuffer;
      nodeBuffer = new Node[capacity];
      System.arraycopy(oldBuffer, srcTail, nodeBuffer, dstTail, length);
    } else {
      final int tailChunkLength = srcCapacity - srcTail;
      final int dstHeadChunkStart = (dstTail + tailChunkLength) & indexMask;
      final Node[] oldBuffer = nodeBuffer;
      nodeBuffer = new Node[capacity];
      System.arraycopy(oldBuffer, srcTail, nodeBuffer, dstTail, tailChunkLength);
      System.arraycopy(oldBuffer, 0, nodeBuffer, dstHeadChunkStart, srcHead);
    }
  }
  
  private final PriorityQueue<Node> minEndQ = new PriorityQueue<>(MIN_POS_COMPARATOR);
  private final PriorityQueue<Node> maxEndQ = new PriorityQueue<>(MAX_POS_COMPARATOR);
  
  private static final Comparator<Node> MIN_POS_COMPARATOR = new Comparator<Node>() {

    @Override
    public int compare(Node o1, Node o2) {
      return Integer.compare(o1.endPosition, o2.endPosition);
    }
  };

  private static final Comparator<Node> MAX_POS_COMPARATOR = new Comparator<Node>() {

    @Override
    public int compare(Node o1, Node o2) {
      return Integer.compare(o2.endPosition, o1.endPosition);
    }
  };

  public int minStart() {
    return tailNode == null ? -1 : tailNode.startPosition;
  }
  
  public int minEnd() {
    if (tailNode == null) {
      return -1;
    } else {
      int tailEnd = tailNode.endPosition;
      Node n;
      while ((n = minEndQ.peek()) != null) {
        if ((n.validity & (VALID_MINQ)) != (VALID_MINQ)) {
          minEndQ.remove();
          n.validity &= ~IN_MINQ;
        } else {
          return Math.min(tailEnd, n.endPosition);
        }
      }
      return tailEnd;
    }
  }

  public int maxEnd() {
    if (headNode == null) {
      return -1;
    } else {
      int headEnd = headNode.endPosition;
      Node n;
      while ((n = maxEndQ.peek()) != null) {
        if ((n.validity & (VALID_MAXQ)) != (VALID_MAXQ)) {
          maxEndQ.remove();
          n.validity &= ~IN_MAXQ;
        } else {
          return Math.max(headEnd, n.endPosition);
        }
      }
      return headEnd;
    }
  }
  
  private Node headNode;
  private Node tailNode;
  
  private final LocalNodeArrayList conserveNodes = new LocalNodeArrayList(16);
  
  Node add(final Node n, int startPosition, int endPosition, int width) {
    final int size = head - tail;
    final int bufferIdx;
    n.init(startPosition, endPosition, width);
    if (size == 0) {
      bufferIdx = head & indexMask;
      //n = new Node(this, head, phraseIndex, startPosition, endPosition, width, bufferIdx);
      n.validity = VALID;
      n.prev = null;
      n.next = null;
      tailNode = n;
    } else {
      if (size >= capacity) {
        increaseCapacity();
        if (size > capacity) {
          throw new AssertionError();
        }
      }
      bufferIdx = head & indexMask;
      headNode.next = n;
      n.prev = headNode;
      n.next = null;
      final int prevEndPosition = headNode.endPosition;
      if (endPosition >= prevEndPosition) {
        n.validity = VALID;
      } else {
        n.validity = (VALID_MINQ | IN_MINQ);
        minEndQ.add(n);
        headNode.validity |= (VALID_MAXQ | IN_MAXQ);
        if (maxEndQ.size() > 100 + (size << 1)) {
          // purge stale refs
          conserveNodes.reset(size);
          for (Node n1 : maxEndQ) {
            switch (n1.validity & (VALID_MAXQ | IN_MAXQ)) {
              case VALID_MAXQ | IN_MAXQ:
                conserveNodes.add(n1);
                break;
              case IN_MAXQ:
                n1.validity &= ~IN_MAXQ;
                break;
              case VALID_MAXQ:
              default:
                throw new AssertionError("not marked as being in maxQ!");
            }
          }
          maxEndQ.clear();
          maxEndQ.addAll(conserveNodes);
        }
        maxEndQ.add(headNode);
      }
    }
    nodeBuffer[bufferIdx] = n;
    headNode = n;
    head++;
    return n;
  }

  public final void clear(boolean repool, boolean newDoc) {
    if (uninitializedForDoc) {
      return;
    } else if (newDoc) {
      uninitializedForDoc = true;
    }
    if (repool) {
      if (tail != head) {
        Node n = tailNode;
        do {
          if (n.validity != 0 || n.initialzedReverseLinks != null) {
            enqueueForPoolReturn(n);
          }
        } while ((n = n.next) != null);
      }
      initProvisional(newDoc);
      purgeProvisional();
      final DLLReturnNode anchor = returned.anchor;
      DLLReturnNode drn = returned.next;
      if (drn != anchor) {
        do {
          if (drn.node.validity == 0) {
            enqueueForPoolReturn(drn.node);
          }
        } while ((drn = drn.next) != anchor);
      }
      returnNodesToPool();
    }
    iterNode = null;
    headNode = null;
    tailNode = null;
    tail = head;
    minEndQ.clear();
    maxEndQ.clear();
    if (revisit != null) {
      revisit.clear();
    }
  }

  public static void main(String[] args) {
    PositionDeque p = new PositionDeque(1, true, null, null, null, false, ComboMode.PER_END_POSITION);
    for (int i = 0; i < 5; i++) {
      for (int j = i; j < i + 5; j++) {
        p.add(null, i, j, 0);
      }
      for (int k = 0; k <= i + 1; k++) {
        //System.out.println("iter from "+k);
        Iterator<Spans> iter = p.iteratorMinStart(-1, k);
        while (iter.hasNext()) {
          Spans next = iter.next();
          //System.out.println("\t"+next.startPosition()+"=>"+next.endPosition());
        }
      }
      Iterator<Spans> iter = p.descendingIterator();
      while (iter.hasNext()) {
        Spans next = iter.next();
        //System.out.println("descend! " + next.startPosition() + "=>" + next.endPosition());
      }
      boolean drop = true;
      iter = p.iterator();
      while (iter.hasNext()) {
        Spans next = iter.next();
        if (drop) {
          //System.out.println("remove "+next.startPosition()+"=>"+next.endPosition());
          iter.remove();
          drop = false;
        } else {
          drop = true;
        }
      }
    }
  }

  static final class StackFrame {

    final PositionDeque d;
    private Node caller;
    int hardMinStart;
    int softMinStart;
    int startCeiling;
    int minEnd;
    private int remainingSlopToCaller;
    private int previousMaxSlopToCaller;
    int start;
    private Node leastSloppyPathToPhraseEnd;
    private Node maxSlopRemainingEndNode;
    int maxSlopRemainingToPhraseEnd;
    Node nextNode;
    private int updateSealedToThreshold;
    private int updateSealedTo;
    boolean keepUpdatingSealedTo;
    final boolean defaultCacheNodePaths;
    int remainingSlop;
    private int maxSlopRemainingCandidate;
    int remainingSlopToEnd;
    private SLLNode revisitFrom;
    boolean pseudoReturn = false;

    public StackFrame(PositionDeque d, boolean greedyReturn) {
      this.d = d;
      this.defaultCacheNodePaths = !greedyReturn;
    }

    public void initArgs(Node caller, int hardMinStart, int softMinStart, int startCeiling, int minEnd, int remainingSlopToCaller) {
      this.caller = caller;
      this.hardMinStart = hardMinStart;
      this.softMinStart = softMinStart;
      this.startCeiling = startCeiling;
      this.minEnd = minEnd;
      this.remainingSlopToCaller = remainingSlopToCaller;
    }

    public boolean notPseudoReturn() {
      if (pseudoReturn) {
        remainingSlopToEnd = nextNode.maxSlopRemainingToEnd;
        pseudoReturn = false;
        return false;
      } else {
        return true;
      }
    }

    public boolean initAlways() throws IOException {
      if (pseudoReturn) {
        return true;
      }
      this.previousMaxSlopToCaller = caller.maxSlopToCaller;
      final int extantStart = d.driver.startPosition();
      final boolean ret;
      if (d.phraseIndex == 0 || (extantStart >= softMinStart && extantStart < startCeiling && d.driver.endPosition() >= minEnd)) {
        this.start = extantStart;
        ret = true;
      } else {
        this.start = d.driver.nextMatch(hardMinStart, softMinStart, startCeiling, minEnd);
        ret = this.start >= 0;
      }
      if (remainingSlopToCaller > caller.maxSlopToCaller) {
        caller.maxSlopToCaller = remainingSlopToCaller;
      }
      return ret;
    }

    public void init() {
      if (!pseudoReturn) {
        leastSloppyPathToPhraseEnd = null;
        maxSlopRemainingEndNode = null;
        maxSlopRemainingToPhraseEnd = Integer.MIN_VALUE;
        this.nextNode = d.getLastNode();
        updateSealedToThreshold = Integer.MIN_VALUE;
        updateSealedTo = nextNode.index;
        keepUpdatingSealedTo = defaultCacheNodePaths;
      }
    }
  }
}
