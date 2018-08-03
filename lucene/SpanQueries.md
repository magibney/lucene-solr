# Bug in ordered SpanNearQuery 

Matching for ordered SpanNearQuery in Lucene is only guaranteed
to be accurate if the length of each subSpans is fixed for a
given document. There are many common use cases that do not meet
this condition, and thus result in the skipping of legitimate
matches (positions and documents). 

The problem is well summarized and diagnosed in Christoph Goller's
comment for the relevant Lucene Jira issue, [LUCENE-7398](https://issues.apache.org/jira/browse/LUCENE-7398#comment-15466888).

This document provides a high-level overview of the approach
used to implement a candidate fix for this bug.

## Lazy matching will not work
As Goller correctly points out in the comment linked above, lazy
iteration (moving subSpans forward only, without the ability to 
backtrack) is incapable of guaranteeing accuracy.

The starting point for this bugfix candidate was to wrap each subSpans
in a backtracking-capable Spans implementation. In place of the
`int nextStartPosition()` method, callers interact with the wrapper
through an interface
`int nextMatch(int hardMinStart, int softMinStart, int startCeiling, int minEnd)`,
which abstracts all the information necessary to determine when
positions must be stored for possible future backtracking, and
when they may be discarded:
  * hardMinStart: forever discard spans with start < this parameter
  * softMinStart: skip (for purpose of returning nextMatch, but do not discard) spans with start < this parameter
  * startCeiling: disregard (for purpose of returning nextMatch, but do not discard) spans with start >= this parameter
  * minEnd: when non-negative, defines a minimum threshold for span endPositions. Spans with endPosition < this value should be discarded forever.

To provide effcient navigation and resetting of stored spans (for
backtracking) the `nextMatch()` wrapper is backed by a `PositionDeque` --
storage consisting of a sorted linked deque, implemented as a
circular buffer over a dynamically resizable backing array (which,
being sorted, supports O(log n) random-access seek).

## Build matches across adjacent phrase positions
Storage nodes in the `PositionDeque` (referred to hereafter as `Node`s)
serve a dual purpose. They store information about stored/buffered positions
for a backing Spans at their associated `PositionDeque`'s phrase index,
but they *also* store references to "reachable" (within slop constraints)
`Node`s in the `PositionDeque` of previous and subsequent phrase indexes,
and to "reachable" `Node`s in the `PositionDeque` of the *last* phrase index.

These references are used to build a "lattice" of `Node`s consisting of (at
any point in time): a list of valid `Node`s at a given position, a
directed acyclic graph of valid paths from phrase start to phrase end,
and a directed acyclic graph of valid paths from phrase end to phrase start.

The lattice is built by using the `nextMatch()` wrapper to drive a
depth-first search to discover (and build/cache) edges in the dynamic
graph represented by valid `Node`s at a point in time. The graph is "dynamic"
in the sense that subsequent calls to `nextMatch()` can invalidate
extant `Node`s, requiring special handling (e.g., removing or updating
graph edges).

This approach relies heavily on simple linking nodes (used to build the
the lattice/graphs/etc.) ... a *lot* of nodes. To avoid excessive GC,
`Node`s are pooled and reused per-`PositionDeque` (and thus, per phrase
index); simple linking nodes are also pooled per-`PositionDeque`, and
are returned to the pool upon release of their current associated `Node`.

## New types of information in the Lucene index
These were driven by new possibilities exposed by this development, which
make certain additional indexed information extremely useful at query-time.
As proof-of-concept all have been implemented using `Payload`s. 

### We can do something useful with positionLength now!
Currently, absent a query implementation that can do something useful
with positionLength, Lucene simply ignores the `PositionLengthAttribute`
at index time and implicitly assumes a positionLength of `1` for all
terms at query time. (See brief discussion under [LUCENE-4312](https://issues.apache.org/jira/browse/LUCENE-4312#comment-13437824))

Now that we have a query implementation that respects positionLength,
we can take the advice in the second comment on [LUCENE-4312](https://issues.apache.org/jira/browse/LUCENE-4312#comment-13437827), and write it to the `Payload`.
This is done using the `PositionLengthOrderTokenFilterFactory`,
which in addition to simply storing the positionLength in the `Payload`,
must reorder tokens to conform to the ordering prescribed by the `Spans`
specification. (Recall that there would previously have been no point to
such reordering of tokens, since `PositionLengthAttribute` was ignored
anyway at index time). In addition to VInt-encoded positionLength,
`PositionLengthOrderTokenFilterFactory` records a "magic" prefix in the
`Payload` to prevent confusion among fields that use `Payload` differently.

### Index lookahead (don't buffer positions if you don't have to!)
In order to perform exhaustive matching, we must progress through all possible
valid matches for a given subSpan, buffering as we go. But with atomic
TermSpans in stock Lucene (and consequently for higher-level Spans --
e.g., Or, Near, etc.), lazy iteration means that the only way to
know whether we've seen all relevant positions is to iterate *past*
all relevant positions, to the first *irrelevant* position. This has
the nasty consequence that we are *always* forced to buffer *every*
relevant position. 

To handle this case and prevent unnecessary buffering, as a proof-of-concept
we record per-term startPosition lookahead directly in the index, leveraging
`Payload`s. For ease of adoption and backward compatibility, this is triggered
by the presence of a "magic" value that may be placed in the `Payload`
by the `PositionLengthOrderTokenFilterFactory`.

Spans may now determine at query-time a floor for the value to be returned
for `nextStartPosition()`, and prevent progressing beyond the last relevant
match. Each `Node` thus begins its life as a "provisional" `Node`, which
initially represents an unbuffered layer over the backing Spans. As a result,
in many common cases (e.g., no-slop phrase searching of simple terms),
buffering of position data may be entirely avoided.

### subSpans positionLength edge cases, new matches exposed
In some cases, a new match possibility may be exposed by a decrease in the
endPosition of a subSpans, or by a decrease in slop caused by an increase
in the match length of a subSpans. In order to determine when it's worth
checking for these cases at the atomic TermSpans level, it would be helpful to
know the following at query time for a given Term in a given Doc:
1. Does the endPosition ever decrease (this could happen in the event of
   increased startPosition and decreased positionLength)?
2. What is the maximum positionLength for an instance of this Term in this Doc?

To make this information available at query time, we will at index time
allocate 4 bytes in the `Payload` of the first instance of a Term in each
Doc. When the doc is finished processing, as part of flushing the doc, the
maximum position length for each term is written to the allocated space;
endPositions are tracked, and if we have seen a decrease in endPosition for
a term in this doc, the value written will be `(-maxPositionLength) - 1`.
This information may be parsed and exposed by the atomic `TermSpans`
implementation at query time.

### shingle-based pre-filtering of ConjunctionSpans

For accuracy and consistency, we would like to use true graph queries
for *all* phrase searching. Keeping in mind that `DismaxQueryParser` boosts
scores by running phrase searches over the `pf` fields for *all* user queries
(not only for explicit phrase queries), it would be desirable for graph
`SpanNearQuery` performance to be on par with performance of the extant default
`PhraseQuery` implementation.

This is particularly challenging for queries consisting mainly of common terms,
especially because stop words are incompatible with `SpanNearQuery` without
significant sacrifices in functionality
(see [documentation for ComplexPhraseQParser](https://lucene.apache.org/solr/guide/6_6/other-parsers.html#OtherParsers-Stopwords)).

`CommonGramsFilter` provides a possible solution, but only by sacrificing a
considerable amount of flexibility (consider that with stopword `the`, we might
reasonably want `"the caterpillar"~2` to match `the very hungry caterpillar`).

To address this challenge, we have implemented an index-time
`ShingleFilterUpdateRequestProcessor`, which behaves similar to the
`CommonGramsFilter`, but instead of emitting shingle tokens inline on the source
token stream, it buffers shingles (of configurable slop limit for a configurable
list of stopwords) as it evaluates the entire source token stream, then writes
these unique shingles to a *separate* field that indexes term frequency, but
*not* positions. We then hijack the destination field's `TermFrequency` attribute
(which, after all, is just a positive integer) to record (for each unique
shingle/token) the min slop <= the configured slop limit.

At query time, a corresponding filter on the `ExtendedDismaxQParser` examines
the parsed graph query for `SpanNearQuery`s containing shingled stopwords, and
constructs `TwoPhaseIterator`s for the shingles. These `TwoPhaseIterator`s are
passed as pre-filters (respecting configured phrase slop) into the `ConjunctionSpans`
`approximation` for the associated `SpanNearQuery`, significantly reducing the
overhead of setting up `Spans` for each document in the intersection of query terms.

For our use cases, this approach has resulted in `SpanNearQuery` performance
nearly identical to extant `PhraseQuery` performance, and we are now running
full graph queries in production for every user query.
