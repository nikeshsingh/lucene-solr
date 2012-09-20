package org.apache.lucene.search;

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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ToStringUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;


/**
 * A query that applies a filter to the results of another query.
 *
 * <p>Note: the bits are retrieved from the filter each time this
 * query is used in a search - use a CachingWrapperFilter to avoid
 * regenerating the bits every time.
 * @since   1.4
 * @see     CachingWrapperFilter
 */
public class FilteredQuery extends Query {

  private final Query query;
  private final Filter filter;
  private final FilterStrategy strategy;

  /**
   * Constructs a new query which applies a filter to the results of the original query.
   * {@link Filter#getDocIdSet} will be called every time this query is used in a search.
   * @param query  Query to be filtered, cannot be <code>null</code>.
   * @param filter Filter to apply to query results, cannot be <code>null</code>.
   */
  public FilteredQuery (Query query, Filter filter) {
    this(query, filter, null);
  }
  
  /**
   * Expert: Constructs a new query which applies a filter to the results of the original query.
   * {@link Filter#getDocIdSet} will be called every time this query is used in a search.
   * @param query  Query to be filtered, cannot be <code>null</code>.
   * @param filter Filter to apply to query results, cannot be <code>null</code>.
   * @param strategy a filter strategy used to create a filtered scorer. 
   * 
   * @see FilterStrategy
   */
  public FilteredQuery (Query query, Filter filter, FilterStrategy strategy) {
    if (query == null || filter == null)
      throw new IllegalArgumentException("Query and filter cannot be null.");
    this.strategy = strategy == null ? RANDOM_ACCESS_FILTER_STRATEGY : strategy;
    this.query = query;
    this.filter = filter;
  }
  
  /**
   * Returns a Weight that applies the filter to the enclosed query's Weight.
   * This is accomplished by overriding the Scorer returned by the Weight.
   */
  @Override
  public Weight createWeight(final IndexSearcher searcher) throws IOException {
    final Weight weight = query.createWeight (searcher);
    return new Weight() {
      
      @Override
      public boolean scoresDocsOutOfOrder() {
        return true;
      }

      @Override
      public float getValueForNormalization() throws IOException { 
        return weight.getValueForNormalization() * getBoost() * getBoost(); // boost sub-weight
      }

      @Override
      public void normalize (float norm, float topLevelBoost) { 
        weight.normalize(norm, topLevelBoost * getBoost()); // incorporate boost
      }

      @Override
      public Explanation explain (AtomicReaderContext ir, int i) throws IOException {
        Explanation inner = weight.explain (ir, i);
        Filter f = FilteredQuery.this.filter;
        DocIdSet docIdSet = f.getDocIdSet(ir, ir.reader().getLiveDocs());
        DocIdSetIterator docIdSetIterator = docIdSet == null ? DocIdSet.EMPTY_DOCIDSET.iterator() : docIdSet.iterator();
        if (docIdSetIterator == null) {
          docIdSetIterator = DocIdSet.EMPTY_DOCIDSET.iterator();
        }
        if (docIdSetIterator.advance(i) == i) {
          return inner;
        } else {
          Explanation result = new Explanation
            (0.0f, "failure to match filter: " + f.toString());
          result.addDetail(inner);
          return result;
        }
      }

      // return this query
      @Override
      public Query getQuery() { return FilteredQuery.this; }

      // return a filtering scorer
      @Override
      public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder, boolean topScorer, final Bits acceptDocs) throws IOException {
        assert filter != null;

        final DocIdSet filterDocIdSet = filter.getDocIdSet(context, acceptDocs);
        if (filterDocIdSet == null) {
          // this means the filter does not accept any documents.
          return null;
        }
        return strategy.filteredScorer(context, scoreDocsInOrder, topScorer, weight, filterDocIdSet);
        
      }
    };
  }
  
  /**
   * A Scorer that uses a "leap-frog" approach (also called "zig-zag join"). The scorer and the filter
   * take turns trying to advance to each other's next matching document, often
   * jumping past the target document. When both land on the same document, it's
   * collected.
   */
  private static final class LeapFrogScorer extends Scorer {
    private final Scorer scorer;
    private final DocIdSetIterator filterIter;
    private final int firstFilteredDoc;
    private int scorerDoc = -1;
    private int filterDoc;

    protected LeapFrogScorer(Weight weight, int firstFilteredDoc, DocIdSetIterator filterIter, Scorer other) {
      super(weight);
      this.filterDoc = this.firstFilteredDoc = firstFilteredDoc;
      this.scorer = other;
      this.filterIter = filterIter;
    }
    
    // optimization: we are topScorer and collect directly using short-circuited algo
    @Override
    public void score(Collector collector) throws IOException {
      int filterDoc = firstFilteredDoc;
      int scorerDoc = scorer.advance(filterDoc);
      // the normalization trick already applies the boost of this query,
      // so we can use the wrapped scorer directly:
      collector.setScorer(scorer);
      for (;;) {
        if (scorerDoc == filterDoc) {
          // Check if scorer has exhausted, only before collecting.
          if (scorerDoc == DocIdSetIterator.NO_MORE_DOCS) {
            break;
          }
          collector.collect(scorerDoc);
          filterDoc = filterIter.nextDoc();
          scorerDoc = scorer.advance(filterDoc);
        } else if (scorerDoc > filterDoc) {
          filterDoc = filterIter.advance(scorerDoc);
        } else {
          scorerDoc = scorer.advance(filterDoc);
        }
      }
    }
    
    private int advanceToNextCommonDoc() throws IOException {
      for (;;) {
        if (scorerDoc < filterDoc) {
          scorerDoc = scorer.advance(filterDoc);
        } else if (scorerDoc == filterDoc) {
          return scorerDoc;
        } else {
          filterDoc = filterIter.advance(scorerDoc);
        }
      }
    }

    @Override
    public int nextDoc() throws IOException {
      // don't go to next doc on first call
      // (because filterIter is already on first doc):
      if (scorerDoc != -1) {
        filterDoc = filterIter.nextDoc();
      }
      return advanceToNextCommonDoc();
    }
    
    @Override
    public int advance(int target) throws IOException {
      if (target > filterDoc) {
        filterDoc = filterIter.advance(target);
      }
      return advanceToNextCommonDoc();
    }

    @Override
    public int docID() {
      return scorerDoc;
    }
    
    @Override
    public float score() throws IOException {
      return scorer.score();
    }
    
    @Override
    public float freq() throws IOException { return scorer.freq(); }
    
    @Override
    public Collection<ChildScorer> getChildren() {
      return Collections.singleton(new ChildScorer(scorer, "FILTERED"));
    }
  }
  
  /**
   * A scorer that consults the filter iff a document was matched by the
   * delegate scorer. This is useful if the filter computation is more expensive
   * than document scoring or if the filter has a linear running time to compute
   * the next matching doc like exact geo distances.
   */
  private static final class DocFirstScorer extends Scorer {
    private final Scorer scorer;
    private int scorerDoc = -1;
    private Bits filterbits;

    protected DocFirstScorer(Weight weight, Bits filterBits, Scorer other) {
      super(weight);
      this.scorer = other;
      this.filterbits = filterBits;
    }
    
    
    // optimization: we are topScorer and collect directly
    @Override
    public void score(Collector collector) throws IOException {
      // the normalization trick already applies the boost of this query,
      // so we can use the wrapped scorer directly:
      collector.setScorer(scorer);
      for (;;) {
        final int scorerDoc = scorer.nextDoc();
        if (scorerDoc == DocIdSetIterator.NO_MORE_DOCS) {
          break;
        }
        if (filterbits.get(scorerDoc)) {
          collector.collect(scorerDoc);
        }
      }
    }
    
    @Override
    public int nextDoc() throws IOException {
      int doc;
      for(;;) {
        doc = scorer.nextDoc();
        if (doc == Scorer.NO_MORE_DOCS || filterbits.get(doc)) {
          return scorerDoc = doc;
        }
      } 
    }
    
    @Override
    public int advance(int target) throws IOException {
      
      int doc = scorer.advance(target);
      if (doc != Scorer.NO_MORE_DOCS && !filterbits.get(doc)) {
        return scorerDoc = nextDoc();
      } else {
        return scorerDoc = doc;
      }
      
    }

    @Override
    public int docID() {
      return scorerDoc;
    }
    
    @Override
    public float score() throws IOException {
      return scorer.score();
    }
    
    @Override
    public float freq() throws IOException { return scorer.freq(); }
    
    @Override
    public Collection<ChildScorer> getChildren() {
      return Collections.singleton(new ChildScorer(scorer, "FILTERED"));
    }
  }
  
   

  /** Rewrites the query. If the wrapped is an instance of
   * {@link MatchAllDocsQuery} it returns a {@link ConstantScoreQuery}. Otherwise
   * it returns a new {@code FilteredQuery} wrapping the rewritten query. */
  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    final Query queryRewritten = query.rewrite(reader);
    
    if (queryRewritten instanceof MatchAllDocsQuery) {
      // Special case: If the query is a MatchAllDocsQuery, we only
      // return a CSQ(filter).
      final Query rewritten = new ConstantScoreQuery(filter);
      // Combine boost of MatchAllDocsQuery and the wrapped rewritten query:
      rewritten.setBoost(this.getBoost() * queryRewritten.getBoost());
      return rewritten;
    }
    
    if (queryRewritten != query) {
      // rewrite to a new FilteredQuery wrapping the rewritten query
      final Query rewritten = new FilteredQuery(queryRewritten, filter);
      rewritten.setBoost(this.getBoost());
      return rewritten;
    } else {
      // nothing to rewrite, we are done!
      return this;
    }
  }

  public final Query getQuery() {
    return query;
  }

  public final Filter getFilter() {
    return filter;
  }

  // inherit javadoc
  @Override
  public void extractTerms(Set<Term> terms) {
    getQuery().extractTerms(terms);
  }

  /** Prints a user-readable version of this query. */
  @Override
  public String toString (String s) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("filtered(");
    buffer.append(query.toString(s));
    buffer.append(")->");
    buffer.append(filter);
    buffer.append(ToStringUtils.boost(getBoost()));
    return buffer.toString();
  }

  /** Returns true iff <code>o</code> is equal to this. */
  @Override
  public boolean equals(Object o) {
    if (o == this)
      return true;
    if (!super.equals(o))
      return false;
    assert o instanceof FilteredQuery;
    final FilteredQuery fq = (FilteredQuery) o;
    return fq.query.equals(this.query) && fq.filter.equals(this.filter) && fq.strategy.equals(this.strategy);
  }

  /** Returns a hash code value for this object. */
  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = hash * 31 + strategy.hashCode();
    hash = hash * 31 + query.hashCode();
    hash = hash * 31 + filter.hashCode();
    return hash;
  }
  
  /**
   * A {@link FilterStrategy} that conditionally uses a random access filter if
   * the given {@link DocIdSet} supports random access (returns a non-null value
   * from {@link DocIdSet#bits()}) and
   * {@link RandomAccessFilterStrategy#useRandomAccess(Bits, int)} returns
   * <code>true</code>. Otherwise this strategy falls back to a "zig-zag join" (
   * {@link FilteredQuery#LEAP_FROG_FILTER_STRATEGY}) strategy.
   * 
   * <p>
   * Note: this strategy is the default strategy in {@link FilteredQuery}
   * </p>
   */
  public static final FilterStrategy RANDOM_ACCESS_FILTER_STRATEGY = new RandomAccessFilterStrategy();
  
  /**
   * A filter strategy that uses a "leap-frog" approach (also called "zig-zag join"). 
   * The scorer and the filter
   * take turns trying to advance to each other's next matching document, often
   * jumping past the target document. When both land on the same document, it's
   * collected.
   */
  public static final FilterStrategy LEAP_FROG_FILTER_STRATEGY = new LeapFrogFilterStragey();
  
  /**
   * A filter strategy that advances the {@link Scorer} first and consults the
   * {@link DocIdSet} for each matched document.
   * <p>
   * Use this strategy if the filter computation is more expensive than document
   * scoring or if the filter has a linear running time to compute the next
   * matching doc like exact geo distances.
   * </p>
   */
  public static final FilterStrategy DOC_FIRST_FILTER_STRATEGY = new DocFirstFilterStrategy();
  
  /** Abstract class that defines how the filter ({@link DocIdSet}) applied during document collection. */
  public static abstract class FilterStrategy {
    
    /**
     * Returns a filtered {@link Scorer} based on this strategy.
     * 
     * @param context
     *          the {@link AtomicReaderContext} for which to return the {@link Scorer}.
     * @param scoreDocsInOrder
     *          specifies whether in-order scoring of documents is required. Note
     *          that if set to false (i.e., out-of-order scoring is required),
     *          this method can return whatever scoring mode it supports, as every
     *          in-order scorer is also an out-of-order one. However, an
     *          out-of-order scorer may not support {@link Scorer#nextDoc()}
     *          and/or {@link Scorer#advance(int)}, therefore it is recommended to
     *          request an in-order scorer if use of these methods is required.
     * @param topScorer
     *          if true, {@link Scorer#score(Collector)} will be called; if false,
     *          {@link Scorer#nextDoc()} and/or {@link Scorer#advance(int)} will
     *          be called.
     * @param weight the {@link FilteredQuery} {@link Weight} to create the filtered scorer.
     * @param docIdSet the filter {@link DocIdSet} to apply
     * @return a filtered scorer
     * 
     * @throws IOException if an {@link IOException} occurs
     */
    public abstract Scorer filteredScorer(AtomicReaderContext context,
        boolean scoreDocsInOrder, boolean topScorer, Weight weight,
        DocIdSet docIdSet) throws IOException;
  }
  
  /**
   * A {@link FilterStrategy} that conditionally uses a random access filter if
   * the given {@link DocIdSet} supports random access (returns a non-null value
   * from {@link DocIdSet#bits()}) and
   * {@link RandomAccessFilterStrategy#useRandomAccess(Bits, int)} returns
   * <code>true</code>. Otherwise this strategy falls back to a "zig-zag join" (
   * {@link FilteredQuery#LEAP_FROG_FILTER_STRATEGY}) strategy .
   */
  public static class RandomAccessFilterStrategy extends FilterStrategy {

    @Override
    public Scorer filteredScorer(AtomicReaderContext context, boolean scoreDocsInOrder, boolean topScorer, Weight weight, DocIdSet docIdSet) throws IOException {
      final DocIdSetIterator filterIter = docIdSet.iterator();
      if (filterIter == null) {
        // this means the filter does not accept any documents.
        return null;
      }  

      final int firstFilterDoc = filterIter.nextDoc();
      if (firstFilterDoc == DocIdSetIterator.NO_MORE_DOCS) {
        return null;
      }
      
      final Bits filterAcceptDocs = docIdSet.bits();
        // force if RA is requested
      final boolean useRandomAccess = (filterAcceptDocs != null && (useRandomAccess(filterAcceptDocs, firstFilterDoc)));
      if (useRandomAccess) {
        // if we are using random access, we return the inner scorer, just with other acceptDocs
        return weight.scorer(context, scoreDocsInOrder, topScorer, filterAcceptDocs);
      } else {
        assert firstFilterDoc > -1;
        // we are gonna advance() this scorer, so we set inorder=true/toplevel=false
        // we pass null as acceptDocs, as our filter has already respected acceptDocs, no need to do twice
        final Scorer scorer = weight.scorer(context, true, false, null);
        return (scorer == null) ? null : new LeapFrogScorer(weight, firstFilterDoc, filterIter, scorer);
      }
    }
    
    /**
     * Expert: decides if a filter should be executed as "random-access" or not.
     * random-access means the filter "filters" in a similar way as deleted docs are filtered
     * in lucene. This is faster when the filter accepts many documents.
     * However, when the filter is very sparse, it can be faster to execute the query+filter
     * as a conjunction in some cases.
     * 
     * The default implementation returns <code>true</code> if the first document accepted by the
     * filter is < 100.
     * 
     * @lucene.internal
     */
    protected boolean useRandomAccess(Bits bits, int firstFilterDoc) {
      return firstFilterDoc < 100;
    }
  }
  
  private static final class LeapFrogFilterStragey extends FilterStrategy {

    @Override
    public Scorer filteredScorer(AtomicReaderContext context,
        boolean scoreDocsInOrder, boolean topScorer, Weight weight,
        DocIdSet docIdSet) throws IOException {
      final DocIdSetIterator filterIter = docIdSet.iterator();
      if (filterIter == null) {
        // this means the filter does not accept any documents.
        return null;
      }
      
      final int firstFilterDoc = filterIter.nextDoc();
      if (firstFilterDoc == DocIdSetIterator.NO_MORE_DOCS) {
        return null;
      }
      assert firstFilterDoc > -1;
      // we are gonna advance() this scorer, so we set inorder=true/toplevel=false
      // we pass null as acceptDocs, as our filter has already respected acceptDocs, no need to do twice
      final Scorer scorer = weight.scorer(context, true, false, null);
      return (scorer == null) ? null : new LeapFrogScorer(weight, firstFilterDoc, filterIter, scorer);
    }
    
  }
  
  /**
   * A filter strategy that advances the {@link Scorer} first and consults the
   * {@link DocIdSet} for each matched document.
   * <p>
   * Use this strategy if the filter computation is more expensive than document
   * scoring or if the filter has a linear running time to compute the next
   * matching doc like exact geo distances.
   * </p>
   */
  private static final class DocFirstFilterStrategy extends FilterStrategy {

    @Override
    public Scorer filteredScorer(final AtomicReaderContext context,
        boolean scoreDocsInOrder, boolean topScorer, Weight weight,
        DocIdSet docIdSet) throws IOException {
      Bits filterAcceptDocs = docIdSet.bits();
      if (filterAcceptDocs == null) {
        final DocIdSetIterator filterIter = docIdSet.iterator();
        if (filterIter == null) {
          // this means the filter does not accept any documents.
          return null;
        }
        // TODO maybe we should fall back to leap frog 
        filterAcceptDocs = new Bits() {
          @Override
          public boolean get(int index) {
            try {
              return filterIter.docID() == index
                  || (filterIter.docID() < index && filterIter.advance(index) == index);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
          
          @Override
          public int length() {
            return context.reader().maxDoc();
          }
          
        };
      }
      final Scorer scorer = weight.scorer(context, true, false, null);
      return (scorer == null) ? null : new DocFirstScorer(weight,
          filterAcceptDocs, scorer);
    }
    
  }
  
}
