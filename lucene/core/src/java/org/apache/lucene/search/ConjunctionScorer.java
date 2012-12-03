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

import org.apache.lucene.util.ArrayUtil;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;

/** Scorer for conjunctions, sets of queries, all of which are required. */
class ConjunctionScorer extends Scorer {
  
  private final Scorer[] scorers;
  private int lastDoc = -1;
  private final Scorer lead;

  public ConjunctionScorer(Weight weight, Collection<Scorer> scorers) throws IOException {
    this(weight, scorers.toArray(new Scorer[scorers.size()]));
  }

  public ConjunctionScorer(Weight weight, Scorer... scorers) throws IOException {
    super(weight);
    this.scorers = scorers;
    // Sort the array the first time to allow the least frequent Scorer to
    // lead the matching.
    ArrayUtil.mergeSort(scorers, new Comparator<Scorer>() { // sort the array
      public int compare(Scorer o1, Scorer o2) {
        return Long.signum(o1.estimateCost() - o2.estimateCost());
      }
    });
    lead = scorers[0]; // least frequent Scorer leads the intersection
  }

  private int doNext(int doc) throws IOException {
    do {
      if (lead.docID() == DocIdSetIterator.NO_MORE_DOCS) {
        return NO_MORE_DOCS;
      }
      advanceHead: do {
        for (int i = 1; i < scorers.length; i++) {
          int currentDoc = scorers[i].docID();
          if (currentDoc < doc) {
            currentDoc = scorers[i].advance(doc);
          }
          if (currentDoc > doc) {
            // DocsEnum beyond the current doc - break and advance lead
            break advanceHead;
          }
        }
        // success - all DocsEnums are on the same doc
        return doc;
      } while (true);
      // advance head for next iteration
      doc = lead.nextDoc();  
    } while (true);
  }
  
  @Override
  public int advance(int target) throws IOException {
    int doc = lead.advance(target);
    return lastDoc = doNext(doc);
  }

  @Override
  public int docID() {
    return lastDoc;
  }
  
  @Override
  public int nextDoc() throws IOException {
    int doc = lead.nextDoc();
    return lastDoc = doNext(doc);
  }
  
  @Override
  public float score() throws IOException {
    // TODO: sum into a double and cast to float if we ever send required clauses to BS1
    float sum = 0.0f;
    for (Scorer scorer : scorers) {
      sum += scorer.score();
    }
    return sum;
  }

  @Override
  public int freq() throws IOException {
    return scorers.length;
  }

  @Override
  public Collection<ChildScorer> getChildren() {
    ArrayList<ChildScorer> children = new ArrayList<ChildScorer>(scorers.length);
    for (Scorer scorer : scorers) {
      children.add(new ChildScorer(scorer, "MUST"));
    }
    return children;
  }

  @Override
  public long estimateCost() {
    return lead.estimateCost() * scorers.length;
  }
}
