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

import java.io.IOException;
import java.util.Collection;

/**  A <code>FilteredScorer</code> contains another Scorer, which it
 * uses as its basic source of data, possibly transforming the data along the
 * way or providing additional functionality. The class
 * <code>FilteredScorer</code> itself simply implements all abstract methods
 * of <code>Scorer</code> with versions that pass all requests to the
 * contained scorer. Subclasses of <code>FilteredScorer</code> may
 * further override some of these methods and may also provide additional
 * methods and fields.
 */
public abstract class FilteredScorer extends Scorer {
  protected final Scorer in;
  
  public FilteredScorer(Scorer in) {
    super(in.weight);
    this.in = in;
  }

  @Override
  public float score() throws IOException {
    return in.score();
  }

  @Override
  public float freq() throws IOException {
    return in.freq();
  }

  @Override
  public Collection<ChildScorer> getChildren() {
    return in.getChildren();
  }

  @Override
  public long estimateCost() {
    return in.estimateCost();
  }

  @Override
  public int docID() {
    return in.docID();
  }

  @Override
  public int nextDoc() throws IOException {
    return in.nextDoc();
  }

  @Override
  public int advance(int target) throws IOException {
    return in.advance(target);
  }
}