package org.apache.lucene.index;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.suggest.SuggestFSTBuilder;
import org.apache.lucene.codecs.suggest.SuggestPostingsFormat;
import org.apache.lucene.codecs.suggest.WFSTSuggestPostingsFormat;
import org.apache.lucene.codecs.suggest.WeightedSuggestFSTBuilder;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.fst.WFSTCompletionLookup;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.packed.PackedInts;

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

public class WeightedCompletionBuilder extends CompletionBuilder {
  final WFSTSuggestPostingsFormat format = new WFSTSuggestPostingsFormat();
  public WeightedCompletionBuilder(IndexWriter writer, String field) {
    super(writer, field);
  }

  public WeightedCompletionBuilder(IndexWriterConfig config, Directory dir)
      throws CorruptIndexException, LockObtainFailedException, IOException {
    super(config, dir);
  }

  @Override
  protected SuggestFSTBuilder<Long> newFSTBuilder() {
    return new WeightedSuggestFSTBuilder(false, PackedInts.DEFAULT);
  }
  
  @Override
  protected PostingsFormat newSuggestPostingsFormat() {
    return format;
  }
  

  @Override
  protected long encodeWeight(long value) {
    /** weight -> cost */
    if (value < 0 || value > Integer.MAX_VALUE) {
      throw new UnsupportedOperationException("cannot encode value: " + value);
    }
    return Integer.MAX_VALUE - (int) value;
  }
  
}
