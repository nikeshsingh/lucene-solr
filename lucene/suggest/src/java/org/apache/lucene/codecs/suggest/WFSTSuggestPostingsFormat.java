package org.apache.lucene.codecs.suggest;

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

public class WFSTSuggestPostingsFormat extends SuggestPostingsFormat<Long> {
  
  private final boolean doPackFST;
  private final float acceptableOverheadRatio;
  
  public WFSTSuggestPostingsFormat() {
    super("WFSTSuggest");
    doPackFST = false;
    acceptableOverheadRatio = PackedInts.DEFAULT;
  }
  
  public WFSTSuggestPostingsFormat(boolean doPackFST,
      float acceptableOverheadRatio) {
    super("wfst-suggest");
    this.doPackFST = doPackFST;
    this.acceptableOverheadRatio = acceptableOverheadRatio;
  }
  
  @Override
  public SuggestFSTBuilder<Long> newBuilder() {
    return new WeightedSuggestFSTBuilder(doPackFST, acceptableOverheadRatio);
  }
  
}
