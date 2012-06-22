package org.apache.lucene.index.suggest.codecs;

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

import org.apache.lucene.analysis.tokenattributes.CharTermAttributeImpl;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.BytesRef;

/**
 */
public class SuggestTermAttributeImpl extends CharTermAttributeImpl implements SuggestTermAttribute {
  private final TermWeightProcessor processor;
  private long weight;
  
  public SuggestTermAttributeImpl(TermWeightProcessor processor) {
    this.processor = processor;
  }
  
  @Override
  public int fillBytesRef() {
    BytesRef bytes = getBytesRef();
    processor.combine(bytes, bytes, weight);
    return bytes.hashCode();
  }

  @Override
  public void setWeight(long weight) {
    this.weight = weight;
  }

  @Override
  public long getWeight() {
    return weight;
  }

  @Override
  public void clear() {
    super.clear();
    weight = 0;
  }

  @Override
  public void copyTo(AttributeImpl target) {
    super.copyTo(target);
    ((SuggestTermAttribute)target).setWeight(weight);
  }

}
