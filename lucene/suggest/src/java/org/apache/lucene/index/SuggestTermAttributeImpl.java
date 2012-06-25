package org.apache.lucene.index;

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
import org.apache.lucene.codecs.suggest.SuggestFSTBuilder;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.fst.BytesRefFSTEnum.InputOutput;

/**
 */
public class SuggestTermAttributeImpl extends CharTermAttributeImpl implements SuggestTermAttribute {
  private final SuggestFSTBuilder<Long> processor;
  private long weight;
  private BytesRef bytesRef = new BytesRef();
  private InputOutput<Long> inputOutput = new InputOutput<Long>();
  
  public SuggestTermAttributeImpl(SuggestFSTBuilder<Long> builder) {
    this.processor = builder;
    inputOutput.input = bytesRef;
    
  }
  
  @Override
  public int fillBytesRef() {
    inputOutput.output = weight;
    processor.combine(getBytesRef(), inputOutput);
    return getBytesRef().hashCode();
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
    bytesRef.length = 0;
  }

  @Override
  public void copyTo(AttributeImpl target) {
    super.copyTo(target);
    ((SuggestTermAttribute)target).setWeight(weight);
    ((SuggestTermAttribute)target).setBytesRef(bytesRef);
  }

  @Override
  public void setBytesRef(BytesRef ref) {
    this.bytesRef.copyBytes(ref);
  }

}
