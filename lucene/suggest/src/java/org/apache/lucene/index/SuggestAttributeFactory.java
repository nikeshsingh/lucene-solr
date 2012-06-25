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

import org.apache.lucene.codecs.suggest.SuggestFSTBuilder;
import org.apache.lucene.util.Attribute;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeSource;

/**
 */
public class SuggestAttributeFactory extends AttributeSource.AttributeFactory {
  private final SuggestFSTBuilder<Long> builder;
  private final AttributeSource.AttributeFactory delegate;
  
  public SuggestAttributeFactory(SuggestFSTBuilder<Long> builder) {
    this(AttributeSource.AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY, builder);
  }
  
  public SuggestAttributeFactory(AttributeSource.AttributeFactory delegate, SuggestFSTBuilder<Long> builder) {
    this.delegate = delegate;
    this.builder = builder;
  }
  
  @Override
  public AttributeImpl createAttributeInstance(
      Class<? extends Attribute> attClass) {
    return attClass.isAssignableFrom(SuggestTermAttributeImpl.class)
    ? new SuggestTermAttributeImpl(builder)
    : delegate.createAttributeInstance(attClass);
  }
}
