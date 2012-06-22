package org.apache.lucene.index.suggest.codecs;

import java.io.IOException;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.UnicodeUtil;

/**
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

public final class SuggestTermTokenStream extends TokenStream {
  
  private final SuggestTermAttribute weightAttr;
  private final CharTermAttribute charTermAttr;
  
  private final CharsRef term = new CharsRef();
  private long weight; 
  private boolean exhausted = false;

  public SuggestTermTokenStream(TermWeightProcessor processor) {
    super(new SuggestAttributeFactory(processor));
    this.weightAttr = addAttribute(SuggestTermAttribute.class);
    this.charTermAttr = addAttribute(CharTermAttribute.class);
  }

  @Override
  public final boolean incrementToken() throws IOException {
    if (!exhausted) {
      charTermAttr.copyBuffer(term.chars, term.offset, term.length);
      weightAttr
      .setWeight(weight);
      return exhausted=true;
    }
    return false;
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    exhausted = false;
  }
  
  public void set(BytesRef ref, long weight) {
    UnicodeUtil.UTF8toUTF16(ref, term);
    this.weight = weight;
  }
  
  
  
  
  
}
