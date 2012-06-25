package org.apache.lucene.codecs.suggest;

import java.io.IOException;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.BytesRefFSTEnum;
import org.apache.lucene.util.fst.BytesRefFSTEnum.InputOutput;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PairOutputs;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;

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

public class WeightedSuggestFSTBuilder extends SuggestFSTBuilder<Long> {
  
  
  private Builder<Long> builder;
  private final IntsRef scratchIntsRef = new IntsRef();
  private final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton(true);
  private boolean doPackFST;
  private float acceptableOverheadRatio;
  private BytesRef firstTerm = new BytesRef();
  private int totalNumTerms = 0;
  private int currentNumTerms = 0;
  private BytesRef previousTerm = new BytesRef();

  public WeightedSuggestFSTBuilder(boolean doPackFST, float acceptableOverheadRatio)  {
    this.doPackFST = doPackFST;
    this.acceptableOverheadRatio = acceptableOverheadRatio;
    builder = newBuilder(doPackFST, acceptableOverheadRatio);
  }
  private Builder<Long> newBuilder(boolean doPackFST, float acceptableOverheadRatio) {
    return new Builder<Long>(FST.INPUT_TYPE.BYTE1, 0, 0, true, true, Integer.MAX_VALUE, outputs, null, doPackFST, acceptableOverheadRatio);
  }
  
  private long split(BytesRef term) {
    term.length -= 5;
    return asInt(term, term.offset + term.length + 1);
  }
  
  public BytesRef combine(BytesRef spare, InputOutput<Long> inputOutput) {
    BytesRef term = inputOutput.input;
    if (term.length + 5 >= spare.length) {
      spare.grow(term.length + 5);
    }
    spare.copyBytes(term);
    spare.bytes[spare.length] = (byte) 0;
    copyInternal(spare, (int) inputOutput.output.longValue(), spare.length + 1);
    spare.length = term.length + 5;
    return spare;
  }
  
  @Override
  public BuildStatus add(BytesRef term) throws IOException {
    
    
    long weight = split(term);
    if (totalNumTerms != 0 &&term.bytesEquals(previousTerm)) {
      return BuildStatus.Duplicate;
    }
    builder.add(Util.toIntsRef(term, scratchIntsRef ), weight);
    previousTerm.copyBytes(term);
    if (currentNumTerms == 0) {
      firstTerm.copyBytes(term);
    }
    totalNumTerms++;
    currentNumTerms++;
    return BuildStatus.Added;
  }
  
  @Override
  public void finish() {
  }
  
  @Override
  public FST<Long> finishCurrentFST(BytesRef upper, BytesRef lower) throws IOException {
    FST<Long> fst = builder.finish();
    if (doPackFST) {
      fst = fst.pack(3, Math.max(10, fst.getNodeCount()/4), acceptableOverheadRatio);
    }
    upper.copyBytes(firstTerm);
    lower.copyBytes(previousTerm);
    firstTerm.copyBytes(previousTerm); // reset
    return fst;
  }
  
  
  // copied from DocValuesArraySource -- maybe this is generally useful
  
  private static void copyInternal(BytesRef ref, int value, int startOffset) {
    ref.bytes[startOffset] = (byte) (value >> 24);
    ref.bytes[startOffset + 1] = (byte) (value >> 16);
    ref.bytes[startOffset + 2] = (byte) (value >> 8);
    ref.bytes[startOffset + 3] = (byte) (value);
  }
  
  private static int asInt(BytesRef b, int pos) {
    return ((b.bytes[pos++] & 0xFF) << 24) | ((b.bytes[pos++] & 0xFF) << 16)
        | ((b.bytes[pos++] & 0xFF) << 8) | (b.bytes[pos] & 0xFF);
  }
  @Override
  public long totalTermCount() {
    return totalNumTerms;
  }
  @Override
  public FST<Long> load(IndexInput input) throws IOException {
    return new FST<Long>(input, PositiveIntOutputs.getSingleton(false));
  }
  @Override
  public BytesRefFSTEnum<Long> openEnum(FST<Long> fst) {
    return new BytesRefFSTEnum<Long>(fst);
  }
  
}
