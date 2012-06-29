package org.apache.lucene.search.suggest.fst;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.index.FSTIterator;
import org.apache.lucene.search.spell.TermFreqIterator;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.BytesRefFSTEnum;
import org.apache.lucene.util.fst.BytesRefFSTEnum.InputOutput;
import org.apache.lucene.util.fst.FST.Arc;
import org.apache.lucene.util.fst.FST.BytesReader;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.UpToTwoPositiveIntOutputs;
import org.apache.lucene.util.fst.Util;
import org.apache.lucene.util.fst.UpToTwoPositiveIntOutputs.TwoLongs;
import org.apache.pdfbox.pdmodel.graphics.predictor.Up;

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

public class CompositeWFSTCompletionLookup extends Lookup {
  private final WFSTCompletionLookup[] leaves;
  private final CharSequence[] upperBounds;
  private final CharSequence[] lowerBounds;
  
  public CompositeWFSTCompletionLookup(FSTIterator<Long> iterator)
      throws IOException {
    List<WFSTCompletionLookup> lookups = new ArrayList<WFSTCompletionLookup>();
    List<CharSequence> upperB = new ArrayList<CharSequence>();
    List<CharSequence> lowerB = new ArrayList<CharSequence>();
    
    FST<Long> fst;
    final BytesRef spare = new BytesRef();
    while ((fst = iterator.next()) != null) {
      WFSTCompletionLookup lookup = new WFSTCompletionLookup();
      lookup.setFST(fst);
      lookups.add(lookup);
      
      iterator.fillUpper(spare);
      CharsRef upper = new CharsRef();
      UnicodeUtil.UTF8toUTF16(spare, upper);
      upperB.add(upper);
      iterator.fillLower(spare);
      CharsRef lower = new CharsRef();
      UnicodeUtil.UTF8toUTF16(spare, lower);
      lowerB.add(lower);
    }
    upperBounds = upperB.toArray(new CharSequence[0]);
    lowerBounds = lowerB.toArray(new CharSequence[0]);
    leaves = lookups.toArray(new WFSTCompletionLookup[0]);
  }
  
  @Override
  public void build(TermFreqIterator tfit) throws IOException {
    throw new UnsupportedOperationException();
    
  }
  
  // private static int bounds(CharSequence key, CharSequence[] arr) {
  // int binarySearch = Arrays.binarySearch(arr, key,
  // Lookup.CHARSEQUENCE_COMPARATOR);
  // }
  
  @Override
  public List<LookupResult> lookup(CharSequence key, boolean onlyMorePopular,
      int num) {
    if (leaves.length > 1) {
      int upper = Arrays.binarySearch(upperBounds, key,
          Lookup.CHARSEQUENCE_COMPARATOR);
      int lower = Arrays.binarySearch(lowerBounds, key,
          Lookup.CHARSEQUENCE_COMPARATOR);
      
    } else {
      return leaves[0].lookup(key, onlyMorePopular, num);
    }
    
    return null;
  }
  
  @Override
  public boolean store(OutputStream output) throws IOException {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public boolean load(InputStream input) throws IOException {
    throw new UnsupportedOperationException();
  }
  
  public static void main(String[] args) throws IOException {
    UpToTwoPositiveIntOutputs outputs = UpToTwoPositiveIntOutputs
        .getSingleton(true);
    Builder<Object> builder = new Builder(FST.INPUT_TYPE.BYTE1, outputs);
    String[] upper = new String[] {"a", "abc", "b"};
    String[] lower = new String[] {"ab", "abce", "bx"};
    buildFst(toBRArray(upper), toBRArray(lower), builder);
    
    FST<Object> fst = builder.finish();
    BytesRefFSTEnum<Object> en = new BytesRefFSTEnum<Object>(fst);
    InputOutput<Object> seekFloor = en.seekFloor(new BytesRef("ab"));
    Util.toDot(fst, new OutputStreamWriter(new FileOutputStream(new File(
        "/tmp/t.dot"))), true, true);
     System.out.println(seekFloor.output);
    Object walk = walk(fst, new BytesRef("ab"));
    if (walk instanceof Long) {
      System.out.println(walk);
    } else {
      TwoLongs l = (TwoLongs) walk;
      System.out.println(l);
      System.out.println("from: " + (Integer.MAX_VALUE - l.first));
      System.out.println("to : " + l.second);
    }
    
    System.out.println();
    
  }
  
  public static Object walk(FST<Object> fst, BytesRef scratch)
      throws IOException {
    BytesReader bytesReader = fst.getBytesReader(0);
    Arc<Object> arc = new Arc<Object>();
    fst.getFirstArc(arc);
    arc = fst.readFirstTargetArc(arc, arc, bytesReader);
    Object output = arc.output;
    byte[] bytes = scratch.bytes;
    int pos = scratch.offset;
    int end = pos + scratch.length;
    int targetLabel = bytes[pos] & 0xff;
    do {
      
      if (arc.label == targetLabel) {
        output = fst.outputs.add(output, arc.output);
        if (targetLabel == FST.END_LABEL) {
          return output;
        }
        arc = fst.readFirstTargetArc(arc, arc, bytesReader);
        if (++pos < end) {
          targetLabel = bytes[pos] & 0xff;  
        } else {
          targetLabel = FST.END_LABEL;
        }
      } else if (arc.label > targetLabel) {
        arc = fst.readFirstTargetArc(arc, arc, bytesReader);
        output = fst.outputs.add(output, arc.output);
        return output;
      } else if (!arc.isLast()) {
        // System.out.println("  check next label=" + fst.readNextArcLabel(arc)
        // + " (" + (char) fst.readNextArcLabel(arc) + ")");
        if (fst.readNextArcLabel(arc, bytesReader) > targetLabel) {
          output = fst.outputs.add(output, arc.output);
          return output;
        } else {
          // keep scanning
          fst.readNextArc(arc, bytesReader);
        }
      } else {
        output = fst.outputs.add(output, arc.output);
        return output;
      }
    } while (true);
  }
  
  public static BytesRef[] toBRArray(String[] arr) {
    BytesRef[] a = new BytesRef[arr.length];
    for (int i = 0; i < a.length; i++) {
      a[i] = new BytesRef(arr[i]);
    }
    return a;
  }
  
  public static void buildFst(BytesRef[] upper, BytesRef[] lower,
      Builder<Object> builder) throws IOException {
    IntsRef scratchIntsRef = new IntsRef();
    int x = 0;;
    for (int i = 0; i < lower.length; i++) {
      Util.toIntsRef(upper[i], scratchIntsRef);
      System.out.println(upper[i].utf8ToString());
      builder.add(scratchIntsRef, (long) Integer.MAX_VALUE - i);
      builder.add(scratchIntsRef, (long) i);
      System.out.println(lower[i].utf8ToString());

       Util.toIntsRef(lower[i], scratchIntsRef);
       builder.add(scratchIntsRef, (long) Integer.MAX_VALUE - i);
       builder.add(scratchIntsRef, (long) i);
    }
    
  }
  
  public static int sharedPrefix(BytesRef term, BytesRef lastTerm) {
    int start = 0;
    final int limit = term.length < lastTerm.length ? term.length
        : lastTerm.length;
    while (start < limit) {
      if (term.bytes[start + term.offset] != lastTerm.bytes[start
          + lastTerm.offset]) break;
      start++;
    }
    
    return start;
  }
  
}
