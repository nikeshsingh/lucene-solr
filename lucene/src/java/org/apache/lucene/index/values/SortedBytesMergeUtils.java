package org.apache.lucene.index.values;
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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.index.codecs.MergeState;
import org.apache.lucene.index.codecs.MergeState.IndexReaderAndLiveDocs;
import org.apache.lucene.index.values.IndexDocValues.SortedSource;
import org.apache.lucene.index.values.IndexDocValues.Source;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.packed.PackedInts;

/**
 * 
 * @lucene.internal
 *
 */
public final class SortedBytesMergeUtils {

  static MergeContext init(ValueType type, IndexDocValues[] docValues,
      Comparator<BytesRef> comp, MergeState mergeState) {
    int size = 0;
    for (IndexDocValues indexDocValues : docValues) {
      if (indexDocValues != null) {
        size = indexDocValues.getValueSize();
        break;
      }
    }

    MergeContext ctx = new MergeContext(comp, mergeState, size, type);

    return ctx;
  }

  public static final class MergeContext {
    private final Comparator<BytesRef> comp;
    private final BytesRef missingValue = new BytesRef();
    final int size;
    final ValueType type;
    final int[] docToEntry;
    long[] offsets;

    public MergeContext(Comparator<BytesRef> comp, MergeState mergeState,
        int size, ValueType type) {
      this.comp = comp;
      this.size = size;
      this.type = type;
      if (size >= 0) {
        missingValue.grow(size);
        missingValue.length = size;
      }
      docToEntry = new int[mergeState.mergedDocCount];
    }
  }

  static List<SortedSourceSlice> buildSlices(MergeState mergeState,
      IndexDocValues[] docValues, MergeContext ctx) throws IOException {
    List<SortedSourceSlice> slices = new ArrayList<SortedSourceSlice>();
    int currentOrdBase = 0;
    for (int i = 0; i < docValues.length; i++) {
      if (docValues[i] != null) {
        Source directSource = docValues[i].getDirectSource();
        if (directSource != null) {
          final SortedSourceSlice slice = new SortedSourceSlice(i,
              directSource.asSortedSource(), ctx.docToEntry);
          currentOrdBase += slice.ordsMap.length;
          slices.add(slice);
          continue;
        }
      }
      slices.add(new SortedSourceSlice(i, new MissingValueSource(
          ctx.missingValue, ctx.comp, ctx.type), ctx.docToEntry));

    }
    createOrdMapping(mergeState, ctx.docToEntry, slices);
    return slices;
  }

  static int mergeRecords(MergeContext ctx, IndexOutput datOut,
      List<SortedSourceSlice> slices) throws IOException {
    final RecordMerger merger = new RecordMerger(new MergeQueue(slices.size(),
        ctx.comp), slices.toArray(new SortedSourceSlice[0]));
    long[] offsets = ctx.offsets;
    final boolean recordOffsets = offsets != null;
    long offset = 0;
    BytesRef spare;
    merger.pushTop();
    while (merger.queue.size() > 0) {
      merger.pullTop();
      spare = merger.current;
      assert ctx.size == -1 || ctx.size == spare.length : "size: " + ctx.size + " spare: "
          + spare.length;

      if (recordOffsets) {
        offset+=spare.length;
        if (merger.currentOrd >= offsets.length) {
          offsets = ArrayUtil.grow(offsets, merger.currentOrd+1);
        }
        offsets[merger.currentOrd] = offset; 
      }
      datOut.writeBytes(spare.bytes, spare.offset, spare.length);
      merger.pushTop();
    }
    ctx.offsets = offsets;
    assert offsets == null ||  offsets[merger.currentOrd-1] == offset;
    return merger.currentOrd;
  }
  
  static private void createOrdMapping(MergeState mergeState, int[] docToEntry,
      List<SortedSourceSlice> slices) {
    for (SortedSourceSlice currentSlice : slices) {
      final int readerIdx = currentSlice.readerIdx;
      final int[] currentDocMap = mergeState.docMaps[readerIdx];
      if (currentSlice != null) {
        currentSlice.docToOrd = docToEntry;
        int docBase = mergeState.docBase[readerIdx];
        currentSlice.docToOrdStart = docBase;
        int numDocs = 0;
        if (currentDocMap != null) {
          for (int j = 0; j < currentDocMap.length; j++) {
            int ord = currentSlice.source.ord(j);
            if (currentDocMap[j] != -1) { // not deleted
              final int doc = currentDocMap[j];
              numDocs++;
              currentSlice.ordsMap[ord] = docToEntry[docBase + doc] = ord;
            }
           
          }
        } else { // no deletes
          final IndexReaderAndLiveDocs indexReaderAndLiveDocs = mergeState.readers
              .get(readerIdx);
          numDocs = indexReaderAndLiveDocs.reader.numDocs();
          assert indexReaderAndLiveDocs.liveDocs == null;
          for (int doc = 0; doc < numDocs; doc++) {
            final int ord = currentSlice.source.ord(doc);
            currentSlice.ordsMap[ord] = docToEntry[docBase + doc] = ord;
          }
        }
        currentSlice.docToOrdEnd = currentSlice.docToOrdStart + numDocs;
      }
    }
  }

  private static final class RecordMerger {
    final MergeQueue queue;
    final SortedSourceSlice[] top;
    int numTop;
    BytesRef current;
    int currentOrd = -1;

    RecordMerger(MergeQueue queue, SortedSourceSlice[] top) {
      super();
      this.queue = queue;
      this.top = top;
      this.numTop = top.length;

    }

    private void pullTop() {
      // extract all subs from the queue that have the same
      // top record
      assert numTop == 0;
      assert currentOrd >= 0;
      while (true) {
        SortedSourceSlice popped = top[numTop++] = queue.pop();
        popped.ordsMap[popped.relativeOrd] = currentOrd;
        if (queue.size() == 0
            || !(queue.top()).current.bytesEquals(top[0].current)) {
          break;
        }
      }
      current = top[0].current;
    }

    private void pushTop() throws IOException {
      // call next() on each top, and put back into queue
      for (int i = 0; i < numTop; i++) {
        top[i].current = top[i].next();
        if (top[i].current != null) {
          queue.add(top[i]);
        }
      }
      currentOrd++;
      numTop = 0;
    }
  }

  static class SortedSourceSlice {
    final SortedSource source;
    final int readerIdx;
    int[] docToOrd;
    int docToOrdStart;
    int docToOrdEnd;
    BytesRef current = new BytesRef();
    int relativeOrd = -1;
    int[] ordsMap;

    SortedSourceSlice(int readerIdx, SortedSource source, int[] docToOrd) {
      super();
      this.readerIdx = readerIdx;
      this.source = source;
      this.docToOrd = docToOrd;
      this.ordsMap = new int[source.getValueCount()];
      Arrays.fill(this.ordsMap, -1);
    }

    BytesRef next() {
      for (int i = relativeOrd + 1; i < ordsMap.length; i++) {
        if (ordsMap[i] != -1) {
          source.getByOrd(i, current);
          relativeOrd = i;
          return current;
        }
      }
      return null;
    }

    void writeOrds(PackedInts.Writer writer) throws IOException {
      for (int i = docToOrdStart; i < docToOrdEnd; i++) {
        final int mappedOrd = docToOrd[i];
        assert mappedOrd < ordsMap.length;
        assert ordsMap[mappedOrd] > -1;
        writer.add(ordsMap[mappedOrd]);
      }
    }
  }

  private static class MissingValueSource extends SortedSource {

    private BytesRef missingValue;

    public MissingValueSource(BytesRef missingValue, Comparator<BytesRef> comp,
        ValueType type) {
      super(type, comp);
      this.missingValue = missingValue;
    }

    @Override
    public int ord(int docID) {
      return 0;
    }

    @Override
    public BytesRef getByOrd(int ord, BytesRef bytesRef) {
      bytesRef.copy(missingValue);
      return bytesRef;
    }

    @Override
    public int getValueCount() {
      return 1;
    }

  }

  private static class MergeQueue extends PriorityQueue<SortedSourceSlice> {
    Comparator<BytesRef> comp;

    public MergeQueue(int maxSize, Comparator<BytesRef> comp) {
      super(maxSize);
      this.comp = comp;
    }

    @Override
    protected boolean lessThan(SortedSourceSlice a, SortedSourceSlice b) {
      int cmp = comp.compare(a.current, b.current);
      if (cmp != 0) {
        return cmp < 0;
      } else {
        return a.docToOrdStart < b.docToOrdStart;
      }
    }

  }
}
