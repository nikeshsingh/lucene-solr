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
final class SortedBytesMergeUtils {

  private SortedBytesMergeUtils() {
    // no instance
  }

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
          currentOrdBase += slice.ordMapping.length;
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
      assert ctx.size == -1 || ctx.size == spare.length : "size: " + ctx.size
          + " spare: " + spare.length;

      if (recordOffsets) {
        offset += spare.length;
        if (merger.currentOrd >= offsets.length) {
          offsets = ArrayUtil.grow(offsets, merger.currentOrd + 1);
        }
        offsets[merger.currentOrd] = offset;
      }
      datOut.writeBytes(spare.bytes, spare.offset, spare.length);
      merger.pushTop();
    }
    ctx.offsets = offsets;
    assert offsets == null || offsets[merger.currentOrd - 1] == offset;
    return merger.currentOrd;
  }

  /*
   * In order to merge we need to map the ords used in each segment to the new
   * global ords in the new segment. Additionally we need to drop values that
   * are not referenced anymore due to deleted documents. This method walks all
   * live documents and fetches their current ordinal. We store this ordinal per
   * slice and (SortedSourceSlice#ordMapping) and remember the doc to ord
   * mapping in docIDToRelativeOrd. After the merge SortedSourceSlice#ordMapping
   * contains the new global ordinals for the relative index.
   */
  private static void createOrdMapping(MergeState mergeState, int[] docToOrd,
      List<SortedSourceSlice> slices) {
    for (SortedSourceSlice currentSlice : slices) {
      final int readerIdx = currentSlice.readerIdx;
      final int[] currentDocMap = mergeState.docMaps[readerIdx];
      if (currentSlice != null) {
        currentSlice.docIDToRelativeOrd = docToOrd;
        int docBase = mergeState.docBase[readerIdx];
        currentSlice.docToOrdStart = docBase;
        int numDocs = 0;
        if (currentDocMap != null) {
          for (int j = 0; j < currentDocMap.length; j++) {
            int ord = currentSlice.source.ord(j);
            if (currentDocMap[j] != -1) { // not deleted
              final int doc = currentDocMap[j];
              numDocs++;
              docToOrd[docBase + doc] = ord;
              // use ord + 1 to identify unreferenced values (ie. == 0)
              currentSlice.ordMapping[ord] = ord + 1;
            }

          }
        } else { // no deletes
          final IndexReaderAndLiveDocs indexReaderAndLiveDocs = mergeState.readers
              .get(readerIdx);
          numDocs = indexReaderAndLiveDocs.reader.numDocs();
          assert indexReaderAndLiveDocs.liveDocs == null;
          for (int doc = 0; doc < numDocs; doc++) {
            final int ord = currentSlice.source.ord(doc);
            docToOrd[docBase + doc] = ord;
            // use ord + 1 to identify unreferenced values (ie. == 0)
            currentSlice.ordMapping[ord] = ord + 1;
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
        final SortedSourceSlice popped = top[numTop++] = queue.pop();
        // use ord + 1 to identify unreferenced values (ie. == 0)
        popped.ordMapping[popped.relativeOrd] = currentOrd + 1;
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
    /* global array indexed by docID containg the relative ord for the doc */
    int[] docIDToRelativeOrd;
    /*
     * maps relative ords to merged global ords - index is relative ord value
     * new global ord this map gets updates as we merge ords. later we use the
     * docIDtoRelativeOrd to get the previous relative ord to get the new ord
     * from the relative ord map.
     */
    int[] ordMapping;

    /* start index into docIDToRelativeOrd */
    int docToOrdStart;
    /* end index into docIDToRelativeOrd */
    int docToOrdEnd;
    BytesRef current = new BytesRef();
    /* the currently merged relative ordinal */
    int relativeOrd = -1;

    SortedSourceSlice(int readerIdx, SortedSource source, int[] docToOrd) {
      super();
      this.readerIdx = readerIdx;
      this.source = source;
      this.docIDToRelativeOrd = docToOrd;
      this.ordMapping = new int[source.getValueCount()];
    }

    BytesRef next() {
      for (int i = relativeOrd + 1; i < ordMapping.length; i++) {
        if (ordMapping[i] != 0) { // skip ords that are not referenced anymore
          source.getByOrd(i, current);
          relativeOrd = i;
          return current;
        }
      }
      return null;
    }

    void writeOrds(PackedInts.Writer writer) throws IOException {
      for (int i = docToOrdStart; i < docToOrdEnd; i++) {
        final int mappedOrd = docIDToRelativeOrd[i];
        assert mappedOrd < ordMapping.length;
        assert ordMapping[mappedOrd] > 0 : "illegal mapping ord mapps to an unreferenced value";
        writer.add(ordMapping[mappedOrd] - 1);
      }
    }
  }

  /*
   * if a segment has no values at all we use this source to fill in the missing
   * value in the right place (depending on the comparator used)
   */
  private static final class MissingValueSource extends SortedSource {

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

  /*
   * merge queue
   */
  private static final class MergeQueue extends
      PriorityQueue<SortedSourceSlice> {
    final Comparator<BytesRef> comp;

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
