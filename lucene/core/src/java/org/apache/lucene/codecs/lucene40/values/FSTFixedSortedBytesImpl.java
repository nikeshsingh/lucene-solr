package org.apache.lucene.codecs.lucene40.values;

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

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.codecs.lucene40.values.Bytes.BytesReaderBase;
import org.apache.lucene.codecs.lucene40.values.Bytes.DerefBytesWriterBase;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValues.SortedSource;
import org.apache.lucene.index.DocValues.Type;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SortedBytesMergeUtils;
import org.apache.lucene.index.SortedBytesMergeUtils.MergeContext;
import org.apache.lucene.index.SortedBytesMergeUtils.SortedSourceSlice;
import org.apache.lucene.index.SortedBytesMergeUtils.ToFSTBytesRefConsumer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;
import org.apache.lucene.util.packed.PackedInts;

// Stores fixed-length byte[] by deref, ie when two docs
// have the same value, they store only 1 byte[]

/**
 * @lucene.experimental
 */
class FSTFixedSortedBytesImpl {

  static final String CODEC_NAME_IDX = "FSTFixedSortedBytesIdx";
  static final String CODEC_NAME_DAT = "FSTFixedSortedBytesDat";
  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;

  static final class Writer extends DerefBytesWriterBase {
    private final Comparator<BytesRef> comp;
    private final Type type;

    public Writer(Directory dir, String id, Comparator<BytesRef> comp,
        Counter bytesUsed, IOContext context, float acceptableOverheadRatio, boolean fixed) throws IOException {
      super(dir, id, CODEC_NAME_IDX, CODEC_NAME_DAT, VERSION_CURRENT, bytesUsed, context, acceptableOverheadRatio, Type.BYTES_FIXED_SORTED);
      this.comp = comp;
      if (fixed) {
        type = Type.BYTES_FIXED_SORTED;
      } else {
        size = 0;
        type = Type.BYTES_VAR_SORTED;
      }
    }

    @Override
    public void merge(MergeState mergeState, DocValues[] docValues)
        throws IOException {
      boolean success = false;
      try {
        final MergeContext ctx = SortedBytesMergeUtils.init(type, docValues, comp, mergeState.segmentInfo.getDocCount());
        List<SortedSourceSlice> slices = SortedBytesMergeUtils.buildSlices(mergeState.docBase, mergeState.docMaps, docValues, ctx);
        final IndexOutput datOut = getOrCreateDataOut();
        datOut.writeInt(ctx.sizePerValues);
        final int maxOrd = SortedBytesMergeUtils.mergeRecords(ctx, new ToFSTBytesRefConsumer(datOut, acceptableOverheadRatio), slices);
        
        final IndexOutput idxOut = getOrCreateIndexOut();
        idxOut.writeInt(maxOrd);
        final PackedInts.Writer ordsWriter = PackedInts.getWriter(idxOut, ctx.docToEntry.length,
            PackedInts.bitsRequired(maxOrd), PackedInts.DEFAULT);
        for (SortedSourceSlice slice : slices) {
          slice.writeOrds(ordsWriter);
        }
        ordsWriter.finish();
        success = true;
      } finally {
        releaseResources();
        if (success) {
          IOUtils.close(getIndexOut(), getDataOut());
        } else {
          IOUtils.closeWhileHandlingException(getIndexOut(), getDataOut());
        }

      }
    }
    
    @Override
    protected void checkSize(BytesRef bytes) {
      if (type == Type.BYTES_FIXED_SORTED) {
        super.checkSize(bytes);
      }
    }

    // Important that we get docCount, in case there were
    // some last docs that we didn't see
    @Override
    public void finishInternal(int docCount) throws IOException {
      fillDefault(docCount);
      final IndexOutput datOut = getOrCreateDataOut();
      final int count = hash.size();
      final int[] address = new int[count];
      datOut.writeInt(type == Type.BYTES_VAR_SORTED ? -1 : size);
      final ToFSTBytesRefConsumer consumer = new ToFSTBytesRefConsumer(datOut, acceptableOverheadRatio);
      final int[] sortedEntries = hash.sort(comp);
      // first dump bytes data, recording address as we go
      final BytesRef spare = new BytesRef(size);
      for (int i = 0; i < count; i++) {
        final int e = sortedEntries[i];
        final BytesRef bytes = hash.get(e, spare);
        assert type == Type.BYTES_VAR_SORTED || bytes.length == size : bytes.length + " " + size + " " + type;
        consumer.consume(bytes, i, -1);
        address[e] = i;
      }
      consumer.flush();
      final IndexOutput idxOut = getOrCreateIndexOut();
      idxOut.writeInt(count);
      writeIndex(idxOut, docCount, count, address, docToEntry);
    }
  }

  static final class Reader extends BytesReaderBase {
    private final int size;
    private final int valueCount;
    private final Comparator<BytesRef> comparator;

    public Reader(Directory dir, String id, int maxDoc, IOContext context,
        Type type, Comparator<BytesRef> comparator) throws IOException {
      super(dir, id, CODEC_NAME_IDX, CODEC_NAME_DAT, VERSION_START, true, context, type);
      size = datIn.readInt();
      valueCount = idxIn.readInt();
      this.comparator = comparator;
    }

    @Override
    public Source load() throws IOException {
      return new FixedSortedSource(cloneData(), cloneIndex(), valueCount,
          comparator);
    }

    @Override
    public Source getDirectSource() throws IOException {
      return this.getSource(); //nocommit doesn't support direct source for now
    }

    @Override
    public int getValueSize() {
      return size;
    }
  }

  static final class FixedSortedSource extends SortedSource {
    private final int valueCount;
    private final PackedInts.Reader docToOrdIndex;
    private final FST<Long> fst;

    FixedSortedSource(IndexInput datIn, IndexInput idxIn,
        int numValues, Comparator<BytesRef> comp) throws IOException {
      super(Type.BYTES_FIXED_SORTED, comp);
      docToOrdIndex = PackedInts.getReader(idxIn);
      fst = new FST<Long>(datIn, PositiveIntOutputs.getSingleton(true)); 
      this.valueCount = numValues;
      IOUtils.close(datIn, idxIn);
    }

    @Override
    public int getValueCount() {
      return valueCount;
    }
    
    @Override
    public boolean hasPackedDocToOrd() {
      return true;
    }

    @Override
    public PackedInts.Reader getDocToOrd() {
      return docToOrdIndex;
    }
    
    @Override
    public int ord(int docID) {
      assert docToOrdIndex.get(docID) < getValueCount();
      return (int) docToOrdIndex.get(docID);
    }

    @Override
    public BytesRef getByOrd(int ord, BytesRef bytesRef) {
      try {
        final IntsRef ic = Util.getByOutput(fst, ord);
        assert ic != null : "ord=" + ord;
        assert bytesRef != null;
        return Util.toBytesRef(ic, bytesRef);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

}
