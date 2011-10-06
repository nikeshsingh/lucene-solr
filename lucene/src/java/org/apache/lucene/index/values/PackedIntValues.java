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

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.values.FixedStraightBytesImpl.FixedBytesWriterBase;
import org.apache.lucene.index.values.IndexDocValues.Source;
import org.apache.lucene.index.values.IndexDocValuesArray.LongValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CodecUtil;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Stores integers using {@link PackedInts}
 * 
 * @lucene.experimental
 * */
class PackedIntValues {

  private static final String CODEC_NAME = "PackedInts";
  private static final byte PACKED = 0x00;
  private static final byte FIXED_64 = 0x01;

  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = VERSION_START;

  static class PackedIntsWriter extends FixedBytesWriterBase {

    private long minValue;
    private long maxValue;
    private boolean started;
    private int lastDocId = -1;

    protected PackedIntsWriter(Directory dir, String id, Counter bytesUsed,
        IOContext context) throws IOException {
      super(dir, id, CODEC_NAME, VERSION_CURRENT, bytesUsed, context);
      bytesRef = new BytesRef(8);
    }

    @Override
    public void add(int docID, long v) throws IOException {
      assert lastDocId < docID;
      if (!started) {
        started = true;
        minValue = maxValue = v;
      } else {
        if (v < minValue) {
          minValue = v;
        } else if (v > maxValue) {
          maxValue = v;
        }
      }
      lastDocId = docID;
      bytesRef.copy(v);
      add(docID, bytesRef);
    }

    @Override
    public void finish(int docCount) throws IOException {
      boolean success = false;
      final IndexOutput dataOut = getOrCreateDataOut();
      try {
        if (!started) {
          minValue = maxValue = 0;
        }
        final long delta = maxValue - minValue;
        // if we exceed the range of positive longs we must switch to fixed
        // ints
        if (delta <= (maxValue >= 0 && minValue <= 0 ? Long.MAX_VALUE
            : Long.MAX_VALUE - 1) && delta >= 0) {
          dataOut.writeByte(PACKED);
          writePackedInts(dataOut, docCount);
          return; // done
        } else {
          dataOut.writeByte(FIXED_64);
        }
        writeData(dataOut);
        writeZeros(docCount - (lastDocID + 1), dataOut);
        success = true;
      } finally {
        resetPool();
        if (success) {
          IOUtils.close(dataOut);
        } else {
          IOUtils.closeWhileHandlingException(dataOut);
        }
      }
    }

    @Override
    protected void mergeDoc(int docID, int sourceDoc) throws IOException {
      assert docID > lastDocId : "docID: " + docID
          + " must be greater than the last added doc id: " + lastDocId;
        add(docID, currentMergeSource.getInt(sourceDoc));
    }

    private void writePackedInts(IndexOutput datOut, int docCount) throws IOException {
      datOut.writeLong(minValue);
      
      // write a default value to recognize docs without a value for that
      // field
      final long defaultValue = maxValue >= 0 && minValue <= 0 ? 0 - minValue
          : ++maxValue - minValue;
      datOut.writeLong(defaultValue);
      PackedInts.Writer w = PackedInts.getWriter(datOut, docCount,
          PackedInts.bitsRequired(maxValue - minValue));
      for (int i = 0; i < lastDocID + 1; i++) {
        set(bytesRef, i);
        long asLong = bytesRef.asLong();
        w.add(asLong == 0 ? defaultValue : asLong - minValue);
      }
      for (int i = lastDocID + 1; i < docCount; i++) {
        w.add(defaultValue);
      }
      w.finish();
    }

    @Override
    public void add(int docID, PerDocFieldValues docValues) throws IOException {
      add(docID, docValues.getInt());
    }
  }

  /**
   * Opens all necessary files, but does not read any data in until you call
   * {@link #load}.
   */
  static class PackedIntsReader extends IndexDocValues {
    private final IndexInput datIn;
    private final byte type;
    private final int numDocs;
    private final LongValues values;

    protected PackedIntsReader(Directory dir, String id, int numDocs,
        IOContext context) throws IOException {
      datIn = dir.openInput(
          IndexFileNames.segmentFileName(id, "", Writer.DATA_EXTENSION),
          context);
      this.numDocs = numDocs;
      boolean success = false;
      try {
        CodecUtil.checkHeader(datIn, CODEC_NAME, VERSION_START, VERSION_START);
        type = datIn.readByte();
        values = type == FIXED_64 ? new LongValues() : null;
        success = true;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(datIn);
        }
      }
    }


    /**
     * Loads the actual values. You may call this more than once, eg if you
     * already previously loaded but then discarded the Source.
     */
    @Override
    public Source load() throws IOException {
      boolean success = false;
      final Source source;
      IndexInput input = null;
      try {
        input = (IndexInput) datIn.clone();
        
        if (values == null) {
          source = new PackedIntsSource(input);
        } else {
          source = values.newFromInput(input, numDocs);
        }
        success = true;
        return source;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(input, datIn);
        }
      }
    }

    @Override
    public void close() throws IOException {
      super.close();
      datIn.close();
    }


    @Override
    public ValueType type() {
      return ValueType.VAR_INTS;
    }


    @Override
    public Source getDirectSource() throws IOException {
      return values != null ? new FixedStraightBytesImpl.DirectFixedStraightSource((IndexInput) datIn.clone(), 8, ValueType.FIXED_INTS_64) : new DirectPackedIntsSource((IndexInput) datIn.clone());
    }
  }

  
  static class PackedIntsSource extends Source {
    private final long minValue;
    private final long defaultValue;
    private final PackedInts.Reader values;

    public PackedIntsSource(IndexInput dataIn) throws IOException {
      super(ValueType.VAR_INTS);
      minValue = dataIn.readLong();
      defaultValue = dataIn.readLong();
      values = PackedInts.getReader(dataIn);
    }

    @Override
    public long getInt(int docID) {
      // TODO -- can we somehow avoid 2X method calls
      // on each get? must push minValue down, and make
      // PackedInts implement Ints.Source
      assert docID >= 0;
      final long value = values.get(docID);
      return value == defaultValue ? 0 : minValue + value;
    }
  }

  private static final class DirectPackedIntsSource extends Source {
    private final PackedInts.RandomAccessReaderIterator ints;
    private long minValue;
    private final long defaultValue;

    private DirectPackedIntsSource(IndexInput dataIn)
        throws IOException {
      super(ValueType.VAR_INTS);
      minValue = dataIn.readLong();
      defaultValue = dataIn.readLong();
      this.ints = PackedInts.getRandomAccessReaderIterator(dataIn);
    }

    @Override
    public double getFloat(int docID) {
      return getInt(docID);
    }

    @Override
    public BytesRef getBytes(int docID, BytesRef ref) {
      ref.grow(8);
      ref.copy(getInt(docID));
      return ref;
    }

    @Override
    public long getInt(int docID) {
      try {
      final long val = ints.get(docID);
      return val == defaultValue ? 0 : minValue + val;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

}