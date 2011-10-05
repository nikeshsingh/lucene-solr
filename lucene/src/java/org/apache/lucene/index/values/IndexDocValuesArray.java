package org.apache.lucene.index.values;

import java.io.IOException;

import org.apache.lucene.index.values.IndexDocValues.Source;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/**
 * @lucene.experimental
 */
abstract class IndexDocValuesArray extends Source {

  protected final int bytesPerValue;
  private final ValueType type;
  private final boolean isFloat;
  protected int maxDocID = -1;

  IndexDocValuesArray(int bytesPerValue, ValueType type) {
    this.bytesPerValue = bytesPerValue;
    this.type = type;
    switch (type) {
    case FIXED_INTS_16:
    case FIXED_INTS_32:
    case FIXED_INTS_64:
    case FIXED_INTS_8:
      isFloat = false;
      break;
    case FLOAT_32:
    case FLOAT_64:
      isFloat = true;
      break;
    default:
      throw new IllegalStateException("illegal type: " + type);

    }
  }

  public abstract IndexDocValuesArray newFromInput(IndexInput input, int numDocs)
      throws IOException;

  @Override
  public final int getValueCount() {
    return maxDocID + 1;
  }

  @Override
  public final ValueType type() {
    return type;
  }

  @Override
  public final boolean hasArray() {
    return true;
  }

  final static class ByteValues extends IndexDocValuesArray {
    private final byte[] values;

    ByteValues() {
      super(1, ValueType.FIXED_INTS_8);
      values = new byte[0];
    }

    private ByteValues(IndexInput input, int numDocs) throws IOException {
      super(1, ValueType.FIXED_INTS_8);
      values = new byte[numDocs];
      input.readBytes(values, 0, values.length, false);
      maxDocID = numDocs - 1;
    }

    @Override
    public byte[] getArray() {
      return values;
    }

    @Override
    public long getInt(int docID) {
      assert docID >= 0 && docID < values.length;
      return values[docID];
    }

    @Override
    public IndexDocValuesArray newFromInput(IndexInput input, int numDocs)
        throws IOException {
      return new ByteValues(input, numDocs);
    }

  };

  final static class ShortValues extends IndexDocValuesArray {
    private final short[] values;

    ShortValues() {
      super(RamUsageEstimator.NUM_BYTES_SHORT, ValueType.FIXED_INTS_16);
      values = new short[0];
    }

    private ShortValues(IndexInput input, int numDocs) throws IOException {
      super(RamUsageEstimator.NUM_BYTES_SHORT, ValueType.FIXED_INTS_16);
      values = new short[numDocs];
      for (int i = 0; i < values.length; i++) {
        values[i] = input.readShort();
      }
      maxDocID = numDocs - 1;
    }

    @Override
    public short[] getArray() {
      return values;
    }

    @Override
    public long getInt(int docID) {
      assert docID >= 0 && docID < values.length;
      return values[docID];
    }

    @Override
    public IndexDocValuesArray newFromInput(IndexInput input, int numDocs)
        throws IOException {
      return new ShortValues(input, numDocs);
    }

  };

  final static class IntValues extends IndexDocValuesArray {
    private final int[] values;

    IntValues() {
      super(RamUsageEstimator.NUM_BYTES_INT, ValueType.FIXED_INTS_32);
      values = new int[0];
    }

    private IntValues(IndexInput input, int numDocs) throws IOException {
      super(RamUsageEstimator.NUM_BYTES_INT, ValueType.FIXED_INTS_32);
      values = new int[numDocs];
      for (int i = 0; i < values.length; i++) {
        values[i] = input.readInt();
      }
      maxDocID = numDocs - 1;
    }

    @Override
    public int[] getArray() {
      return values;
    }

    @Override
    public long getInt(int docID) {
      assert docID >= 0 && docID < values.length;
      return 0xFFFFFFFF & values[docID];
    }

    @Override
    public IndexDocValuesArray newFromInput(IndexInput input, int numDocs)
        throws IOException {
      return new IntValues(input, numDocs);
    }

  };

  final static class LongValues extends IndexDocValuesArray {
    private final long[] values;

    LongValues() {
      super(RamUsageEstimator.NUM_BYTES_LONG, ValueType.FIXED_INTS_64);
      values = new long[0];
    }

    private LongValues(IndexInput input, int numDocs) throws IOException {
      super(RamUsageEstimator.NUM_BYTES_LONG, ValueType.FIXED_INTS_64);
      values = new long[numDocs];
      for (int i = 0; i < values.length; i++) {
        values[i] = input.readLong();
      }
      maxDocID = numDocs - 1;
    }

    @Override
    public long[] getArray() {
      return values;
    }

    @Override
    public long getInt(int docID) {
      assert docID >= 0 && docID < values.length;
      return values[docID];
    }

    @Override
    public IndexDocValuesArray newFromInput(IndexInput input, int numDocs)
        throws IOException {
      return new LongValues(input, numDocs);
    }

  };

  final static class FloatValues extends IndexDocValuesArray {
    private final float[] values;

    FloatValues() {
      super(RamUsageEstimator.NUM_BYTES_FLOAT, ValueType.FLOAT_32);
      values = new float[0];
    }

    private FloatValues(IndexInput input, int numDocs) throws IOException {
      super(RamUsageEstimator.NUM_BYTES_FLOAT, ValueType.FLOAT_32);
      values = new float[numDocs];
      /* we always read BIG_ENDIAN here since the writer serialized plain bytes
       * we can simply read the ints / longs
       * back in using readInt / readLong */
      for (int i = 0; i < values.length; i++) {
        values[i] = Float.intBitsToFloat(input.readInt());
      }
      maxDocID = numDocs - 1;
    }

    @Override
    public float[] getArray() {
      return values;
    }

    @Override
    public double getFloat(int docID) {
      assert docID >= 0 && docID < values.length;
      return values[docID];
    }

    @Override
    public IndexDocValuesArray newFromInput(IndexInput input, int numDocs)
        throws IOException {
      return new FloatValues(input, numDocs);
    }
  };
  
  final static class DoubleValues extends IndexDocValuesArray {
    private final double[] values;

    DoubleValues() {
      super(RamUsageEstimator.NUM_BYTES_DOUBLE, ValueType.FLOAT_64);
      values = new double[0];
    }

    private DoubleValues(IndexInput input, int numDocs) throws IOException {
      super(RamUsageEstimator.NUM_BYTES_DOUBLE, ValueType.FLOAT_64);
      values = new double[numDocs];
      /* we always read BIG_ENDIAN here since the writer serialized plain bytes
       * we can simply read the ints / longs
       * back in using readInt / readLong */
      for (int i = 0; i < values.length; i++) {
        values[i] = Double.longBitsToDouble(input.readLong());
      }
      maxDocID = numDocs - 1;
    }

    @Override
    public double[] getArray() {
      return values;
    }

    @Override
    public double getFloat(int docID) {
      assert docID >= 0 && docID < values.length;
      return values[docID];
    }

    @Override
    public IndexDocValuesArray newFromInput(IndexInput input, int numDocs)
        throws IOException {
      return new DoubleValues(input, numDocs);
    }

  };

}
