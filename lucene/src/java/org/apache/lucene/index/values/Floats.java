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

import org.apache.lucene.index.values.IndexDocValues.Source;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.FloatsRef;

/**
 * Exposes {@link Writer} and reader ({@link Source}) for 32 bit and 64 bit
 * floating point values.
 * <p>
 * Current implementations store either 4 byte or 8 byte floating points with
 * full precision without any compression.
 * 
 * @lucene.experimental
 */
public class Floats {
  
  public static Writer getWriter(Directory dir, String id, int precisionBytes,
      Counter bytesUsed, IOContext context) throws IOException {
    if (precisionBytes != 4 && precisionBytes != 8) {
      throw new IllegalArgumentException("precisionBytes must be 4 or 8; got "
          + precisionBytes);
    }
    return new FloatsWriter(dir, id, bytesUsed, context, precisionBytes);

  }

  public static IndexDocValues getValues(Directory dir, String id, int maxDoc, IOContext context)
      throws IOException {
    return new FloatsReader(dir, id, maxDoc, context);
  }
  
  final static class FloatsWriter extends FixedStraightBytesImpl.Writer {
    private final int size;
    private final ValueType valueType;
    private FloatsRef floatsRef;
    public FloatsWriter(Directory dir, String id, Counter bytesUsed,
        IOContext context, int size) throws IOException {
      super(dir, id, bytesUsed, context);
      this.bytesRef = new BytesRef(size);
      assert size == 4 || size == 8;
      this.size = size;
      valueType = size == 4 ? ValueType.FLOAT_32 : ValueType.FLOAT_64;
      bytesRef.length = size;
    }
    
    public void add(int docID, double v) throws IOException {
      toBytesRef(v);
      add(docID, bytesRef);
    }

    private void toBytesRef(double v) {
      switch(valueType) {
      case FLOAT_32:
        bytesRef.copy(Float.floatToRawIntBits((float)v));
        break;
      case FLOAT_64:
        bytesRef.copy(Double.doubleToRawLongBits(v));        
        break;
      }
    }
    
    @Override
    protected boolean tryBulkMerge(IndexDocValues docValues) {
      // only bulk merge is value type is the same otherwise size differs
      return super.tryBulkMerge(docValues) && docValues.type() == valueType;
    }
    
    @Override
    public void add(int docID, PerDocFieldValues docValues) throws IOException {
      add(docID, docValues.getFloat());
    }
    
    @Override
    protected void setNextEnum(ValuesEnum valuesEnum) {
      if (valuesEnum.type() == valueType) {
        super.setNextEnum(valuesEnum);
        floatsRef = null;
      } else {
        floatsRef = valuesEnum.getFloat();
        bytesRef = new BytesRef(size);
      }
    }

    @Override
    protected void mergeDoc(int docID) throws IOException {
      if (floatsRef != null) {
        toBytesRef(floatsRef.get());
      }
      super.mergeDoc(docID);
    }
  }

  final static class FloatsReader extends FixedStraightBytesImpl.Reader {
    final IndexDocValuesArray arrayTemplate;
    FloatsReader(Directory dir, String id, int maxDoc, IOContext context)
        throws IOException {
      super(dir, id, maxDoc, context);
      assert size == 4 || size == 8;
      if (size == 4) {
        arrayTemplate = new IndexDocValuesArray.FloatValues();
      } else {
        arrayTemplate = new IndexDocValuesArray.DoubleValues();
      }
    }
    
    @Override
    public Source load() throws IOException {
      final IndexInput indexInput = cloneData();
      try {
        return arrayTemplate.newFromInput(indexInput, maxDoc);
      } finally {
        indexInput.close();
      }
    }
    
    public ValuesEnum getEnum(AttributeSource source) throws IOException {
      IndexInput indexInput = (IndexInput) datIn.clone();
      return arrayTemplate.getDirectEnum(source, indexInput, maxDoc);
    }

    @Override
    public ValueType type() {
      return arrayTemplate.type();
    }
  }

}