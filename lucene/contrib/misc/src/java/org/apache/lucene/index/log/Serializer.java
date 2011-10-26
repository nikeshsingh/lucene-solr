package org.apache.lucene.index.log;


import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

public abstract class Serializer<T extends LogRecord> {
  public abstract void write(T record, DataOutput output);
  public abstract T read(DataInput input);
}
