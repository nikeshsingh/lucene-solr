package org.apache.lucene.index.log;

import java.io.IOException;

public abstract class Logger <R extends LogRecord>{

  protected final Serializer<R> recordSerializer;

  public Logger(Serializer<R> recordSerializer) {
    this.recordSerializer = recordSerializer;
  }
  
  public abstract void add(R record, boolean sync) throws IOException;
  public abstract LogLocation add(R record, boolean sync, LogLocation loc) throws IOException;

  public abstract void sync() throws IOException;
  
  
  
}
