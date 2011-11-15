package org.apache.lucene.index.log;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public abstract class WriteStrategy<Rec extends LogRecord>{
  
  protected final LogFile file;
  protected final Serializer<Rec> recordSerializer;
  protected final FileChannel channel;

  public WriteStrategy(LogFile file, Serializer<Rec> recordSerializer) {
    this.file = file;
    this.recordSerializer = recordSerializer;
    this.channel = file.channel;
  }
  
  public abstract LogLocation add(Rec record, boolean sync, LogLocation location) throws IOException;

  protected void flushBuffer(boolean syncAfter, ByteBuffer buffer) throws IOException {
    while (buffer.hasRemaining()) {
      channel.write(buffer);
    }
    buffer.clear();
    if (syncAfter) {
      channel.force(false);
    }
  }
  
  protected void flushBuffer(boolean syncAfter, ByteBuffer buffer, long pos) throws IOException {
    while (buffer.hasRemaining()) {
      pos += channel.write(buffer, pos);
    }
    buffer.clear();
    if (syncAfter) {
      channel.force(false);
    }
  }
  
}
