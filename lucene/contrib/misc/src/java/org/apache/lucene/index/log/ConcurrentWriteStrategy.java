package org.apache.lucene.index.log;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.OutputStreamDataOutput;

public class ConcurrentWriteStrategy<Rec extends LogRecord> extends WriteStrategy<Rec> {
  private final AtomicLong position = new AtomicLong();
  public ConcurrentWriteStrategy(LogFile file, Serializer<Rec> recordSerializer) throws IOException {
    super(file, recordSerializer);
    position.set(file.channel.position());
  }

  @Override
  public LogLocation add(Rec record, boolean sync, LogLocation location)
      throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutput output = new OutputStreamDataOutput(out);
    recordSerializer.write(record, output);
    out.flush();
    final byte[] byteArray = out.toByteArray();
    final long pos = position.getAndAdd(byteArray.length);
    flushBuffer(sync, ByteBuffer.wrap(byteArray), pos);
    
    if (location != null) {
      location.offset = pos;
      location.size = byteArray.length;
    }
    return location;
  }

}
