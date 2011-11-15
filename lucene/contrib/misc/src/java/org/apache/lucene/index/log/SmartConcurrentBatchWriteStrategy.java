package org.apache.lucene.index.log;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.OutputStreamDataOutput;

public class SmartConcurrentBatchWriteStrategy<Rec extends LogRecord> extends
    WriteStrategy<Rec> {
  private final ConcurrentLinkedQueue<LogItem> queue = new ConcurrentLinkedQueue<SmartConcurrentBatchWriteStrategy.LogItem>();
  private final ReentrantLock lock = new ReentrantLock();

  public SmartConcurrentBatchWriteStrategy(LogFile file,
      Serializer<Rec> recordSerializer) {
    super(file, recordSerializer);

  }

  @Override
  public LogLocation add(Rec record, boolean sync, LogLocation location)
      throws IOException {

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutput output = new OutputStreamDataOutput(out);
    recordSerializer.write(record, output);
    out.flush();
    final byte[] byteArray = out.toByteArray();
    LogItem logItem = new LogItem();
    logItem.buffer = byteArray;
    logItem.location = location;
    logItem.sync = sync;
    queue.add(logItem);
    if (lock.tryLock()) {
      try {
        while ((logItem = queue.poll()) != null) {
          flushBuffer(logItem.sync, ByteBuffer.wrap(logItem.buffer));
        }
      } finally {
        lock.unlock();
      }
    }

    return null;
  }

  private static class LogItem {
    byte[] buffer;
    LogLocation location;
    boolean sync;

  }

}
