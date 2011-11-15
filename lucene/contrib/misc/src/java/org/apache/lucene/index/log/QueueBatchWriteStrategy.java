package org.apache.lucene.index.log;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.index.log.LogBatch.Node;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.ThreadInterruptedException;

public class QueueBatchWriteStrategy<Rec extends LogRecord> extends
    WriteStrategy<Rec> {

  private ReentrantLock batchLock = new ReentrantLock();
  private final LogBatch batch = new LogBatch(new BatchNode(null, null, false));
  private final LogBatch.BatchSlice slice = batch.newSlice();
  private final ByteBuffer buffer;
  private volatile long lastflushedSeqID = -1;

  public QueueBatchWriteStrategy(LogFile file, Serializer<Rec> recordSerializer) {
    super(file, recordSerializer);
    buffer = ByteBuffer.allocateDirect(4096);
  }

  @Override
  public LogLocation add(Rec record, boolean sync, LogLocation location)
      throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutput output = new OutputStreamDataOutput(out);
    // got a slot
    recordSerializer.write(record, output);
    out.flush();
    BatchNode batchNode = new BatchNode(out.toByteArray(), location, sync);
    batch.add(batchNode);

   if (batchLock.tryLock()) {
        try {
          while(true) {
          batch.updateSlice(slice);
          if (slice.isEmpty()) {
            break;
          }
          processBatch();
          }
        } finally {
          batchLock.unlock();
        }
   }

    return location;
  }

  private void processBatch() throws IOException {
    BatchNode node = (BatchNode) slice.sliceHead;
    long offset = channel.position();
    boolean syncAfterBatch = false;
    BytesRef spare = new BytesRef();
    while (node.next != null) {
      node = (BatchNode) node.next;
      spare.bytes = node.item;
      spare.offset = 0;
      spare.length = spare.bytes.length;
      while (spare.length > 0) {
        final int remaining = buffer.remaining();
        if (spare.length > remaining) {
          buffer.put(spare.bytes, spare.offset, remaining);
          spare.offset += remaining;
          spare.length -= remaining;
          buffer.flip();
          syncAfterBatch |= node.sync;
          flushBuffer(syncAfterBatch, buffer);
        } else {
          buffer.put(spare.bytes, spare.offset, spare.length);
          spare.length = 0;
          syncAfterBatch |= node.sync;
          if (node.location != null) {
            node.location.offset = offset;
            node.location.size = spare.length;
          }
          offset += spare.length;
        }
      }
    }
    if (buffer.position() != 0) {
      buffer.flip();
      flushBuffer(syncAfterBatch, buffer);
    } else if (syncAfterBatch){
//      channel.force(false);
    }
    slice.sliceHead = node;
    lastflushedSeqID = node.seqId;
  }
  
//  private void processBatch() throws IOException {
//  BatchNode node = (BatchNode) slice.sliceHead;
//  boolean syncAfterBatch = false;
//  while (node.next != null) {
//    node = (BatchNode) node.next;
//    ByteBuffer wrap = ByteBuffer.wrap(node.item);
//    flushBuffer(false, wrap);
//    syncAfterBatch |= node.sync;
//  }
//  if (syncAfterBatch) {
//    channel.force(false);
//  }
//  slice.sliceHead = node;
//  lastflushedSeqID = node.seqId;
//}


  private static final class BatchNode extends Node<byte[]> {
    private final LogLocation location;
    private boolean sync;

    BatchNode(byte[] bytes, LogLocation location, boolean sync) {
      super(bytes);
      this.location = location;
      this.sync = sync;
    }

  }
}
