package org.apache.lucene.index.log;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.index.log.LogBatch.Node;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.ArrayUtil;

public class FileSystemLogger<Rec extends LogRecord> extends Logger<Rec> {
  private volatile LogFile currentLogFile;
  private File logDirectory;
  private int logFileId;
  private ReentrantLock batchLock = new ReentrantLock();
  private LogBatch batch = new LogBatch();
  private LogBatch.BatchSlice slice = batch.newSlice();
  private ByteBuffer buffer;

  public FileSystemLogger(Serializer<Rec> recordSerializer, File logDirectory) {
    super(recordSerializer);
    if (!logDirectory.isDirectory()) {
      throw new IllegalArgumentException("logDirectory must be a directory");
    }
    this.logDirectory = logDirectory;
    buffer = ByteBuffer.allocateDirect(8192);

  }

  public synchronized boolean createNewLogFile(long sizeInMB) throws IOException {
    if (currentLogFile == null) {
      currentLogFile = new LogFile(
          new File(logDirectory, "translog_" + ".log"), logFileId++);
      currentLogFile.preallocate(sizeInMB);
      return true;
    }
    return false;
  }

  @Override
  public void add(Rec record, boolean sync) throws IOException {
    add(record, sync, null);

  }

  @Override
  public LogLocation add(Rec record, boolean sync, LogLocation location)
      throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutput output = new OutputStreamDataOutput(out);
    // got a slot
    recordSerializer.write(record, output);
    out.flush();
    BatchNode batchNode = new BatchNode(out.toByteArray(),location, sync);
    batch.add(batchNode);

    batchLock.lock();
    try {
      if (batchNode.seqId > slice.sliceHead.seqId) {
        processBatch();
      }
    } finally {
      batchLock.unlock();
    }
    return null;
  }

  private void processBatch() throws IOException {
    BatchNode node = (BatchNode) slice.sliceHead;
    long offset = currentLogFile.channel.position();
    boolean syncAfterBatch = false;
    while (node.next != null) {
      node = (BatchNode) node.next;
      final byte[] recordBytes = node.item;
      if (recordBytes.length > buffer.remaining()) {
        if (buffer.position() == 0) {
          buffer = ByteBuffer.allocateDirect(ArrayUtil.oversize(recordBytes.length, 1));
          buffer.put(node.item);
        }
        flushBuffer(syncAfterBatch);
        syncAfterBatch = false;
        slice.sliceHead = node;
        break;
      } else {
        buffer.put(node.item);
        syncAfterBatch |= node.sync;
        if (node.location != null) {
          node.location.offset = offset;
          node.location.size = recordBytes.length;
        }
        offset += recordBytes.length;
      }
    }
    if (buffer.position() != 0) {
      flushBuffer(syncAfterBatch);
      slice.sliceHead = node;
    }
  }

  private void flushBuffer(boolean syncAfterBatch) throws IOException {
    buffer.flip();
    final FileChannel channel = currentLogFile.channel;
    while (buffer.hasRemaining()) {
      channel.write(buffer);
    }
    buffer.clear();
    if (syncAfterBatch) {
      channel.force(false);
    }
  }

  @Override
  public void sync() throws IOException {
    // TODO Auto-generated method stub

  }

  private final static class LogFile {

    private final File file;

    private final int id;

    private final RandomAccessFile raf;

    private final FileChannel channel;

    private final AtomicInteger refCount = new AtomicInteger();

    public LogFile(File file, int id) throws FileNotFoundException {
      this.file = file;
      this.raf = new RandomAccessFile(file, "rw");
      this.id = id;
      this.channel = raf.getChannel();
      this.refCount.incrementAndGet();
    }

    void preallocate(long mb) throws IOException {
      ByteBuffer buffer = ByteBuffer.allocate(1);
      buffer.limit(1).flip();
      channel.write(buffer, raf.length() + mb * 1024 * 1024);
      channel.force(true);
    }

    public File file() {
      return this.file;
    }

    public FileChannel channel() {
      return this.channel;
    }

    public RandomAccessFile raf() {
      return this.raf;
    }

    /**
     * Increases the ref count, and returns <tt>true</tt> if it managed to
     * actually increment it.
     */
    public boolean increaseRefCount() {
      return refCount.incrementAndGet() > 1;
    }

    public void decreaseRefCount(boolean delete) {
      if (refCount.decrementAndGet() <= 0) {
        try {
          raf.close();
          if (delete) {
            file.delete();
          }
        } catch (IOException e) {
          // ignore
        }
      }
    }
  }

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
