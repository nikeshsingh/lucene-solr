package org.apache.lucene.index.log;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

final class LogFile {

  private final File file;

  private final int id;

  private final RandomAccessFile raf;

  final FileChannel channel;

  private final AtomicInteger refCount = new AtomicInteger();

  public LogFile(File file, int id) throws FileNotFoundException {
    this.file = file;
    this.raf = new RandomAccessFile(file, "rw");
    this.id = id;
    this.channel = raf.getChannel();
    this.refCount.incrementAndGet();
  }

  void preallocate(int mb) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocateDirect(4096);
    final int iters = (mb * 1024 * 1024) / 4096;
    long position = channel.position();
    for (int i = 0; i < iters; i++) {
      buffer.position(4096);
      buffer.flip();
      channel.write(buffer);  
    }
    channel.position(position);
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
        channel.force(true);
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