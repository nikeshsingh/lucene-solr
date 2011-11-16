package org.apache.solr.update;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.util.IOUtils;

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
public class TransactionLogFileHandle implements Closeable {
  private final File tlogFile;
  private final RandomAccessFile raf;
  private final FileChannel channel;
  private final AtomicInteger refCount = new AtomicInteger();
  private final boolean deleteOnClose;

  public TransactionLogFileHandle(File tlogFile, boolean deleteOnClose) throws IOException {
    this.tlogFile = tlogFile;
    raf = new RandomAccessFile(this.tlogFile, "rw");
    boolean success = false;
    try {
      long start = raf.length();
      assert start == 0;
      if (start > 0) {
        raf.setLength(0);
      }
      // System.out.println("###start= "+start);
      channel = raf.getChannel();
      this.deleteOnClose = deleteOnClose;
      success = true;
    } finally {
      if (!success) {
        IOUtils.close(raf);
      }
    }
  }

  public void preallocate(long mb) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocateDirect(4096);
    final long iters = (mb * 1024 * 1024) / 4096;
    long position = channel.position();
    for (int i = 0; i < iters; i++) {
      buffer.position(4096);
      buffer.flip();
      channel.write(buffer);
    }
    channel.position(position);
    sync(true);
  }

  public FileChannel getChannel() {
    return channel;
  }

  public long getLength() throws IOException {
    return raf.length();
  }

  public void reset() throws IOException {
    raf.setLength(0);
  }

  public void readAt(long pos, byte[] buffer) throws IOException {
    raf.seek(pos);
    raf.readFully(buffer);
  }

  @Override
  public void close() throws IOException {
    decRef();
  }

  public void deleteFile() {
    tlogFile.delete();
  }

  public File getFile() {
    return tlogFile;
  }

  public void sync(boolean metaData) throws IOException {
    channel.force(metaData);
  }

  /**
   * Increases the ref count, and returns <tt>true</tt> if it managed to
   * actually increment it.
   */
  public boolean incRef() {
    int count;
    while((count = refCount.get()) > 0) {
      if (refCount.compareAndSet(count, count+1))
        return true;
    }
    return false;
  }

  public boolean decRef() throws IOException {
    if (refCount.decrementAndGet() <= 0) {
      sync(true);
      IOUtils.close(channel, raf);
      if (deleteOnClose) {
        tlogFile.delete();
      }
      return true;
    }
    return false;
  }
}
