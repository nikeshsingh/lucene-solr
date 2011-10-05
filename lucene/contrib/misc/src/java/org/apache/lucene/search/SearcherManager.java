package org.apache.lucene.search;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NRTManager; // javadocs
import org.apache.lucene.search.IndexSearcher; // javadocs
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;

/**
 * Utility class to safely share {@link IndexSearcher} instances across multiple
 * threads, while periodically reopening. This class ensures each IndexSearcher
 * instance is not closed until it is no longer needed.
 * 
 * <p>
 * Use {@link #acquire} to obtain the current searcher, and {@link #release} to
 * release it, like this:
 * 
 * <pre>
 * IndexSearcher s = manager.acquire();
 * try {
 *   // Do searching, doc retrieval, etc. with s
 * } finally {
 *   manager.release(s);
 * }
 * // Do not use s after this!
 * s = null;
 * </pre>
 * 
 * <p>
 * In addition you should periodically call {@link #maybeReopen}. While it's
 * possible to call this just before running each query, this is discouraged
 * since it penalizes the unlucky queries that do the reopen. It's better to use
 * a separate background thread, that periodically calls maybeReopen. Finally,
 * be sure to call {@link #close} once you are done.
 * 
 * <p>
 * <b>NOTE</b>: if you have an {@link IndexWriter}, it's better to use
 * {@link NRTManager} since that class pulls near-real-time readers from the
 * IndexWriter.
 * 
 * @lucene.experimental
 */

public abstract class SearcherManager {

  protected volatile IndexSearcher currentSearcher;
  protected final ExecutorService es;
  protected final SearcherWarmer warmer;
  protected final Semaphore reopenLock = new Semaphore(1);

  public SearcherManager(IndexReader openedReader, SearcherWarmer warmer,
      ExecutorService es) throws IOException {
    this.es = es;
    this.warmer = warmer;
    currentSearcher = new IndexSearcher(openedReader, es);
  }

  /**
   * You must call this, periodically, to perform a reopen. This calls
   * {@link IndexReader#reopen} on the underlying reader, and if that returns a
   * new reader, it's warmed (if you provided a {@link SearcherWarmer} and then
   * swapped into production.
   * 
   * <p>
   * <b>Threads</b>: it's fine for more than one thread to call this at once.
   * Only the first thread will attempt the reopen; subsequent threads will see
   * that another thread is already handling reopen and will return immediately.
   * Note that this means if another thread is already reopening then subsequent
   * threads will return right away without waiting for the reader reopen to
   * complete.
   * </p>
   * 
   * <p>
   * This method returns true if a new reader was in fact opened.
   * </p>
   */
  public boolean maybeReopen() throws IOException {
    ensureOpen();
    // Ensure only 1 thread does reopen at once; other
    // threads just return immediately:
    if (reopenLock.tryAcquire()) {
      try {
        IndexReader newReader = openIfChanged(currentSearcher.getIndexReader());
        if (newReader != null) {
          IndexSearcher newSearcher = new IndexSearcher(newReader, es);
          boolean success = false;
          try {
            if (warmer != null) {
              warmer.warm(newSearcher);
            }
            swapSearcher(newSearcher);
            success = true;
          } finally {
            if (!success) {
              release(newSearcher);
            }
          }
          return true;
        } else {
          return false;
        }
      } finally {
        reopenLock.release();
      }
    } else {
      return false;
    }
  }

  private void ensureOpen() {
    if (currentSearcher == null) {
      throw new AlreadyClosedException("this SearcherManager is closed");
    }
  }
  
  public boolean isSearcherCurrent() throws CorruptIndexException,
      IOException {
    final IndexSearcher searcher = acquire();
    try {
      return searcher.getIndexReader().isCurrent();
    } finally {
      release(searcher);
    }
  }

  protected void swapSearcher(IndexSearcher newSearcher) throws IOException {
    ensureOpen();
    final IndexSearcher oldSearcher = currentSearcher;
    currentSearcher = newSearcher;
    release(oldSearcher);
  }

  /**
   * Release the searcher previously obtained with {@link #acquire}.
   * 
   * <p>
   * <b>NOTE</b>: it's safe to call this after {@link #close}.
   */
  public void release(IndexSearcher searcher) throws IOException {
    assert searcher != null;
    searcher.getIndexReader().decRef();
  }

  /**
   * Close this SearcherManager to future searching. Any searches still in
   * process in other threads won't be affected, and they should still call
   * {@link #release} after they are done.
   */
  public synchronized void close() throws IOException {
    if (currentSearcher != null) {
      // make sure we can call this more than once
      // closeable javadoc says:
      // if this is already closed then invoking this method has no effect.
      swapSearcher(null);
    }
  }

  /**
   * Obtain the current IndexSearcher. You must match every call to acquire with
   * one call to {@link #release}; it's best to do so in a finally clause.
   */
  public IndexSearcher acquire() {
    IndexSearcher searcher;
    do {
      if ((searcher = currentSearcher) == null) {
        throw new AlreadyClosedException("this SearcherManager is closed");
      }
    } while (!searcher.getIndexReader().tryIncRef());
    return searcher;
  }

  protected abstract IndexReader openIfChanged(IndexReader oldReader)
      throws IOException;

  public static SearcherManager open(IndexWriter writer, boolean applyDeletes,
      SearcherWarmer warmer, ExecutorService es) throws CorruptIndexException,
      IOException {
    final IndexReader open = IndexReader.open(writer, true);
    boolean success = false;
    try {
      SearcherManager manager = new NRTSearchManager(writer, applyDeletes,
          open, warmer, es);
      success = true;
      return manager;
    } finally {
      if (!success) {
        open.close();
      }
    }
  }

  public static SearcherManager open(Directory dir, SearcherWarmer warmer)
      throws IOException {
    return open(dir, warmer, null);
  }

  public static SearcherManager open(Directory dir, SearcherWarmer warmer,
      ExecutorService es) throws IOException {
    final IndexReader open = IndexReader.open(dir, true);
    boolean success = false;
    try {
      SearcherManager manager = new DirectorySearchManager(open, warmer, es);
      success = true;
      return manager;
    } finally {
      if (!success) {
        open.close();
      }
    }
  }

  private static final class NRTSearchManager extends SearcherManager {
    private final IndexWriter writer;
    private final boolean applyDeletes;

    public NRTSearchManager(IndexWriter writer, boolean applyDeletes,
        IndexReader openedReader, SearcherWarmer warmer, ExecutorService es)
        throws IOException {
      super(openedReader, warmer, es);
      this.writer = writer;
      this.applyDeletes = applyDeletes;
      if (warmer != null) {
        writer.getConfig().setMergedSegmentWarmer(
            new IndexWriter.IndexReaderWarmer() {
              @Override
              public void warm(IndexReader reader) throws IOException {
                NRTSearchManager.this.warmer.warm(new IndexSearcher(reader,
                    NRTSearchManager.this.es));
              }
            });
      }
    }

    @Override
    protected IndexReader openIfChanged(IndexReader oldReader)
        throws IOException {
      // TODO Auto-generated method stub
      return IndexReader.openIfChanged(oldReader, writer, applyDeletes);
    }

  }

  private static final class DirectorySearchManager extends SearcherManager {
    public DirectorySearchManager(IndexReader openedReader,
        SearcherWarmer warmer, ExecutorService es) throws IOException {
      super(openedReader, warmer, es);
    }

    @Override
    protected IndexReader openIfChanged(IndexReader oldReader)
        throws IOException {
      // TODO Auto-generated method stub
      return IndexReader.openIfChanged(oldReader, true);
    }
  }

}
