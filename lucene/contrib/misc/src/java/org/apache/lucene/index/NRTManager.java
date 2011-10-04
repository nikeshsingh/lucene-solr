package org.apache.lucene.index;

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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader; // javadocs
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearchManager;
import org.apache.lucene.search.SearcherWarmer;
import org.apache.lucene.util.ThreadInterruptedException;

// TODO
//   - we could make this work also w/ "normal" reopen/commit?

/**
 * Utility class to manage sharing near-real-time searchers
 * across multiple searching threads.
 *
 * <p>NOTE: to use this class, you must call reopen
 * periodically.  The {@link NRTManagerReopenThread} is a
 * simple class to do this on a periodic basis.  If you
 * implement your own reopener, be sure to call {@link
 * #addWaitingListener} so your reopener is notified when a
 * caller is waiting for a specific generation searcher. </p>
 *
 * @lucene.experimental
 */

public class NRTManager extends SearchManager {
  private final IndexWriter writer;
  private final AtomicLong indexingGen;
  private final AtomicLong searchingGen;
  private final Semaphore reopening = new Semaphore(1);
  private final List<WaitingListener> waitingListeners = new CopyOnWriteArrayList<WaitingListener>();
  private final boolean applyAllDeletes;

  /**
   * Create new NRTManager.
   * 
   *  @param writer IndexWriter to open near-real-time
   *         readers
   *  @param es optional ExecutorService so different segments can
   *         be searched concurrently (see {@link
   *         IndexSearcher#IndexSearcher(IndexReader,ExecutorService)}.  Pass null
   *         to search segments sequentially.
   *  @param warmer optional {@link SearcherWarmer}.  Pass
   *         null if you don't require the searcher to warmed
   *         before going live.  If this is non-null then a
   *         merged segment warmer is installed on the
   *         provided IndexWriter's config.
   *
   *  <p><b>NOTE</b>: the provided {@link SearcherWarmer} is
   *  not invoked for the initial searcher; you shouldDeletes
   *  warm it yourself if necessary.
   */
  public NRTManager(IndexWriter writer, ExecutorService es,
      SearcherWarmer warmer, boolean applyDeletes) throws IOException {
    super(warmer, es);
    this.writer = writer;
    this.applyAllDeletes = applyDeletes;
    indexingGen = new AtomicLong(1);
    searchingGen = new AtomicLong(-1);
    // Create initial reader:
    currentSearcher = new IndexSearcher(IndexReader.open(writer, true), es);
    searchingGen.set(0);

    if (this.warmer != null) {
      writer.getConfig().setMergedSegmentWarmer(
         new IndexWriter.IndexReaderWarmer() {
           @Override
           public void warm(IndexReader reader) throws IOException {
             NRTManager.this.warmer.warm(new IndexSearcher(reader, NRTManager.this.es));
           }
         });
    }
  }

  /** NRTManager invokes this interface to notify it when a
   *  caller is waiting for a specific generation searcher
   *  to be visible. */
  public static interface WaitingListener {
    public void waiting(boolean requiresDeletes, long targetGen);
  }

  /** Adds a listener, to be notified when a caller is
   *  waiting for a specific generation searcher to be
   *  visible. */
  public void addWaitingListener(WaitingListener l) {
    waitingListeners.add(l);
  }

  /** Remove a listener added with {@link
   *  #addWaitingListener}. */
  public void removeWaitingListener(WaitingListener l) {
    waitingListeners.remove(l);
  }

  public long updateDocument(Term t, Iterable<? extends IndexableField> d, Analyzer a) throws IOException {
    writer.updateDocument(t, d, a);
    // Return gen as of when indexing finished:
    return indexingGen.get();
  }

  public long updateDocument(Term t, Iterable<? extends IndexableField> d) throws IOException {
    writer.updateDocument(t, d);
    // Return gen as of when indexing finished:
    return indexingGen.get();
  }

  public long updateDocuments(Term t, Iterable<? extends Iterable<? extends IndexableField>> docs, Analyzer a) throws IOException {
    writer.updateDocuments(t, docs, a);
    // Return gen as of when indexing finished:
    return indexingGen.get();
  }

  public long updateDocuments(Term t, Iterable<? extends Iterable<? extends IndexableField>> docs) throws IOException {
    writer.updateDocuments(t, docs);
    // Return gen as of when indexing finished:
    return indexingGen.get();
  }

  public long deleteDocuments(Term t) throws IOException {
    writer.deleteDocuments(t);
    // Return gen as of when indexing finished:
    return indexingGen.get();
  }

  public long deleteDocuments(Query q) throws IOException {
    writer.deleteDocuments(q);
    // Return gen as of when indexing finished:
    return indexingGen.get();
  }

  public long addDocument(Iterable<? extends IndexableField> d, Analyzer a) throws IOException {
    writer.addDocument(d, a);
    // Return gen as of when indexing finished:
    return indexingGen.get();
  }

  public long addDocuments(Iterable<? extends Iterable<? extends IndexableField>> docs, Analyzer a) throws IOException {
    writer.addDocuments(docs, a);
    // Return gen as of when indexing finished:
    return indexingGen.get();
  }

  public long addDocument(Iterable<? extends IndexableField> d) throws IOException {
    writer.addDocument(d);
    // Return gen as of when indexing finished:
    return indexingGen.get();
  }

  public long addDocuments(Iterable<? extends Iterable<? extends IndexableField>> docs) throws IOException {
    writer.addDocuments(docs);
    // Return gen as of when indexing finished:
    return indexingGen.get();
  }

  public void waitForGeneration(long targetGen) {
    if (targetGen > getCurrentSearchingGen()) {
      // Must wait
      // final long t0 = System.nanoTime();
      for (WaitingListener listener : waitingListeners) {
        listener.waiting(applyAllDeletes, targetGen);
      }
      while (targetGen > getCurrentSearchingGen()) {
        // System.out.println(Thread.currentThread().getName() +
        // ": wait fresh searcher targetGen=" + targetGen + " vs searchingGen="
        // + getCurrentSearchingGen(requireDeletes) + " requireDeletes=" +
        // requireDeletes);
        synchronized (this) {
          try {
            wait();
          } catch (InterruptedException ie) {
            throw new ThreadInterruptedException(ie);
          }
        }
      }
    }
  }

  /** Returns generation of current searcher. */
  public long getCurrentSearchingGen() {
    return searchingGen.get();
  }

  /**
   * Call this when you need the NRT reader to reopen.
   * 
   * @param applyAllDeletes
   *          If true, the newly opened reader will reflect all deletes
   */
  @Override
  public boolean maybeReopen() throws IOException {
    if (reopening.tryAcquire()) {
     try {
       // Mark gen as of when reopen started:
      final long newSearcherGen = indexingGen.getAndIncrement();
      if (currentSearcher.getIndexReader().isCurrent()) {
        searchingGen.set(newSearcherGen);
        synchronized (this) {
          notifyAll();
        }
      }
      return super.maybeReopen();
      
    } finally {
        reopening.release();
      }
    } else {
      return false;
    }
  }
  
  

  // Steals a reference from newSearcher:
  private synchronized void swapSearcher(IndexSearcher newSearcher,
      long newSearchingGen) throws IOException {
    try {
      super.swapSearcher(newSearcher);
      assert newSearchingGen > searchingGen.get() : "newSearchingGen="
          + newSearchingGen + " searchingGen=" + searchingGen;
      searchingGen.set(newSearchingGen);
    } finally {
      notifyAll();
    }
  }

  /**
   * Close this NRTManager to future searching. Any searches still in process in
   * other threads won't be affected, and they should still call
   * {@link #release} after they are done.
   * 
   * <p>
   * <b>NOTE</b>: caller must separately close the writer.
   */
  @Override
  public void close() throws IOException {
    swapSearcher(null, indexingGen.getAndIncrement());
  }

  @Override
  protected IndexReader openIfChanged(IndexReader oldReader) throws IOException {
    return IndexReader.openIfChanged(oldReader, writer, applyAllDeletes);
  }
}
