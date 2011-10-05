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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader; // javadocs
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.SearcherWarmer;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.ThreadInterruptedException;

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

public class NRTManager implements Closeable {
  private final IndexWriter writer;
  private final SearcherRef withoutDeletes;
  private final SearcherRef withDeltes;
  private final AtomicLong indexingGen;
  private final List<WaitingListener> waitingListeners = new CopyOnWriteArrayList<WaitingListener>();
  private final Lock reopenLock = new ReentrantLock();
  private final Condition newGeneration = reopenLock.newCondition();

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
   *  @param applyDeletes if <code>true</code> the NRTManage will force applyDeletes 
   *         during reopen operations. 
   *
   *  <p><b>NOTE</b>: the provided {@link SearcherWarmer} is
   *  not invoked for the initial searcher; you shouldDeletes
   *  warm it yourself if necessary.
   */
  public NRTManager(IndexWriter writer, ExecutorService es,
      SearcherWarmer warmer, boolean alwaysApplyDeletes) throws IOException {
    this.writer = writer;
    if (alwaysApplyDeletes) {
      withoutDeletes = withDeltes = new SearcherRef(true, 0,  SearcherManager.open(writer, true, warmer, es));
    } else {
      withDeltes = new SearcherRef(true, 0,  SearcherManager.open(writer, true, warmer, es));
      withoutDeletes = new SearcherRef(false, 0,  SearcherManager.open(writer, false, warmer, es));
    }
    indexingGen = new AtomicLong(1);
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
 
  
  public SearcherManager waitForGeneration(long targetGen, boolean applyAllDeletes) {
    return waitForGeneration(targetGen, applyAllDeletes, -1,  TimeUnit.NANOSECONDS);
  }

  public SearcherManager waitForGeneration(long targetGen, boolean applyAllDeletes, long time, TimeUnit unit) {
    if (targetGen > getCurrentSearchingGen(applyAllDeletes)) {
      // Must wait
      // final long t0 = System.nanoTime();
      for (WaitingListener listener : waitingListeners) {
        listener.waiting(applyAllDeletes, targetGen);
      }
      while (targetGen > getCurrentSearchingGen(applyAllDeletes)) {
        // System.out.println(Thread.currentThread().getName() +
        // ": wait fresh searcher targetGen=" + targetGen + " vs searchingGen="
        // + getCurrentSearchingGen(requireDeletes) + " requireDeletes=" +
        // requireDeletes);
        if (!waitOnGenCondition(time, unit)) {
          return getSearcherManager(applyAllDeletes);
        }
      }
    }
    return getSearcherManager(applyAllDeletes);
  }
  
  private boolean waitOnGenCondition(long time, TimeUnit unit) {
    try {
      reopenLock.lockInterruptibly();
      try {
        if (time < 0) {
          newGeneration.await();
          return true;
        } else {
          return newGeneration.await(time, unit);
        }
      } finally {
        reopenLock.unlock();
      }
    } catch (InterruptedException e) {
      throw new ThreadInterruptedException(e);
    }
  }

  /** Returns generation of current searcher. */
  public long getCurrentSearchingGen(boolean applyAllDeletes) {
    if (applyAllDeletes) {
      return withDeltes.generation;
    } else {
      return Math.max(withoutDeletes.generation, withDeltes.generation);
    }
  }

  public boolean maybeReopen(boolean applyAllDeletes) throws IOException {
    if (reopenLock.tryLock()) {
      try {
        SearcherRef reference = applyAllDeletes ? withDeltes : withoutDeletes;
        // Mark gen as of when reopen started:
        final long newSearcherGen = indexingGen.getAndIncrement();
        boolean setSearchGen = false;
        if (reference.manager.isSearcherCurrent()) {
          setSearchGen = true;
        } else {
          setSearchGen = reference.manager.maybeReopen();
        }
        if (setSearchGen) {
          reference.generation = newSearcherGen;// update searcher gen
          newGeneration.signalAll(); // wake up threads if we have a new generation
        }
        return setSearchGen;
      } finally {
        reopenLock.unlock();
      }
    }
    return false;
  }

  /**
   * Close this NRTManager to future searching. Any searches still in process in
   * other threads won't be affected, and they should still call
   * {@link #release} after they are done.
   * 
   * <p>
   * <b>NOTE</b>: caller must separately close the writer.
   */
  public synchronized void close() throws IOException {
    reopenLock.lock();
    try {
      IOUtils.close(withDeltes, withoutDeletes);
      newGeneration.signalAll();
    } finally {
      reopenLock.unlock();
    }
  }
  
  public IndexSearcher acquireLatest() {
    return getSearcherManager(false).acquire();
  }
  
  public void release(IndexSearcher searcher) throws IOException {
    getSearcherManager(false).release(searcher);
  }
  
  public SearcherManager getSearcherManager(boolean applyAllDeletes) {
    if (applyAllDeletes) {
      return withDeltes.manager;
    } else {
      if (withDeltes.generation > withoutDeletes.generation) {
        return withDeltes.manager;
      } else {
        return withoutDeletes.manager;
      }
    }
  }
  
  static class SearcherRef implements Closeable {
    final boolean applyDeletes;
    volatile long generation;
    final SearcherManager manager;

    SearcherRef(boolean applyDeletes, long generation, SearcherManager manager) {
      super();
      this.applyDeletes = applyDeletes;
      this.generation = generation;
      this.manager = manager;
    }
    
    public void close() throws IOException {
      generation = Long.MAX_VALUE;
      manager.close();
    }
  }
}
