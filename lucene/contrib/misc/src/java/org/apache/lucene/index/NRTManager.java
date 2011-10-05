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
 * <p>NOTE: to use this class, you must call {@link #maybeReopen(boolean)}
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
  private final SearcherManagerRef withoutDeletes;
  private final SearcherManagerRef withDeletes;
  private final AtomicLong indexingGen;
  private final List<WaitingListener> waitingListeners = new CopyOnWriteArrayList<WaitingListener>();
  private final ReentrantLock reopenLock = new ReentrantLock();
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
   *  @param alwaysApplyDeletes if <code>true</code> the NRTManage will force applyDeletes 
   *         during reopen operations. 
   *
   *  <p><b>NOTE</b>: the provided {@link SearcherWarmer} is
   *  not invoked for the initial searcher; you should
   *  warm it yourself if necessary.
   */
  public NRTManager(IndexWriter writer, ExecutorService es,
      SearcherWarmer warmer, boolean alwaysApplyDeletes) throws IOException {
    this.writer = writer;
    if (alwaysApplyDeletes) {
      withoutDeletes = withDeletes = new SearcherManagerRef(true, 0,  SearcherManager.open(writer, true, warmer, es));
    } else {
      withDeletes = new SearcherManagerRef(true, 0,  SearcherManager.open(writer, true, warmer, es));
      withoutDeletes = new SearcherManagerRef(false, 0,  SearcherManager.open(writer, false, warmer, es));
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
 
  
  //nocommit javadoc 
  public SearcherManager waitForGeneration(long targetGen, boolean requireDeletes) {
    return waitForGeneration(targetGen, requireDeletes, -1,  TimeUnit.NANOSECONDS);
  }

  //nocommit javadoc
  public SearcherManager waitForGeneration(long targetGen, boolean requireDeletes, long time, TimeUnit unit) {
    try {
      reopenLock.lockInterruptibly();
      try {
        if (targetGen > getCurrentSearchingGen(requireDeletes)) {
          for (WaitingListener listener : waitingListeners) {
            listener.waiting(requireDeletes, targetGen);
          }
          while (targetGen > getCurrentSearchingGen(requireDeletes)) {
            if (!waitOnGenCondition(time, unit)) {
              return getSearcherManager(requireDeletes);
            }
          }
        }

      } finally {
        reopenLock.unlock();
      }
    } catch (InterruptedException ie) {
      throw new ThreadInterruptedException(ie);
    }
    return getSearcherManager(requireDeletes);
  }
  
  //nocommit javadoc
  private boolean waitOnGenCondition(long time, TimeUnit unit)
      throws InterruptedException {
    assert reopenLock.isHeldByCurrentThread();
    if (time < 0) {
      newGeneration.await();
      return true;
    } else {
      return newGeneration.await(time, unit);
    }
  }

  /** Returns generation of current searcher. */
  public long getCurrentSearchingGen(boolean applyAllDeletes) {
    if (applyAllDeletes) {
      return withDeletes.generation;
    } else {
      return Math.max(withoutDeletes.generation, withDeletes.generation);
    }
  }

  public boolean maybeReopen(boolean applyAllDeletes) throws IOException {
    if (reopenLock.tryLock()) {
      try {
        final SearcherManagerRef reference = applyAllDeletes ? withDeletes : withoutDeletes;
        // Mark gen as of when reopen started:
        final long newSearcherGen = indexingGen.getAndIncrement();
        boolean setSearchGen = false;
        if (!(setSearchGen = reference.manager.isSearcherCurrent())) {
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
      IOUtils.close(withDeletes, withoutDeletes);
      newGeneration.signalAll();
    } finally {
      reopenLock.unlock();
    }
  }
  
  //nocommit javadoc
  public IndexSearcher acquireLatest() {
    return getSearcherManager(false).acquire();
  }
  
  //nocommit javadoc
  public void release(IndexSearcher searcher) throws IOException {
    getSearcherManager(false).release(searcher);
  }
  
  //nocommit javadoc
  public SearcherManager getSearcherManager(boolean applyAllDeletes) {
    if (applyAllDeletes) {
      return withDeletes.manager;
    } else {
      if (withDeletes.generation > withoutDeletes.generation) {
        return withDeletes.manager;
      } else {
        return withoutDeletes.manager;
      }
    }
  }
  
  static final class SearcherManagerRef implements Closeable {
    final boolean applyDeletes;
    volatile long generation;
    final SearcherManager manager;

    SearcherManagerRef(boolean applyDeletes, long generation, SearcherManager manager) {
      super();
      this.applyDeletes = applyDeletes;
      this.generation = generation;
      this.manager = manager;
    }
    
    public void close() throws IOException {
      generation = Long.MAX_VALUE; // max it out to make sure nobody can wait on another gen
      manager.close();
    }
  }
}
