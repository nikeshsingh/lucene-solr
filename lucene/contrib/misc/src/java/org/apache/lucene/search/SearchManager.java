package org.apache.lucene.search;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.AlreadyClosedException;

public abstract class SearchManager implements Closeable {
  protected volatile IndexSearcher currentSearcher;
  protected final ExecutorService es;
  protected final SearcherWarmer warmer;
  private final Semaphore reopening = new Semaphore(1);
  
  public SearchManager(SearcherWarmer warmer, ExecutorService es) throws IOException {
    this.es = es;
    this.warmer = warmer;
  }

  /** You must call this, periodically, to perform a
   *  reopen.  This calls {@link IndexReader#reopen} on the
   *  underlying reader, and if that returns a new reader,
   *  it's warmed (if you provided a {@link SearcherWarmer}
   *  and then swapped into production.
   *
   *  <p><b>Threads</b>: it's fine for more than one thread to
   *  call this at once.  Only the first thread will attempt
   *  the reopen; subsequent threads will see that another
   *  thread is already handling reopen and will return
   *  immediately.  Note that this means if another thread
   *  is already reopening then subsequent threads will
   *  return right away without waiting for the reader
   *  reopen to complete.</p>
   *
   *  <p>This method returns true if a new reader was in
   *  fact opened.</p>
   */
  public boolean maybeReopen()
    throws  IOException {

    if (currentSearcher == null) {
      throw new AlreadyClosedException("this SearcherManager is closed");
    }

    // Ensure only 1 thread does reopen at once; other
    // threads just return immediately:
    if (reopening.tryAcquire()) {
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
        reopening.release();
      }
    } else {
      return false;
    }
  }

  protected void swapSearcher(IndexSearcher newSearcher) throws IOException {
    IndexSearcher oldSearcher = currentSearcher;
    if (oldSearcher == null) {
      throw new AlreadyClosedException("this SearcherManager is closed");
    }
    currentSearcher = newSearcher;
    release(oldSearcher);
  }

  /** Release the searcher previously obtained with {@link
   *  #acquire}.
   *
   *  <p><b>NOTE</b>: it's safe to call this after {@link
   *  #close}. */
  public void release(IndexSearcher searcher) throws IOException {
    assert searcher != null;
    searcher.getIndexReader().decRef();
  }
  
  /** Close this SearcherManager to future searching.  Any
   *  searches still in process in other threads won't be
   *  affected, and they should still call {@link #release}
   *  after they are done. */
  public abstract void close() throws IOException;

  /** Obtain the current IndexSearcher.  You must match
   *  every call to acquire with one call to {@link #release};
   *  it's best to do so in a finally clause. */
  public IndexSearcher acquire() {
    IndexSearcher searcher;
    do {
      if ((searcher = currentSearcher) == null) {
        throw new AlreadyClosedException("this SearcherManager is closed");
      }
    } while (!searcher.getIndexReader().tryIncRef());
    return searcher;
  }
  
  protected abstract IndexReader openIfChanged(IndexReader oldReader) throws IOException;
  
  
}