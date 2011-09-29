package org.apache.lucene.search;

import java.io.Closeable;
import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.AlreadyClosedException;

public abstract class SearchManager implements Closeable {
  protected volatile IndexSearcher currentSearcher;
  

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
  public abstract boolean maybeReopen() throws IOException;

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
  
  
}