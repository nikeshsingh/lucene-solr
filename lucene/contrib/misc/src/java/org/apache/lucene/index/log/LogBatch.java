package org.apache.lucene.index.log;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;


final class LogBatch {

  private volatile Node<?> tail;
  
  @SuppressWarnings("rawtypes")
  private static final AtomicReferenceFieldUpdater<LogBatch,Node> tailUpdater = AtomicReferenceFieldUpdater
      .newUpdater(LogBatch.class, Node.class, "tail");
  
  
  LogBatch() {
    /*
     * we use a sentinel instance as our initial tail. No slice will ever try to
     * apply this tail since the head is always omitted.
     */
    tail = new Node<Object>(null); // sentinel
  }


  void add(Node<?> item) {
    /*
     * this non-blocking / 'wait-free' linked list add was inspired by Apache
     * Harmony's ConcurrentLinkedQueue Implementation.
     */
    while (true) {
      final Node<?> currentTail = this.tail;
      final Node<?> tailNext = currentTail.next;
      if (tail == currentTail) {
        if (tailNext != null) {
          /*
           * we are in intermediate state here. the tails next pointer has been
           * advanced but the tail itself might not be updated yet. help to
           * advance the tail and try again updating it.
           */
          tailUpdater.compareAndSet(this, currentTail, tailNext); // can fail
        } else {
          /*
           * we are in quiescent state and can try to insert the item to the
           * current tail if we fail to insert we just retry the operation since
           * somebody else has already added its item
           */
          if (currentTail.casNext(null, item.updateSeqId(currentTail.seqId + 1))) {
            /*
             * now that we are done we need to advance the tail while another
             * thread could have advanced it already so we can ignore the return
             * type of this CAS call
             */
            tailUpdater.compareAndSet(this, currentTail, item);
            return;
          }
        }
      }
    }
  }



  BatchSlice newSlice() {
    return new BatchSlice(tail);
  }

  boolean updateSlice(BatchSlice slice) {
    if (slice.sliceTail != tail) { // If we are the same just
      slice.sliceTail = tail;
      return true;
    }
    return false;
  }

  static class BatchSlice {
    // No need to be volatile, slices are thread captive (only accessed by one thread)!
    Node<?> sliceHead; // we don't apply this one
    Node<?> sliceTail;

    BatchSlice(Node<?> currentTail) {
      assert currentTail != null;
      /*
       * Initially this is a 0 length slice pointing to the 'current' tail of
       * the queue. Once we update the slice we only need to assign the tail and
       * have a new slice
       */
      sliceHead = sliceTail = currentTail;
    }

    void reset() {
      // Reset to a 0 length slice
      sliceHead = sliceTail;
    }

    /**
     * Returns <code>true</code> iff the given item is identical to the item
     * hold by the slices tail, otherwise <code>false</code>.
     */
    boolean isTailItem(Object item) {
      return sliceTail.item == item;
    }

    boolean isEmpty() {
      return sliceHead == sliceTail;
    }
  }


  static class Node<T> {
    volatile Node<?> next;
    final T item;
    long seqId = -1;

    Node(T item) {
      this.item = item;
    }
    
    public Node<T> updateSeqId(long seqId) {
      this.seqId = seqId;
      return this;
    }

    @SuppressWarnings("rawtypes")
    static final AtomicReferenceFieldUpdater<Node,Node> nextUpdater = AtomicReferenceFieldUpdater
        .newUpdater(Node.class, Node.class, "next");

    boolean casNext(Node<?> cmp, Node<?> val) {
      return nextUpdater.compareAndSet(this, cmp, val);
    }
  }


}
