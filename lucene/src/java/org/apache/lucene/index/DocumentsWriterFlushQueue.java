package org.apache.lucene.index;
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
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.index.DocumentsWriterPerThread.FlushedSegment;


/**
 * 
 * @lucene.internal 
 */
public class DocumentsWriterFlushQueue {
  private final Queue<FlushTicket> queue = new LinkedList<FlushTicket>();
  // we track tickets separately since count must be present even before the ticket is
  // constructed ie. queue.size would not reflect it.
  private final AtomicInteger ticketCount = new AtomicInteger();
  private final ReentrantLock purgeLock = new ReentrantLock();

  synchronized void addDeletesAndPurge(DocumentsWriter writer,
      DocumentsWriterDeleteQueue deleteQueue) throws IOException {
    ticketCount.incrementAndGet(); // first inc the ticket count - freeze opens
                                   // a window for #anyChanges to fail
    boolean success = false;
    try {
      queue.add(new GlobalDeletesTicket(deleteQueue.freezeGlobalBuffer(null)));
      success = true;
    } finally {
      if (!success) {
        ticketCount.decrementAndGet();
      }
    }
    forcePurge(writer);
  }

  synchronized SegmentFlushTicket addFlushTicket(DocumentsWriterPerThread dwpt) {
    // Each flush is assigned a ticket in the order they acquire the ticketQueue
    // lock
    ticketCount.incrementAndGet();
    boolean success = false;
    try {
      final SegmentFlushTicket ticket = new SegmentFlushTicket(dwpt.prepareFlush());
      queue.add(ticket);
      success = true;
      return ticket;
    } finally {
      if (!success) {
        ticketCount.decrementAndGet();
      }
    }
  }
  
  synchronized void addSegment(SegmentFlushTicket ticket, FlushedSegment segment) {
    ticket.setSegment(segment);
  }

  synchronized void markTicketFailed(SegmentFlushTicket ticket) {
    ticket.setFailed();
  }

  boolean hasTickets() {
    assert ticketCount.get() >= 0 : "ticketCount should be >= 0 but was: " + ticketCount.get();
    return ticketCount.get() != 0;
  }


  private void innerPurge(DocumentsWriter writer) throws IOException {
    assert purgeLock.isHeldByCurrentThread();
    while (true) {
      final FlushTicket head;
      final boolean canPublish;
      synchronized (this) {
        head = queue.peek();
        canPublish = head != null && head.canPublish();
      }
      if (canPublish) {
        try {
          head.publish(writer);
        } finally {
          synchronized (this) {
            queue.poll();
            ticketCount.decrementAndGet();
          }
        }
      } else {
        break;
      }
    }
  }

  void forcePurge(DocumentsWriter writer) throws IOException {
    purgeLock.lock();
    try {
      innerPurge(writer);
    } finally {
      purgeLock.unlock();
    }
  }

  void tryPurge(DocumentsWriter writer) throws IOException {
    if (purgeLock.tryLock()) {
      try {
        innerPurge(writer);
      } finally {
        purgeLock.unlock();
      }
    }
  }

  FlushTicket poll() {
    try {
      return queue.poll();
    } finally {
      ticketCount.decrementAndGet();
    }
  }

  synchronized void clear() {
    queue.clear();
    ticketCount.set(0);
  }

  static abstract class FlushTicket {
    protected FrozenBufferedDeletes frozenDeletes;

    protected FlushTicket(FrozenBufferedDeletes frozenDeletes) {
      assert frozenDeletes != null;
      this.frozenDeletes = frozenDeletes;
    }

    protected abstract void publish(DocumentsWriter writer) throws IOException;
    protected abstract boolean canPublish();
  }
  
  static final class GlobalDeletesTicket extends FlushTicket{

    protected GlobalDeletesTicket(FrozenBufferedDeletes frozenDeletes) {
      super(frozenDeletes);
    }
    protected void publish(DocumentsWriter writer) throws IOException {
      writer.finishFlush(null, frozenDeletes);
    }

    protected boolean canPublish() {
      return true;
    }
  }
  
  static final class SegmentFlushTicket extends FlushTicket {
    private FlushedSegment segment;
    private boolean failed = false;
    
    protected SegmentFlushTicket(FrozenBufferedDeletes frozenDeletes) {
      super(frozenDeletes);
    }
    
    protected void publish(DocumentsWriter writer) throws IOException {
      writer.finishFlush(segment, frozenDeletes);
    }
    
    protected void setSegment(FlushedSegment segment) {
      this.segment = segment;
    }
    
    protected void setFailed() {
      failed = true;
    }

    protected boolean canPublish() {
      return segment != null || failed;
    }
  }
}
