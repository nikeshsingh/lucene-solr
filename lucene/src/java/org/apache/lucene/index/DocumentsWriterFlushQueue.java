package org.apache.lucene.index;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene.index.DocumentsWriterPerThread.FlushedSegment;

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

public class DocumentsWriterFlushQueue {
  private final Queue<FlushTicket> queue = new LinkedList<FlushTicket>();
  private final AtomicInteger ticketCount = new AtomicInteger();
  private final ReentrantLock purgeLock = new ReentrantLock();

  synchronized void addDeletesAndPurge(DocumentsWriter writer,
      DocumentsWriterDeleteQueue deleteQueue) throws IOException {
    ticketCount.incrementAndGet(); // first inc the ticket count - freeze opens
                                   // a window for #anyChanges to fail
    boolean success = false;
    try {
      add(new FlushTicket(deleteQueue.freezeGlobalBuffer(null), false));
      success = true;
    } finally {
      if (!success) {
        ticketCount.decrementAndGet();
      }
    }
    forcePurge(writer);
  }

  synchronized FlushTicket addFlushTicket(DocumentsWriterPerThread dwpt) {
    // Each flush is assigned a ticket in the order they acquire the ticketQueue
    // lock
    FlushTicket ticket = new FlushTicket(dwpt.prepareFlush(), true);
    add(ticket);
    ticketCount.incrementAndGet();
    return ticket;
  }

  synchronized void addSegment(FlushTicket ticket, FlushedSegment segment) {
    ticket.segment = segment;
  }

  synchronized void markTicketFailed(FlushTicket ticket) {
    ticket.isSegmentFlush = false;
  }

  synchronized boolean hasTickets() {
    assert ticketCount.get() >= 0;
    return ticketCount.get() != 0;
  }

  private synchronized void add(FlushTicket ticket) {
    queue.add(ticket);
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
          writer.finishFlush(head.segment, head.frozenDeletes);
        } finally {
          synchronized (this) {
            queue.poll();
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

  static final class FlushTicket {
    final FrozenBufferedDeletes frozenDeletes;
    /* access to non-final members must be synchronized on DW#ticketQueue */
    private FlushedSegment segment;
    private boolean isSegmentFlush;

    FlushTicket(FrozenBufferedDeletes frozenDeletes, boolean isSegmentFlush) {
      this.frozenDeletes = frozenDeletes;
      this.isSegmentFlush = isSegmentFlush;
    }

    private boolean canPublish() {
      return (!isSegmentFlush || segment != null);
    }
  }
}
