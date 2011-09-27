package org.apache.lucene.util;

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
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;

import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.util.IntSkipList.SeekableView;
import org.apache.lucene.util.IntSkipList.TypeTemplate;

public class TestIntSkipList extends LuceneTestCase {
  private static final IntTemplate INT_TEMPLATE = new IntTemplate();

  public void testInsert() {
    IntSkipList<Integer> intSkipList = new IntSkipList<Integer>(INT_TEMPLATE,
        Counter.newCounter());
    TreeSet<Integer> uniqueNumbers = new TreeSet<Integer>();
    final int num = atLeast(10000);
    int[] unsortedUnique = new int[num];
    for (int i = 0; i < num; i++) {
      int nextInt;
      do {
        nextInt = random.nextInt();
      } while (uniqueNumbers.contains(nextInt));
      uniqueNumbers.add(nextInt);
      unsortedUnique[i] = nextInt;
      intSkipList.insert(nextInt);
    }
    SeekableView<Integer> view = intSkipList.getView();
    IntSkipList.SkipListKey spare = new IntSkipList.SkipListKey();
    // test lookups of existing values
    Integer toSeek;
    for (int j = 0; j < unsortedUnique.length; j++) {
      toSeek = unsortedUnique[j];
      assertEquals(SeekStatus.FOUND, view.seekCeil(spare, toSeek));
      assertEquals(spare.key(), unsortedUnique[j]);
    }
    // test seek ceil
    for (int j = 0; j < num; j++) {
      int nextInt = random.nextInt();
      toSeek = nextInt;
      if (uniqueNumbers.contains(nextInt)) {
        assertEquals(SeekStatus.FOUND, view.seekCeil(spare, toSeek));
      } else {
        Integer ceil = uniqueNumbers.ceiling(nextInt);
        if (ceil == null) {
          assertEquals("ceiling: " + ceil + " actual: " + nextInt,
              SeekStatus.END, view.seekCeil(spare, toSeek));
        } else {
          assertEquals("ceiling: " + ceil + " actual: " + nextInt,
              SeekStatus.NOT_FOUND, view.seekCeil(spare, toSeek));
          assertEquals(spare.key(), ceil.intValue());
        }
      }
    }
  }

  public void testWithBytesRefHash() {
    BytesRef ref = new BytesRef();
    BytesRef scratch = new BytesRef();
    BytesRefHash hash = new BytesRefHash();
    Comparator<BytesRef> comp = BytesRef.getUTF8SortedAsUnicodeComparator();

    IntSkipList<BytesRef> list = new IntSkipList<BytesRef>(
        new BytesRefOrdComparator(hash, comp), Counter.newCounter());
    Set<String> strings = new HashSet<String>();
    int uniqueCount = 0;
    for (int i = 0; i < 797; i++) {
      String str;
      do {
        str = _TestUtil.randomRealisticUnicodeString(random, 1000);
      } while (str.length() == 0);
      ref.copy(str);
      int count = hash.size();
      int key = hash.add(ref);

      if (key >= 0) {
        assertTrue(strings.add(str));
        assertEquals(uniqueCount, key);
        assertEquals(hash.size(), count + 1);
        uniqueCount++;
        list.insert(key);
      } else {
        assertFalse(strings.add(str));
        assertTrue((-key) - 1 < count);
        assertEquals(str, hash.get((-key) - 1, scratch).utf8ToString());
        assertEquals(count, hash.size());
      }
    }

    SeekableView<BytesRef> view = list.getView();
    IntSkipList.SkipListKey spare = new IntSkipList.SkipListKey();
    int count = 0;
    for (String string : strings) {
      ref.copy(string);
      SeekStatus seekCeil = view.seekCeil(spare, ref);
      assertEquals(SeekStatus.FOUND, seekCeil);
      assertEquals(view.lastSeeked(), ref);
      assertEquals(string, view.lastSeeked().utf8ToString());
      count++;
    }
    hash.clear();
    assertEquals(0, hash.size());
    hash.reinit();
  }

  public void testNext() {
    IntSkipList<Integer> intSkipList = new IntSkipList<Integer>(INT_TEMPLATE,
        Counter.newCounter());
    TreeSet<Integer> uniqueNumbers = new TreeSet<Integer>();
    final int num = atLeast(10000);
    for (int i = 0; i < num; i++) {
      int nextInt;
      do {
        nextInt = random.nextInt();
      } while (uniqueNumbers.contains(nextInt));
      uniqueNumbers.add(nextInt);
      intSkipList.insert(nextInt);
    }
    SeekableView<Integer> view = intSkipList.getView();
    IntSkipList.SkipListKey spare = new IntSkipList.SkipListKey();
    Integer[] array = uniqueNumbers.toArray(new Integer[num]);
    // test seek ceil & next
    Integer toSeek;
    for (int j = 0; j < num; j++) {
      int nextInt = random.nextInt(num);
      toSeek = array[nextInt];
      assertEquals(SeekStatus.FOUND, view.seekCeil(spare, toSeek));
      assertEquals(spare.key(), array[nextInt].intValue());
      int upto = Math.min(nextInt + 1 + random.nextInt(10), array.length);
      for (int i = nextInt + 1; i < upto; i++) {
        SeekStatus next = view.next(spare);
        assertEquals(SeekStatus.FOUND, next);
        assertEquals(array[i].intValue(), spare.key());
      }
    }
    toSeek = array[array.length - 1];
    assertEquals(SeekStatus.FOUND, view.seekCeil(spare, toSeek));
    SeekStatus next = view.next(spare);
    assertEquals(SeekStatus.END, next);
  }

  public void testSingleWriterMultipleReaders() throws Throwable {
    for (int j = 0; j < RANDOM_MULTIPLIER; j++) {

      int numThreads = atLeast(3);
      int numIter = atLeast(50000);
      CountDownLatch latch = new CountDownLatch(numThreads);
      WriterThread thread = new WriterThread(latch);
      ReaderThread[] readers = new ReaderThread[numThreads];
      for (int i = 0; i < readers.length; i++) {
        readers[i] = new ReaderThread(thread, numIter, random, latch);
        readers[i].start();
      }
      thread.start();

      thread.join();
      for (int i = 0; i < readers.length; i++) {
        readers[i].join();
      }
      if (thread.error != null) {
        throw thread.error;
      }
      for (int i = 0; i < readers.length; i++) {
        if (readers[i].error != null) {
          throw readers[i].error;
        }
      }
    }
  }

  public static class WriterThread extends Thread {
    private final IntSkipList<Integer> list = new IntSkipList<Integer>(
        INT_TEMPLATE, Counter.newCounter());
    private ConcurrentSkipListMap<Integer, Integer> map = new ConcurrentSkipListMap<Integer, Integer>();
    volatile Throwable error;
    private volatile Holder holder = new Holder();
    private CountDownLatch latch;

    public WriterThread(CountDownLatch latch) {
      this.latch = latch;
    }

    public Holder newView(Holder holder) {
      Holder copy = this.holder;
      holder.version = copy.version;
      if (copy.view != null) {
        holder.view = copy.view.clone();
      }
      return holder;
    }

    @Override
    public void run() {
      try {
        latch.await();
        final int num = atLeast(30000);
        for (int i = 0; i < num; i++) {
          int nextInt;
          do {
            nextInt = random.nextInt(Integer.MAX_VALUE);
          } while (map.containsKey(nextInt));
          Holder newHolder = new Holder();
          newHolder.version = holder.version + 1;
          map.put(nextInt, newHolder.version);
          list.insert(nextInt);
          newHolder.view = list.getView();
          holder = newHolder;
        }
      } catch (Throwable e) {
        error = e;
      }
    }
  }

  public static class ReaderThread extends Thread {
    final WriterThread thread;
    final int numIter;
    final Random random;
    Throwable error;
    private CountDownLatch latch;

    public ReaderThread(WriterThread thread, int numIter, Random random,
        CountDownLatch latch) {
      this.thread = thread;
      this.numIter = numIter;
      this.random = random;
      this.latch = latch;
    }

    @Override
    public void run() {
      Holder holder = new Holder();
      IntSkipList.SkipListKey spare = new IntSkipList.SkipListKey();
      final int reopen = 10 + random.nextInt(40);
      try {
        latch.countDown();
        while (holder.version == 0) {
          Thread.yield();
          thread.newView(holder);
        }
        assertNotNull(holder.view);
        for (int i = 0; i < numIter; i++) {
          if (random.nextInt(reopen) == 0) {
            thread.newView(holder);
            assertNotNull(holder.view);
          }
          Integer toSeek = random.nextInt();
          SeekStatus seekCeil = holder.view.seekCeil(spare, toSeek);
          if (seekCeil != SeekStatus.END) {
            Entry<Integer, Integer> ceilingEntry = thread.map
                .ceilingEntry(spare.key());
            int version = ceilingEntry.getValue().intValue();
            final int value = ceilingEntry.getKey().intValue();
            if (version > holder.version) {
              assertEquals(SeekStatus.NOT_FOUND, seekCeil);
            } else {
              assertEquals(value == toSeek.intValue() ? SeekStatus.FOUND
                  : SeekStatus.NOT_FOUND, seekCeil);
            }
            assertEquals("actual version: " + version + " holder version: "
                + holder.version, value, spare.key());
            assertTrue("actual version: " + version + "holder version: "
                + holder.version, version <= holder.version);

            if (random.nextInt(10) == 0) {
              int num = 1 + random.nextInt(30);
              for (int j = 0; j < num; j++) {
                int prevKey = spare.key();
                SeekStatus next = holder.view.next(spare);
                if (next != SeekStatus.END) {
                  Integer integer = thread.map.get(spare.key());
                  assertNotNull("expected: " + spare.key() + " prev key: "
                      + prevKey, integer);
                  version = integer.intValue();
                  assertTrue("actual version: " + version + " holder version: "
                      + holder.version, version <= holder.version);
                }
              }
            }
          }

        }
      } catch (Throwable e) {
        this.error = e;
      }
    }
  }

  static class Holder {
    SeekableView<Integer> view;
    int version;
  }

  static class IntTemplate implements TypeTemplate<Integer> {

    @Override
    public Integer newTemplate() {
      return new Integer(0);
    }

    @Override
    public Integer load(Integer template, int ordinal) {
      return Integer.valueOf(ordinal);
    }

    @Override
    public Integer load(int ordinal) {
      return Integer.valueOf(ordinal);
    }

  }

  static class BytesRefOrdComparator implements TypeTemplate<BytesRef> {
    final BytesRefHash hash;
    final BytesRef spareLeft = new BytesRef();
    final BytesRef spareRight = new BytesRef();
    private Comparator<BytesRef> comparator;
    BytesRef ref;

    public BytesRefOrdComparator(BytesRefHash hash,
        Comparator<BytesRef> comparator) {
      this.hash = hash;
      this.comparator = comparator;
    }

    @Override
    public BytesRef newTemplate() {
      return new BytesRef();
    }

    @Override
    public BytesRef load(BytesRef template, int ordinal) {

      return hash.get(ordinal, template);
    }

    @Override
    public BytesRef load(int ordinal) {
      return hash.get(ordinal, newTemplate());
    }

  }
  
  
  static class TEHolder {
    TermsEnum termsEnum;
    int version;
  }

}
