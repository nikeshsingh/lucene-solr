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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.lucene.util.BytesRefHash.MaxBytesLengthExceededException;
import org.apache.lucene.util.BytesRefHash.BytesRefHashView;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class TestBytesRefHash extends LuceneTestCase {

  BytesRefHash hash;
  ByteBlockPool pool;

  /**
   */
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    pool = newPool();
    hash = newHash(pool);
  }

  private ByteBlockPool newPool() {
    return random.nextBoolean() && pool != null ? pool : new ByteBlockPool(
        new RecyclingByteBlockAllocator(ByteBlockPool.BYTE_BLOCK_SIZE,
            random.nextInt(25)));
  }

  private BytesRefHash newHash(ByteBlockPool blockPool) {
    final int initSize = 2 << 1 + random.nextInt(5);
    return random.nextBoolean() ? new BytesRefHash(blockPool)
        : new BytesRefHash(blockPool, initSize,
            new BytesRefHash.DirectBytesStartArray(initSize));
  }

  /**
   * Test method for {@link org.apache.lucene.util.BytesRefHash#size()}.
   */
  @Test
  public void testSize() {
    BytesRef ref = new BytesRef();
    int num = atLeast(2);
    for (int j = 0; j < num; j++) {
      final int mod = 1 + random.nextInt(39);
      for (int i = 0; i < 797; i++) {
        String str;
        do {
          str = _TestUtil.randomRealisticUnicodeString(random, 1000);
        } while (str.length() == 0);
        ref.copy(str);
        int count = hash.size();
        int key = hash.add(ref);
        if (key < 0)
          assertEquals(hash.size(), count);
        else
          assertEquals(hash.size(), count + 1);
        if (i % mod == 0) {
          hash.clear();
          assertEquals(0, hash.size());
          hash.reinit();
        }
      }
    }
  }

  /**
   * Test method for
   * {@link org.apache.lucene.util.BytesRefHash#get(org.apache.lucene.util.BytesRefHash.Entry)}
   * .
   */
  @Test
  public void testGet() {
    BytesRef ref = new BytesRef();
    BytesRef scratch = new BytesRef();
    int num = atLeast(2);
    for (int j = 0; j < num; j++) {
      Map<String, Integer> strings = new HashMap<String, Integer>();
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
          assertNull(strings.put(str, Integer.valueOf(key)));
          assertEquals(uniqueCount, key);
          uniqueCount++;
          assertEquals(hash.size(), count + 1);
        } else {
          assertTrue((-key) - 1 < count);
          assertEquals(hash.size(), count);
        }
      }
      for (Entry<String, Integer> entry : strings.entrySet()) {
        ref.copy(entry.getKey());
        assertEquals(ref, hash.get(entry.getValue().intValue(), scratch));
      }
      hash.clear();
      assertEquals(0, hash.size());
      hash.reinit();
    }
  }

  /**
   * Test method for {@link org.apache.lucene.util.BytesRefHash#compact()}.
   */
  @Test
  public void testCompact() {
    BytesRef ref = new BytesRef();
    int num = atLeast(2);
    for (int j = 0; j < num; j++) {
      int numEntries = 0;
      final int size = 797;
      BitSet bits = new BitSet(size);
      for (int i = 0; i < size; i++) {
        String str;
        do {
          str = _TestUtil.randomRealisticUnicodeString(random, 1000);
        } while (str.length() == 0);
        ref.copy(str);
        final int key = hash.add(ref);
        if (key < 0) {
          assertTrue(bits.get((-key) - 1));
        } else {
          assertFalse(bits.get(key));
          bits.set(key);
          numEntries++;
        }
      }
      assertEquals(hash.size(), bits.cardinality());
      assertEquals(numEntries, bits.cardinality());
      assertEquals(numEntries, hash.size());
      int[] compact = hash.compact();
      assertTrue(numEntries < compact.length);
      for (int i = 0; i < numEntries; i++) {
        bits.set(compact[i], false);
      }
      assertEquals(0, bits.cardinality());
      hash.clear();
      assertEquals(0, hash.size());
      hash.reinit();
    }
  }

  /**
   * Test method for
   * {@link org.apache.lucene.util.BytesRefHash#sort(java.util.Comparator)}.
   */
  @Test
  public void testSort() {
    BytesRef ref = new BytesRef();
    int num = atLeast(2);
    for (int j = 0; j < num; j++) {
      SortedSet<String> strings = new TreeSet<String>();
      for (int i = 0; i < 797; i++) {
        String str;
        do {
          str = _TestUtil.randomRealisticUnicodeString(random, 1000);
        } while (str.length() == 0);
        ref.copy(str);
        hash.add(ref);
        strings.add(str);
      }
      // We use the UTF-16 comparator here, because we need to be able to
      // compare to native String.compareTo() [UTF-16]:
      int[] sort = hash.sort(BytesRef.getUTF8SortedAsUTF16Comparator());
      assertTrue(strings.size() < sort.length);
      int i = 0;
      BytesRef scratch = new BytesRef();
      for (String string : strings) {
        ref.copy(string);
        assertEquals(ref, hash.get(sort[i++], scratch));
      }
      hash.clear();
      assertEquals(0, hash.size());
      hash.reinit();

    }
  }

  /**
   * Test method for
   * {@link org.apache.lucene.util.BytesRefHash#add(org.apache.lucene.util.BytesRef)}
   * .
   */
  @Test
  public void testAdd() {
    BytesRef ref = new BytesRef();
    BytesRef scratch = new BytesRef();
    int num = atLeast(2);
    for (int j = 0; j < num; j++) {
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
        } else {
          assertFalse(strings.add(str));
          assertTrue((-key) - 1 < count);
          assertEquals(str, hash.get((-key) - 1, scratch).utf8ToString());
          assertEquals(count, hash.size());
        }
      }

      assertAllIn(strings, hash);
      hash.clear();
      assertEquals(0, hash.size());
      hash.reinit();
    }
  }

  @Test(expected = MaxBytesLengthExceededException.class)
  public void testLargeValue() {
    int[] sizes = new int[] { random.nextInt(5),
        ByteBlockPool.BYTE_BLOCK_SIZE - 33 + random.nextInt(31),
        ByteBlockPool.BYTE_BLOCK_SIZE - 1 + random.nextInt(37) };
    BytesRef ref = new BytesRef();
    for (int i = 0; i < sizes.length; i++) {
      ref.bytes = new byte[sizes[i]];
      ref.offset = 0;
      ref.length = sizes[i];
      try {
        assertEquals(i, hash.add(ref));
      } catch (MaxBytesLengthExceededException e) {
        if (i < sizes.length - 1)
          fail("unexpected exception at size: " + sizes[i]);
        throw e;
      }
    }
  }

  /**
   * Test method for
   * {@link org.apache.lucene.util.BytesRefHash#addByPoolOffset(int)} .
   */
  @Test
  public void testAddByPoolOffset() {
    BytesRef ref = new BytesRef();
    BytesRef scratch = new BytesRef();
    BytesRefHash offsetHash = newHash(pool);
    int num = atLeast(2);
    for (int j = 0; j < num; j++) {
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
          int offsetKey = offsetHash.addByPoolOffset(hash.byteStart(key));
          assertEquals(uniqueCount, offsetKey);
          assertEquals(offsetHash.size(), count + 1);
          uniqueCount++;
        } else {
          assertFalse(strings.add(str));
          assertTrue((-key) - 1 < count);
          assertEquals(str, hash.get((-key) - 1, scratch).utf8ToString());
          assertEquals(count, hash.size());
          int offsetKey = offsetHash
              .addByPoolOffset(hash.byteStart((-key) - 1));
          assertTrue((-offsetKey) - 1 < count);
          assertEquals(str, hash.get((-offsetKey) - 1, scratch).utf8ToString());
          assertEquals(count, hash.size());
        }
      }

      assertAllIn(strings, hash);
      for (String string : strings) {
        ref.copy(string);
        int key = hash.add(ref);
        BytesRef bytesRef = offsetHash.get((-key) - 1, scratch);
        assertEquals(ref, bytesRef);
      }

      hash.clear();
      assertEquals(0, hash.size());
      offsetHash.clear();
      assertEquals(0, offsetHash.size());
      hash.reinit(); // init for the next round
      offsetHash.reinit();
    }
  }

  private void assertAllIn(Set<String> strings, BytesRefHash hash) {
    BytesRef ref = new BytesRef();
    BytesRef scratch = new BytesRef();
    int count = hash.size();
    for (String string : strings) {
      ref.copy(string);
      int key = hash.add(ref); // add again to check duplicates
      assertEquals(string, hash.get((-key) - 1, scratch).utf8ToString());
      assertEquals(count, hash.size());
      assertTrue("key: " + key + " count: " + count + " string: " + string,
          key < count);
    }
  }

  public void testViewSeekExact() {
    BytesRef ref = new BytesRef();
    BytesRef scratch = new BytesRef();
    int num = atLeast(2);
    for (int j = 0; j < num; j++) {
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
        } else {
          assertFalse(strings.add(str));
          assertTrue((-key) - 1 < count);
          assertEquals(str, hash.get((-key) - 1, scratch).utf8ToString());
          assertEquals(count, hash.size());
        }
      }

      Comparator<BytesRef> comp = BytesRef.getUTF8SortedAsUnicodeComparator();
      BytesRefHashView readOnly = hash.getView(comp, false);
      int count = 0;
      for (String string : strings) {
        ref.copy(string);
        assertTrue(readOnly.seekExact(ref, scratch));
        assertEquals(scratch, ref);
        assertEquals(string, ref.utf8ToString());
        count++;
      }
      hash.clear();
      assertEquals(0, hash.size());
      hash.reinit();
    }
  }

  public void testViewSeekCeil() {
    BytesRef ref = new BytesRef();
    BytesRef scratch = new BytesRef();
    int num = atLeast(2);
    for (int j = 0; j < num; j++) {
      Set<BytesRef> strings = new HashSet<BytesRef>();
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
          assertTrue(strings.add(new BytesRef(str)));
          assertEquals(uniqueCount, key);
          assertEquals(hash.size(), count + 1);
          uniqueCount++;
        } else {
          assertFalse(strings.add(new BytesRef(str)));
          assertTrue((-key) - 1 < count);
          assertEquals(str, hash.get((-key) - 1, scratch).utf8ToString());
          assertEquals(count, hash.size());
        }
      }
      Comparator<BytesRef> comp = BytesRef.getUTF8SortedAsUnicodeComparator();
      BytesRef[] sorted = strings.toArray(new BytesRef[0]);
      Arrays.sort(sorted, comp);

      BytesRefHashView readOnly = hash.getView(comp, true);

      assertEquals(readOnly.size(), hash.size());

      for (int i = 0; i < sorted.length; i++) {
        int next = readOnly.next(i - 1, scratch);
        assertTrue(next >= 0);
        assertEquals(sorted[i].utf8ToString(), scratch.utf8ToString());

      }
      for (BytesRef term : strings) {
        int seekCeil = readOnly.seekCeil(term, scratch);
        assertTrue(readOnly.seekExact(term, scratch));
        assertEquals(scratch, term);
        assertTrue(seekCeil >= 0);
        assertEquals(scratch, term);

      }
      int iter = atLeast(100);

      for (int i = 0; i < iter; i++) {
        ref.copy(_TestUtil.randomRealisticUnicodeString(random, 1000));
        int seekCeil = readOnly.seekCeil(ref, scratch);
        if (!readOnly.seekExact(ref, scratch)) {
          assertTrue(seekCeil < 0);
        } else {
          assertTrue(seekCeil >= 0);
        }
        int binarySearch = Arrays.binarySearch(sorted, ref, comp);
        assertEquals(seekCeil, binarySearch);
      }
      hash.clear();
      assertEquals(0, hash.size());
      hash.reinit();
    }
  }

  public void testSingleWriterMultipleReaders() throws Throwable {

    int numThreads = atLeast(2);
    int numIter = atLeast(40000);
    WriterThread thread = new WriterThread();
    ReaderThread[] readers = new ReaderThread[numThreads];
    for (int i = 0; i < readers.length; i++) {
      readers[i] = new ReaderThread(thread, numIter, random);
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

  public void testIncrementallySortedView() {
    BytesRef ref = new BytesRef();
    BytesRef scratch = new BytesRef();
    int num = atLeast(2);
    Comparator<BytesRef> comp = BytesRef.getUTF8SortedAsUnicodeComparator();

    for (int j = 0; j < num; j++) {
      TreeSet<BytesRef> strings = new TreeSet<BytesRef>(comp);
      BytesRefHashView readOnly = null;
      for (int i = 0; i < 797; i++) {
        String str;
        do {
          str = _TestUtil.randomRealisticUnicodeString(random, 1000);
        } while (str.length() == 0);
        ref.copy(str);
        hash.add(ref);
        strings.add(new BytesRef(str));

        if (random.nextInt(20) == 0) {
          readOnly = hash.getView(comp, readOnly, true);
          Iterator<BytesRef> iterator = strings.iterator();
          for (int k = 0; k < strings.size(); k++) {
            int next = readOnly.next(k - 1, scratch);
            assertTrue("" + k + " i: " + i + " next: " + next, next >= 0);
            assertTrue(iterator.hasNext());
            String utf8ToString = iterator.next().utf8ToString();
            if (!utf8ToString.equals(scratch.utf8ToString())) {
              System.out.println();
            }
            assertEquals("" + k + " i: " + i, utf8ToString,
                scratch.utf8ToString());
          }
          assertFalse(iterator.hasNext());
        }
      }

      hash.clear();
      assertEquals(0, hash.size());
      hash.reinit();

    }
  }

  public static class WriterThread extends Thread {
    BytesRefHash hash = new BytesRefHash();
    volatile Throwable error;
    volatile Holder holder = new Holder();

    @Override
    public void run() {
      try {
        Comparator<BytesRef> comp = BytesRef.getUTF8SortedAsUnicodeComparator();
        BytesRef ref = new BytesRef();
        ArrayList<String> strings = new ArrayList<String>();
        for (int i = 0; i < atLeast(20000); i++) {
          String str;
          do {
            str = _TestUtil.randomRealisticUnicodeString(random, 1000);
          } while (str.length() == 0);
          ref.copy(str);
          int add = hash.add(ref);
          if (add >= 0) {
            strings.add(str);
          }

          if (random.nextInt(10) == 0) {
            Holder newHolder = new Holder();
            newHolder.view = hash.getView(comp, holder.view, true);
            newHolder.keys = strings.toArray(new String[0]);
            assertEquals(newHolder.keys.length, newHolder.view.size());
            holder = newHolder;
          }
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
    BytesRef key = new BytesRef();
    BytesRef spare = new BytesRef();
    Throwable error;

    public ReaderThread(WriterThread thread, int numIter, Random random) {
      this.thread = thread;
      this.numIter = numIter;
      this.random = random;
    }

    @Override
    public void run() {
      Comparator<BytesRef> comp = BytesRef.getUTF8SortedAsUnicodeComparator();
      try {
        while (thread.holder.keys == null) {
          Thread.yield();
          if (thread.error != null) {
            return;
          }
        }
        for (int i = 0; i < numIter; i++) {
          Holder holder = thread.holder;
          int nextInt = random.nextInt(holder.keys.length);
          key.copy(holder.keys[nextInt]);
          int seekCeil = holder.view.seekCeil(key, spare);
          assertTrue(seekCeil >= 0);
          holder = thread.holder;
          assertTrue(holder.view.seekExact(key, spare));
          // seek again to make sure we don't get a new holder since we got the ord
          seekCeil = holder.view.seekCeil(key, spare);
          int randomNext = random.nextInt(100);
          for (int j = 0; j < randomNext; j++) {
            int next = holder.view.next(seekCeil+j, spare);
            if (next != -1) {
              assertTrue(comp.compare(key, spare) < 0);
            }
          }
        }
      } catch (Throwable e) {
        this.error = e;
      }
    }

  }

  public static class Holder {
    public BytesRefHashView view;
    public String[] keys;
  }
}
