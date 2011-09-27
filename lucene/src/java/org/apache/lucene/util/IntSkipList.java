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
import java.util.Arrays;
import java.util.Random;

import org.apache.lucene.index.TermsEnum.SeekStatus;

/**
 * {@link IntSkipList} is a insert unique only skip list implementation that
 * operates on int[] blocks instead of using objects to hold skip towers. The
 * map is designed to be used in a MRSW (single writer multiple readers)
 * environment. Changes to the list are not reflected to reader immediately.
 * Readers operate on a {@link SeekableView} which provides a point in time
 * semantics reflecting all changes until the point in time the view was
 * created.
 * 
 * <p>
 * nocommit document relationship to {@link TypeTemplate} and how it can be
 * used with BytesRefHash
 */
public class IntSkipList<T extends Comparable<T>> {
  private static final Random seedGenerator = new Random();
  private int maxHeight = 1;
  private final static int HEAD_ADDR = 0;
  private final static int TAIL_ADDR = 33;

  private final int MAX_LEVELS = 31;
  private final int[] path = new int[MAX_LEVELS];
  private final SkipListKey insertSpare = new SkipListKey();
  private final SkipListKey insertSpare1 = new SkipListKey();
  private final T spareTemplate;
  private volatile int poolOffset = 0;

  private final IntBlockPool pool;
  private TypeTemplate<T> template;
  private int randomSeed;

  public IntSkipList(TypeTemplate<T> template, Counter counter) {
    randomSeed = 1 + seedGenerator.nextInt();
    pool = new IntBlockPool(counter);
    pool.nextBuffer();
    createHead();
    createTail();
    this.template = template;
    spareTemplate = template.newTemplate();

  }

  private void createTail() {
    SkipListKey tail = new SkipListKey();
    tail.allocateLevels(MAX_LEVELS);
    for (int i = 0; i < MAX_LEVELS; i++) {
      tail.setLevel(i, 0);
    }
    tail.setKey(Integer.MAX_VALUE);
    poolOffset = tail.write(pool);
  }

  private void createHead() {
    SkipListKey head = new SkipListKey();
    head.allocateLevels(MAX_LEVELS);
    for (int i = 0; i < MAX_LEVELS; i++) {
      head.setLevel(i, TAIL_ADDR);
    }
    head.setKey(Integer.MIN_VALUE);
    head.write(pool);
  }

  public void insert(int ordinal) {
    final T toInsert = template.load(template.newTemplate(), ordinal);
    final SkipListKey spare = insertSpare;
    spare.init(pool, HEAD_ADDR);
    final SkipListKey next = insertSpare1;
    // optimizes inner loop for expensive comparisons
    int addressAlreadyChecked = 0;
    for (int i = maxHeight - 1; i >= 0; i--) {
      next.init(pool, spare.levelAddress(i));
      assert assertLevels(next, spare.numLevels());
      while (addressAlreadyChecked != next.address && next.key != Integer.MAX_VALUE && template.load(spareTemplate, next.key).compareTo(toInsert) < 0) {
        spare.advanceToLevel(pool, i);
        if (next.address == spare.levelAddress(i)) {
          break; // we can skip comparisons on the same level
          /*
           * TODO if the next level has the same pointer we can skip another
           * comparison
           */
        }
        next.init(pool, spare.levelAddress(i));
      }
      addressAlreadyChecked = next.address;
      path[i] = spare.address;
    }
    assert next.key == Integer.MAX_VALUE || toInsert.compareTo(template.load(spareTemplate, next.key)) < 0 : ordinal + " "
        + next.key;

    final int newTowerLevels = randomLevel();
    final SkipListKey insertKey = new SkipListKey();
    insertKey.allocateLevels(newTowerLevels);
    insertKey.setKey(ordinal);
    // get the address where this tower will be written to
    final int addr = insertKey.reservedAddress(pool);
    /*
     * we might increase the max tower due to randomLevel - make sure we update
     * it to start searching from a higher level. Here we don't need to
     * initialize the higher levels in the path[] since they haven't been used
     * yet and point to address 0 which is our HEAD tower.
     */
    maxHeight = Math.max(maxHeight, newTowerLevels);
    for (int i = 0; i < newTowerLevels; i++) {
      /*
       * first prepare the new tower and allocate the memory for it. once this
       * is written we can cross the memory barrier and ensure that the memory
       * is allocated in the pool.
       */
      spare.init(pool, path[i]);
      insertKey.setLevel(i, spare.levelAddress(i));
      assert spare.key == Integer.MIN_VALUE || template.load(spareTemplate, spare.key).compareTo(template.load(insertKey.key)) < 0;
      assert assertLevels(insertKey, i + 1);
    }
    /*
     * here we cross the memory barrier, if a reader sees a stale pointer (one
     * of the pointers we are going to write below) the reader can cross the mem
     * barrier again which ensures that it sees valid values. In our model it is
     * ok to follow a stale pointer since the values we are looking for is
     * strickly after such a new pointer.
     */
    poolOffset = insertKey.write(pool);

    for (int i = 0; i < newTowerLevels; i++) {
      spare.init(pool, path[i]);
      spare.setLevel(i, addr);
    }
    // TODO do we need to cross the mem barrier here again?
    // poolOffset = poolOffset;
  }

  private boolean assertLevels(SkipListKey tower, int numLevels) {
    numLevels = Math.min(tower.numLevels(), numLevels);

    if (tower.key == Integer.MAX_VALUE) {
      for (int i = 0; i < numLevels; i++) {
        assert HEAD_ADDR == tower.levelAddress(i);
      }
      return true;
    }

    final SkipListKey spare = new SkipListKey();
    final SkipListKey spare2 = new SkipListKey();
    int last = tower.levelAddress(0);
    for (int i = 0; i < numLevels; i++) {
      final int levelAddress = tower.levelAddress(i);
      spare2.init(pool, last);
      spare.init(pool, levelAddress);
//      assert comparator.compare(spare2.key, spare.key) <= 0;
//      assert comparator.compare(tower.key, spare.key) < 0;
      last = levelAddress;
    }
    return true;
  }

  // copied from 
  // https://svn.apache.org/repos/asf/harmony/enhanced/java/branches/java6/classlib/modules/concurrent/src/main/java/java/util/concurrent/ConcurrentSkipListMap.java
  private int randomLevel() {
    int x = randomSeed;
    x ^= x << 13;
    x ^= x >>> 17;
    randomSeed = x ^= x << 5;
    if ((x & 0x8001) != 0)
      return 1; // level 1 is base level
    int level = 1;
    while (((x >>>= 1) & 1) != 0)
      ++level;
    return level;
  }

  public SeekableView<T> getView() {
    return new SeekableView<T>(this);
  }

  public static final class SeekableView<T extends Comparable<T>> {
    private int upTo;
    private int memBarrier = 0;
    private final SkipListKey spare = new SkipListKey();
    private final T spareTemplate;
    private final int maxHeight;
    private final IntBlockPool pool;
    private final IntSkipList<T> list;
    private final TypeTemplate<T> template;

    public SeekableView(IntSkipList<T> list) {
      /*
       * first cross the mem barrier and safe the max address of values we are
       * interested in due do the nature of this skip list all entries added
       * after this point will have a higher pool address since we only append
       * to the pool
       */
      upTo = memBarrier = list.poolOffset;
      this.maxHeight = list.maxHeight;
      this.list = list;
      this.pool = list.pool;
      this.template = list.template;
      spareTemplate = template.newTemplate();
    }
    
    public SeekableView(SeekableView<T> view) {
      upTo = memBarrier = view.upTo;
      maxHeight = view.maxHeight;
      list = view.list;
      pool = view.pool;
      template = view.template;
      spareTemplate = template.newTemplate();
    }
    
    public SeekableView<T> clone() {
      return new SeekableView<T>(this);
    }
    
    public T lastSeeked() {
      return spareTemplate;
    }

    public SeekStatus seekCeil(SkipListKey next, T value) {
      spare.init(pool, HEAD_ADDR);
      int addressAlreadyChecked = 0;
      for (int i = maxHeight - 1; i >= 0; i--) {
        int levelAddress = spare.levelAddress(i);
        if (levelAddress > memBarrier) {
          // cross the memory barrier if we see some stale pointer
          memBarrier = list.poolOffset;
        }
        next.init(pool, levelAddress);
        // prevent unnecessary checks on addresses since comparisons might be expensive
        while (addressAlreadyChecked != next.address && next.key != Integer.MAX_VALUE && (template.load(spareTemplate, next.key).compareTo(value)) < 0) {
          spare.init(pool, next.address);
          if (next.address == spare.levelAddress(i)) {
            break;
          }
          levelAddress = spare.levelAddress(i);
          if (levelAddress > memBarrier) {
            // cross the memory barrier if we see some stale pointer
            memBarrier = list.poolOffset;
          }
          next.init(pool, levelAddress);
        }
        addressAlreadyChecked = next.address;
      }
      while (next.address >= upTo) {
        /*
         * if we are already >= upTo we know this entry is not found even if the
         * keys are equal yet, we need to advance to the next value < upto to
         * get the actual seekCeil properties here
         */
        if (next.nextAddr() > memBarrier) {
          memBarrier = list.poolOffset;
        }
        next.next(pool);
      }
      // if we are at the tail simply return END
      if (next.address == TAIL_ADDR) {
        return SeekStatus.END;
      }
      return template.load(spareTemplate, next.key).compareTo(value) == 0? SeekStatus.FOUND : SeekStatus.NOT_FOUND;
    }

    public SeekStatus next(SkipListKey toAdvance) {
      int nextAddr = toAdvance.nextAddr();
      if (nextAddr > memBarrier) {
        memBarrier = list.poolOffset;
      }
      /*
       * we can reach the tail at any time - we must make sure we are not
       * advancing beyond the tail
       */
      if (nextAddr == TAIL_ADDR) {
        return SeekStatus.END;
      }
      toAdvance.next(pool);
      while (toAdvance.address >= upTo) {
        nextAddr = toAdvance.nextAddr();
        if (nextAddr > memBarrier) {
          memBarrier = list.poolOffset;
        } else if (nextAddr == TAIL_ADDR) {
          return SeekStatus.END;
        }
        toAdvance.next(pool);
      }
      template.load(spareTemplate, toAdvance.key);
      return SeekStatus.FOUND;
    }
    
    public SeekStatus position(SkipListKey key) {
      key.init(pool, HEAD_ADDR);
      key.next(pool);
      if (key.address == TAIL_ADDR) {
        return SeekStatus.END;
      }
      if (key.address > memBarrier) {
        memBarrier = list.poolOffset;
      }
      return SeekStatus.FOUND;
    }

  }
  
  

  public static final class SkipListKey  {
    final IntsRef data = new IntsRef(32);
    int address = -1;
    int key;

    public SkipListKey() {
      data.length = data.ints.length;
    }

    int nextAddr() {
      return data.ints[1 + data.offset];
    }

    void next(IntBlockPool pool) {
      this.address = data.ints[1 + data.offset];
      pool.set(data, address);
      key = data.ints[data.offset];
    }

    int reservedAddress(IntBlockPool pool) {
      return pool.nextAddress(data);
    }

    int levelAddress(int level) {
      return data.ints[1 + data.offset + level];
    }

    public int key() {
      return key;
    }

    void setKey(int key) {
      data.ints[data.offset] = this.key = key;
    }

    void setLevel(int level, int pointer) {
      data.ints[1 + data.offset + level] = pointer;
    }

    void allocateLevels(int levels) {
      data.length = levels + 1;
      data.grow(data.length);

    }

    int write(IntBlockPool pool) {
      return pool.append(data);
    }

    void advanceToLevel(IntBlockPool pool, int level) {
      this.address = data.ints[1 + data.offset + level];
      pool.set(data, address);
      key = data.ints[data.offset];
    }

    void init(IntBlockPool pool, int address) {
      if (this.address == address) {
        return;
      }
      pool.set(data, this.address = address);
      key = data.ints[data.offset];
    }

    int numLevels() {
      return data.length - 1;
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("Tower: [");
      builder.append(key);
      builder.append(" levelAddr: [");
      for (int i = 0; i < data.length - 1; i++) {
        builder.append(levelAddress(i)).append(", ");
      }

      builder.append("] ] ");
      return builder.toString();
    }

  }

  // nocommit document this
  public static interface TypeTemplate<T extends Comparable<T>> {
    public T newTemplate();
    public T load(T template, int ordinal);
    public T load(int ordinal);
  }
  
  static final class IntBlockPool {
    final static int INT_BLOCK_SHIFT = 15;
    final static int INT_BLOCK_SIZE = 1 << INT_BLOCK_SHIFT;
    final static int INT_BLOCK_MASK = INT_BLOCK_SIZE - 1;
    public int[][] buffers = new int[20][];

    int bufferUpto = -1; // Which buffer we are upto
    public int intUpto = INT_BLOCK_SIZE; // Where we are in head buffer

    public int[] buffer; // Current head buffer
    public int intOffset = -INT_BLOCK_SIZE; // Current head offset
    private final Counter counter;

    public IntBlockPool(Counter counter) {
      this.counter = counter;
    }

    public IntsRef set(IntsRef values, int start) {
      final int[] ints = values.ints = buffers[start >> INT_BLOCK_SHIFT];
      int pos = start & INT_BLOCK_MASK;
      values.length = ints[pos];
      values.offset = pos + 1;
      return values;
    }

    public int nextAddress(IntsRef values) {
      if (((values.length + intUpto + 1) - INT_BLOCK_SIZE) > 0) {
        return intOffset + INT_BLOCK_SIZE;
      }
      return intOffset + intUpto;
    }

    public int append(IntsRef values) {
      final int length = values.length;
      final int offset = values.offset;
      if (((length + intUpto + 1) - INT_BLOCK_SIZE) > 0) {
        nextBuffer();
      }
      buffer[intUpto++] = length;
      System.arraycopy(values.ints, offset, buffer, intUpto, length);
      intUpto += length;
      return intOffset + intUpto;
    }

    public void reset() {
      if (bufferUpto != -1) {
        // Reuse first buffer
        if (bufferUpto > 0) {
          Arrays.fill(buffers, 1, bufferUpto, null);
        }
        if (bufferUpto > 0) {
          counter.addAndGet(-INT_BLOCK_SIZE * bufferUpto + 1);
        }

        bufferUpto = 0;
        intUpto = 0;
        intOffset = 0;
        buffer = buffers[0];
      }
    }

    public void nextBuffer() {
      if (1 + bufferUpto == buffers.length) {
        int[][] newBuffers = new int[(int) (buffers.length * 1.5)][];
        System.arraycopy(buffers, 0, newBuffers, 0, buffers.length);
        buffers = newBuffers;
      }
      buffer = buffers[1 + bufferUpto] = new int[INT_BLOCK_SIZE];
      counter.addAndGet(INT_BLOCK_SIZE);
      bufferUpto++;

      intUpto = 0;
      intOffset += INT_BLOCK_SIZE;
    }
  }

}
