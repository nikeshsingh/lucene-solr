package org.apache.lucene.util.automaton;

/*
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

import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.ArrayUtil;

import java.util.List;
import java.util.ArrayList;

// TODO
//   - do we really need the .bits...?  if not we can make util in UnicodeUtil to convert 1 char into a BytesRef

/** 
 * Converts UTF-32 automata to the equivalent UTF-8 representation.
 * @lucene.internal 
 */
public final class UTF32ToUTF8 {

  // Unicode boundaries for UTF8 bytes 1,2,3,4
  private static final int[] startCodes = new int[] {0, 128, 2048, 65536};
  private static final int[] endCodes = new int[] {127, 2047, 65535, 1114111};

  static int[] MASKS = new int[32];
  static {
    int v = 2;
    for(int i=0;i<32;i++) {
      MASKS[i] = v-1;
      v *= 2;
    }
  }

  // Represents one of the N utf8 bytes that (in sequence)
  // define a code point.  value is the byte value; bits is
  // how many bits are "used" by utf8 at that byte
  private static class UTF8Byte {
    int value;                                    // TODO: change to byte
    byte bits;
  }

  // Holds a single code point, as a sequence of 1-4 utf8 bytes:
  // TODO: maybe move to UnicodeUtil?
  private static class UTF8Sequence {
    private final UTF8Byte[] bytes;
    private int len;

    public UTF8Sequence() {
      bytes = new UTF8Byte[4];
      for(int i=0;i<4;i++) {
        bytes[i] = new UTF8Byte();
      }
    }

    public int byteAt(int idx) {
      return bytes[idx].value;
    }

    public int numBits(int idx) {
      return bytes[idx].bits;
    }

    private void set(int code) {
      if (code < 128) {
        // 0xxxxxxx
        bytes[0].value = code;
        bytes[0].bits = 7;
        len = 1;
      } else if (code < 2048) {
        // 110yyyxx 10xxxxxx
        bytes[0].value = (6 << 5) | (code >> 6);
        bytes[0].bits = 5;
        setRest(code, 1);
        len = 2;
      } else if (code < 65536) {
        // 1110yyyy 10yyyyxx 10xxxxxx
        bytes[0].value = (14 << 4) | (code >> 12);
        bytes[0].bits = 4;
        setRest(code, 2);
        len = 3;
      } else {
        // 11110zzz 10zzyyyy 10yyyyxx 10xxxxxx
        bytes[0].value = (30 << 3) | (code >> 18);
        bytes[0].bits = 3;
        setRest(code, 3);
        len = 4;
      }
    }

    private void setRest(int code, int numBytes) {
      for(int i=0;i<numBytes;i++) {
        bytes[numBytes-i].value = 128 | (code & MASKS[5]);
        bytes[numBytes-i].bits = 6;
        code = code >> 6;
      }
    }

    @Override
    public String toString() {
      StringBuilder b = new StringBuilder();
      for(int i=0;i<len;i++) {
        if (i > 0) {
          b.append(' ');
        }
        b.append(Integer.toBinaryString(bytes[i].value));
      }
      return b.toString();
    }
  }

  private final UTF8Sequence startUTF8 = new UTF8Sequence();
  private final UTF8Sequence endUTF8 = new UTF8Sequence();

  private final UTF8Sequence tmpUTF8a = new UTF8Sequence();
  private final UTF8Sequence tmpUTF8b = new UTF8Sequence();

  // Builds necessary utf8 edges between start & end
  void convertOneEdge(int start, int end, int startCodePoint, int endCodePoint, Transitions transitions) {
    startUTF8.set(startCodePoint);
    endUTF8.set(endCodePoint);
    //System.out.println("start = " + startUTF8);
    //System.out.println("  end = " + endUTF8);
    build(start, end, startUTF8, endUTF8, 0, transitions);
  }

  private void build(int start, int end, UTF8Sequence startUTF8, UTF8Sequence endUTF8, int upto, Transitions transitions) {

    // Break into start, middle, end:
    if (startUTF8.byteAt(upto) == endUTF8.byteAt(upto)) {
      // Degen case: lead with the same byte:
      if (upto == startUTF8.len-1 && upto == endUTF8.len-1) {
        // Super degen: just single edge, one UTF8 byte:
        transitions.addTransition(start, end, startUTF8.byteAt(upto), endUTF8.byteAt(upto));
        return;
      } else {
        assert startUTF8.len > upto+1;
        assert endUTF8.len > upto+1;
        
        // Single value leading edge
        int n = transitions.addTransition(start, startUTF8.byteAt(upto), endUTF8.byteAt(upto));  // type=single

        // Recurse for the rest
        build(n, end, startUTF8, endUTF8, 1+upto, transitions);
      }
    } else if (startUTF8.len == endUTF8.len) {
      if (upto == startUTF8.len-1) {
        transitions.addTransition(start, end, startUTF8.byteAt(upto), endUTF8.byteAt(upto));        // type=startend
      } else {
        start(start, end, startUTF8, upto, false, transitions);
        if (endUTF8.byteAt(upto) - startUTF8.byteAt(upto) > 1) {
          // There is a middle
          all(start, end, startUTF8.byteAt(upto)+1, endUTF8.byteAt(upto)-1, startUTF8.len-upto-1, transitions);
        }
        end(start, end, endUTF8, upto, false, transitions);
      }
    } else {

      // start
      start(start, end, startUTF8, upto, true, transitions);

      // possibly middle, spanning multiple num bytes
      int byteCount = 1+startUTF8.len-upto;
      final int limit = endUTF8.len-upto;
      while (byteCount < limit) {
        // wasteful: we only need first byte, and, we should
        // statically encode this first byte:
        tmpUTF8a.set(startCodes[byteCount-1]);
        tmpUTF8b.set(endCodes[byteCount-1]);
        all(start, end,
            tmpUTF8a.byteAt(0),
            tmpUTF8b.byteAt(0),
            tmpUTF8a.len - 1, transitions);
        byteCount++;
      }

      // end
      end(start, end, endUTF8, upto, true, transitions);
    }
  }

  private void start(int start, int end, UTF8Sequence utf8, int upto, boolean doAll, Transitions transitions) {
    if (upto == utf8.len-1) {
      // Done recursing
      transitions.addTransition(start, end, utf8.byteAt(upto), utf8.byteAt(upto) | MASKS[utf8.numBits(upto)-1]);  // type=start
    } else {
      int n = transitions.addTransition(start, utf8.byteAt(upto), utf8.byteAt(upto));// type=start
      start(n, end, utf8, 1+upto, true, transitions);
      int endCode = utf8.byteAt(upto) | MASKS[utf8.numBits(upto)-1];
      if (doAll && utf8.byteAt(upto) != endCode) {
        all(start, end, utf8.byteAt(upto)+1, endCode, utf8.len-upto-1, transitions);
      }
    }
  }

  private void end(int start, int end, UTF8Sequence utf8, int upto, boolean doAll, Transitions transitions) {
    if (upto == utf8.len-1) {
      // Done recursing
      transitions.addTransition(start, end, utf8.byteAt(upto) & (~MASKS[utf8.numBits(upto)-1]), utf8.byteAt(upto)); // type=end
    } else {
      final int startCode;
      if (utf8.numBits(upto) == 5) {
        // special case -- avoid created unused edges (utf8
        // doesn't accept certain byte sequences) -- there
        // are other cases we could optimize too:
        startCode = 194;
      } else {
        startCode = utf8.byteAt(upto) & (~MASKS[utf8.numBits(upto)-1]);
      }
      if (doAll && utf8.byteAt(upto) != startCode) {
        all(start, end, startCode, utf8.byteAt(upto)-1, utf8.len-upto-1, transitions);
      }
      int n = transitions.addTransition(start, utf8.byteAt(upto), utf8.byteAt(upto)); // type=end
      end(n, end, utf8, 1+upto, true, transitions);
    }
  }

  private void all(int start, int end, int startCode, int endCode, int left, Transitions transitions) {
    if (left == 0) {
      transitions.addTransition(start, end, startCode, endCode);  // type=all
    } else {
      int lastN = transitions.addTransition(start, startCode, endCode); // type=all
      while (left > 1) {
        lastN = transitions.addTransition(lastN, 128, 191); // type=all*
        left--;
      }
      transitions.addTransition(lastN, end, 128, 191); // type = all*
    }
  }

  private State[] utf8States;
  private int utf8StateCount;

  /** Converts an incoming utf32 automaton to an equivalent
   *  utf8 one.  The incoming automaton need not be
   *  deterministic.  Note that the returned automaton will
   *  not in general be deterministic, so you must
   *  determinize it if that's needed. */
  public Automaton convert(Automaton utf32) {
    if (utf32.isSingleton()) {
      utf32 = utf32.cloneExpanded();
    }

    State[] map = new State[utf32.getNumberedStates().length];
    List<State> pending = new ArrayList<State>();
    State utf32State = utf32.getInitialState();
    pending.add(utf32State);
    Automaton utf8 = new Automaton();
    utf8.setDeterministic(false);

    State utf8State = utf8.getInitialState();

    utf8States = new State[5];
    utf8StateCount = 0;
    utf8State.number = utf8StateCount;
    utf8States[utf8StateCount] = utf8State;
    utf8StateCount++;

    utf8State.setAccept(utf32State.isAccept());

    map[utf32State.number] = utf8State;
    Transitions trans = new Transitions();
    while(pending.size() != 0) {
      utf32State = pending.remove(pending.size()-1);
      utf8State = map[utf32State.number];
      for(int i=0;i<utf32State.numTransitions;i++) {
        final Transition t = utf32State.transitionsArray[i];
        final State destUTF32 = t.to;
        State destUTF8 = map[destUTF32.number];
        if (destUTF8 == null) {
          destUTF8 = newUTF8State();
          destUTF8.accept = destUTF32.accept;
          map[destUTF32.number] = destUTF8;
          pending.add(destUTF32);
        }
        trans.reset(utf8StateCount);
        convertOneEdge(utf8State.number, destUTF8.number, t.min, t.max, trans);
        convertTransitions(trans);
      }
    }

    utf8.setNumberedStates(utf8States, utf8StateCount);

    return utf8;
  }

  private State newUTF8State() {
    State s = new State();
    if (utf8StateCount == utf8States.length) {
      final State[] newArray = new State[ArrayUtil.oversize(1+utf8StateCount, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
      System.arraycopy(utf8States, 0, newArray, 0, utf8StateCount);
      utf8States = newArray;
    }
    utf8States[utf8StateCount] = s;
    s.number = utf8StateCount;
    utf8StateCount++;
    return s;
  }
  
  private void convertTransitions(Transitions trans) {
    int addedStates = trans.addedStates;
    if (utf8StateCount+addedStates >=  utf8States.length) {
      final State[] newArray = new State[ArrayUtil.oversize(addedStates+utf8StateCount, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
      System.arraycopy(utf8States, 0, newArray, 0, utf8StateCount);
      utf8States = newArray;
    }
    for (int i = 0; i < addedStates; i++) {
      State s = new State();
      utf8States[utf8StateCount] = s;
      s.number = utf8StateCount;
      utf8StateCount++;  
    }
    for (int i = 0; i < trans.offset; i++) {
      State state = utf8States[trans.transitions[i++]];
      State end = utf8States[trans.transitions[i++]];
      state.addTransition(new Transition(trans.transitions[i++], trans.transitions[i], end));
    }
    
  }
  
  static class Transitions {
    int[] transitions = new int[4];
    int stateCounter;
    int addedStates;
    int offset;
    
    public void reset(int counter) {
      this.stateCounter = counter;
      addedStates = 0;
      offset = 0;
    }
    
    public int addTransition(int start, int min, int max) {
      assert start < stateCounter + addedStates;
      if (transitions.length <= offset+4) {
        transitions = ArrayUtil.grow(transitions, offset+4);
      }
      transitions[offset++] = start;
      transitions[offset++] = addedStates+stateCounter;
      transitions[offset++] = min;
      transitions[offset++] = max;
      return (addedStates++)+stateCounter;
    }
    
    public void addTransition(int start, int end, int min, int max) {
      assert start < stateCounter + addedStates : "start: " + start + " current: " + (stateCounter + addedStates);
      assert end < stateCounter + addedStates : "end: " + end + " expected: " + (stateCounter + addedStates);
      if (transitions.length <= offset+4) {
        transitions = ArrayUtil.grow(transitions, offset+4);
      }
      transitions[offset++] = start;
      transitions[offset++] = end;
      transitions[offset++] = min;
      transitions[offset++] = max;
      
    }
  }
}
