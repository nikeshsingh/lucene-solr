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

import java.util.Arrays;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IntBlockPool;
import org.apache.lucene.util.SorterTemplate;
import org.apache.lucene.util.IntBlockPool.SliceWriter;
import org.apache.lucene.util.IntsRef;

/**
 * Class to construct DFAs that match a word within some edit distance.
 * <p>
 * Implements the algorithm described in:
 * Schulz and Mihov: Fast String Correction with Levenshtein Automata
 * <p>
 * @lucene.experimental
 */
public class LevenshteinAutomata {
  /** @lucene.internal */
  public static final int MAXIMUM_SUPPORTED_DISTANCE = 2;
  /* input word */
  final int word[];
  /* the automata alphabet. */
  final int alphabet[];
  /* the maximum symbol in the alphabet (e.g. 255 for UTF-8 or 10FFFF for UTF-32) */
  final int alphaMax;

  /* the ranges outside of alphabet */
  final int rangeLower[];
  final int rangeUpper[];
  final int numRanges;
  
  ParametricDescription descriptions[]; 
  
  /**
   * Create a new LevenshteinAutomata for some input String.
   * Optionally count transpositions as a primitive edit.
   */
  public LevenshteinAutomata(String input, boolean withTranspositions) {
    this(codePoints(input), Character.MAX_CODE_POINT, withTranspositions);
  }

  /**
   * Expert: specify a custom maximum possible symbol
   * (alphaMax); default is Character.MAX_CODE_POINT.
   */
  public LevenshteinAutomata(int[] word, int alphaMax, boolean withTranspositions) {
    this.word = word;
    this.alphaMax = alphaMax;

    // calculate the alphabet
    SortedSet<Integer> set = new TreeSet<Integer>();
    for (int i = 0; i < word.length; i++) {
      int v = word[i];
      if (v > alphaMax) {
        throw new IllegalArgumentException("alphaMax exceeded by symbol " + v + " in word");
      }
      set.add(v);
    }
    alphabet = new int[set.size()];
    Iterator<Integer> iterator = set.iterator();
    for (int i = 0; i < alphabet.length; i++)
      alphabet[i] = iterator.next();
      
    rangeLower = new int[alphabet.length + 2];
    rangeUpper = new int[alphabet.length + 2];
    // calculate the unicode range intervals that exclude the alphabet
    // these are the ranges for all unicode characters not in the alphabet
    int lower = 0;
    int numRanges = 0;
    for (int i = 0; i < alphabet.length; i++) {
      int higher = alphabet[i];
      if (higher > lower) {
        rangeLower[numRanges] = lower;
        rangeUpper[numRanges] = higher - 1;
        numRanges++;
      }
      lower = higher + 1;
    }
    /* add the final endpoint */
    if (lower <= alphaMax) {
      rangeLower[numRanges] = lower;
      rangeUpper[numRanges] = alphaMax;
      numRanges++;
    }
    this.numRanges = numRanges;
    descriptions = new ParametricDescription[] {
        null, /* for n=0, we do not need to go through the trouble */
        withTranspositions ? new Lev1TParametricDescription(word.length) : new Lev1ParametricDescription(word.length),
        withTranspositions ? new Lev2TParametricDescription(word.length) : new Lev2ParametricDescription(word.length),
    };
  }
  
  private static int[] codePoints(String input) {
    int length = Character.codePointCount(input, 0, input.length());
    int word[] = new int[length];
    for (int i = 0, j = 0, cp = 0; i < input.length(); i += Character.charCount(cp)) {
      word[j++] = cp = input.codePointAt(i);
    }
    return word;
  }
  
  /**
   * Compute a DFA that accepts all strings within an edit distance of <code>n</code>.
   * <p>
   * All automata have the following properties:
   * <ul>
   * <li>They are deterministic (DFA).
   * <li>There are no transitions to dead states.
   * <li>They are not minimal (some transitions could be combined).
   * </ul>
   * </p>
   */
  public Automaton toAutomaton(int n) {
    if (n == 0) {
      return BasicAutomata.makeString(word, 0, word.length);
    }
    
    if (n >= descriptions.length)
      return null;
    
    final int range = 2*n+1;
    ParametricDescription description = descriptions[n];
    // the number of states is based on the length of the word and n
    State states[] = new State[description.size()];
    // create all states, and mark as accept states if appropriate
    for (int i = 0; i < states.length; i++) {
      states[i] = new State();
      states[i].number = i;
      states[i].setAccept(description.isAccept(i));
    }
    // create transitions from state to state
    for (int k = 0; k < states.length; k++) {
      final int xpos = description.getPosition(k);
      if (xpos < 0)
        continue;
      final int end = xpos + Math.min(word.length - xpos, range);
      
      for (int x = 0; x < alphabet.length; x++) {
        final int ch = alphabet[x];
        // get the characteristic vector at this position wrt ch
        final int cvec = getVector(ch, xpos, end);
        int dest = description.transition(k, xpos, cvec);
        if (dest >= 0)
          states[k].addTransition(new Transition(ch, states[dest]));
      }
      // add transitions for all other chars in unicode
      // by definition, their characteristic vectors are always 0,
      // because they do not exist in the input string.
      int dest = description.transition(k, xpos, 0); // by definition
      if (dest >= 0)
        for (int r = 0; r < numRanges; r++)
          states[k].addTransition(new Transition(rangeLower[r], rangeUpper[r], states[dest]));      
    }

    Automaton a = new Automaton(states[0]);
    a.setDeterministic(true);
    // we create some useless unconnected states, and its a net-win overall to remove these,
    // as well as to combine any adjacent transitions (it makes later algorithms more efficient).
    // so, while we could set our numberedStates here, its actually best not to, and instead to
    // force a traversal in reduce, pruning the unconnected states while we combine adjacent transitions.
    //a.setNumberedStates(states);
    a.reduce();
    // we need not trim transitions to dead states, as they are not created.
    //a.restoreInvariant();
    return a;
  }
  
  
  public CompiledAutomaton toRunAutomaton(IntsRef prefix, int n) {
    if (n == 0) {
      if (prefix == null || prefix.length == 0) {
        return new CompiledAutomaton(BasicAutomata.makeString(word, 0, word.length),  true, false);
      }
      return new CompiledAutomaton(BasicAutomata.makeString(prefix.ints, prefix.offset, prefix.length).
          concatenate(BasicAutomata.makeString(word, 0, word.length)), true, false);
    }
    
    if (n >= descriptions.length)
      return null;
    final int[] transitionBuffer = new int[(alphabet.length + numRanges) * 3];
    int transitionUpTo = 0;
    final int range = 2*n+1;
    ParametricDescription description = descriptions[n];
    // the number of states is based on the length of the word and n
    SliceTransitionBuilder builder = new SliceTransitionBuilder(description.size());
    // create all states, and mark as accept states if appropriate
    final int numInitStates = description.size();
    // create transitions from state to state
    for (int k = 0; k < numInitStates; k++) {
      final int xpos = description.getPosition(k);
      if (xpos < 0)
        continue;
      final int end = xpos + Math.min(word.length - xpos, range);
      
      for (int x = 0; x < alphabet.length; x++) {
        final int ch = alphabet[x];
        // get the characteristic vector at this position wrt ch
        final int cvec = getVector(ch, xpos, end);
        int dest = description.transition(k, xpos, cvec);
        if (dest >= 0) {
          transitionBuffer[transitionUpTo++] = ch;
          transitionBuffer[transitionUpTo++] = ch;
          transitionBuffer[transitionUpTo++] = dest;
        }
      }
      // add transitions for all other chars in unicode
      // by definition, their characteristic vectors are always 0,
      // because they do not exist in the input string.
      int dest = description.transition(k, xpos, 0); // by definition
      if (dest >= 0) {
        for (int r = 0; r < numRanges; r++) {
          transitionBuffer[transitionUpTo++] = rangeLower[r];
          transitionBuffer[transitionUpTo++] = rangeUpper[r];
          transitionBuffer[transitionUpTo++] = dest;
        }
      }
      builder.addBuffer(k, transitionBuffer, transitionUpTo);
      transitionUpTo = 0;
      
    }
   
   
    SlicedTransitions slicedTransitions = builder.toSlicedTransitions(prefix, description);
    ByteRunAutomaton runAutomaton = new ByteRunAutomaton(slicedTransitions);
//    return new CompiledAutomaton(slicedTransitions, runAutomaton);
    
    
    boolean[] isAccept = new boolean[builder.numStates];
    int limit = description.size();
    for (int i = 0; i < limit; i++) {
      isAccept[i] = description.isAccept(i);
    }
    Automaton automaton = builder.toAutomaton(isAccept);
    automaton.setDeterministic(true);
    if (prefix != null) {
      automaton = new UTF32ToUTF8().convert(BasicAutomata.makeString(prefix.ints, prefix.offset, prefix.length)).concatenate(automaton);
      automaton.setDeterministic(true);
    }
    SlicedTransitions slicedTransitions2 = automaton.getSlicedTransitions();
    return new CompiledAutomaton(automaton.getSlicedTransitions(), new ByteRunAutomaton(automaton.getSlicedTransitions()));
  }
  
  public static void main(String[] args) {
    IntsRef prefix = toUTF32("落".toCharArray(), 0, "落".toCharArray().length, new IntsRef());
    SlicedTransitions prefixTransitions = new UTF32ToUTF8().convert(BasicAutomata.makeString(prefix.ints, prefix.offset, prefix.length)).getSlicedTransitions();
    System.out.println(Arrays.toString(prefixTransitions.from));
    System.out.println(Arrays.toString(prefixTransitions.transitions));
    System.out.println(Arrays.toString(prefixTransitions.accept));
    
    int[] from = new int[prefixTransitions.from.length];
    int[] trans = new int[prefixTransitions.transitions.length];
    SliceTransitionBuilder.insertSingletonPrefix(from, trans, prefixTransitions);
    
    System.out.println(Arrays.toString(from));
    System.out.println(Arrays.toString(trans));
  }
  
  
  public static IntsRef toUTF32(char[] s, int offset, int length, IntsRef scratch) {
    int charIdx = offset;
    int intIdx = 0;
    final int charLimit = offset + length;
    while(charIdx < charLimit) {
      scratch.grow(intIdx+1);
      final int utf32 = Character.codePointAt(s, charIdx);
      scratch.ints[intIdx] = utf32;
      charIdx += Character.charCount(utf32);
      intIdx++;
    }
    scratch.length = intIdx;
    return scratch;
  }
  
  private static class SliceTransitionBuilder {
    UTF32ToUTF8.Transitions utf8Transitions =  new UTF32ToUTF8.Transitions();
    UTF32ToUTF8 utf32ToUTF8 = new UTF32ToUTF8();
    final IntBlockPool pool = new IntBlockPool();
    IntBlockPool.SliceWriter writer = new IntBlockPool.SliceWriter(pool);
    int[] stateStart;
    int[] stateEnd;
    int numStates;
    int numTransitions;
    
    public SliceTransitionBuilder(int numUTF32States) {
      stateStart = new int[numUTF32States];
      stateEnd = new int[numUTF32States];
      numStates = numUTF32States;
      writer.startNewSlice();// ignore first slice to safe a fill 
    }
    
    public void addBuffer(int start, int[] transitionBuffer, int transitionUpTo) {
      // TODO can we a.reduce() this on the fly?
      for (int i = 0; i < transitionUpTo; i++) {
        int min = transitionBuffer[i++];
        int max = transitionBuffer[i++];
        int transDest = transitionBuffer[i];
        utf8Transitions.reset(numStates);
        utf32ToUTF8.convertOneEdge(start, transDest, min, max, utf8Transitions);
        stateStart = ArrayUtil.grow(stateStart, utf8Transitions.addedStates + utf8Transitions.stateCounter);
        stateEnd = ArrayUtil.grow(stateEnd, utf8Transitions.addedStates + utf8Transitions.stateCounter);
        for (int j = 0; j < utf8Transitions.offset; j++) {
            int fromState = utf8Transitions.transitions[j++];
            int toState = utf8Transitions.transitions[j++];
            int minT = utf8Transitions.transitions[j++];
            int maxT = utf8Transitions.transitions[j];
            if (stateStart[fromState] <= 0) {
              stateEnd[fromState] = stateStart[fromState] = writer.startNewSlice();
            } else {
              writer.reset(stateEnd[fromState]);
            }
            writer.writeInt(minT);
            writer.writeInt(maxT);
            writer.writeInt(toState);
            numTransitions++;
            stateEnd[fromState] = writer.getCurrentOffset();
        }
        numStates += utf8Transitions.addedStates;
      }
    }
    
    static int insertSingletonPrefix(int[] from, int[] transitions, SlicedTransitions singleton) {
      int state = 0;
      int transIndex = 0;
      for (int i = 0; i < singleton.numStates-1; i++) {
        from[i] = i*3;
        int offset = singleton.from[state];
        transitions[transIndex++] = singleton.transitions[offset++];
        transitions[transIndex++] = singleton.transitions[offset++];
        transitions[transIndex++] = i+1;
        state = singleton.transitions[offset];
      }
      assert singleton.accept[state];
      return singleton.numStates-1;
    }
    
    public SlicedTransitions toSlicedTransitions(IntsRef prefix, ParametricDescription description) {
      // TODO do this on the fly
      int numPrefixStates = 0;
      int numPrefixTransitions = 0;
      int fromOffset = 0;
      final int[] from;
      final int[] transitions;
      if (prefix == null || prefix.length == 0) {
        from = new int[numStates + 1];
        transitions = new int[numTransitions * 3];
      } else {
        SlicedTransitions singleton = this.utf32ToUTF8
            .convert(
                BasicAutomata.makeString(prefix.ints, prefix.offset,
                    prefix.length)).getSlicedTransitions();
        numPrefixStates = singleton.numStates;
        numPrefixTransitions = singleton.transitions.length;
        from = new int[singleton.numStates + numStates + 1];
        transitions = new int[singleton.transitions.length + numTransitions * 3];
        fromOffset = insertSingletonPrefix(from, transitions, singleton);
      }
      IntBlockPool.SliceReader reader = new IntBlockPool.SliceReader(pool);
      int transIndex = numPrefixTransitions;
      int descLimit = description.size();
      boolean[] accept = new boolean[numPrefixStates + numStates];
      for (int i = 0; i < numStates; i++) {
        if (i < descLimit) {
          accept[numPrefixStates + i] = description.isAccept(i);
        }
        reader.reset(stateStart[i], stateEnd[i]);
        from[fromOffset + i] = transIndex;
        int numTrans = 0;
        int startOffset = transIndex;
        while (!reader.endOfSlice()) {
          numTrans++;
          transitions[transIndex++] = reader.readInt();
          transitions[transIndex++] = reader.readInt();
          transitions[transIndex++] = numPrefixStates + reader.readInt();
        }
//        System.out.println(Arrays.toString(transitions));
        sortTransitions(transitions, startOffset, numTrans);
//        System.out.println(Arrays.toString(transitions));
      }
      from[from.length - 1] = transIndex;
      
      return new SlicedTransitions(from, transitions, numStates, accept);
    }
    
    public Automaton toAutomaton(boolean[] accept) {
      State[] states = new State[numStates];
      IntBlockPool.SliceReader reader = new IntBlockPool.SliceReader(pool);
      for (int i = 0; i < states.length; i++) {
        states[i] = new State();
        if (i < accept.length) {
          states[i].setAccept(accept[i]);
        }
      }
      for (int i = 0; i < numStates; i++) {
        State s = states[i];
        reader.reset(stateStart[i], stateEnd[i]);
        while(!reader.endOfSlice()) {
          s.addTransition(new Transition(reader.readInt(), reader.readInt(), states[reader.readInt()]));
        }
      }
      return new Automaton(states[0]);
    }
    private final int[] pivot = new int[3];
    
    private void sortTransitions(final int[] transitions, final int start, final int num) {
      if (num==1) {
        return;// sorted only one transition!
      }
      
      new SorterTemplate() {
        
        @Override
        protected void swap(int i, int j) {
          for (int k = 0; k < 3; k++) {
            int tmp = transitions[start+3*i+k];
            transitions[start+3*i+k] = transitions[start+3*j+k];
            transitions[start+3*j+k] = tmp; 
          }
        }
        
        @Override
        protected void setPivot(int i) {
          for (int k = 0; k < 3; k++) {
            pivot[k] = transitions[start+3*i+k];
          }
          
        }
        
        @Override
        protected int comparePivot(int j) {
          int rOffset = start + 3 * j;
          if (pivot[0] < transitions[rOffset]) return -1;
          if (pivot[0] > transitions[rOffset]) return 1;
          if (pivot[1] > transitions[rOffset + 1]) return -1;
          if (pivot[1] < transitions[rOffset + 1]) return 1;
          if (pivot[2] != transitions[rOffset + 2]) {
            if (pivot[2] < transitions[rOffset + 2]) return -1;
            if (pivot[2] > transitions[rOffset + 2]) return 1;
          }
          return 0;
        }
        
        @Override
        protected int compare(int i, int j) {
          int lOffset = start+3*i;
          int rOffset = start+3*j;
          if (transitions[lOffset] < transitions[rOffset]) return -1;
          if (transitions[lOffset] > transitions[rOffset]) return 1;
          if (transitions[lOffset+1] > transitions[rOffset+1]) return -1;
          if (transitions[lOffset+1] < transitions[rOffset+1]) return 1;
          if (transitions[lOffset+2] != transitions[rOffset+2]) {
            if (transitions[lOffset+2] < transitions[rOffset+2]) return -1;
            if (transitions[lOffset+2] > transitions[rOffset+2]) return 1;
          }
          return 0;
        }
      }.quickSort(0, num-1);
    }
  }
  
  /**
   * Get the characteristic vector <code>X(x, V)</code> 
   * where V is <code>substring(pos, end)</code>
   */
  int getVector(int x, int pos, int end) {
    int vector = 0;
    for (int i = pos; i < end; i++) {
      vector <<= 1;
      if (word[i] == x)
        vector |= 1;
    }
    return vector;
  }
    
  /**
   * A ParametricDescription describes the structure of a Levenshtein DFA for some degree n.
   * <p>
   * There are four components of a parametric description, all parameterized on the length
   * of the word <code>w</code>:
   * <ol>
   * <li>The number of states: {@link #size()}
   * <li>The set of final states: {@link #isAccept(int)}
   * <li>The transition function: {@link #transition(int, int, int)}
   * <li>Minimal boundary function: {@link #getPosition(int)}
   * </ol>
   */
  static abstract class ParametricDescription {
    protected final int w;
    protected final int n;
    private final int[] minErrors;
    
    ParametricDescription(int w, int n, int[] minErrors) {
      this.w = w;
      this.n = n;
      this.minErrors = minErrors;
    }
    
    /**
     * Return the number of states needed to compute a Levenshtein DFA
     */
    int size() {
      return minErrors.length * (w+1);
    };

    /**
     * Returns true if the <code>state</code> in any Levenshtein DFA is an accept state (final state).
     */
    boolean isAccept(int absState) {
      // decode absState -> state, offset
      int state = absState/(w+1);
      int offset = absState%(w+1);
      assert offset >= 0;
      return w - offset + minErrors[state] <= n;
    }

    /**
     * Returns the position in the input word for a given <code>state</code>.
     * This is the minimal boundary for the state.
     */
    int getPosition(int absState) {
      return absState % (w+1);
    }
    
    /**
     * Returns the state number for a transition from the given <code>state</code>,
     * assuming <code>position</code> and characteristic vector <code>vector</code>
     */
    abstract int transition(int state, int position, int vector);

    private final static long[] MASKS = new long[] {0x1,0x3,0x7,0xf,
                                                    0x1f,0x3f,0x7f,0xff,
                                                    0x1ff,0x3ff,0x7ff,0xfff,
                                                    0x1fff,0x3fff,0x7fff,0xffff,
                                                    0x1ffff,0x3ffff,0x7ffff,0xfffff,
                                                    0x1fffff,0x3fffff,0x7fffff,0xffffff,
                                                    0x1ffffff,0x3ffffff,0x7ffffff,0xfffffff,
                                                    0x1fffffff,0x3fffffff,0x7fffffffL,0xffffffffL,
                                                    0x1ffffffffL,0x3ffffffffL,0x7ffffffffL,0xfffffffffL,
                                                    0x1fffffffffL,0x3fffffffffL,0x7fffffffffL,0xffffffffffL,
                                                    0x1ffffffffffL,0x3ffffffffffL,0x7ffffffffffL,0xfffffffffffL,
                                                    0x1fffffffffffL,0x3fffffffffffL,0x7fffffffffffL,0xffffffffffffL,
                                                    0x1ffffffffffffL,0x3ffffffffffffL,0x7ffffffffffffL,0xfffffffffffffL,
                                                    0x1fffffffffffffL,0x3fffffffffffffL,0x7fffffffffffffL,0xffffffffffffffL,
                                                    0x1ffffffffffffffL,0x3ffffffffffffffL,0x7ffffffffffffffL,0xfffffffffffffffL,
                                                    0x1fffffffffffffffL,0x3fffffffffffffffL,0x7fffffffffffffffL};
  
    protected int unpack(long[] data, int index, int bitsPerValue) {
      final long bitLoc = bitsPerValue * index;
      final int dataLoc = (int) (bitLoc >> 6);
      final int bitStart = (int) (bitLoc & 63);
      //System.out.println("index=" + index + " dataLoc=" + dataLoc + " bitStart=" + bitStart + " bitsPerV=" + bitsPerValue);
      if (bitStart + bitsPerValue <= 64) {
        // not split
        return (int) ((data[dataLoc] >> bitStart) & MASKS[bitsPerValue-1]);
      } else {
        // split
        final int part = 64-bitStart;
        return (int) (((data[dataLoc] >> bitStart) & MASKS[part-1]) +
                      ((data[1+dataLoc] & MASKS[bitsPerValue-part-1]) << part));
      }
    }
  }
}
