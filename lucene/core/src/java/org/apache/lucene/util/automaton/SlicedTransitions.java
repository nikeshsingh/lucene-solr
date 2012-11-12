package org.apache.lucene.util.automaton;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

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

public class SlicedTransitions {

  public final int[] from;
  public final int[] transitions;
  public final int numStates;
  public final boolean[] accept;
  
  public SlicedTransitions(int[] from, int[] transitions, int numStates, boolean[] accept) {
    this.accept = accept;
    this.from = from;
    this.transitions = transitions;
    this.numStates = numStates;
  }
  
  public SlicedTransitions(int[] from, int[] transitions, int numStates) {
    this(from, transitions, numStates, null);
  }

  public int[] getPoints() {
    // nocommit maybe we can precompute this?
    Set<Integer> pointset = new HashSet<Integer>();
    pointset.add(Character.MIN_CODE_POINT);
    for (int i = 0; i < numStates; i++) {
      int end = from[i+1];
      for (int j=from[i];j<end;j+=3) {
        pointset.add(transitions[j]);
        if (transitions[j+1] < Character.MAX_CODE_POINT) pointset.add((transitions[j+1]+1));

      }
    }
    int[] points = new int[pointset.size()];
    int n = 0;
    for (Integer m : pointset)
      points[n++] = m;
    Arrays.sort(points);
    return points;
  }
  
  public int step(int state, int c) {
    assert state < from.length-1;
    assert c >= 0;
    int end = from[state+1];
    for (int i=from[state];i<end;i+=3) {
      if (transitions[i] <= c && c <= transitions[i+1]) return transitions[i+2];
    }
    return -1;
  }
  
}
