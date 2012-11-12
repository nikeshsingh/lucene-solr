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

public class SlicedTransitions {

  public final int[] from;
  public final int[] transitions;
  public final int numStates;
  
  public SlicedTransitions(int[] from, int[] transitions, int numStates) {
    super();
    this.from = from;
    this.transitions = transitions;
    this.numStates = numStates;
  }



  public int getTransitionsLength(int state) {
    return from[state+1] - from[state];   
  }
  
}
