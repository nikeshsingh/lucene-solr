package org.apache.lucene.analysis.pattern;
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
import java.io.StringReader;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;

/**
 *
 */
public class TestPatterCaptureGroupTokenFilter extends BaseTokenStreamTestCase {
  
  public void testSingleTermNonCaptureGroup() throws Exception {
    String input = "foobarb";
    TokenStream ts = new PatterCaptureGroupTokenFilter(new MockTokenizer(
        new StringReader(input), MockTokenizer.WHITESPACE, false),
        Pattern.compile("(foo(bar))(?:b.*)"));
    String[] tokens = new String[] {"foobar", "bar"};
    int[] startOffsets = new int[] {0, 3};
    int[] endOffsets = new int[] {6, 6};
    int[] positions = new int[] {1, 0};
    assertTokenStreamContents(ts, tokens, startOffsets, endOffsets, positions);
  }
  
  public void testSingleTerm() throws Exception {
    String input = "foobarbaz";
    TokenStream ts = new PatterCaptureGroupTokenFilter(new MockTokenizer(
        new StringReader(input), MockTokenizer.WHITESPACE, false),
        Pattern.compile("(foo(bar))(b.*)"));
    String[] tokens = new String[] {"foobar", "bar", "baz"};
    int[] startOffsets = new int[] {0, 3, 6};
    int[] endOffsets = new int[] {6, 6, 9};
    int[] positions = new int[] {1, 0, 0};
    assertTokenStreamContents(ts, tokens, startOffsets, endOffsets, positions);
  }
  
  public void testMultiTerm() throws Exception {
    String input = "foobarbaz foobarbeer foobeer foobeerbuzz";
    TokenStream ts = new PatterCaptureGroupTokenFilter(new MockTokenizer(
        new StringReader(input), MockTokenizer.WHITESPACE, false),
        Pattern.compile("(foo(bar))(b.*)"));
    String[] tokens = new String[] {"foobar", "bar", "baz", "foobar", "bar",
        "beer", "foobeer", "foobeerbuzz"};
    
    int[] startOffsets = new int[] {0, 3, 6, 10, 13, 16, 21, 29};
    int[] endOffsets = new int[] {6, 6, 9, 16, 16, 20, 28, 40};
    int[] positions = new int[] {1, 0, 0, 1, 0, 0, 1, 1};
    assertTokenStreamContents(ts, tokens, startOffsets, endOffsets, positions);
  }
  
  public void testMultiTermNonCaptureGroup() throws Exception {
    String input = "foobarbaz foobarbeer foobeer foobeerbuzz";
    TokenStream ts = new PatterCaptureGroupTokenFilter(new MockTokenizer(
        new StringReader(input), MockTokenizer.WHITESPACE, false),
        Pattern.compile("(foo(bar))(?:b.*)"));
    String[] tokens = new String[] {"foobar", "bar", "foobar", "bar",
        "foobeer", "foobeerbuzz"};
    int[] startOffsets = new int[] {0, 3, 10, 13, 21, 29};
    int[] endOffsets = new int[] {6, 6, 16, 16, 28, 40};
    int[] positions = new int[] {1, 0, 1, 0, 1, 1};
    assertTokenStreamContents(ts, tokens, startOffsets, endOffsets, positions);
  }
}
