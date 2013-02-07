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
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.CharsRef;

/**
 * TODO
 */
public final class PatterCaptureGroupTokenFilter extends TokenFilter {
  
  private final CharTermAttribute charTermAttr = addAttribute(CharTermAttribute.class);
  private final PositionIncrementAttribute posAttr = addAttribute(PositionIncrementAttribute.class);
  private final OffsetAttribute offsetAttr = addAttribute(OffsetAttribute.class);
  private final Matcher matcher;
  private final CharsRef spare = new CharsRef();
  private int groupCount = -1;
  private int currentGroup = -1;
  private int charOffsetStart;
  
  /**
   * TODO
   * @param input the input {@link TokenStream}
   * @param pattern the pattern to obtain the capturing groups from.
   */
  public PatterCaptureGroupTokenFilter(TokenStream input, Pattern pattern) {
    super(input);
    this.matcher = pattern.matcher("");
  }
  
  @Override
  public boolean incrementToken() throws IOException {
    if (groupCount != -1) {
      for (int i = currentGroup; i < groupCount + 1; i++) {
        final int start = matcher.start(i);
        final int end = matcher.end(i);
        if (start != end) {
          clearAttributes();
          currentGroup = i + 1;
          charTermAttr.copyBuffer(spare.chars, start, end - start);
          posAttr.setPositionIncrement(0);
          offsetAttr.setOffset(charOffsetStart + start, charOffsetStart + end);
          return true;
        }
      }
      groupCount = currentGroup = -1;
    }
    
    if (input.incrementToken()) {
      char[] buffer = charTermAttr.buffer();
      int length = charTermAttr.length();
      spare.copyChars(buffer, 0, length);
      matcher.reset(spare);
      if (matcher.find()) {
        groupCount = matcher.groupCount();
        for (int i = 1; i < groupCount + 1; i++) {
          final int start = matcher.start(i);
          final int end = matcher.end(i);
          if (start != end) {
            currentGroup = i + 1;
            if (start == 0) {
              // if we start at 0 we can simply set the length and safe the copy
              charTermAttr.setLength(end);
            } else {
              charTermAttr.copyBuffer(spare.chars, start, end - start);
            }
            charOffsetStart = offsetAttr.startOffset();
            offsetAttr
                .setOffset(charOffsetStart + start, charOffsetStart + end);
            return true;
          }
        }
        groupCount = currentGroup = -1;
      }
      return true;
    }
    return false;
  }
  
  @Override
  public void reset() throws IOException {
    super.reset();
    groupCount = -1;
    currentGroup = -1;
    matcher.reset("");
  }
}