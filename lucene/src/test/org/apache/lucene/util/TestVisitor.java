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

import org.apache.lucene.util.InvocationDispatcher.AmbigousMethodException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestVisitor {

  private interface TopLevel {}
  private interface A extends TopLevel {}
  private interface B extends TopLevel {}
  private class AImpl implements A {}
  private class ABImpl implements A, B {}

  @Test
  public void testVisitorSimpleDispatch() {
    class AVisitor extends Visitor<A, String> {

      AVisitor() {
        super(AVisitor.class, A.class, String.class);
      }

      public String visit(A a) {
        return "value";
      }
    }

    Visitor<A, String> visitor = new AVisitor();
    String visitorOutput = visitor.apply(new AImpl());
    assertEquals("value", visitorOutput);
  }

  @Test
  public void testVisitorMultipleMethodsOnlyOneDispatchable() {
    class AVisitor extends Visitor<A, String> {

      AVisitor() {
        super(AVisitor.class, A.class, String.class);
      }

      public String visit(A a) {
        return "Visited A";
      }

      public String visit(B b) {
        return "Visited B";
      }
    }

    Visitor<A, String> visitor = new AVisitor();
    String visitorOutput = visitor.apply(new AImpl());
    assertEquals("Visited A", visitorOutput);
  }

  @Test(expected = AmbigousMethodException.class)
  public void testVisitorMultipleDispatchableMethods() {
    class TopLevelVisitor extends Visitor<TopLevel, String> {

      TopLevelVisitor() {
        super(TopLevelVisitor.class, TopLevel.class, String.class);
      }

      public String visit(A a) {
        return "Visited A";
      }

      public String visit(B b) {
        return "Visited B";
      }
    }

    Visitor<TopLevel, String> visitor = new TopLevelVisitor();
    visitor.apply(new ABImpl());
  }

  @Test
  public void testVisitorCatchAll() {
    class TopLevelVisitor extends Visitor<TopLevel, String> {

      TopLevelVisitor() {
        super(TopLevelVisitor.class, TopLevel.class, String.class);
      }

      String visit(B b) {
        return "Visited B";
      }

      protected String visit(TopLevel topLevel) {
        return "Visited Catchall";
      }
    }

    Visitor<TopLevel, String> visitor = new TopLevelVisitor();
    String visitorOutput = visitor.apply(new AImpl());
    assertEquals("Visited Catchall", visitorOutput);
  }
}
