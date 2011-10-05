package org.apache.lucene.index.values;

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
import java.util.HashMap;
import java.util.Map;

/**
 * 
 * @lucene.internal
 */
//nocommit - javadoc
public class TypePromoter {
  private static final int IS_INT = 1 << 0;
  private static final int IS_BYTE = 1 << 1;
  private static final int IS_FLOAT = 1 << 2;
  private static final int IS_FIXED = 1 << 3 | 1 << 4;
  private static final int IS_VAR = 1 << 4;
  private static final int IS_STRAIGHT = 1 << 5;
  private static final int IS_DEREF = 1 << 5 | 1 << 6;
  private static final int IS_SORTED = 1 << 7;
  private static final int IS_8_BIT = 1 << 8 | 1 << 9 | 1 << 10 | 1 << 11;
  private static final int IS_16_BIT = 1 << 9 | 1 << 10 | 1 << 11;
  private static final int IS_32_BIT = 1 << 10 | 1 << 11;
  private static final int IS_64_BIT = 1 << 11;
  protected final ValueType type;
  protected final int flags;
  protected int valueSize;

  final static Map<Integer, ValueType> FLAGS_MAP = new HashMap<Integer, ValueType>();

  static {
    for (ValueType type : ValueType.values()) {
      TypePromoter create = create(type);
      FLAGS_MAP.put(create.flags, type);
    }
  }

  public TypePromoter(ValueType type, int flags, int valueSize) {
    super();
    this.type = type;
    this.flags = flags;
    this.valueSize = valueSize;
  }

  public TypePromoter(ValueType type, int flags) {
    this(type, flags, -1);
  }

  public static TypePromoter create(ValueType type) {
    return create(type, -1);
  }

  public static TypePromoter create(ValueType type, int valueSize) {
    if (type == null) {
      return null;
    }
    switch (type) {
    case BYTES_FIXED_DEREF:
      return new TypePromoter(type, IS_BYTE | IS_FIXED | IS_DEREF, valueSize);
    case BYTES_FIXED_SORTED:
      return new TypePromoter(type, IS_BYTE | IS_FIXED | IS_SORTED, valueSize);
    case BYTES_FIXED_STRAIGHT:
      return new TypePromoter(type, IS_BYTE | IS_FIXED | IS_STRAIGHT, valueSize);
    case BYTES_VAR_DEREF:
      return new TypePromoter(type, IS_BYTE | IS_VAR | IS_DEREF);
    case BYTES_VAR_SORTED:
      return new TypePromoter(type, IS_BYTE | IS_VAR | IS_SORTED);
    case BYTES_VAR_STRAIGHT:
      return new TypePromoter(type, IS_BYTE | IS_VAR | IS_STRAIGHT);
    case FIXED_INTS_16:
      return new TypePromoter(type, IS_INT | IS_FIXED | IS_STRAIGHT | IS_16_BIT);
    case FIXED_INTS_32:
      return new TypePromoter(type, IS_INT | IS_FIXED | IS_STRAIGHT | IS_32_BIT);
    case FIXED_INTS_64:
      return new TypePromoter(type, IS_INT | IS_FIXED | IS_STRAIGHT | IS_64_BIT);
    case FIXED_INTS_8:
      return new TypePromoter(type, IS_INT | IS_FIXED | IS_STRAIGHT | IS_8_BIT);
    case FLOAT_32:
      return new TypePromoter(type, IS_FLOAT | IS_FIXED | IS_STRAIGHT
          | IS_32_BIT);
    case FLOAT_64:
      return new TypePromoter(type, IS_FLOAT | IS_FIXED | IS_STRAIGHT
          | IS_64_BIT);
    case VAR_INTS:
      return new TypePromoter(type, IS_INT | IS_VAR | IS_STRAIGHT);
    default:
      throw new IllegalStateException();
    }
  }

  public TypePromoter promote(TypePromoter promoter) {

    int promotedFlags = promoter.flags & this.flags;
    TypePromoter promoted = create(FLAGS_MAP.get(promotedFlags), valueSize);
    if (promoted == null) {
      return promoted;
    }
    if ((promoted.flags & IS_BYTE) != 0 && (promoted.flags & IS_FIXED) != 0) {
      if (this.valueSize == promoter.valueSize) {
        return promoted;
      }
      return create(FLAGS_MAP.get(promoted.flags & ~(1 << 3)));
    }
    return promoted;

  }

  public ValueType type() {
    return type;
  }

  @Override
  public String toString() {
    return "TypePromoter [type=" + type + ", sizeInBytes=" + valueSize + "]";
  }
}