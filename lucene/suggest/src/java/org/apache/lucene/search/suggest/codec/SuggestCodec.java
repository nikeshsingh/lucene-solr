package org.apache.lucene.search.suggest.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.LiveDocsFormat;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.TermVectorsFormat;
import org.apache.lucene.codecs.lucene40.Lucene40DocValuesFormat;
import org.apache.lucene.codecs.lucene40.Lucene40FieldInfosFormat;
import org.apache.lucene.codecs.lucene40.Lucene40LiveDocsFormat;
import org.apache.lucene.codecs.lucene40.Lucene40NormsFormat;
import org.apache.lucene.codecs.lucene40.Lucene40SegmentInfoFormat;

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

public class SuggestCodec extends Codec {
  private final FieldInfosFormat fieldInfosFormat = new Lucene40FieldInfosFormat();
  private final SegmentInfoFormat infosFormat = new Lucene40SegmentInfoFormat();
  private final LiveDocsFormat liveDocsFormat = new Lucene40LiveDocsFormat();

  public SuggestCodec() {
    super("suggest");
  }

  @Override
  public PostingsFormat postingsFormat() {
    return null;
  }
  
  @Override
  public DocValuesFormat docValuesFormat() {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public StoredFieldsFormat storedFieldsFormat() {
    throw new UnsupportedOperationException();  }
  
  @Override
  public TermVectorsFormat termVectorsFormat() {
    throw new UnsupportedOperationException();  }
  
  @Override
  public FieldInfosFormat fieldInfosFormat() {
    return fieldInfosFormat;
  }
  
  @Override
  public SegmentInfoFormat segmentInfoFormat() {
    return infosFormat;
  }
  
  @Override
  public NormsFormat normsFormat() {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public LiveDocsFormat liveDocsFormat() {
    return liveDocsFormat;
  }
  
}
