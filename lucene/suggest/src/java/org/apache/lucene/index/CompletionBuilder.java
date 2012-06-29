package org.apache.lucene.index;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene40.Lucene40Codec;
import org.apache.lucene.codecs.suggest.SuggestFSTBuilder;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.fst.WFSTCompletionLookup;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.fst.FST;

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

public abstract class CompletionBuilder {
  
  protected static final String DEFAULT_FIELD = "suggest";
  private final IndexWriter writer;
  private static final FieldType TYPE;
  private final SuggestTermTokenStream stream;// nocommit - threadlocal for
                                              // concurrency?
  private final BytesRef scratch = new BytesRef();
  private String field;
  
  static {
    TYPE = new FieldType();
    TYPE.setIndexed(true);
    TYPE.setIndexOptions(IndexOptions.DOCS_ONLY);
    TYPE.setOmitNorms(true);
    TYPE.setStored(false);
    TYPE.setTokenized(true);
    TYPE.freeze();
  }
  
  public CompletionBuilder(IndexWriterConfig config, Directory dir)
      throws CorruptIndexException, LockObtainFailedException, IOException {
    config.setMaxBufferedDocs(2); // NOCOMMIT
    config.setCodec(new Lucene40Codec() {
      
      @Override
      public PostingsFormat getPostingsFormatForField(String field) {
        if (field.equals(DEFAULT_FIELD)) {
          return newSuggestPostingsFormat();
        }
        return super.getPostingsFormatForField(field);
      }
      
    });
    writer = new IndexWriter(dir, config);
    stream = new SuggestTermTokenStream(newFSTBuilder());
    field = DEFAULT_FIELD;
  }
  
  protected abstract SuggestFSTBuilder<Long> newFSTBuilder();
  
  protected abstract PostingsFormat newSuggestPostingsFormat();
  
  public CompletionBuilder(IndexWriter writer, String field) {
    this.writer = writer;
    this.field = field;
    stream = new SuggestTermTokenStream(newFSTBuilder());
  }
  
  public void add(BytesRef ref, long weight) throws CorruptIndexException,
      IOException {
    Field field = new Field(this.field, stream, TYPE);
    Document doc = new Document();
    doc.add(field);
    scratch.copyBytes(ref);
    stream.set(scratch, encodeWeight(weight));
    writer.addDocument(doc);
  }
  
  public FST<Long> build() throws CorruptIndexException, IOException {
    DirectoryReader reader = null;
    try {
    writer.forceMerge(1); // nocommit maybe not needed in the future?
    writer.commit();
    reader = DirectoryReader.open(writer, false);
    List<? extends AtomicReader> sequentialSubReaders = reader.getSequentialSubReaders();
    assert sequentialSubReaders.size() == 1;
    Terms terms = sequentialSubReaders.get(0).terms(getSuggestField());
    @SuppressWarnings("unchecked")
    FSTIterator<Long> iter = ((ToFST<Long>)terms).getIterator();
    FST<Long> fst = iter.next();
    assert iter.next() == null;
    return fst;
    } finally {
      IOUtils.close(reader, writer);
    }
  }
  
  protected abstract long encodeWeight(long input);
  
  protected String getSuggestField() {
    return field;
  }
}
