package org.apache.lucene.codecs.suggest;

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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsConsumer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.codecs.TermsConsumer;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FSTIterator;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.ToFST;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.fst.BytesRefFSTEnum;
import org.apache.lucene.util.fst.FST;

public abstract class SuggestPostingsFormat<T> extends PostingsFormat {
  
  protected SuggestPostingsFormat(String name) {
    super(name);
  }
  
  public abstract SuggestFSTBuilder<T> newBuilder();
  
  @Override
  public String toString() {
    return "SuggestPostingsFormat(name=" + getName() + ")";
  }
  
  private final class TermsWriter extends TermsConsumer {
    private final IndexOutput fstOut;
    private final IndexOutput postingsOut;
    private final FieldInfo field;
    private final List<BytesRef> bounds = new ArrayList<BytesRef>();
    private final long fstOutOffset;
    private final SuggestFSTBuilder<T> processor;
    
    public TermsWriter(IndexOutput fstOut, IndexOutput postingsOut,
        FieldInfo field) throws IOException {
      this.fstOut = fstOut;
      fstOutOffset = fstOut.getFilePointer();
      fstOut.seek(fstOutOffset + 36);
      
      this.postingsOut = postingsOut;
      this.field = field;
      this.processor = newBuilder();
    }
    
    private class PostingsWriter extends PostingsConsumer {
      private int lastDocID;
      
      // NOTE: not private so we don't pay access check at runtime:
      int docCount;
      RAMOutputStream buffer = new RAMOutputStream();
      
      @Override
      public void startDoc(int docID, int termDocFreq) throws IOException {
        // System.out.println("    startDoc docID=" + docID + " freq=" +
        // termDocFreq);
        final int delta = docID - lastDocID;
        assert docID == 0 || delta > 0;
        lastDocID = docID;
        docCount++;
        buffer.writeVInt(delta);
      }
      
      @Override
      public void addPosition(int pos, BytesRef payload, int startOffset,
          int endOffset) throws IOException {
        throw new UnsupportedOperationException("positions are not supporte");
      }
      
      @Override
      public void finishDoc() {}
      
      public PostingsWriter reset() {
        assert buffer.getFilePointer() == 0;
        lastDocID = 0;
        docCount = 0;
        // force first offset to write its length
        return this;
      }
    }
    
    private final PostingsWriter postingsWriter = new PostingsWriter();
    
    @Override
    public PostingsConsumer startTerm(BytesRef text) {
      // System.out.println("  startTerm term=" + text.utf8ToString());
      return postingsWriter.reset();
    }
    
    private final RAMOutputStream buffer2 = new RAMOutputStream();
    private final BytesRef previousTerm = new BytesRef();
    private final BytesRef spare = new BytesRef();
    private byte[] finalBuffer = new byte[128];
    private int writtenFSTs = 0;
    
    @Override
    public void finishTerm(BytesRef text, TermStats stats) throws IOException {
      spare.copyBytes(text);
      switch (processor.add(spare)) {
        case Added:
          finishTermInternal(stats);
          break;
        case Duplicate:
          buffer2.reset();
          postingsWriter.buffer.reset();
          return;
        case MustSerialize:
          finishTermInternal(stats);
          writeCurrentFST();
          break;
        
      }
      
      // System.out.println("add term: " + spare.utf8ToString() + " weight:" +
      // weight);
      
    }
    
    public void finishTermInternal(TermStats stats) throws IOException {
      assert postingsWriter.docCount == stats.docFreq;
      
      assert buffer2.getFilePointer() == 0;
      
      assert stats.docFreq > 0;
      buffer2.writeVInt(stats.docFreq);
      int pos = (int) buffer2.getFilePointer();
      buffer2.writeTo(finalBuffer, 0);
      buffer2.reset();
      final int totalBytes = pos + (int) postingsWriter.buffer.getFilePointer();
      if (totalBytes > finalBuffer.length) {
        finalBuffer = ArrayUtil.grow(finalBuffer, totalBytes);
      }
      postingsWriter.buffer.writeTo(finalBuffer, pos);
      postingsWriter.buffer.reset();
      postingsOut.writeBytes(finalBuffer, totalBytes);
      previousTerm.copyBytes(spare);
    }
    
    private void writeCurrentFST() throws IOException {
      BytesRef upper = new BytesRef();
      BytesRef lower = new BytesRef();
      FST<Long> fst = processor.finishCurrentFST(upper, lower);
      if (fst != null) {
        writtenFSTs++;
        bounds.add(upper);
        bounds.add(lower);
        assert (bounds.size() % 2 == 0);
        writePrefixBytes(upper, fstOut);
        writePrefixBytes(lower, fstOut);
        fst.save(fstOut);
      }
      
    }
    
    private void writePrefixBytes(BytesRef ref, IndexOutput out)
        throws IOException {
      out.writeVInt(ref.length);
      out.writeBytes(ref.bytes, ref.offset, ref.length);
    }
    
    @Override
    public void finish(long sumTotalTermFreq, long sumDocFreq, int docCount)
        throws IOException {
      
      if (processor.totalTermCount() > 0) {
        writeCurrentFST();
        long filePointer = fstOut.getFilePointer();
        fstOut.seek(fstOutOffset);
        fstOut.writeLong(processor.totalTermCount());
        fstOut.writeInt(field.number);
        fstOut.writeLong(sumDocFreq);
        fstOut.writeInt(docCount);
        fstOut.writeInt(writtenFSTs);
        fstOut.writeLong(filePointer);
        fstOut.seek(filePointer);
      } 
    }
    
    @Override
    public Comparator<BytesRef> getComparator() {
      return BytesRef.getUTF8SortedAsUnicodeComparator();
    }
  }
  
  private static String EXTENSION_FST = "fst";
  private static String EXTENSION_FRQ = "frq";
  
  @Override
  public FieldsConsumer fieldsConsumer(SegmentWriteState state)
      throws IOException {
    
    final String fstFileName = IndexFileNames.segmentFileName(
        state.segmentInfo.name, state.segmentSuffix, EXTENSION_FST);
    final String postingsFileName = IndexFileNames.segmentFileName(
        state.segmentInfo.name, state.segmentSuffix, EXTENSION_FRQ);
    
    final IndexOutput fstOut = state.directory.createOutput(fstFileName,
        state.context);
    final IndexOutput postingsOut = state.directory.createOutput(
        postingsFileName, state.context);
    
    return new FieldsConsumer() {
      @Override
      public TermsConsumer addField(FieldInfo field) throws IOException {
        // System.out.println("\naddField field=" + field.name);
        return new TermsWriter(fstOut, postingsOut, field);
      }
      
      @Override
      public void close() throws IOException {
        // EOF marker:
        try {
          fstOut.writeLong(0);
        } finally {
          IOUtils.close(fstOut, postingsOut);
        }
      }
    };
  }
  
  private final static class FSTDocsEnum extends DocsEnum {
    private Bits liveDocs;
    private int docUpto;
    private int docID = -1;
    private int accum;
    private int currentOrd = 0;
    private IndexInput input;
    private int numDocs;
    
    public FSTDocsEnum(IndexInput input) {
      this.input = input;
      currentOrd = -1;
    }
    
    public FSTDocsEnum reset(IndexInput input, int ord, Bits liveDocs,
        int numDocs) throws IOException {
      assert numDocs > 0;
      this.liveDocs = liveDocs;
      docID = -1;
      accum = 0;
      docUpto = 0;
      this.numDocs = numDocs;
      this.input = input;
      assert currentOrd == ord;
      return this;
    }
    
    @Override
    public int nextDoc() throws IOException {
      while (true) {
        // System.out.println("  nextDoc cycle docUpto=" + docUpto + " numDocs="
        // + numDocs + " fp=" + input.getFilePointer() + " this=" + this);
        if (docUpto == numDocs) {
          // System.out.println("    END");
          return docID = NO_MORE_DOCS;
        }
        docUpto++;
        accum += input.readVInt();
        
        if (liveDocs == null || liveDocs.get(accum)) {
          // System.out.println("    return docID=" + accum);
          return (docID = accum);
        }
      }
    }
    
    @Override
    public int docID() {
      return docID;
    }
    
    @Override
    public int advance(int target) throws IOException {
      // TODO: we could make more efficient version, but, it
      // should be rare that this will matter in practice
      // since usually apps will not store "big" fields in
      // this codec!
      // System.out.println("advance start docID=" + docID + " target=" +
      // target);
      while (nextDoc() < target) {}
      return docID;
    }
    
    @Override
    public int freq() throws IOException {
      return -1;
    }
    
    public int nextDocFreq() throws IOException {
      currentOrd++;
      if (currentOrd == 0) {
        return input.readVInt();
      }
      
      while (nextDoc() < NO_MORE_DOCS) {}
      // System.out.println("  nextDocFreq cycle docUpto=" + docUpto +
      // " numDocs=" + numDocs + " fp=" + input.getFilePointer() + " this=" +
      // this);
      int docFreq = input.readVInt();
      assert docFreq > 0;
      return docFreq;
    }
  }
  
  private final static class FSTTermsEnum<T> extends TermsEnum {
    private BytesRefFSTEnum<T> fstEnum;
    private final BytesRef spare = new BytesRef();
    private int docFreq;
    private BytesRefFSTEnum.InputOutput<T> current;
    private final SuggestFSTBuilder<T> processor;
    private final IndexInput postingsInput;
    private final FSTDocsEnum internalEnum;
    private int ord = -1;
    private final FSTIterator<T> fstIterator;
    
    public FSTTermsEnum(FieldInfo field, FSTIterator<T> fstIterator, IndexInput postingsInput,
        SuggestFSTBuilder<T> processor) throws IOException {
      
      fstEnum = processor.openEnum(fstIterator.next());
      this.fstIterator = fstIterator;
      this.processor = processor;
      this.postingsInput = postingsInput;
      internalEnum = new FSTDocsEnum(postingsInput);
    }
    
    @Override
    public boolean seekExact(BytesRef text, boolean useCache /* ignored */)
        throws IOException {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public SeekStatus seekCeil(BytesRef text, boolean useCache /* ignored */)
        throws IOException {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public DocsEnum docs(Bits liveDocs, DocsEnum reuse, boolean needsFreqs)
        throws IOException {
      // System.out.println("DocsEnum for term: " +
      // current.input.utf8ToString());
      return internalEnum.reset(postingsInput, ord, liveDocs, docFreq);
    }
    
    private int readDocFreq() throws IOException {
      return internalEnum.nextDocFreq();
    }
    
    @Override
    public DocsAndPositionsEnum docsAndPositions(Bits liveDocs,
        DocsAndPositionsEnum reuse, boolean needsOffsets) throws IOException {
      return null;
    }
    
    @Override
    public BytesRef term() {
      return processor.combine(spare, current);
    }
    
    @Override
    public BytesRef next() throws IOException {
      // System.out.println("te.next");
      current = fstEnum.next();
      if (current == null) {
        FST<T> next;
        if ((next = fstIterator.next()) != null) {
          fstEnum = processor.openEnum(next);
        } else {
          // System.out.println("  END");
          return null;
        }
      }
      // System.out.println(current.input.utf8ToString() + " " +
      // current.output);
      ord++;
      docFreq = readDocFreq();
      // System.out.println("  term=" + field.name + ":" +
      // current.input.utf8ToString());
      return processor.combine(spare, current);
    }
    
    @Override
    public int docFreq() throws IOException {
      return docFreq;
    }
    
    @Override
    public long totalTermFreq() throws IOException {
      return -1;
    }
    
    @Override
    public Comparator<BytesRef> getComparator() {
      return BytesRef.getUTF8SortedAsUnicodeComparator();
    }
    
    @Override
    public void seekExact(long ord) {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public long ord() {
      throw new UnsupportedOperationException();
    }
  }
  
  private final class TermsReader extends Terms implements ToFST<T> {
    
    private final long sumTotalTermFreq;
    private final long sumDocFreq;
    private final int docCount;
    private final long termCount;
    private final FieldInfo field;
    private final IndexInput postingsInput;
    private final IndexInput fstInput;
    private final SuggestFSTBuilder<T> processor;
    private int numFSTs;
    private final long fstOffset;
    private final SuggestFSTIterator iterator;
    private final long afterFSTOffset;
    
    public TermsReader(FieldInfos fieldInfos, IndexInput fstInput,
        IndexInput postingsInput, long termCount) throws IOException {
      this.termCount = termCount;
      final int fieldNumber = fstInput.readInt();
      field = fieldInfos.fieldInfo(fieldNumber);
      sumTotalTermFreq = -1;
      sumDocFreq = fstInput.readLong();
      docCount = fstInput.readInt();
      numFSTs = fstInput.readInt();
      afterFSTOffset = fstInput.readLong();
      this.processor = newBuilder();
      this.postingsInput = postingsInput;
      this.fstInput = fstInput;
      fstOffset = fstInput.getFilePointer();
      this.iterator = new SuggestFSTIterator(fstInput, numFSTs, processor);
    }
    
    @Override
    public long getSumTotalTermFreq() {
      return sumTotalTermFreq;
    }
    
    @Override
    public long getSumDocFreq() throws IOException {
      return sumDocFreq;
    }
    
    @Override
    public int getDocCount() throws IOException {
      return docCount;
    }
    
    @Override
    public long size() throws IOException {
      return termCount;
    }
    
    @Override
    public TermsEnum iterator(TermsEnum reuse) throws IOException {
      // nocommit reuse?
      return new FSTTermsEnum<T>(field, iterator,
          (IndexInput) postingsInput.clone(), processor);
    }
    
    @Override
    public Comparator<BytesRef> getComparator() {
      return BytesRef.getUTF8SortedAsUnicodeComparator();
    }
    
    @Override
    public FSTIterator<T> getIterator() throws IOException{
      IndexInput clone = (IndexInput) fstInput.clone();
      clone.seek(fstOffset);
      return new SuggestFSTIterator(clone, numFSTs, processor);
    }
  }
  
  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state)
      throws IOException {
    final String fileName = IndexFileNames.segmentFileName(
        state.segmentInfo.name, state.segmentSuffix, EXTENSION_FST);
    final String fileNamePostings = IndexFileNames.segmentFileName(
        state.segmentInfo.name, state.segmentSuffix, EXTENSION_FRQ);
    final IndexInput in = state.dir.openInput(fileName, IOContext.READONCE);
    final IndexInput postingsInput = state.dir.openInput(fileNamePostings,
        IOContext.READ);
    
    final SortedMap<String,TermsReader> fields = new TreeMap<String,TermsReader>();
    
    try {
      while (true) {
        final long termCount = in.readLong();
        if (termCount == 0) {
          break;
        }
        final TermsReader termsReader = new TermsReader(state.fieldInfos, (IndexInput) in.clone(),
            postingsInput, termCount);
        // System.out.println("load field=" + termsReader.field.name);
        fields.put(termsReader.field.name, termsReader);
        in.seek(termsReader.afterFSTOffset);
      }
    } finally {
      in.close();
    }
    
    return new FieldsProducer() {
      @Override
      public FieldsEnum iterator() {
        final Iterator<TermsReader> iter = fields.values().iterator();
        
        return new FieldsEnum() {
          
          private TermsReader current;
          
          @Override
          public String next() {
            current = iter.next();
            return current.field.name;
          }
          
          @Override
          public Terms terms() {
            return current;
          }
        };
      }
      
      @Override
      public Terms terms(String field) {
        return fields.get(field);
      }
      
      @Override
      public int size() {
        return fields.size();
      }
      
      @Override
      public void close() {
      }
    };
  }
  
  private class SuggestFSTIterator extends FSTIterator<T> {
    private final BytesRef upper = new BytesRef();
    private final BytesRef lower = new BytesRef();
    private DataInput input;
    private final int size;
    private int currentFST = 0;
    private final SuggestFSTBuilder<T> processor;
    
    SuggestFSTIterator(DataInput input, int numFsts,
        SuggestFSTBuilder<T> processor) {
      this.input = input;
      this.size = numFsts;
      this.processor = processor;
    }
    
    @Override
    public FST<T> next() throws IOException {
      if (currentFST++ < size) {
        fillPrefix(upper);
        fillPrefix(lower);
        return processor.load(input);
      }
      return null;
    }
    
    private final void fillPrefix(BytesRef ref) throws IOException {
      int len = input.readVInt();
      ref.grow(len);
      input.readBytes(ref.bytes, ref.offset, len);
      ref.length = len;
    }
    
    @Override
    public void fillUpper(BytesRef bytesRef) {
      bytesRef.copyBytes(upper);
    }
    
    @Override
    public void fillLower(BytesRef bytesRef) {
      bytesRef.copyBytes(lower);
      
    }
    
  }
}
