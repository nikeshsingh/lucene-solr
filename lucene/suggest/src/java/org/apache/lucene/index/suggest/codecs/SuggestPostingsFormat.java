package org.apache.lucene.index.suggest.codecs;

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
import java.util.Comparator;
import java.util.Iterator;
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
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.suggest.fst.WFSTCompletionLookup;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.BytesRefFSTEnum;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;
import org.apache.lucene.util.packed.PackedInts;

public class SuggestPostingsFormat extends PostingsFormat {

  private final boolean doPackFST;
  private final float acceptableOverheadRatio;
  private final TermWeightProcessor processor;
  
  public SuggestPostingsFormat() {
    this(new WFSTCompletionLookup.WFSTSuggestTermProcessor()); // nocommit 
  }

  public SuggestPostingsFormat(TermWeightProcessor processor) {
    this(false, PackedInts.DEFAULT, processor);
  }

  public SuggestPostingsFormat(boolean doPackFST, float acceptableOverheadRatio, TermWeightProcessor processor) {
    super("Suggest");
    this.doPackFST = doPackFST;
    this.acceptableOverheadRatio = acceptableOverheadRatio;
    this.processor = processor;
  }
  
  @Override
  public String toString() {
    return "PostingsFormat(name=" + getName() + " doPackFST= " + doPackFST + ")";
  }

  private final static class TermsWriter extends TermsConsumer {
    private final IndexOutput fstOut;
    private final IndexOutput postingsOut;
    private final FieldInfo field;
    private final Builder<Long> builder;
    private final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton(true);

    private final boolean doPackFST;
    private final float acceptableOverheadRatio;
    private int termCount;
    private final TermWeightProcessor processor;

    public TermsWriter(IndexOutput fstOut, IndexOutput postingsOut, FieldInfo field, boolean doPackFST, float acceptableOverheadRatio, TermWeightProcessor processor) {
      this.fstOut = fstOut;
      this.postingsOut = postingsOut;
      this.field = field;
      this.doPackFST = doPackFST;
      this.acceptableOverheadRatio = acceptableOverheadRatio;
      this.processor = processor;
      builder = new Builder<Long>(FST.INPUT_TYPE.BYTE1, 0, 0, true, true, Integer.MAX_VALUE, outputs, null, doPackFST, acceptableOverheadRatio);
    }
    

    private class PostingsWriter extends PostingsConsumer {
      private int lastDocID;

      // NOTE: not private so we don't pay access check at runtime:
      int docCount;
      RAMOutputStream buffer = new RAMOutputStream();
      

      @Override
      public void startDoc(int docID, int termDocFreq) throws IOException {
        //System.out.println("    startDoc docID=" + docID + " freq=" + termDocFreq);
        final int delta = docID - lastDocID;
        assert docID == 0 || delta > 0;
        lastDocID = docID;
        docCount++;
        buffer.writeVInt(delta);
      }

      @Override
      public void addPosition(int pos, BytesRef payload, int startOffset, int endOffset) throws IOException {
        throw new UnsupportedOperationException("positions are not supporte");
      }

      @Override
      public void finishDoc() {
      }

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
      //System.out.println("  startTerm term=" + text.utf8ToString());
      return postingsWriter.reset();
    }

    private final RAMOutputStream buffer2 = new RAMOutputStream();
    private final BytesRef previousTerm = new BytesRef();
    private final BytesRef spare = new BytesRef();
    private byte[] finalBuffer = new byte[128];

    private final IntsRef scratchIntsRef = new IntsRef();

    
    @Override
    public void finishTerm(BytesRef text, TermStats stats) throws IOException {
      spare.copyBytes(text);
      Long weight = processor.spit(spare);
      //System.out.println("add term: " + spare.utf8ToString() + " weight:" + weight);
       
      if (termCount > 0 && text.bytesEquals(previousTerm)) {
        buffer2.reset();
        postingsWriter.buffer.reset();
        return;
      }
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

      //System.out.println("    finishTerm term=" + text.utf8ToString() + " " + totalBytes + " bytes totalTF=" + stats.totalTermFreq);
      //for(int i=0;i<totalBytes;i++) {
      //  System.out.println("      " + Integer.toHexString(finalBuffer[i]&0xFF));
      //}
      
      builder.add(Util.toIntsRef(spare, scratchIntsRef), weight);
      termCount++;
      previousTerm.copyBytes(spare);
    }

    @Override
    public void finish(long sumTotalTermFreq, long sumDocFreq, int docCount) throws IOException {
      if (termCount > 0) {
        fstOut.writeVInt(termCount);
        fstOut.writeVInt(field.number);
        fstOut.writeVLong(sumDocFreq);
        fstOut.writeVInt(docCount);
        FST<Long> fst = builder.finish();
        if (doPackFST) {
          fst = fst.pack(3, Math.max(10, fst.getNodeCount()/4), acceptableOverheadRatio);
        }
        fst.save(fstOut);
        //System.out.println("finish field=" + field.name + " fp=" + out.getFilePointer());
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
  public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {

    final String fstFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, EXTENSION_FST);
    final String postingsFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, EXTENSION_FRQ);

    final IndexOutput fstOut = state.directory.createOutput(fstFileName, state.context);
    final IndexOutput postingsOut = state.directory.createOutput(postingsFileName, state.context);

    
    return new FieldsConsumer() {
      @Override
      public TermsConsumer addField(FieldInfo field) {
        //System.out.println("\naddField field=" + field.name);
        return new TermsWriter(fstOut, postingsOut, field, doPackFST, acceptableOverheadRatio, processor);
      }

      @Override
      public void close() throws IOException {
        // EOF marker:
        try {
          fstOut.writeVInt(0);
        } finally {
          IOUtils.close(fstOut, postingsOut);
        }
      }
    };
  }

  private final static class FSTDocsEnum extends DocsEnum {
    private byte[] buffer = new byte[16];

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
    
    public FSTDocsEnum reset(IndexInput input, int ord, Bits liveDocs, int numDocs) throws IOException {
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
      while(true) {
        //System.out.println("  nextDoc cycle docUpto=" + docUpto + " numDocs=" + numDocs + " fp=" + input.getFilePointer() + " this=" + this);
        if (docUpto == numDocs) {
          // System.out.println("    END");
          return docID = NO_MORE_DOCS;
        }
        docUpto++;
        accum += input.readVInt();

        if (liveDocs == null || liveDocs.get(accum)) {
          //System.out.println("    return docID=" + accum);
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
      //System.out.println("advance start docID=" + docID + " target=" + target);
      while(nextDoc() < target) {
      }
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
      
      while(nextDoc() < NO_MORE_DOCS) {
      }
      //System.out.println("  nextDocFreq cycle docUpto=" + docUpto + " numDocs=" + numDocs + " fp=" + input.getFilePointer() + " this=" + this);
      int docFreq = input.readVInt();
      assert docFreq > 0;
      return docFreq;
    }
  }

 
  private final static class FSTTermsEnum extends TermsEnum  {
    private final BytesRefFSTEnum<Long> fstEnum;
    private final BytesRef spare = new BytesRef();
    private int docFreq;
    private BytesRefFSTEnum.InputOutput<Long> current;
    private final TermWeightProcessor processor;
    private final IndexInput postingsInput;
    private final FSTDocsEnum internalEnum;
    private int ord = -1;

    public FSTTermsEnum(FieldInfo field, FST<Long> fst, IndexInput postingsInput, TermWeightProcessor processor) {
      fstEnum = new BytesRefFSTEnum<Long>(fst);
      this.processor = processor;
      this.postingsInput = postingsInput;
      internalEnum = new FSTDocsEnum(postingsInput); 
    }


    @Override
    public boolean seekExact(BytesRef text, boolean useCache /* ignored */) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public SeekStatus seekCeil(BytesRef text, boolean useCache /* ignored */) throws IOException {
      throw new UnsupportedOperationException();
    }
    
    @Override
    public DocsEnum docs(Bits liveDocs, DocsEnum reuse, boolean needsFreqs) throws IOException {
     // System.out.println("DocsEnum for term: " + current.input.utf8ToString());
      return internalEnum.reset(postingsInput, ord, liveDocs, docFreq); 
    }
    
    private int readDocFreq() throws IOException {
      return internalEnum.nextDocFreq();
    }

    @Override
    public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, boolean needsOffsets) throws IOException {
       return null;
    }

    @Override
    public BytesRef term() {
      return processor.combine(current.input, spare, current.output);
    }

    @Override
    public BytesRef next() throws IOException {
      //System.out.println("te.next");
      current = fstEnum.next();
      if (current == null) {
        //System.out.println("  END");
        return null;
      }
//      System.out.println(current.input.utf8ToString() + " " + current.output);
      ord++;
      docFreq = readDocFreq();
      //System.out.println("  term=" + field.name + ":" + current.input.utf8ToString());
      return processor.combine(current.input, spare, current.output);
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

  private final static class TermsReader extends Terms implements ToFST {

    private final long sumTotalTermFreq;
    private final long sumDocFreq;
    private final int docCount;
    private final int termCount;
    private FST<Long> fst;
    private final PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton(false);
    private final FieldInfo field;
    private final IndexInput postingsInput;
    private final TermWeightProcessor processor;

    public TermsReader(FieldInfos fieldInfos, IndexInput fstInput, IndexInput postingsInput, int termCount, TermWeightProcessor processor) throws IOException {
      this.termCount = termCount;
      final int fieldNumber = fstInput.readVInt();
      field = fieldInfos.fieldInfo(fieldNumber);
      sumTotalTermFreq = -1;
      sumDocFreq = fstInput.readVLong();
      docCount = fstInput.readVInt();
      this.processor = processor;
      
      fst = new FST<Long>(fstInput, outputs);
      this.postingsInput = postingsInput;
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
    public TermsEnum iterator(TermsEnum reuse) {
      // nocommit reuse?
      return new FSTTermsEnum(field, fst, (IndexInput)postingsInput.clone(), processor );
    }

    @Override
    public Comparator<BytesRef> getComparator() {
      return BytesRef.getUTF8SortedAsUnicodeComparator();
    }

    @Override
    public FST<Long> get() {
      return fst;
    }
  }

  @Override
  public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
    final String fileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, EXTENSION_FST);
    final String fileNamePostings = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, EXTENSION_FRQ);
    final IndexInput in = state.dir.openInput(fileName, IOContext.READONCE);
    final IndexInput postingsInput = state.dir.openInput(fileNamePostings, IOContext.READ);

    final SortedMap<String,TermsReader> fields = new TreeMap<String,TermsReader>();

    try {
      while(true) {
        final int termCount = in.readVInt();
        if (termCount == 0) {
          break;
        }
        final TermsReader termsReader = new TermsReader(state.fieldInfos, in, postingsInput, termCount, processor);
        // System.out.println("load field=" + termsReader.field.name);
        fields.put(termsReader.field.name, termsReader);
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
        // Drop ref to FST:
        for(TermsReader termsReader : fields.values()) {
          termsReader.fst = null;
        }
      }
    };
  }
}
