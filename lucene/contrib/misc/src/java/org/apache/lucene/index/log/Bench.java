package org.apache.lucene.index.log;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.document.Document;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.LineFileDocs;

public class Bench {
  public static void main(String[] args) throws IOException,
      InterruptedException {
    LogFile file = getFile("warmup_batch.log");
    
    WriteStrategy<StringLogRecord> strategy = new QueueBatchWriteStrategy<StringLogRecord>(
        file, new StringRecordSerializer());
//    run(file, strategy, 10000);
//    file.decreaseRefCount(false);
//    System.gc();

    long time = System.currentTimeMillis();
//    file = getFile("batch.log");
//    strategy = new QueueBatchWriteStrategy<StringLogRecord>(file,
//        new StringRecordSerializer());
//    run(file, strategy, 100000);
//    file.decreaseRefCount(false);
//    System.out.println("batching: " + (System.currentTimeMillis() - time));
//    System.gc();

    file = getFile("warmup_concurrent.log");
    strategy = new ConcurrentWriteStrategy<StringLogRecord>(file,
        new StringRecordSerializer());
    run(file, strategy, 10000);
    file.decreaseRefCount(false);

    System.gc();
    time = System.currentTimeMillis();
    file = getFile("concurrent.log");
    strategy = new ConcurrentWriteStrategy<StringLogRecord>(file,
        new StringRecordSerializer());
    run(file, strategy, 100000);
    System.out.println("concurrent: " + (System.currentTimeMillis() - time));
    file.decreaseRefCount(false);

  }

  private static LogFile getFile(String name) throws FileNotFoundException, IOException {
    File physicalFile = new File("/media/benchmark/logging_test/"+ name);
//    File physicalFile = new File("/media/work/logging_test/"+ name);

    if (physicalFile.exists()) {
      physicalFile.delete();
    }
    LogFile file = new LogFile(physicalFile, 1);
    file.preallocate(128);
    return file;
  }

  public static void run(LogFile file, WriteStrategy<StringLogRecord> strategy,
      long times) throws IOException, InterruptedException {
    Random random = new Random(0);
    LineFileDocs docs = new LineFileDocs(random);
    LogThread[] threads = new LogThread[10];
    CountDownLatch latch = new CountDownLatch(1);
    AtomicLong count = new AtomicLong();
    for (int i = 0; i < threads.length; i++) {
      threads[i] = new LogThread(times, strategy, docs, random, latch, count);
      threads[i].start();
    }
    latch.countDown();
    for (int i = 0; i < threads.length; i++) {
      threads[i].join();
    }
    docs.close();
  }

  public static class StringLogRecord extends LogRecord {
    public Document doc;
  }

  public static class StringRecordSerializer extends
      Serializer<StringLogRecord> {

    @Override
    public void write(StringLogRecord record, DataOutput output)
        throws IOException {
      output.writeString(record.doc.toString() + "\n");

    }

    @Override
    public StringLogRecord read(DataInput input) {
      // TODO Auto-generated method stub
      return null;
    }

  }

  public static class LogThread extends Thread {
    public final AtomicLong count;
    public final long times;
    public final WriteStrategy<StringLogRecord> strategy;
    public final LineFileDocs docs;
    public final Random random;
    private CountDownLatch latch;

    LogThread(long times, WriteStrategy<StringLogRecord> strategy,
        LineFileDocs docs, Random random, CountDownLatch latch, AtomicLong count) {
      super();
      this.times = times;
      this.strategy = strategy;
      this.docs = docs;
      
      this.random = random;
      this.latch = latch;
      this.count = count;
    }

    @Override
    public void run() {
      try {
        StringLogRecord record = new StringLogRecord();
        record.doc = docs.nextDoc();
        latch.await();
        for (; count.incrementAndGet() < times;) {
          strategy.add(record, random.nextInt(10) == 0, null);
        }
      } catch (Exception e) {
        e.printStackTrace()
        ;
      }
    }

  }
}
