package org.apache.solr.update;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.FastInputStream;
import org.apache.solr.common.util.FastOutputStream;
import org.apache.solr.common.util.JavaBinCodec;

public class SolrTransactionLogCodec
    extends
    TransactionLogCodec<AddUpdateCommand, DeleteUpdateCommand, CommitUpdateCommand, DeleteUpdateCommand> {
  public final static String END_MESSAGE = "SOLR_TLOG_END";
  FastOutputStream fos;
  OutputStream os;
  List<String> globalStringList = new ArrayList<String>();
  Map<String, Integer> globalStringMap = new HashMap<String, Integer>();
  private TransactionLogFileHandle handle;

  public SolrTransactionLogCodec(TransactionLogFileHandle handle,
      Collection<String> globalStrings) {
    try {
      this.handle = handle;
      long start = handle.getLength();
      assert start == 0;
      if (start > 0) {
        handle.reset();
      }
      // System.out.println("###start= "+start);
      os = Channels.newOutputStream(handle.getChannel());
      fos = FastOutputStream.wrap(os);
      addGlobalStrings(globalStrings);
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  Collection<String> getGlobalStrings() {
    synchronized (fos) {
      return new ArrayList<String>(globalStringList);
    }
  }

  @Override
  public long writeAdd(AddUpdateCommand cmd) {
    LogCodec codec = new LogCodec();
    synchronized (fos) {
      try {
        long pos = fos.size(); // if we had flushed, this should be equal to
                               // channel.position()
        SolrInputDocument sdoc = cmd.getSolrInputDocument();

        if (pos == 0) { // TODO: needs to be changed if we start writing a
                        // header first
          addGlobalStrings(sdoc.getFieldNames());
          writeLogHeader(codec);
          pos = fos.size();
        }

        /***
         * System.out.println("###writing at " + pos + " fos.size()=" +
         * fos.size() + " raf.length()=" + raf.length()); if (pos != fos.size())
         * { throw new RuntimeException("ERROR" + "###writing at " + pos +
         * " fos.size()=" + fos.size() + " raf.length()=" + raf.length()); }
         ***/

        codec.init(fos);
        codec.writeTag(JavaBinCodec.ARR, 3);
        codec.writeInt(UpdateLog.ADD); // should just take one byte
        codec.writeLong(cmd.getVersion());
        codec.writeSolrInputDocument(cmd.getSolrInputDocument());
        // fos.flushBuffer(); // flush later

        return pos;
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }
  }

  @Override
  public long writeDelete(DeleteUpdateCommand cmd) {
    LogCodec codec = new LogCodec();
    synchronized (fos) {
      try {
        long pos = fos.size(); // if we had flushed, this should be equal to
                               // channel.position()
        if (pos == 0) {
          writeLogHeader(codec);
          pos = fos.size();
        }
        codec.init(fos);
        codec.writeTag(JavaBinCodec.ARR, 3);
        codec.writeInt(UpdateLog.DELETE); // should just take one byte
        codec.writeLong(cmd.getVersion());
        BytesRef br = cmd.getIndexedId();
        codec.writeByteArray(br.bytes, br.offset, br.length);
        // fos.flushBuffer(); // flush later
        return pos;
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }
  }

  @Override
  public long writeCommit(CommitUpdateCommand cmd) {
    LogCodec codec = new LogCodec();
    synchronized (fos) {
      try {
        long pos = fos.size(); // if we had flushed, this should be equal to
                               // channel.position()
        if (pos == 0) {
          writeLogHeader(codec);
          pos = fos.size();
        }
        codec.init(fos);
        codec.writeTag(JavaBinCodec.ARR, 3);
        codec.writeInt(UpdateLog.COMMIT); // should just take one byte
        codec.writeLong(cmd.getVersion());
        codec.writeStr(END_MESSAGE); // ensure these bytes are the last in the
                                     // file
        // fos.flushBuffer(); // flush later
        return pos;
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }
  }

  @Override
  public long writeDeleteByQuery(DeleteUpdateCommand cmd) {
    LogCodec codec = new LogCodec();
    synchronized (fos) {
      try {
        long pos = fos.size(); // if we had flushed, this should be equal to
                               // channel.position()
        if (pos == 0) {
          writeLogHeader(codec);
          pos = fos.size();
        }
        codec.init(fos);
        codec.writeTag(JavaBinCodec.ARR, 3);
        codec.writeInt(UpdateLog.DELETE_BY_QUERY); // should just take one byte
        codec.writeLong(cmd.getVersion());
        codec.writeStr(cmd.query);
        // fos.flushBuffer(); // flush later
        return pos;
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      }
    }
  }

  private void writeLogHeader(LogCodec codec) throws IOException {
    Map header = new LinkedHashMap<String, Object>();
    header.put("SOLR_TLOG", 1); // a magic string + version number
    header.put("strings", globalStringList);
    codec.marshal(header, fos);
  }

  private void addGlobalStrings(Collection<String> strings) {
    if (strings == null)
      return;
    int origSize = globalStringMap.size();
    for (String s : strings) {
      Integer idx = null;
      if (origSize > 0) {
        idx = globalStringMap.get(s);
      }
      if (idx != null)
        continue; // already in list
      globalStringList.add(s);
      globalStringMap.put(s, globalStringList.size());
    }
    assert globalStringMap.size() == globalStringList.size();
  }

  @Override
  public void flush() throws IOException {
    synchronized (fos) {
      fos.flushBuffer();
    }
  }

  // write a BytesRef as a byte array
  JavaBinCodec.ObjectResolver resolver = new JavaBinCodec.ObjectResolver() {
    @Override
    public Object resolve(Object o, JavaBinCodec codec) throws IOException {
      if (o instanceof BytesRef) {
        BytesRef br = (BytesRef) o;
        codec.writeByteArray(br.bytes, br.offset, br.length);
        return null;
      }
      return o;
    }
  };

  public class LogCodec extends JavaBinCodec {
    public LogCodec() {
      super(resolver);
    }

    @Override
    public void writeExternString(String s) throws IOException {
      if (s == null) {
        writeTag(NULL);
        return;
      }

      // no need to synchronize globalStringMap - it's only updated before the
      // first record is written to the log
      Integer idx = globalStringMap.get(s);
      if (idx == null) {
        // write a normal string
        writeStr(s);
      } else {
        // write the extern string
        writeTag(EXTERN_STRING, idx);
      }
    }

    @Override
    public String readExternString(FastInputStream fis) throws IOException {
      int idx = readSize(fis);
      if (idx != 0) {// idx != 0 is the index of the extern string
        // no need to synchronize globalStringList - it's only updated before
        // the first record is written to the log
        return globalStringList.get(idx - 1);
      } else {// idx == 0 means it has a string value
        // this shouldn't happen with this codec subclass.
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Corrupt transaction log");
      }
    }
  }

  @Override
  public void close() throws IOException {
    flush();
  }

  @Override
  public Object read(long pos) throws IOException {
    // make sure any unflushed buffer has been flushed
    synchronized (fos) {
      // TODO: optimize this by keeping track of what we have flushed up to
      fos.flushBuffer();
      /***
       * System.out.println("###flushBuffer to " + fos.size() + " raf.length()="
       * + raf.length() + " pos="+pos); if (fos.size() != raf.length() || pos >=
       * fos.size() ) { throw new RuntimeException("ERROR" +
       * "###flushBuffer to " + fos.size() + " raf.length()=" + raf.length() +
       * " pos="+pos); }
       ***/
    }

    ChannelFastInputStream fis = new ChannelFastInputStream(
        handle.getChannel(), pos);
    LogCodec codec = new LogCodec();
    return codec.readVal(fis);
  }
  
  static class ChannelFastInputStream extends FastInputStream {
    FileChannel ch;
    long chPosition;

    public ChannelFastInputStream(FileChannel ch, long chPosition) {
      super(null);
      this.ch = ch;
      this.chPosition = chPosition;
    }

    @Override
    public int readWrappedStream(byte[] target, int offset, int len) throws IOException {
      ByteBuffer bb = ByteBuffer.wrap(target, offset, len);
      int ret = ch.read(bb, chPosition);
      if (ret >= 0) {
        chPosition += ret;
      }
      return ret;
    }

    @Override
    public void close() throws IOException {
      ch.close();
    }
  }

}
