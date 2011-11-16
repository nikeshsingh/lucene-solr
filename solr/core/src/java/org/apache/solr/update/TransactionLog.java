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

package org.apache.solr.update;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.common.SolrException;

/**
 * Log Format: List{Operation, Version, ...} ADD, VERSION, DOC DELETE, VERSION,
 * ID_BYTES DELETE_BY_QUERY, VERSION, String
 * 
 * TODO: keep two files, one for [operation, version, id] and the other for the
 * actual document data. That way we could throw away document log files more
 * readily while retaining the smaller operation log files longer (and we can
 * retrieve the stored fields from the latest documents from the index).
 * 
 * This would require keeping all source fields stored of course.
 * 
 * This would also allow to not log document data for requests with commit=true
 * in them (since we know that if the request succeeds, all docs will be
 * committed)
 * 
 */
public class TransactionLog {

  final TransactionLogFileHandle handle;
  long id;
  TransactionLogCodec<AddUpdateCommand, DeleteUpdateCommand, CommitUpdateCommand, DeleteUpdateCommand> codec;
  volatile boolean deleteOnClose = true; // we can delete old tlogs since they
                                         // are currently only used for
                                         // real-time-get (and in the future,
                                         // recovery)

  AtomicInteger refcount = new AtomicInteger(1);

  TransactionLog(File tlogFile, Collection<String> globalStrings) {
    try {
      handle = new TransactionLogFileHandle(tlogFile, deleteOnClose);
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
    codec = new SolrTransactionLogCodec(handle, globalStrings);
  }

  public long write(AddUpdateCommand cmd) {
    return codec.writeAdd(cmd);
  }

  public long writeDelete(DeleteUpdateCommand cmd) {
    return codec.writeDelete(cmd);
  }

  public long writeDeleteByQuery(DeleteUpdateCommand cmd) {
    return codec.writeDeleteByQuery(cmd);
  }

  public long writeCommit(CommitUpdateCommand cmd) {
    return codec.writeCommit(cmd);
  }

  /* This method is thread safe */
  public Object lookup(long pos) {
    try {
      return codec.read(pos);
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  public void incref() {
    refcount.incrementAndGet();
  }

  public void decref() {
    if (refcount.decrementAndGet() == 0) {
      close();
    }
  }

  public void finish(UpdateLog.SyncLevel syncLevel) {
    if (syncLevel == UpdateLog.SyncLevel.NONE)
      return;
    try {
      codec.flush();

      if (syncLevel == UpdateLog.SyncLevel.FSYNC) {
        // Since fsync is outside of synchronized block, we can end up with a
        // partial
        // last record on power failure (which is OK, and does not represent an
        // error...
        // we just need to be aware of it when reading).
        handle.sync(false);
      }

    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  private void close() {
    try {
      codec.close();
      handle.close();
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  public String toString() {
    return handle.getFile().toString();
  }

}
