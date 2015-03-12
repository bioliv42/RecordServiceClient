// Copyright 2014 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.recordservice.client;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

import org.apache.thrift.TException;

import sun.misc.Unsafe;

import com.cloudera.recordservice.thrift.TFetchResult;
import com.cloudera.recordservice.thrift.TRowBatchFormat;
import com.cloudera.recordservice.thrift.TSchema;
import com.cloudera.recordservice.thrift.TStats;
import com.cloudera.recordservice.thrift.TTypeId;
import com.cloudera.recordservice.thrift.TUniqueId;
import com.google.common.base.Preconditions;

/**
 * Abstraction over records returned from the RecordService.
 * This class is extremely performance sensitive.
 * Not thread-safe.
 */
@SuppressWarnings("restriction")
public class Records {
  private static final Unsafe unsafe;
  static {
    try {
      Field field = Unsafe.class.getDeclaredField("theUnsafe");
      field.setAccessible(true);
      unsafe = (Unsafe)field.get(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static final class Record {
    // For each column, the current offset to return (column data is sparse)
    private final long[] colOffsets_;

    // For each column, the serialized data values.
    private final ByteBuffer[] colData_;
    private final ByteBuffer[] nulls_;

    // Only used if col[i] is a String to prevent object creation.
    private final ByteArray[] byteArrayVals_;

    // Current record idx (in the current batch)  being returned
    private int recordIdx_;

    // If the type is a CHAR, this contains the length of the value.
    // -1 otherwise.
    private final int[] byteArrayLen_;

    // Schema of the record
    private final TSchema schema_;

    // Returns if the col at 'colIdx' is NULL.
    public boolean isNull(int colIdx) {
      return nulls_[colIdx].get(recordIdx_) != 0;
    }

    private static final long byteArrayOffset = unsafe.arrayBaseOffset(byte[].class);

    /**
     * For all these getters, returns the value at 'colIdx'. The type of the column
     * must match. Undefined behavior if it does not match or if called on a value that
     * is NULL. (We don't want to verify because this is the hot path.
     * TODO: add a DEBUG mode which verifies the API is being used correctly.
     */
    public final boolean getBoolean(int colIdx) {
      byte val = unsafe.getByte(colData_[colIdx].array(), colOffsets_[colIdx]);
      colOffsets_[colIdx] += 1;
      return val != 0;
    }

    public final byte getByte(int colIdx) {
      byte val = unsafe.getByte(colData_[colIdx].array(), colOffsets_[colIdx]);
      colOffsets_[colIdx] += 1;
      return val;
    }

    public final short getShort(int colIdx) {
      short val = unsafe.getShort(colData_[colIdx].array(), colOffsets_[colIdx]);
      colOffsets_[colIdx] += 2;
      return val;
    }

    public final int getInt(int colIdx) {
      int val = unsafe.getInt(colData_[colIdx].array(), colOffsets_[colIdx]);
      colOffsets_[colIdx] += 4;
      return val;
    }

    public final long getLong(int colIdx) {
      long val = unsafe.getLong(colData_[colIdx].array(), colOffsets_[colIdx]);
      colOffsets_[colIdx] += 8;
      return val;
    }

    public final float getFloat(int colIdx) {
      float val = unsafe.getFloat(colData_[colIdx].array(), colOffsets_[colIdx]);
      colOffsets_[colIdx] += 4;
      return val;
    }

    public final double getDouble(int colIdx) {
      double val = unsafe.getDouble(colData_[colIdx].array(), colOffsets_[colIdx]);
      colOffsets_[colIdx] += 8;
      return val;
    }

    public final ByteArray getByteArray(int colIdx) {
      int len = byteArrayLen_[colIdx];
      if (len < 0) {
        len = unsafe.getInt(colData_[colIdx].array(), colOffsets_[colIdx]);
        colOffsets_[colIdx] += 4;
      }
      long offset = colOffsets_[colIdx] - byteArrayOffset;
      byteArrayVals_[colIdx].set(colData_[colIdx], (int)offset, len);
      colOffsets_[colIdx] += len;
      return byteArrayVals_[colIdx];
    }

    protected Record(TSchema schema) {
      recordIdx_ = -1;
      colOffsets_ = new long[schema.cols.size()];
      colData_ = new ByteBuffer[schema.cols.size()];
      byteArrayVals_ = new ByteArray[schema.cols.size()];
      nulls_ = new ByteBuffer[schema.cols.size()];
      byteArrayLen_ = new int[schema.cols.size()];
      schema_ = schema;
      for (int i = 0; i < colOffsets_.length; ++i) {
        if (schema_.cols.get(i).type.type_id == TTypeId.STRING ||
            schema_.cols.get(i).type.type_id == TTypeId.VARCHAR) {
          byteArrayVals_[i] = new ByteArray();
        }

        if (schema_.cols.get(i).type.type_id == TTypeId.CHAR) {
          byteArrayVals_[i] = new ByteArray();
          byteArrayLen_[i] = schema_.cols.get(i).type.len;
        } else {
          byteArrayLen_[i] = -1;
        }
      }
    }

    // Resets the state of the row to return the next batch.
    protected void reset(TFetchResult result) throws RuntimeException {
      recordIdx_ = -1;
      for (int i = 0; i < colOffsets_.length; ++i) {
        nulls_[i] = result.columnar_row_batch.cols.get(i).is_null;
        colOffsets_[i] = byteArrayOffset;
        switch (schema_.cols.get(i).type.type_id) {
          case BOOLEAN:
          case TINYINT:
          case SMALLINT:
          case INT:
          case BIGINT:
          case FLOAT:
          case DOUBLE:
          case STRING:
          case VARCHAR:
          case CHAR:
            colData_[i] = result.columnar_row_batch.cols.get(i).data;
            break;
          default:
            throw new RuntimeException("Unknown type");
        }
      }
    }

    public TSchema getSchema() { return schema_; }
  }

  // Client and task handle
  private final RecordServiceWorkerClient worker_;
  private TUniqueId handle_;

  // The current fetchResult from the RecordServiceWorker. Never null.
  private TFetchResult fetchResult_;

  // Record object that is returned. Reused across calls to next()
  private final Record record_;

  // Cache of result from hasNext(). This starts out as true and makes one
  // transition to false.
  private boolean hasNext_;

  // Completion percentage.
  private float progress_;

  /**
   * Returns true if there are more records.
   */
  public boolean hasNext() throws IOException {
    Preconditions.checkNotNull(fetchResult_);
    while (record_.recordIdx_ == fetchResult_.num_rows - 1) {
      if (fetchResult_.done) {
        hasNext_ = false;
        return false;
      }
      nextBatch();
    }
    return true;
  }

  /**
   * Returns and advances to the next record.
   */
  public Record next() throws IOException {
    if (!hasNext_) throw new IOException("End of stream");
    record_.recordIdx_++;
    return record_;
  }

  /*
   * Closes the task. Idempotent.
   */
  public void close() {
    if (handle_ == null) return;
    worker_.closeTask(handle_);
    handle_ = null;
  }

  /*
   * Returns the stats of the underlying task. This issues an RPC to the
   * server and cannot be used in the hot path.
   */
  public TStats getStats() throws TException {
    if (handle_ == null) throw new RuntimeException("Cannot call on closed object.");
    return worker_.getTaskStats(handle_);
  }

  public TSchema getSchema() { return record_.getSchema(); }

  public float progress() { return progress_; }

  protected Records(RecordServiceWorkerClient worker,
      TUniqueId handle, TSchema schema) throws IOException {
    worker_ = worker;
    handle_ = handle;

    record_ = new Record(schema);
    nextBatch();
    hasNext_ = hasNext();
  }

  private void nextBatch() throws IOException {
    if (handle_ == null) {
      throw new RuntimeException("Task has been closed already.");
    }
    try {
      fetchResult_ = worker_.fetch(handle_);
    } catch (TException e) {
      throw new IOException(e);
    }
    if (fetchResult_.row_batch_format != TRowBatchFormat.Columnar) {
      throw new RuntimeException("Unsupported row batch format");
    }
    record_.reset(fetchResult_);
    progress_ = (float)fetchResult_.task_completion_percentage;
  }

}
