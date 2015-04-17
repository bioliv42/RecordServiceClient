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

import sun.misc.Unsafe;

import com.cloudera.recordservice.thrift.TFetchResult;
import com.cloudera.recordservice.thrift.TRecordFormat;
import com.cloudera.recordservice.thrift.TRecordServiceException;
import com.cloudera.recordservice.thrift.TSchema;
import com.cloudera.recordservice.thrift.TTaskStatus;
import com.cloudera.recordservice.thrift.TType;
import com.cloudera.recordservice.thrift.TTypeId;
import com.cloudera.recordservice.thrift.TUniqueId;

/**
 * Abstraction over records returned from the RecordService. This class is
 * extremely performance sensitive. Not thread-safe.
 */
@SuppressWarnings("restriction")
public class Records {
  private static final Unsafe unsafe;
  static {
    try {
      Field field = Unsafe.class.getDeclaredField("theUnsafe");
      field.setAccessible(true);
      unsafe = (Unsafe) field.get(null);
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

    // Only used if col[i] is a TimestampNano to prevent object creation.
    private final TimestampNanos[] timestampNanos_;

    // Only used if col[i] is a Decimal to prevent object creation.
    private final Decimal[] decimalVals_;

    // Current record idx (in the current batch) being returned
    private int recordIdx_;

    // If the type is a CHAR or DECIMAL, this contains the length of the value.
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
     * For all these getters, returns the value at 'colIdx'. The type of the
     * column must match. Undefined behavior if it does not match or if called
     * on a value that is NULL. (We don't want to verify because this is the hot
     * path. TODO: add a DEBUG mode which verifies the API is being used
     * correctly.
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
      short val = unsafe
          .getShort(colData_[colIdx].array(), colOffsets_[colIdx]);
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

    public final TimestampNanos getTimestampNanos(int colIdx) {
      TimestampNanos timestamp = timestampNanos_[colIdx];
      long millis = unsafe.getLong(colData_[colIdx].array(), colOffsets_[colIdx]);
      colOffsets_[colIdx] += 8;
      int nanos = unsafe.getInt(colData_[colIdx].array(), colOffsets_[colIdx]);
      colOffsets_[colIdx] += 4;
      timestamp.set(millis, nanos);
      return timestamp;
    }

    public final Decimal getDecimal(int colIdx) {
      int len = byteArrayLen_[colIdx];
      long offset = colOffsets_[colIdx] - byteArrayOffset;
      decimalVals_[colIdx].set(colData_[colIdx], (int)offset, len);
      colOffsets_[colIdx] += len;
      return decimalVals_[colIdx];
    }

    protected Record(TSchema schema) {
      recordIdx_ = -1;
      colOffsets_ = new long[schema.cols.size()];
      colData_ = new ByteBuffer[schema.cols.size()];
      byteArrayVals_ = new ByteArray[schema.cols.size()];
      timestampNanos_ = new TimestampNanos[schema.cols.size()];
      decimalVals_ = new Decimal[schema.cols.size()];
      nulls_ = new ByteBuffer[schema.cols.size()];
      byteArrayLen_ = new int[schema.cols.size()];
      schema_ = schema;
      for (int i = 0; i < colOffsets_.length; ++i) {
        TType type = schema_.cols.get(i).type;
        if (type.type_id == TTypeId.STRING || type.type_id == TTypeId.VARCHAR) {
          byteArrayVals_[i] = new ByteArray();
        }
        if (type.type_id == TTypeId.TIMESTAMP_NANOS) {
          timestampNanos_[i] = new TimestampNanos();
        }

        if (type.type_id == TTypeId.CHAR) {
          byteArrayVals_[i] = new ByteArray();
          byteArrayLen_[i] = type.len;
        } else {
          byteArrayLen_[i] = -1;
        }

        if (type.type_id == TTypeId.DECIMAL) {
          decimalVals_[i] = new Decimal(type.precision, type.scale);
          byteArrayLen_[i] = Decimal.computeByteSize(type.precision, type.scale);
        }
      }
    }

    // Resets the state of the row to return the next batch.
    protected void reset(TFetchResult result) throws RuntimeException {
      recordIdx_ = -1;
      for (int i = 0; i < colOffsets_.length; ++i) {
        nulls_[i] = result.columnar_records.cols.get(i).is_null;
        colOffsets_[i] = byteArrayOffset;
        TType type = schema_.cols.get(i).type;
        switch (type.type_id) {
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
        case TIMESTAMP_NANOS:
        case DECIMAL:
          colData_[i] = result.columnar_records.cols.get(i).data;
          break;
        default:
          throw new RuntimeException(
              "Service returned type that is not supported. Type = " + type);
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
  public boolean hasNext() throws IOException, TRecordServiceException {
    assert(fetchResult_ != null);
    while (record_.recordIdx_ == fetchResult_.num_records - 1) {
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
   * Returns the status of the underlying task. This issues an RPC to the server
   * and cannot be used in the hot path.
   */
  public TTaskStatus getStatus() throws IOException, TRecordServiceException {
    if (handle_ == null) throw new RuntimeException("Task already closed.");
    return worker_.getTaskStatus(handle_);
  }

  public TSchema getSchema() { return record_.getSchema(); }
  public float progress() { return progress_; }

  protected Records(RecordServiceWorkerClient worker, TUniqueId handle,
      TSchema schema) throws IOException, TRecordServiceException {
    worker_ = worker;
    handle_ = handle;

    record_ = new Record(schema);
    nextBatch();
    hasNext_ = hasNext();
  }

  private void nextBatch() throws IOException, TRecordServiceException {
    if (handle_ == null) {
      throw new RuntimeException("Task has been closed already.");
    }
    fetchResult_ = worker_.fetch(handle_);
    if (fetchResult_.record_format != TRecordFormat.Columnar) {
      throw new RuntimeException("Unsupported record format");
    }
    record_.reset(fetchResult_);
    progress_ = (float)fetchResult_.task_completion_percentage;
  }

}
