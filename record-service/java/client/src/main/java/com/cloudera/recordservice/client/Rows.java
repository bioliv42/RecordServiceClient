package com.cloudera.recordservice.client;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;

import org.apache.thrift.TException;

import sun.misc.Unsafe;

import com.cloudera.recordservice.thrift.TFetchResult;
import com.cloudera.recordservice.thrift.TRowBatchFormat;
import com.cloudera.recordservice.thrift.TSchema;
import com.cloudera.recordservice.thrift.TTypeId;
import com.cloudera.recordservice.thrift.TUniqueId;
import com.google.common.base.Preconditions;

/**
 * Abstraction over rows returned from the RecordService.
 * This class is extremely performance sensitive.
 * Not thread-safe.
 */
@SuppressWarnings("restriction")
public class Rows {
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

  public static final class Row {
    // For each column, the current offset to return (column data is sparse)
    private final long[] colOffsets_;

    // For each column, the serialized data values.
    private final ByteBuffer[] colData_;
    private final ByteBuffer[] nulls_;

    // Only used if col[i] is a String to prevent object creation.
    private final ByteArray[] byteArrayVals_;

    private int rowIdx_;

    // Schema of the row
    private final TSchema schema_;

    // Returns if the col at 'colIdx' is NULL.
    public boolean isNull(int colIdx) {
      return nulls_[colIdx].get(rowIdx_) != 0;
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
      int len = unsafe.getInt(colData_[colIdx].array(), colOffsets_[colIdx]);
      colOffsets_[colIdx] += 4;
      long offset = colOffsets_[colIdx] - byteArrayOffset;
      byteArrayVals_[colIdx].set(colData_[colIdx], (int)offset, len);
      colOffsets_[colIdx] += len;
      return byteArrayVals_[colIdx];
    }

    protected Row(TSchema schema) {
      rowIdx_ = -1;
      colOffsets_ = new long[schema.cols.size()];
      colData_ = new ByteBuffer[schema.cols.size()];
      byteArrayVals_ = new ByteArray[schema.cols.size()];
      nulls_ = new ByteBuffer[schema.cols.size()];
      schema_ = schema;
      for (int i = 0; i < colOffsets_.length; ++i) {
        if (schema_.cols.get(i).type.type_id == TTypeId.STRING) {
          byteArrayVals_[i] = new ByteArray();
        }
      }
    }

    // Resets the state of the row to return the next batch.
    protected void reset(TFetchResult result) throws RuntimeException {
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
  private final TUniqueId handle_;

  // The current fetchResult from the RecordServiceWorker. Never null.
  private TFetchResult fetchResult_;

  // Row object that is returned. Reused across calls to next()
  private final Row row_;

  // Current row idx being returned
  private int currentRow_;

  // Cache of result from hasNext(). This starts out as true and makes one
  // transition to false.
  private boolean hasNext_;

  // Completion percentage.
  private float progress_;

  /**
   * Returns true if there are more rows.
   */
  public boolean hasNext() throws IOException {
    Preconditions.checkNotNull(fetchResult_);
    while (currentRow_ == fetchResult_.num_rows) {
      if (fetchResult_.done) {
        hasNext_ = false;
        return false;
      }
      nextBatch();
    }
    return true;
  }

  /**
   * Returns and advances to the next row.
   */
  public Row next() throws IOException {
    if (!hasNext_) throw new IOException("End of stream");
    row_.rowIdx_ = currentRow_++;
    return row_;
  }

  public void close() {
    worker_.closeTask(handle_);
  }

  public TSchema getSchema() { return row_.getSchema(); }

  public float progress() { return progress_; }

  protected Rows(RecordServiceWorkerClient worker,
      TUniqueId handle, TSchema schema) throws IOException {
    worker_ = worker;
    handle_ = handle;

    row_ = new Row(schema);
    nextBatch();
    hasNext_ = hasNext();
  }

  private void nextBatch() throws IOException {
    try {
      fetchResult_ = worker_.fetch(handle_);
    } catch (TException e) {
      throw new IOException(e);
    }
    currentRow_ = 0;
    if (fetchResult_.row_batch_format != TRowBatchFormat.Columnar) {
      throw new RuntimeException("Unsupported row batch format");
    }
    row_.reset(fetchResult_);
    progress_ = (float)fetchResult_.task_completion_percentage;
  }

}
