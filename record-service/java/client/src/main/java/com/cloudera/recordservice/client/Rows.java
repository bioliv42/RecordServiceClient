package com.cloudera.recordservice.client;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.thrift.TException;

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
public class Rows {
  public static final class Row {
    // For each column, the current offset to return (column data is sparse)
    private int[] colOffsets_;

    // For each column, the serialized data values.
    private ByteBuffer[] colData_;
    private ByteBuffer[] nulls_;

    // Only used if col[i] is a String to prevent object creation.
    private ByteArray[] byteArrayVals_;

    private int rowIdx_;

    // Schema of the row
    private TSchema schema_;

    // Returns if the col at 'colIdx' is NULL.
    public boolean isNull(int colIdx) {
      return nulls_[colIdx].get(rowIdx_) != 0;
    }

    /**
     * For all these getters, returns the value at 'colIdx'. The type of the column
     * must match. Undefined behavior if it does not match or if called on a value that
     * is NULL. (We don't want to verify because this is the hot path.
     */
    public final boolean getBoolean(int colIdx) {
      return colData_[colIdx].get(colOffsets_[colIdx++]) != 0;
    }

    public final byte getByte(int colIdx) {
      return colData_[colIdx].get(colOffsets_[colIdx++]);
    }

    public final short getShort(int colIdx) {
      short val = colData_[colIdx].getShort(colOffsets_[colIdx]);
      colOffsets_[colIdx] += 2;
      return val;
    }

    public final int getInt(int colIdx) {
      int val = colData_[colIdx].getInt(colOffsets_[colIdx]);
      colOffsets_[colIdx] += 4;
      return val;
    }

    public final long getLong(int colIdx) {
      long val = colData_[colIdx].getLong(colOffsets_[colIdx]);
      colOffsets_[colIdx] += 8;
      return val;
    }

    public final float getFloat(int colIdx) {
      float val = colData_[colIdx].getFloat(colOffsets_[colIdx]);
      colOffsets_[colIdx] += 4;
      return val;
    }

    public final double getDouble(int colIdx) {
      double val = colData_[colIdx].getDouble(colOffsets_[colIdx]);
      colOffsets_[colIdx] += 8;
      return val;
    }

    public final ByteArray getByteArray(int colIdx) {
      int len = colData_[colIdx].getInt(colOffsets_[colIdx]);
      colOffsets_[colIdx] += 4;
      // Convert endianness
      len = Integer.reverseBytes(len);
      byteArrayVals_[colIdx].set(colData_[colIdx], colOffsets_[colIdx], len);
      colOffsets_[colIdx] += len;
      return byteArrayVals_[colIdx];
    }

    protected Row(TSchema schema) {
      rowIdx_ = -1;
      colOffsets_ = new int[schema.cols.size()];
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
    protected void reset(TFetchResult result) throws TException {
      for (int i = 0; i < colOffsets_.length; ++i) {
        nulls_[i] = result.parquet_row_batch.cols.get(i).is_null;
        colOffsets_[i] = 0;
        switch (schema_.cols.get(i).type.type_id) {
          case SMALLINT:
          case INT:
          case BIGINT:
            colData_[i] = result.parquet_row_batch.
                cols.get(i).data.order(ByteOrder.LITTLE_ENDIAN);
            break;
          case BOOLEAN:
          case TINYINT:
          case FLOAT:
          case DOUBLE:
          case STRING:
            colData_[i] = result.parquet_row_batch.cols.get(i).data;
            break;
          default:
            throw new TException("Unknown type");
        }
      }
    }
  }

  // Client and task handle
  private RecordServiceWorkerClient worker_;
  private TUniqueId handle_;

  // The current fetchResult from the RecordServiceWorker. Never null.
  private TFetchResult fetchResult_;

  // Row object that is returned. Reused across calls to next()
  private Row row_;

  // Current row idx being returned
  private int currentRow_;

  // Cache of result from hasNext(). This starts out as true and makes one
  // transition to false.
  private boolean hasNext_;

  /**
   * Returns true if there are more rows.
   */
  public boolean hasNext() throws TException {
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
  public Row next() throws TException {
    if (!hasNext_) throw new TException("End of stream");
    row_.rowIdx_ = currentRow_++;
    return row_;
  }

  public void close() {
    try {
      worker_.closeTask(handle_);
    } catch (TException e) {
      // TODO: log
    }
  }

  protected Rows(RecordServiceWorkerClient worker,
      TUniqueId handle, TSchema schema) throws TException {
    worker_ = worker;
    handle_ = handle;

    row_ = new Row(schema);
    nextBatch();
    hasNext_ = hasNext();
  }

  private void nextBatch() throws TException {
    fetchResult_ = worker_.fetch(handle_);
    currentRow_ = 0;
    if (fetchResult_.row_batch_format != TRowBatchFormat.Parquet) {
      throw new TException("Unsupported row batch format");
    }
    row_.reset(fetchResult_);
  }
}
