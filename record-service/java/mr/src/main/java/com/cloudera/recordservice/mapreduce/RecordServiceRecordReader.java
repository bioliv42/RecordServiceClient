// Copyright 2012 Cloudera Inc.
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

package com.cloudera.recordservice.mapreduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.TException;

import com.cloudera.recordservice.client.RecordServiceWorkerClient;
import com.cloudera.recordservice.mapreduce.Schema.ColumnInfo;
import com.cloudera.recordservice.thrift.TColumnData;
import com.cloudera.recordservice.thrift.TColumnarRowBatch;
import com.cloudera.recordservice.thrift.TFetchResult;
import com.cloudera.recordservice.thrift.TRowBatchFormat;
import com.cloudera.recordservice.thrift.TUniqueId;
import com.google.common.base.Preconditions;

public class RecordServiceRecordReader extends
    RecordReader<WritableComparable<?>, RecordServiceRecord> {

  static interface RowBatch extends Iterable<Writable[]>, Iterator<Writable[]> {
    /**
     * Returns the Schema for this RowBatch.
     */
    Schema getSchema();
  }

  private static long keyCounter = 0;
  private RecordServiceWorkerClient worker_;
  private TUniqueId handle_;
  private Schema schema_;
  private RecordServiceRecord currentRecord_;
  private RowBatch currentRowBatch_;
  private final LongWritable currentKey_ = new LongWritable();
  private volatile boolean isInitialized_ = false;
  private boolean isDone_ = false;

  /**
   * Represents RowBatch in the RecordService Thrift Columnar format.
   * This class is not thread safe (TODO: does it need to be?).
   */
  static class ColumnnarRowBatch implements RowBatch {
    private TColumnarRowBatch rowBatch_;
    private final Schema schema_;
    private int size_ = 0;
    // The current row index within the row batch.
    private int currentRowIdx_ = 0;

    // Contains the data for the current row. When next() is called, this array is
    // populated with from the next row in the thrift row batch.
    private final Writable[] currentRow_;

    public ColumnnarRowBatch(TColumnarRowBatch tColRowBatch, Schema schema) {
      schema_ = schema;
      setRowBatch(tColRowBatch);
      currentRow_ = new Writable[schema_.getNumColumns()];
      for (int i = 0; i < schema_.getNumColumns(); ++i) {
        ColumnInfo cInfo = schema_.getColumnInfo(i);
        Preconditions.checkNotNull(cInfo);
        switch (cInfo.getType()) {
          case BIGINT:
            currentRow_[i] = new LongWritable();
            break;
          case BOOLEAN:
            currentRow_[i] = new BooleanWritable();
            break;
          case DOUBLE:
            currentRow_[i] = new DoubleWritable();
            break;
          case INT:
            currentRow_[i] = new IntWritable();
            break;
          case STRING:
            currentRow_[i] = new Text();
            break;
          case BINARY:
            currentRow_[i] = new BytesWritable();
            break;
          case FLOAT:
            currentRow_[i] = new FloatWritable();
            break;
          case SMALLINT:
            currentRow_[i] = new ShortWritable();
            break;
          case TINYINT:
            currentRow_[i] = new ByteWritable();
            break;
          default:
            throw new UnsupportedOperationException(
                "Unsupported data type: " + cInfo.getType());
        }
      }
    }

    public int getNumRows() { return rowBatch_.getCols().get(0).getIs_null().length; }

    @Override
    public Iterator<Writable[]> iterator() {
      return this;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Not supported !!");
    }

    @Override
    public boolean hasNext() {
      return currentRowIdx_ < size_;
    }

    @Override
    public Writable[] next() {
      if (currentRowIdx_ >= size_) {
        throw new RuntimeException("No nore values !!");
      }
      setRow(currentRowIdx_++);
      return currentRow_;
    }

    private void setRow(int rowIdx) {
      for (int colIdx = 0; colIdx < currentRow_.length; ++colIdx) {
        TColumnData cd = rowBatch_.getCols().get(colIdx);
        switch (schema_.getColumnInfo(colIdx).getType()) {
          case BIGINT:
            ((LongWritable) currentRow_[colIdx]).set(cd.getLong_vals().get(rowIdx));
            break;
          case BOOLEAN:
            ((BooleanWritable) currentRow_[colIdx]).set(cd.getBool_vals().get(rowIdx));
            break;
          case DOUBLE:
            ((DoubleWritable) currentRow_[colIdx]).set(cd.getDouble_vals().get(rowIdx));
            break;
          case INT:
            ((IntWritable) currentRow_[colIdx]).set(cd.getInt_vals().get(rowIdx));
            break;
          case STRING:
            ((Text) currentRow_[colIdx]).set(cd.getString_vals().get(rowIdx));
            break;
          case BINARY:
            ((BytesWritable) currentRow_[colIdx]).set(
                new BytesWritable(cd.getBinary_vals().get(rowIdx).array()));
            break;
          case FLOAT:
            ((FloatWritable) currentRow_[colIdx]).set(
                cd.getDouble_vals().get(rowIdx).floatValue());
            break;
          case SMALLINT:
            ((ShortWritable) currentRow_[colIdx]).set(cd.getShort_vals().get(rowIdx));
            break;
          case TINYINT:
            ((ByteWritable) currentRow_[colIdx]).set(cd.getByte_vals().get(rowIdx));
            break;
          default:
            Preconditions.checkState(false);
        }
      }
    }

    /**
     * Initializes this instance with new row batch data. Assumes the new batch matches
     * the existing schema (no validation checks are done). Invalidates any existing
     * iterators.
     */
    public void setRowBatch(TColumnarRowBatch newBatch) {
      Preconditions.checkNotNull(schema_);
      rowBatch_ = newBatch;
      size_ = getNumRows();
      currentRowIdx_ = 0;
    }

    @Override
    public Schema getSchema() { return schema_; }
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    RecordServiceInputSplit rsSplit = (RecordServiceInputSplit)split;
    schema_ = rsSplit.getSchema();
    worker_ = new RecordServiceWorkerClient();
    try {
      // TODO: Make port Configurable.
      worker_.connect(rsSplit.getLocations()[0], 40100);
      handle_ = worker_.execTask(rsSplit.getTaskInfo().getTaskAsByteBuffer());
    } catch (Exception e) {
      throw new IOException(e);
    }
    isInitialized_ = true;
  }

  public boolean fetchNextRowBatch() throws TException {
    TFetchResult fetchResult = worker_.fetch(handle_);
    if (fetchResult.getRow_batch_format() == TRowBatchFormat.ColumnarThrift) {
      if (currentRowBatch_ == null) {
        currentRowBatch_ = new ColumnnarRowBatch(fetchResult.getRow_batch(), schema_);
      } else {
        ((ColumnnarRowBatch) currentRowBatch_).setRowBatch(fetchResult.getRow_batch());
      }
    } else {
      throw new UnsupportedOperationException("RowBatch format not supported: " +
          fetchResult.getRow_batch_format().toString());
    }
    return fetchResult.isDone();
  }

  /**
   * The general contract of the RecordReader is that the client (Mapper) calls
   * this method to load the next Key and Value.. before calling getCurrentKey()
   * and getCurrentValue().
   * When this method if first called, a call is made to the RecordServiceWorker
   * to obtain the first RowBatch. Every subsequent call will iterate thru that
   * RowBatch. Once, the rowBatch iteration has completed, The RecordService
   * Worker will be invoked for the next RowBatch.
   *
   * Returns true if there are more values to retrieve, false otherwise.
   */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (!isInitialized_) {
      throw new IOException("Record Reader not initialized !!");
    }

    // Check if we need to fetch a new row batch.
    if (!isDone_ && (currentRowBatch_ == null || !currentRowBatch_.hasNext())) {
      try {
        isDone_ = fetchNextRowBatch();
      } catch (TException e) {
        throw new IOException(e);
      }
    }

    Preconditions.checkNotNull(currentRowBatch_);

    if (!currentRowBatch_.hasNext()) {
      return !isDone_;
    } else {
      // Initialize a new record.
      if (currentRecord_ == null) {
        currentRecord_ = new RecordServiceRecord(
            currentRowBatch_.getSchema(), currentRowBatch_.next());
      } else {
        currentRecord_.initialize(currentRowBatch_.next());
      }
      currentKey_.set(keyCounter++);
      return true;
    }
  }

  @Override
  public WritableComparable<?> getCurrentKey() throws IOException,
      InterruptedException {
    return currentKey_;
  }

  @Override
  public RecordServiceRecord getCurrentValue() throws IOException,
      InterruptedException {
    return currentRecord_;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 0.0f;
    // TODO: MR calls this so frequently that it kills performance. We need to
    // either cache the result or included it as part of the fetch result.
    //try {
    // return (float) worker_.getTaskStats(handle_).getCompletion_percentage();
    //} catch (TException e) {
    //  throw new IOException(e);
    //}
  }

  @Override
  public void close() throws IOException {
    if (worker_ != null) {
      try {
        if (handle_ != null) {
          worker_.closeTask(handle_);
          handle_ = null;
        }
      } catch (TException e) {
        throw new IOException("Error closing task: ", e);
      } finally {
        worker_.close();
      }
    }
  }

  public boolean isInitialized() { return isInitialized_; }
}