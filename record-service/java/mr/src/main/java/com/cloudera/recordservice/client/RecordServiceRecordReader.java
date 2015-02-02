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

package com.cloudera.recordservice.client;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.thrift.TException;

import com.cloudera.recordservice.client.Schema.ColumnInfo;
import com.cloudera.recordservice.thrift.TColumnData;
import com.cloudera.recordservice.thrift.TColumnarRowBatch;
import com.cloudera.recordservice.thrift.TFetchResult;
import com.cloudera.recordservice.thrift.TRowBatchFormat;
import com.cloudera.recordservice.thrift.TUniqueId;

public class RecordServiceRecordReader extends
    RecordReader<WritableComparable<?>, RecordServiceRecord> {

  static interface RowBatch extends Iterable<RecordServiceRecord>,
  Iterator<RecordServiceRecord> {
  }

  private static long keyCounter = 0;

  private RecordServiceWorkerClient worker_;
  private TUniqueId tUniqId_;
  private RowBatch currentRowBatch_;
  private Schema schema_;
  private RecordServiceRecord currentRecord_;


  static class ColumnnarRowBatch implements RowBatch {

    private TColumnarRowBatch tRowBatch_;
    private Schema schema_;
    private int size_ = 0;
    private int currentRowIdx_ = 0;

    public ColumnnarRowBatch(TColumnarRowBatch tColRowBatch, Schema schema) {
      this.tRowBatch_ = tColRowBatch;
      this.schema_ = schema;
      this.size_ = getNumRows(tColRowBatch);
    }

    @Override
    public Iterator<RecordServiceRecord> iterator() {
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
    public RecordServiceRecord next() {
      if (currentRowIdx_ >= size_) {
        throw new RuntimeException("No nore values !!");
      }
      Writable[] colVals = new Writable[schema_.getNumColumns()];
      for (int i = 0; i < colVals.length; i++) {
        colVals[i] = getColValue(i, currentRowIdx_);
      }
      RecordServiceRecord record = new RecordServiceRecord(schema_, colVals);
      currentRowIdx_++;
      return record;
    }

    private Writable getColValue(int colIdx, int rowIdx) {
      TColumnData cd = tRowBatch_.getCols().get(colIdx);
      int actualIdx = rowIdx;
      for (int i = rowIdx; i > -1; i--) {
        if (cd.getIs_null()[i] == 0) {
          // Column is null
          if (i == rowIdx) return NullWritable.get();
          // decrement rowIdx for all cols that are null
          actualIdx--;
        }
      }
      ColumnInfo cInfo = schema_.getColumnInfo(colIdx);
      switch (cInfo.getType()) {
      case BIGINT:
        return new LongWritable(cd.getLong_vals().get(actualIdx));
      case BOOLEAN:
        return new BooleanWritable(cd.getBool_vals().get(actualIdx));
      case DOUBLE:
        return new DoubleWritable(cd.getDouble_vals().get(actualIdx));
      case INT:
        return new IntWritable(cd.getInt_vals().get(actualIdx));
      case STRING:
        return new Text(cd.getString_vals().get(actualIdx));
      case BINARY:
        return new BytesWritable(cd.getBinary_vals().get(actualIdx).array());
      case FLOAT:
        // TODO : Is this ok ?
        return new DoubleWritable(cd.getDouble_vals().get(actualIdx));
      case SMALLINT:
        return new ShortWritable(cd.getShort_vals().get(actualIdx));
//      case TIMESTAMP:
//        return new LongWritable(cd.get);
      case TINYINT:
        return new ByteWritable(cd.getByte_vals().get(actualIdx));
      default:
        return null;
      }
    }

    private int getNumRows(TColumnarRowBatch rb) {
      return rb.getCols().get(0).getIs_null().length;
    }
  }

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    RecordServiceInputSplit rsSplit = (RecordServiceInputSplit)split;
    schema_ = rsSplit.getSchema();
    String[] closestHostPort =
        RecordServiceUtils.getClosestHostPort(rsSplit.getLocations());
    worker_ = new RecordServiceWorkerClient();
    try {
      worker_.connect(closestHostPort[0], Integer.parseInt(closestHostPort[1]));
      tUniqId_ =
          worker_.execTask(rsSplit.getTaskInfo()
              .getTaskAsByteBuffer());
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * The general contract of the RecordReader is that the client (Mapper) calls
   * this method to load the next Key and Value.. before calling getCurrentKey()
   * and getCurrentValue().
   * When this method if first called, a call is made to the RecordServiceWorker
   * to obtain the first RowBatch. Every subsequent call will iterate thru that
   * RowBatch. Once, the rowBatch iteration has completed, The RecordService
   * Worker will be invoked for the next RowBatch.
   */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if ((currentRowBatch_ == null) || (!currentRowBatch_.hasNext())) {
      try {
        TFetchResult fetch = worker_.fetch(tUniqId_);
        if (fetch.isDone()) {
          return false;
        }
        TRowBatchFormat format = fetch.getRow_batch_format();
        if (format == TRowBatchFormat.ColumnarThrift) {
          TColumnarRowBatch rowBatch = fetch.getRow_batch();
          currentRowBatch_ = new ColumnnarRowBatch(rowBatch, schema_);
        } else if (format == TRowBatchFormat.Parquet) {
          // TODO : need to import Parquet libs and figure out Ser <-> DeSer
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
    } else {
      currentRecord_ = currentRowBatch_.next();
      return true;
    }
    return false;
  }

  @Override
  public WritableComparable<?> getCurrentKey() throws IOException,
      InterruptedException {
    return new LongWritable(keyCounter++);
  }

  @Override
  public RecordServiceRecord getCurrentValue() throws IOException,
      InterruptedException {
    return currentRecord_;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    try {
      return (float)worker_.getTaskStats(tUniqId_).getCompletion_percentage();
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      worker_.closeTask(tUniqId_);
    } catch (TException e) {
      throw new IOException(e);
    }
  }

}
