//Copyright 2014 Cloudera Inc.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.junit.Test;

import com.cloudera.recordservice.mr.DecimalWritable;
import com.cloudera.recordservice.mr.RecordServiceRecord;
import com.cloudera.recordservice.mr.TimestampNanosWritable;
import com.cloudera.recordservice.thrift.TErrorCode;
import com.cloudera.recordservice.thrift.TRecordServiceException;

public class MapReduceTest {

  private void verifyInputSplits(int numSplits, int numCols, Configuration config)
      throws IOException {
    List<InputSplit> splits = RecordServiceInputFormat.getSplits(config);
    assertEquals(splits.size(), numSplits);
    RecordServiceInputSplit split = (RecordServiceInputSplit)splits.get(0);
    assertEquals(split.getSchema().getNumColumns(), numCols);
  }

  private void verifyInputSplitsTable(int numSplits, int numCols,
      String tbl, String... cols) throws IOException {
    Configuration config = new Configuration();
    config.set(RecordServiceInputFormat.TBL_NAME_CONF, tbl);
    if (cols.length > 0) {
      config.setStrings(RecordServiceInputFormat.COL_NAMES_CONF, cols);
    }
    verifyInputSplits(numSplits, numCols, config);
  }

  private void verifyInputSplitsPath(int numSplits, int numCols, String path)
      throws IOException {
    Configuration config = new Configuration();
    config.set(FileInputFormat.INPUT_DIR, path);
    verifyInputSplits(numSplits, numCols, config);
  }

  @Test
  public void testGetSplits() throws IOException {
    Configuration config = new Configuration();

    boolean exceptionThrown = false;
    try {
      RecordServiceInputFormat.getSplits(config);
    } catch (IllegalArgumentException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("No input specified"));
    }
    assertTrue(exceptionThrown);

    // Set db/table and make sure it works.
    config.set(RecordServiceInputFormat.TBL_NAME_CONF, "tpch.nation");
    RecordServiceInputFormat.getSplits(config);

    // Also set input. This should fail.
    config.set(FileInputFormat.INPUT_DIR, "/test");
    exceptionThrown = false;
    try {
      RecordServiceInputFormat.getSplits(config);
    } catch (IllegalArgumentException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains("Cannot specify both"));
    }
    assertTrue(exceptionThrown);

    // Unset the table and set columns. INPUT_DIR and columns don't work now.
    config.unset(RecordServiceInputFormat.TBL_NAME_CONF);
    config.setStrings(RecordServiceInputFormat.COL_NAMES_CONF, "a");
    exceptionThrown = false;
    try {
      RecordServiceInputFormat.getSplits(config);
    } catch (IllegalArgumentException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains(
          "Input specified by path and column projections cannot be used together."));
    }
    assertTrue(exceptionThrown);

    // Test some cases that work
    verifyInputSplitsTable(1, 4, "tpch.nation");
    verifyInputSplitsTable(2, 12, "rs.alltypes");
    verifyInputSplitsTable(1, 1, "tpch.nation", "n_name");
    verifyInputSplitsTable(2, 3, "rs.alltypes", "int_col", "double_col", "string_col");
    verifyInputSplitsPath(1, 1, "/test-warehouse/tpch.nation");

    // Test some cases using the config utility.
    config.clear();
    RecordServiceInputFormat.setInputTable(config, null,
        "tpch.nation", "n_nationkey", "n_comment");
    verifyInputSplits(1, 2, config);

    exceptionThrown = false;
    try {
      verifyInputSplitsTable(1, 1, "tpch.nation", "bad");
    } catch (IOException e) {
      exceptionThrown = true;
      assertTrue(e.getCause() instanceof TRecordServiceException);
      TRecordServiceException ex = (TRecordServiceException)e.getCause();
      assertEquals(ex.code, TErrorCode.INVALID_REQUEST);
    }
    assertTrue(exceptionThrown);

    exceptionThrown = false;
    try {
      verifyInputSplitsPath(1, 1,
          "/test-warehouse/tpch.nation,/test-warehouse/tpch.nation");
    } catch (IllegalArgumentException e) {
      exceptionThrown = true;
      assertTrue(e.getMessage().contains(
          "Only reading a single directory is currently supported."));
    }
    assertTrue(exceptionThrown);
  }

  @Test
  // TODO: make this generic. This should be extensible to test all the input
  // formats we support. How do we do this?
  public void testReadNation() throws IOException, InterruptedException {
    Configuration config = new Configuration();
    RecordServiceInputFormat.RecordServiceRecordReader reader =
        new RecordServiceInputFormat.RecordServiceRecordReader();

    try {
      RecordServiceInputFormat.setInputTable(config, null, "tpch.nation");
      List<InputSplit> splits = RecordServiceInputFormat.getSplits(config);
      reader.initialize((RecordServiceInputSplit)splits.get(0), config);

      int numRows = 0;
      while (reader.nextKeyValue()) {
        LongWritable key = reader.getCurrentKey();
        RecordServiceRecord value = reader.getCurrentValue();
        assertEquals(key.get(), numRows);
        ++numRows;

        if (numRows == 10) {
          assertEquals(value.getColumnValue(1).toString(), "INDONESIA");
        }
      }
      assertFalse(reader.nextKeyValue());
      assertFalse(reader.nextRecord());
      assertEquals(numRows, 25);

      config.clear();
      RecordServiceInputFormat.setInputTable(config, "tpch", "nation", "n_comment");
      splits = RecordServiceInputFormat.getSplits(config);
      reader.initialize((RecordServiceInputSplit)splits.get(0), config);
      numRows = 0;
      while (reader.nextKeyValue()) {
        RecordServiceRecord value = reader.getCurrentValue();
        if (numRows == 12) {
          assertEquals(value.getColumnValue(0).toString(),
              "ously. final, express gifts cajole a");
        }
        ++numRows;
      }
      assertEquals(numRows, 25);
    } finally {
      reader.close();
    }
  }

  @Test
  public void testReadAllTypes() throws IOException, InterruptedException {
    Configuration config = new Configuration();
    RecordServiceInputFormat.RecordServiceRecordReader reader =
        new RecordServiceInputFormat.RecordServiceRecordReader();

    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    format.setTimeZone(TimeZone.getTimeZone("GMT"));

    try {
      RecordServiceInputFormat.setInputTable(config, null, "rs.alltypes");
      List<InputSplit> splits = RecordServiceInputFormat.getSplits(config);

      int numRows = 0;
      for (InputSplit split: splits) {
        reader.initialize((RecordServiceInputSplit)split, config);
        while (reader.nextKeyValue()) {
          RecordServiceRecord value = reader.getCurrentValue();
          if (((BooleanWritable)value.getColumnValue(0)).get()) {
            assertEquals(((ByteWritable)value.getColumnValue(1)).get(), 0);
            assertEquals(((ShortWritable)value.getColumnValue(2)).get(), 1);
            assertEquals(((IntWritable)value.getColumnValue(3)).get(), 2);
            assertEquals(((LongWritable)value.getColumnValue(4)).get(), 3);
            assertEquals(((FloatWritable)value.getColumnValue(5)).get(), 4.0, 0.1);
            assertEquals(((DoubleWritable)value.getColumnValue(6)).get(), 5.0, 0.1);
            assertEquals(((Text)value.getColumnValue(7)).toString(), "hello");
            assertEquals(((Text)value.getColumnValue(8)).toString(), "vchar1");
            assertEquals(((Text)value.getColumnValue(9)).toString(), "char1");
            assertEquals(format.format(
                ((TimestampNanosWritable)value.getColumnValue(10)).get().toTimeStamp()),
                "2015-01-01");
            assertEquals(
                ((DecimalWritable)value.getColumnValue(11)).get().toBigDecimal(),
                new BigDecimal("3.1415920000"));
          } else {
            assertEquals(((ByteWritable)value.getColumnValue(1)).get(), 6);
            assertEquals(((ShortWritable)value.getColumnValue(2)).get(), 7);
            assertEquals(((IntWritable)value.getColumnValue(3)).get(), 8);
            assertEquals(((LongWritable)value.getColumnValue(4)).get(), 9);
            assertEquals(((FloatWritable)value.getColumnValue(5)).get(), 10.0, 0.1);
            assertEquals(((DoubleWritable)value.getColumnValue(6)).get(), 11.0, 0.1);
            assertEquals(((Text)value.getColumnValue(7)).toString(), "world");
            assertEquals(((Text)value.getColumnValue(8)).toString(), "vchar2");
            assertEquals(((Text)value.getColumnValue(9)).toString(), "char2");
            assertEquals(format.format(
                ((TimestampNanosWritable)value.getColumnValue(10)).get().toTimeStamp()),
                "2016-01-01");
            assertEquals(
                ((DecimalWritable)value.getColumnValue(11)).get().toBigDecimal(),
                new BigDecimal("1234.5678900000"));
          }
          ++numRows;
        }
      }
      assertEquals(numRows, 2);
    } finally {
      reader.close();
    }
  }

  @Test
  public void testReadAllTypesNull() throws IOException, InterruptedException {
    Configuration config = new Configuration();
    RecordServiceInputFormat.RecordServiceRecordReader reader =
        new RecordServiceInputFormat.RecordServiceRecordReader();

    try {
      RecordServiceInputFormat.setInputTable(config, null, "rs.alltypes_null");
      List<InputSplit> splits = RecordServiceInputFormat.getSplits(config);

      int numRows = 0;
      for (InputSplit split: splits) {
        reader.initialize((RecordServiceInputSplit)split, config);
        while (reader.nextKeyValue()) {
          RecordServiceRecord value = reader.getCurrentValue();
          for (int i = 0; i < value.getSchema().getNumColumns(); ++i) {
            assertTrue(value.getColumnValue(i) == null);
          }
          ++numRows;
        }
      }
      assertEquals(numRows, 1);
    } finally {
      reader.close();
    }
  }
}
