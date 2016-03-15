/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.cloudera.recordservice.pig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.junit.BeforeClass;
import org.junit.Test;

// TODO: add more tests
public class TestHCatRSLoader {

  private static PigServer server;

  @BeforeClass
  public static void setUpBeforeClass() throws ExecException {
    // Setup log4j for testing.
    org.apache.log4j.BasicConfigurator.configure();
    Properties properties = new Properties();
    properties.setProperty("hive.metastore.uris", "thrift://localhost:9083");
    server = new PigServer(ExecType.LOCAL, properties);
  }

  @Test
  public void testNation() throws IOException {
    server.registerQuery("A = LOAD " +
        "'tpch.nation' USING com.cloudera.recordservice.pig.HCatRSLoader();");

    Schema s = server.dumpSchema("A");
    List<Schema.FieldSchema> fields = s.getFields();
    assertEquals(4, fields.size());
    assertEquals("n_nationkey", fields.get(0).alias);
    assertEquals(DataType.INTEGER, fields.get(0).type);
    assertEquals("n_name", fields.get(1).alias);
    assertEquals(DataType.CHARARRAY, fields.get(1).type);
    assertEquals("n_regionkey", fields.get(2).alias);
    assertEquals(DataType.INTEGER, fields.get(2).type);
    assertEquals("n_comment", fields.get(3).alias);
    assertEquals(DataType.CHARARRAY, fields.get(3).type);

    server.registerQuery("A_GROUP = GROUP A ALL;");
    server.registerQuery("A_COUNT = FOREACH A_GROUP GENERATE COUNT(A);");
    Iterator<Tuple> it = server.openIterator("A_COUNT");
    boolean first = true;
    while (it.hasNext()) {
      assertTrue("Result should only contain one row", first);
      first = false;
      Tuple t = it.next();
      assertEquals(DataType.LONG, t.getType(0));
      assertEquals(1, t.size());
      assertEquals(25L, t.get(0));
    }
  }

  @Test
  // TODO: can't select n_nationkey due to RS-134. Add test once that is resolved.
  public void testNationSelect() throws IOException {
    server.registerQuery("A = LOAD 'select n_name, n_comment from tpch.nation'" +
        " USING com.cloudera.recordservice.pig.HCatRSLoader();");
    Schema s = server.dumpSchema("A");
    // TODO: note: there's a bug in HCatRSLoader#getSchema, which returns the
    // schema for the whole table instead of the selected columns. Fix the test
    // after that is resolved.
    List<Schema.FieldSchema> fields = s.getFields();
    assertEquals(4, fields.size());
    assertEquals("n_nationkey", fields.get(0).alias);
    assertEquals(DataType.INTEGER, fields.get(0).type);
    assertEquals("n_name", fields.get(1).alias);
    assertEquals(DataType.CHARARRAY, fields.get(1).type);
    assertEquals("n_regionkey", fields.get(2).alias);
    assertEquals(DataType.INTEGER, fields.get(2).type);
    assertEquals("n_comment", fields.get(3).alias);
    assertEquals(DataType.CHARARRAY, fields.get(3).type);

    Iterator<Tuple> it = server.openIterator("A");
    int count = 0;
    while (it.hasNext()) {
      count++;
      Tuple t = it.next();
      assertEquals(2, t.size());
      assertEquals(DataType.CHARARRAY, t.getType(0));
      assertEquals(DataType.CHARARRAY, t.getType(1));
      if (count == 1) {
        assertEquals("ALGERIA", t.get(0));
        assertEquals(" haggle. carefully final deposits detect slyly agai", t.get(1));
      }
    }
    assertEquals(25L, count);
  }

}
