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

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

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
    server.registerQuery(
        "A = LOAD 'tpch.nation' USING com.cloudera.recordservice.pig.HCatRSLoader();");
    server.registerQuery("A_GROUP = GROUP A ALL;");
    server.registerQuery("A_COUNT = FOREACH A_GROUP GENERATE COUNT(A);");
    Iterator<Tuple> it = server.openIterator("A_COUNT");
    boolean first = true;
    while (it.hasNext()) {
      Assert.assertTrue("Result should only contain one row", first);
      first = false;
      Tuple t = it.next();
      Assert.assertEquals(1, t.size());
      Assert.assertEquals(25L, t.get(0));
    }
  }
}
