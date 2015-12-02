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

package com.cloudera.recordservice.hcatalog.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.StorerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

class InternalUtil {
  private static final Logger LOG = LoggerFactory.getLogger(InternalUtil.class);

  static StorerInfo extractStorerInfo(StorageDescriptor sd, Map<String, String> properties) throws IOException {
    Properties hcatProperties = new Properties();
    for (String key : properties.keySet()) {
      hcatProperties.put(key, properties.get(key));
    }

    // also populate with StorageDescriptor->SerDe.Parameters
    for (Map.Entry<String, String> param :
      sd.getSerdeInfo().getParameters().entrySet()) {
      hcatProperties.put(param.getKey(), param.getValue());
    }

    // Need to hard code the serDe class for, as views are not typically supported by pig
    return new StorerInfo(
      sd.getInputFormat(), sd.getOutputFormat(), "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
      properties.get(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE),
      hcatProperties);
  }

  static Map<String, String> createPtnKeyValueMap(Table table, Partition ptn)
    throws IOException {
    List<String> values = ptn.getValues();
    if (values.size() != table.getPartitionKeys().size()) {
      throw new IOException(
          "Partition values in partition inconsistent with table definition, table "
              + table.getTableName() + " has "
              + table.getPartitionKeys().size()
              + " partition keys, partition has " + values.size()
              + "partition values");
    }

    Map<String, String> ptnKeyValues = new HashMap<String, String>();

    int i = 0;
    for (FieldSchema schema : table.getPartitionKeys()) {
      // CONCERN : the way this mapping goes, the order *needs* to be
      // preserved for table.getPartitionKeys() and ptn.getValues()
      ptnKeyValues.put(schema.getName().toLowerCase(), values.get(i));
      i++;
    }

    return ptnKeyValues;
  }
}
