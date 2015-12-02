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

package com.cloudera.recordservice.hcatalog.common;

import com.cloudera.recordservice.hcatalog.mapreduce.InputJobInfo;
import com.cloudera.recordservice.hcatalog.mapreduce.PartInfo;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.Credentials;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class HCatRSUtil {

  private static final Logger LOG = LoggerFactory.getLogger(HCatRSUtil.class);


  public static Map<String, String>
  getInputJobProperties(HiveStorageHandler storageHandler,
                        InputJobInfo inputJobInfo) {
    Properties props = inputJobInfo.getTableInfo().getStorerInfo().getProperties();
    props.put(serdeConstants.SERIALIZATION_LIB,storageHandler.getSerDeClass().getName());
    TableDesc tableDesc = new TableDesc(storageHandler.getInputFormatClass(),
            storageHandler.getOutputFormatClass(),props);
    if (tableDesc.getJobProperties() == null) {
      tableDesc.setJobProperties(new HashMap<String, String>());
    }

    Properties mytableProperties = tableDesc.getProperties();
    mytableProperties.setProperty(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_NAME,inputJobInfo.getDatabaseName()+ "." + inputJobInfo.getTableName());

    Map<String, String> jobProperties = new HashMap<String, String>();
    try {
      tableDesc.getJobProperties().put(
              HCatConstants.HCAT_KEY_JOB_INFO,
              HCatRSUtil.serialize(inputJobInfo));

      storageHandler.configureInputJobProperties(tableDesc,
              jobProperties);

    } catch (IOException e) {
      throw new IllegalStateException(
              "Failed to configure StorageHandler", e);
    }

    return jobProperties;
  }

  public static HiveStorageHandler getStorageHandler(Configuration conf, PartInfo partitionInfo) throws IOException {
    return HCatUtil.getStorageHandler(
            conf,
            partitionInfo.getStorageHandlerClassName(),
            partitionInfo.getSerdeClassName(),
            partitionInfo.getInputFormatClassName(),
            partitionInfo.getOutputFormatClassName());
  }


  public static String serialize(Serializable obj) throws IOException {
    if (obj == null) {
      return "";
    }
    try {
      ByteArrayOutputStream serialObj = new ByteArrayOutputStream();
      ObjectOutputStream objStream = new ObjectOutputStream(serialObj);
      objStream.writeObject(obj);
      objStream.close();
      return encodeBytes(serialObj.toByteArray());
    } catch (Exception e) {
      throw new IOException("Serialization error: " + e.getMessage(), e);
    }
  }

  public static Object deserialize(String str) throws IOException {
    if (str == null || str.length() == 0) {
      return null;
    }
    try {
      ByteArrayInputStream serialObj = new ByteArrayInputStream(
        decodeBytes(str));
      ObjectInputStream objStream = new ObjectInputStream(serialObj);
      return objStream.readObject();
    } catch (Exception e) {
      throw new IOException("Deserialization error: " + e.getMessage(), e);
    }
  }

  public static String encodeBytes(byte[] bytes) {
    return new String(Base64.encodeBase64(bytes, false, false));
  }

  public static byte[] decodeBytes(String str) {
    return Base64.decodeBase64(str.getBytes());
  }

  public static void copyCredentialsToJobConf(Credentials cred, JobConf jobConf){
      jobConf.setCredentials(cred);
  }

}
