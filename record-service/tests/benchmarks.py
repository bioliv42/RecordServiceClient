#!/usr/bin/env python
# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This contains descriptions of all the benchmarks. They are broken up into
# suites.

import os

def impala_shell_cmd(query):
  return os.environ['IMPALA_HOME'] + "/bin/impala-shell.sh -B -q \"" + query + "\""

def native_client_cmd(query):
  return os.environ['RECORD_SERVICE_HOME'] +\
      "/cpp/build/release/recordservice/record-service-client \"" +\
      query + "\""

def java_client_cmd(query):
  return "mvn -f " + os.environ['RECORD_SERVICE_HOME'] +\
      "/java/sample/pom.xml exec:java " + \
      "-Dexec.mainClass=com.cloudera.recordservice.sample.SampleClientLib " +\
      "-Dexec.args=\"'" + query + "'\""


benchmarks = [
  [
    # Metadata about this suite. "local" indicates this benchmark should only be
    # run on a single node.
    "Query 1 (Text/6gb)", "local",
    [
      # Each case to run. The first element is the name of the application and
      # the second is the shell command to run to run the benchmark
      ["impala", impala_shell_cmd("select sum(l_partkey) from tpch6gb.lineitem")],
      ["native-client", native_client_cmd("select l_partkey from tpch6gb.lineitem")],
      ["java-client", java_client_cmd("select l_partkey from tpch6gb.lineitem")],
    ]
  ],

  [
    "Query 1 (Parquet/6gb)", "local",
    [
      ["impala", impala_shell_cmd("select sum(l_partkey) from tpch6gb_parquet.lineitem")],
      ["native-client", native_client_cmd(
          "select l_partkey from tpch6gb_parquet.lineitem")],
      ["java-client", java_client_cmd("select l_partkey from tpch6gb_parquet.lineitem")],
    ]
  ],

]
