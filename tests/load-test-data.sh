#!/usr/bin/env bash
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
#
# Loads the test tables.

# Exit on reference to uninitialized variables and non-zero exit codes
set -u
set -e

# Copy the TPC-H dataset
DATASRC="http://util-1.ent.cloudera.com/impala-test-data/"
DATADST=${IMPALA_HOME}/testdata/impala-data

if [ ! -d ${DATADST}/tpch ]; then
  mkdir -p ${DATADST}
  pushd ${DATADST}
  wget -q --no-clobber http://util-1.ent.cloudera.com/impala-test-data/tpch.tar.gz
  tar -xzf tpch.tar.gz
  popd
fi

# Start up Impala
cd $IMPALA_HOME
. ${IMPALA_HOME}/bin/set-pythonpath.sh

bin/start-impala-cluster.py -s 1 --catalogd_args="-load_catalog_in_background=false"

# Load the test data we need.
cd $RECORD_SERVICE_HOME
impala-shell.sh -f tests/create-tbls.sql

# Move any existing data files to where they need to go in HDFS
hadoop fs -put -f $IMPALA_HOME/testdata/impala-data/tpch/nation/*\
    /test-warehouse/tpch.nation/

# Invalidate metadata after all data is moved.
impala-shell.sh -q "invalidate metadata"
