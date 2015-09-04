#!/usr/bin/env bash
# Copyright 2012 Cloudera Inc.
# Confidential Cloudera Information: Covered by NDA.
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

# Runs all the tests.

# Exit on reference to uninitialized variables and non-zero exit codes
set -u
set -e

# Kill the cluster to run the external cluster tests.
cd $IMPALA_HOME
bin/start-impala-cluster.py --kill
source bin/set-classpath.sh

cd $RECORD_SERVICE_HOME
echo "Running mini cluster tests."
export RUN_MINI_CLUSTER_TESTS=true
make test

echo "Running non-mini cluster tests."

# Start up a recordserviced and run the client tests. Note that at this point
# there is no impala cluster running
cd $IMPALA_HOME
bin/start-impala-cluster.py -s 1 --catalogd_args="-load_catalog_in_background=false"
# TODO: update bin/start-impala-cluster.py
killall -9 impalad
killall -9 statestored
killall -9 catalogd

cd $RECORD_SERVICE_HOME
unset RUN_MINI_CLUSTER_TESTS
make test
mvn clean package -f $RECORD_SERVICE_HOME/java/pom.xml

# Start up the cluster for the tests that need an Impala cluster already running.
cd $IMPALA_HOME
bin/start-impala-cluster.py -s 1 --catalogd_args="-load_catalog_in_background=false"

# Run Hive SerDe test
cd $IMPALA_HOME/fe
mvn -Dtest=HiveSerDeExecutorTest test

cd $IMPALA_HOME/tests
./run-tests.py query_test/test_hive_serde.py
