#!/bin/bash
# Copyright (c) 2015, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.

# Loads data and starts the cluster to run local tests

# Builds record service client.
source $WORKSPACE/repos/RecordServiceClient/jenkins/preamble_rs.sh
cd $RECORD_SERVICE_HOME
make clean
# Build
. $RECORD_SERVICE_HOME/jenkins/build_rs.sh

# Builds record service server
source $WORKSPACE/repos/RecordServiceClient/jenkins/preamble_impala.sh
# Build
. $RECORD_SERVICE_HOME/jenkins/build_impala.sh

echo ">>> Starting all services"
cd $IMPALA_HOME
. testdata/bin/run-all.sh

echo ">>> Loading test data"
. $RECORD_SERVICE_HOME/tests/load-test-data.sh
echo ">>> Running tests"
. $RECORD_SERVICE_HOME/tests/run-all-tests.sh

