#!/bin/bash
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

# Copy the TPC-H dataset
DATASRC="http://util-1.ent.cloudera.com/impala-test-data/"
DATADST=${IMPALA_HOME}/testdata/impala-data

mkdir -p ${DATADST}
pushd ${DATADST}
wget -q --no-clobber http://util-1.ent.cloudera.com/impala-test-data/tpch.tar.gz
tar -xzf tpch.tar.gz
popd

echo ">>> Starting all services"
cd $IMPALA_HOME
. testdata/bin/run-all.sh

echo ">>> Loading test data"
. $RECORD_SERVICE_HOME/tests/load-test-data.sh
echo ">>> Running tests"
. $RECORD_SERVICE_HOME/tests/run-all-tests.sh

