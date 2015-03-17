#!/bin/bash
# Loads data and starts the cluster to run local benchmarks.

# Builds and runs the local tests.
source $WORKSPACE/repos/Impala/record-service/jenkins/preamble.sh

cd $IMPALA_HOME/record-service
make clean

# Build
. $RECORD_SERVICE_HOME/jenkins/build.sh

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

