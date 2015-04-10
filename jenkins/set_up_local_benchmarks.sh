#!/bin/bash
# Loads data and starts the cluster to run local benchmarks.

# Setup the environment
export TARGET_BUILD_TYPE=Release
source $WORKSPACE/repos/Impala/record-service/jenkins/preamble.sh

cd $IMPALA_HOME/record-service
make clean

# Build
. $RECORD_SERVICE_HOME/jenkins/build.sh

echo ">>> Starting all services "
cd $IMPALA_HOME
. testdata/bin/run-all.sh || { echo "run-all.sh failed"; exit 1; }

cd $IMPALA_HOME
. ${IMPALA_HOME}/bin/set-pythonpath.sh

set -e

# Move the jars to where we need them
find $RECORD_SERVICE_HOME/java -name "*.jar" -exec cp '{}' ${HIVE_AUX_JARS_PATH} \;
hadoop fs -mkdir -p ${HIVE_AUX_JARS_PATH}
hadoop fs -put -f ${HIVE_AUX_JARS_PATH}/*.jar ${HIVE_AUX_JARS_PATH}

if $LOAD_DATA; then
  # Load test data
  echo ">>> Starting impala and loading benchmark data"

  # Copy the TPC-H dataset
  DATASRC="http://util-1.ent.cloudera.com/impala-test-data/"
  DATADST=${IMPALA_HOME}/testdata/impala-data

  mkdir -p ${DATADST}
  pushd ${DATADST}
  wget -q --no-clobber http://util-1.ent.cloudera.com/impala-test-data/tpch6gb.tar.gz
  wget -q --no-clobber http://util-1.ent.cloudera.com/impala-test-data/tpch6gb_avro_snap.tar.gz
  tar -xzf tpch6gb.tar.gz
  tar -xf tpch6gb_avro_snap.tar.gz
  popd

  cd $IMPALA_HOME/record-service/tests
  ./load-benchmark-data.sh
else
  echo ">>> Starting impala"
  cd $IMPALA_HOME
  bin/start-impala-cluster.py -s 1 --build_type=release --catalogd_args="-load_catalog_in_background=false"
fi

cd $IMPALA_HOME/record-service/java
mvn install -DskipTests

echo ">>> Impala version"
cd $IMPALA_HOME
bin/impala-shell.sh -q "select version()"

