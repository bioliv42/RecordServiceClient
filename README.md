Copyright (c) 2015, Cloudera, inc.

This repro contains the service API and the client integration.

## Building
Prereqs:
- Thrift. The thrift compiler needs to be on the path. Any 0.9.* version will work.

To build this repo the first time:

    source $RECORD_SERVICE_HOME/config.sh
    thirdparty/download_thirdparty.sh
    thirdparty/build_thirdparty.sh
    cd $RECORD_SERVICE_HOME
    cmake .
    make
    cd $RECORD_SERVICE_HOME/java
    mvn package -DskipTests

The thirdparty steps only need to be done once initially.

On OSX:
The above steps will work but the C++ artifacts will not be built.

## Repo structure:
- api/: Thrift file(s) containing the RecordService API definition
- cpp/: cpp sample and client code
- java/: java sample and client code
- tests/: Scripts to load test data, run tests and run benchmarks.
- jenkins/: Scripts intended to be run from jenkins builds.


## Running tests.
To run the tests against a running server with the data loaded, you can do:
export RECORD_SERVICE_PLANNER_HOST=<server name>
cd java
mvn package


To run the server:
After this step, you will want to build the daemons which are based on the Impala repo.

Follow the Impala instructions there. The scripts below generally require 
RECORD_SERVICE_HOME and IMPALA_HOME to be set.

First start up the local CDH cluster. This is done with

    $IMPALA_HOME/testdata/bin/run-all.sh

If you've never started HDFS before, you will need to pass -format to run-all.sh.

Then,
Loading the test tables:

    $RECORD_SERVICE_HOME/tests/load-test-data.sh

Running the tests:

    $RECORD_SERVICE_HOME/tests/run-all-tests.sh

## Running the cluster
The above scripts run the services that the record service needs. To start without
the script, run

    $IMPALA_HOME/bin/start-impala-cluster.py -s 1

which will start catalogd, statestored and impalad (which implements the
RecordService APIs).

