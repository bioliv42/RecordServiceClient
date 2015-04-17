This repro contains the service API and the client integration.

## Building
Prereqs:
- Thrift. Note if you are using the version of thrift shipped with the record service server set THRIFT_HOME=/home/{$USER}/RecordService/thirdparty/thrift-0.9.0/build/
- scala
- TODO: what are the others?

To build this repo the first time:
- source RECORD_SERVICE_HOME/config.sh
- thirdparty/download_thirdparty.sh
- thirdparty/build_thirdparty.sh
- cd $RECORD_SERVICE_HOME
- cmake .
- make
- cd $RECORD_SERVICE_HOME/java
- mvn package -DskipTests

The thirdparty steps only need to be done once initially.

After this step, you will want to build the daemons which are based on the Impala repo.
Follow the Impala instructions there. The scripts below generally require 
RECORD_SERVICE_HOME and IMPALA_HOME to be set.

## Running tests.
Loading the test tables:
- $RECORD_SERVICE_HOME/load-test-data.sh

Running the tests:
- $RECORD_SERVICE_HOME/run-all-tests.sh

## Repo structure:
- api/: Thrift file(s) containing the RecordService API definition
- cpp/: cpp sample and client code
- java/: java sample and client code
- tests/: Scripts to load test data, run tests and run benchmarks.
- jenkins/: Scripts intended to be run from jenkins builds.
