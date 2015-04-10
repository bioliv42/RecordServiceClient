This repro is temporarily in the Impala repo to make it easier to build
and get going. It should be moved out eventually.

To build:
cd IMPALA_HOME
bin/make_debug or bin/make_release -notests

This builds Impala with the RecordServices services enabled.

Inside the record-service directory, we build the thrift client jars as well
as some sample clients.

api/: Thrift file(s) containing the RecordService API definition
cpp/: cpp sample and client code
java/: java sample and client code
