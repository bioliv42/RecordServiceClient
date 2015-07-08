#!/bin/bash
# Copyright (c) 2015, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.

# Sets up a common environment for Jenkins builds.

# Find all java processes by this user that aren't slave.jar-related, and if
# those processes' parents are 1, kill them to death.
echo ">>> Killing left over processes"
(ps -fe -u $USER |grep java|grep -v grep |grep -v "slave.jar" |awk '{ if ($3 ~ "1") { print $2 } }'|xargs kill -9)

set -x

export IMPALA_HOME=$WORKSPACE/repos/Impala
#export IMPALA_LZO=$WORKSPACE/repos/Impala-lzo
export HADOOP_LZO=$WORKSPACE/repos/hadoop-lzo

export LLVM_HOME=/opt/toolchain/llvm-3.3
export PATH=$LLVM_HOME/bin:$PATH

if [ -n "$JENKINS_USE_TOOLCHAIN" ]; then
  export IMPALA_TOOLCHAIN=$IMPALA_HOME/toolchain/build
fi

export LD_LIBRARY_PATH=""
export LD_PRELOAD=""
# Re-source impala-config since JAVA_HOME has changed.
cd $IMPALA_HOME
. bin/impala-config.sh &> /dev/null

export PATH=/usr/lib/ccache:$PATH
export BOOST_ROOT=/opt/toolchain/boost-pic-1.55.0/
export ASAN_OPTIONS="handle_segv=0"

echo ">>> Shell environment"
env
java -version
ulimit -a

if [ -n "$JENKINS_USE_TOOLCHAIN" ]; then
  echo ">>> Bootstrapping toolchain."
  cd $RECORD_SERVICE_HOME
  python jenkins/bootstrap_toolchain.py \
    gcc-$IMPALA_GCC_VERSION \
    avro-$IMPALA_AVRO_VERSION \
    boost-$IMPALA_BOOST_VERSION \
    breakpad-$IMPALA_BREAKPAD_VERSION \
    bzip2-$IMPALA_BZIP2_VERSION \
    cyrus-sasl-$IMPALA_CYRUS_SASL_VERSION \
    gcc-$IMPALA_GCC_VERSION \
    gflags-$IMPALA_GFLAGS_VERSION \
    glog-$IMPALA_GLOG_VERSION \
    gperftools-$IMPALA_GPERFTOOLS_VERSION \
    gtest-$IMPALA_GTEST_VERSION \
    llvm-$IMPALA_LLVM_VERSION \
    llvm-trunk \
    lz4-$IMPALA_LZ4_VERSION \
    openldap-$IMPALA_OPENLDAP_VERSION \
    openssl-$IMPALA_OPENSSL_VERSION \
    rapidjson-$IMPALA_RAPIDJSON_VERSION \
    re2-$IMPALA_RE2_VERSION \
    snappy-$IMPALA_SNAPPY_VERSION \
    thrift-$IMPALA_THRIFT_VERSION \
    zlib-$IMPALA_ZLIB_VERSION \
  || { echo "toolchain bootstrap failed"; exit 1; }
fi

echo "********************************************************************************"
echo " Environment setup for impala complete, build proper follows"
echo "********************************************************************************"
