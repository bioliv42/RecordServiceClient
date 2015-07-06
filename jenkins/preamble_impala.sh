#!/bin/bash
# Copyright (c) 2015, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.

# Sets up a common environment for Jenkins builds.

# Find all java processes by this user that aren't slave.jar-related, and if
# those processes' parents are 1, kill them to death.
echo ">>> Killing left over processes"
(ps -fe -u $USER |grep java|grep -v grep |grep -v "slave.jar" |awk '{ if ($3 ~ "1") { print $2 } }'|xargs kill -9)

export IMPALA_HOME=$WORKSPACE/repos/Impala
#export IMPALA_LZO=$WORKSPACE/repos/Impala-lzo
export HADOOP_LZO=$WORKSPACE/repos/hadoop-lzo

export LLVM_HOME=/opt/toolchain/llvm-3.3
export PATH=$LLVM_HOME/bin:$PATH

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

echo "********************************************************************************"
echo " Environment setup for impala complete, build proper follows"
echo "********************************************************************************"
