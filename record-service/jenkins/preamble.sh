#!/bin/bash
# Sets up a common environment for Jenkins builds.

echo "********************************************************************************"
echo " Building ${JOB_NAME} #${BUILD_NUMBER} "
echo " Node: ${NODE_NAME} / `hostname`"
echo " Branch: ${GIT_BRANCH}@${GIT_COMMIT}"
echo " ${BUILD_URL}"
echo " Path: ${WORKSPACE}"
echo "********************************************************************************"

# Find all java processes by this user that aren't slave.jar-related, and if
# those processes' parents are 1, kill them to death.
echo ">>> Killing left over processes"
(ps -fe -u $USER |grep java|grep -v grep |grep -v "slave.jar" |awk '{ if ($3 ~ "1") { print $2 } }'|xargs kill -9)

echo ">>> Setting up Jenkins environment..."
echo ">>> Mounting toolchain"
. /mnt/toolchain/toolchain.sh
export IMPALA_HOME=$WORKSPACE/repos/Impala
export IMPALA_LZO=$WORKSPACE/repos/Impala-lzo
export HADOOP_LZO=$WORKSPACE/repos/hadoop-lzo

unset THRIFT_HOME
unset THRIFT_CONTRIB_DIR

export LLVM_HOME=/opt/toolchain/llvm-3.3
export PATH=$LLVM_HOME/bin:$PATH

export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"

export JAVA_HOME=$JAVA7_HOME
export JAVA64_HOME=$JAVA7_HOME
export PATH=$JAVA_HOME/bin:$PATH
export LD_LIBRARY_PATH=""
export LD_PRELOAD=""
# Re-source impala-config since JAVA_HOME has changed.
cd $IMPALA_HOME
. bin/impala-config.sh &> /dev/null
export PATH=/usr/lib/ccache:$PATH
export BOOST_ROOT=/opt/toolchain/boost-pic-1.55.0/

# Enable core dumps
ulimit -c unlimited

echo ">>> Shell environment"
env
java -version
ulimit -a

echo "********************************************************************************"
echo " Environment setup complete, build proper follows"
echo "********************************************************************************"
