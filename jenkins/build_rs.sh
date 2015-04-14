#!/bin/bash
# Builds impala, with output redirected to $WORKSPACE/buildall.log
# TARGET_BUILD_TYPE can be set to Release/Debug

pushd $IMPALA_HOME
rm -f ./bin/version.info
rm -f ./CMakeCache.txt
popd

echo
echo "********************************************************************************"
echo " Building clients."
echo "********************************************************************************"
echo

BUILD_ARGS="-notests -skiptests"

# Get the HDFS version. If this changes, we need to format. 
HDFS_VERSION=`/bin/grep layoutVersion $CLUSTER_DIR/cdh5/node-1/data/dfs/nn/current/VERSION | cut -d= -f2`

if [[ $CLEAN ]] && $CLEAN
then
  echo ">>> Cleaning workspace"
  pushd $IMPALA_HOME
  git clean -dfx && git reset --hard HEAD
  BUILD_ARGS="$BUILD_ARGS -format_metastore"
  popd
  
  pushd $RECORD_SERVICE_HOME
  git clean -dfx && git reset --hard HEAD
  popd
fi

# Version for CDH5.4 is -60
if [ $HDFS_VERSION -ne -60 ]
then
  # This is a somewhat general hack for additional steps that need to be taken when
  # upgrading CDH
  pushd $RECORD_SERVICE_HOME
  echo ">>> Formatting HDFS"
  ./bin/build_thirdparty.sh
  BUILD_ARGS="$BUILD_ARGS -format"
  popd
fi

echo "********************************************************************************"
echo " building RecordService client."
echo "********************************************************************************"
pushd $RECORD_SERVICE_HOME
./thirdparty/download_thirdparty.sh
./thirdparty/build_thirdparty.sh
cmake .
make
pushd $RECORD_SERVICE_HOME/java
mvn package -DskipTests
popd

