#!/bin/bash
# Builds impala, with output redirected to $WORKSPACE/buildall.log
# TARGET_BUILD_TYPE can be set to Release/Debug

pushd $IMPALA_HOME

rm -f ./bin/version.info
rm -f ./CMakeCache.txt

echo
echo "********************************************************************************"
echo " Running buildall.sh "
echo "********************************************************************************"
echo

BUILD_ARGS="-notests -skiptests"

# Get the HDFS version. If this changes, we need to format. 
HDFS_VERSION=`/bin/grep layoutVersion $CLUSTER_DIR/cdh5/node-1/data/dfs/nn/current/VERSION | cut -d= -f2`

if [[ $CLEAN ]] && $CLEAN
then
  echo ">>> Cleaning workspace"
  git clean -dfx && git reset --hard HEAD
  BUILD_ARGS="$BUILD_ARGS -format_metastore"
fi

# Version for CDH5.4 is -60
if [ $HDFS_VERSION -ne -60 ]
then
  # This is a somewhat general hack for additional steps that need to be taken when
  # upgrading CDH
  echo ">>> Formatting HDFS"
  ./bin/build_thirdparty.sh
  BUILD_ARGS="$BUILD_ARGS -format"
fi

echo "Build Args: $BUILD_ARGS"

./buildall.sh $BUILD_ARGS \
      > $WORKSPACE/buildall.log 2>&1 || { echo "buildall.sh failed"; exit 1; }

pushd $RECORD_SERVICE_HOME
make
popd

popd

echo
echo "********************************************************************************"
echo " buildall.sh succeeded"
echo "********************************************************************************"
echo
