#!/bin/bash
# Copyright (c) 2015, Cloudera, inc.
# Confidential Cloudera Information: Covered by NDA.

# Builds impala, with output redirected to $WORKSPACE/buildall.log
# TARGET_BUILD_TYPE can be set to Release/Debug

echo "********************************************************************************"
echo " building RecordService daemons."
echo "********************************************************************************"

pushd $IMPALA_HOME
rm -f ./bin/version.info
rm -f ./CMakeCache.txt
echo "Build Args: $BUILD_ARGS"

if [ ! -d "$IMPALA_CYRUS_SASL_INSTALL_DIR" ]; then
  echo "Building sasl"
  bin/build_thirdparty.sh -sasl
fi

./buildall.sh $BUILD_ARGS > $WORKSPACE/buildall.log 2>&1 ||\
    { tail -n 100 $WORKSPACE/buildall.log; echo "buildall.sh failed"; exit 1; }
popd

