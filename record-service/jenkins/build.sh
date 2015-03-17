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

if $CLEAN; then
  echo ">>> Cleaning workspace"
  git clean -dfx && git reset --hard HEAD
  ./buildall.sh -clean -notests -skiptests -format_metastore\
      >> $WORKSPACE/buildall.log 2>&1 || { echo "buildall.sh failed"; exit 1; }
else
  ./buildall.sh -noclean -notests -skiptests \
      >> $WORKSPACE/buildall.log 2>&1 || { echo "buildall.sh failed"; exit 1; }
fi

pushd $RECORD_SERVICE_HOME
make
popd

popd

echo
echo "********************************************************************************"
echo " buildall.sh succeeded"
echo "********************************************************************************"
echo
