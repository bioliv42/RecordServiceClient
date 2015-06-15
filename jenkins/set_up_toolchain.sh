#!/bin/bash
# Set up to get toolchain on the jenkins box. This should only build the
# first time it is run on the machine.
if [ -n "$JENKINS_USE_TOOLCHAIN" ]; then
  # Build this in a location in workspace. This will be shared by all jenkins
  # jobs on that box.
  echo "Using toolchain, building if necessary"
  mkdir -p $WORKSPACE/../RecordServiceToolChain
  cd $WORKSPACE/../RecordServiceToolChain
  git clone http://github.mtv.cloudera.com/mgrund/impala-deps.git
  cd impala-deps
  ./build.sh
  cd build
  export IMPALA_TOOLCHAIN=`pwd`
fi

