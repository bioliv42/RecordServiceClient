#!/bin/bash

set -x
set -e

TP_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
cd $TP_DIR

source versions.sh

if [ ! -d gflags-${GFLAGS_VERSION} ]; then
  echo "Fetching gflags"
  wget -O gflags-${GFLAGS_VERSION}.zip https://github.com/gflags/gflags/archive/v${GFLAGS_VERSION}.zip
  unzip gflags-${GFLAGS_VERSION}.zip
  rm gflags-${GFLAGS_VERSION}.zip
fi

if [ ! -d gtest-${GTEST_VERSION} ]; then
  echo "Fetching gtest"
  curl -OC - https://googletest.googlecode.com/files/gtest-${GTEST_VERSION}.zip
  unzip gtest-${GTEST_VERSION}.zip
  rm gtest-${GTEST_VERSION}.zip
fi
