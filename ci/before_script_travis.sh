#!/usr/bin/env bash

# Build an isolated thirdparty
cp -r $TRAVIS_BUILD_DIR/thirdparty .
./thirdparty/download_thirdparty.sh

if [ $TRAVIS_OS_NAME == "osx" ]; then
  brew update > /dev/null
  brew install thrift
else
  # Use a C++11 compiler on Linux
  export CC="gcc-4.9"
  export CXX="g++-4.9"
fi

export PARQUET_TEST_DATA=$TRAVIS_BUILD_DIR/data
source $TRAVIS_BUILD_DIR/setup_build_env.sh
