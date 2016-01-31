#!/bin/bash

SOURCE_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
: ${BUILD_DIR:=$SOURCE_DIR/build}

mkdir -p $BUILD_DIR
cp -r $SOURCE_DIR/thirdparty $BUILD_DIR
cd $BUILD_DIR
./thirdparty/download_thirdparty.sh
./thirdparty/build_thirdparty.sh
source thirdparty/versions.sh

export SNAPPY_HOME=$BUILD_DIR/thirdparty/installed
export LZ4_HOME=$BUILD_DIR/thirdparty/installed
export ZLIB_HOME=$BUILD_DIR/thirdparty/installed/zlib
# build script doesn't support building thrift on OSX
if [ "$(uname)" != "Darwin" ]; then
  export THRIFT_HOME=$BUILD_DIR/thirdparty/installed
fi

export PARQUET_TEST_DATA=$SOURCE_DIR/data
export GTEST_HOME=$BUILD_DIR/thirdparty/$GTEST_BASEDIR

cmake $SOURCE_DIR

cd $SOURCE_DIR

echo
echo "Build env initialized in $BUILD_DIR."
