#!/usr/bin/env bash

SOURCE_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
: ${BUILD_DIR:=$SOURCE_DIR/build}

# Create an isolated thirdparty
mkdir -p $BUILD_DIR
cp -r $SOURCE_DIR/thirdparty $BUILD_DIR
pushd $BUILD_DIR

./thirdparty/download_thirdparty.sh
./thirdparty/build_thirdparty.sh
source thirdparty/set_thirdparty_env.sh

export PARQUET_TEST_DATA=$SOURCE_DIR/data

# Thrift needs to be installed from brew on OS X
if [ "$(uname)" == "Darwin" ]; then
  export -n THRIFT_HOME
fi

popd

echo
echo "Build env initialized in $BUILD_DIR."
