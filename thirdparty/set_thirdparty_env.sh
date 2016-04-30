#!/usr/bin/env bash

SOURCE_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
source $SOURCE_DIR/versions.sh

if [ -z "$THIRDPARTY_DIR" ]; then
	THIRDPARTY_DIR=$SOURCE_DIR
fi

export SNAPPY_HOME=$THIRDPARTY_DIR/installed
export LZ4_HOME=$THIRDPARTY_DIR/installed
export ZLIB_HOME=$THIRDPARTY_DIR/installed
# build script doesn't support building thrift on OSX
if [ "$(uname)" != "Darwin" ]; then
  export THRIFT_HOME=$THIRDPARTY_DIR/installed
fi

export GTEST_HOME=$THIRDPARTY_DIR/$GTEST_BASEDIR
export GBENCHMARK_HOME=$THIRDPARTY_DIR/installed
