#!/usr/bin/env bash

if [ -n "${BASH_VERSION}" ]; then
    SOURCE_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
elif [ -n "${ZSH_VERSION}" ]; then
    SOURCE_DIR=$(cd "$(dirname "${(%):-%N}")"; pwd)
fi
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
