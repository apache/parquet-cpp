#!/bin/bash

set -e
set -x

cd $RECIPE_DIR

# Build dependencies
export BOOST_ROOT=$PREFIX

export SNAPPY_HOME=$PREFIX
export THRIFT_HOME=$PREFIX
export ZLIB_HOME=$PREFIX

cd ..

mkdir conda-build

cp -r thirdparty conda-build/

# For running the unit tests
export PARQUET_TEST_DATA=`pwd`/data

cd conda-build
pwd

# Build googletest for running unit tests

# Work around conda certificate failure
export PARQUET_INSECURE_CURL=1

./thirdparty/download_thirdparty.sh

./thirdparty/build_thirdparty.sh gtest

source thirdparty/versions.sh
export GTEST_HOME=`pwd`/thirdparty/$GTEST_BASEDIR

cmake \
    -DCMAKE_BUILD_TYPE=release \
    -DCMAKE_INSTALL_PREFIX=$PREFIX \
    -DPARQUET_BUILD_BENCHMARKS=off \
    ..

make
ctest -L unittest
make install
