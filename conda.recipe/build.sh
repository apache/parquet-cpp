#!/bin/bash

set -e
set -x

# FIXME: This is a hack to make sure the environment is activated.
# The reason this is required is due to the conda-build issue
# mentioned below.
#
# https://github.com/conda/conda-build/issues/910
#
source activate "${CONDA_DEFAULT_ENV}"

cd $RECIPE_DIR

# Build dependencies
export BOOST_ROOT=$PREFIX

export SNAPPY_HOME=$PREFIX
export THRIFT_HOME=$PREFIX
export ZLIB_HOME=$PREFIX

cd ..

rm -rf conda-build
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

# PARQUET-638, PARQUET-489: This should be restored after symbol visibility is
# hidden by default
# if [ `uname` == Linux ]; then
#     SHARED_LINKER_FLAGS='-static-libstdc++'
# elif [ `uname` == Darwin ]; then
#     SHARED_LINKER_FLAGS=''
# fi

cmake \
    -DCMAKE_BUILD_TYPE=release \
    -DCMAKE_INSTALL_PREFIX=$PREFIX \
    -DCMAKE_SHARED_LINKER_FLAGS=$SHARED_LINKER_FLAGS \
    -DPARQUET_BUILD_BENCHMARKS=off \
    ..

make
ctest -L unittest
make install
