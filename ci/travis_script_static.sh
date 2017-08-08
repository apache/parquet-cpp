#!/usr/bin/env bash

#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.

set -xe

# Use a C++11 compiler on Linux
export CC="gcc-4.9"
export CXX="g++-4.9"

# ----------------------------------------------------------------------
# Set up external toolchain

MINICONDA_URL="https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh"
export MINICONDA=$HOME/miniconda
wget -O miniconda.sh $MINICONDA_URL
bash miniconda.sh -b -p $MINICONDA
export PATH="$MINICONDA/bin:$PATH"
export CPP_TOOLCHAIN=$TRAVIS_BUILD_DIR/cpp-toolchain

conda update -y -q conda
conda config --set auto_update_conda false
conda info -a

conda config --set show_channel_urls True

# Help with SSL timeouts to S3
conda config --set remote_connect_timeout_secs 12

conda info -a

conda create -y -q -p $CPP_TOOLCHAIN \
      boost-cpp thrift-cpp cmake git \
      -c conda-forge

source activate $CPP_TOOLCHAIN

# ----------------------------------------------------------------------

: ${CPP_BUILD_DIR=$TRAVIS_BUILD_DIR/parquet-build}
export PARQUET_TEST_DATA=$TRAVIS_BUILD_DIR/data
export PARQUET_BUILD_TOOLCHAIN=$CPP_TOOLCHAIN
export LD_LIBRARY_PATH=$CPP_TOOLCHAIN/lib:$LD_LIBRARY_PATH
export BOOST_ROOT=$CPP_TOOLCHAIN
export PARQUET_TEST_DATA=$TRAVIS_BUILD_DIR/data

ARROW_EP=$TRAVIS_BUILD_DIR/parquet-build/arrow_ep-prefix/src/arrow_ep-build
BROTLI_EP=$ARROW_EP/brotli_ep/src/brotli_ep-install/lib/x86_64-linux-gnu

export SNAPPY_STATIC_LIB=$ARROW_EP/snappy_ep/src/snappy_ep-install/lib/libsnappy.a
export BROTLI_STATIC_LIB_ENC=$BROTLI_EP/libbrotlienc.a
export BROTLI_STATIC_LIB_DEC=$BROTLI_EP/libbrotlidec.a
export BROTLI_STATIC_LIB_COMMON=$BROTLI_EP/libbrotlicommon.a
export ZLIB_STATIC_LIB=$ARROW_EP/zlib_ep/src/zlib_ep-install/lib/libz.a

cmake -DPARQUET_CXXFLAGS="$PARQUET_CXXFLAGS" \
      -DPARQUET_TEST_MEMCHECK=ON \
      -DPARQUET_ARROW_LINKAGE="static" \
      -DPARQUET_BUILD_SHARED=OFF \
      -DPARQUET_BOOST_USE_SHARED=OFF \
      -DPARQUET_BUILD_BENCHMARKS=ON \
      -DPARQUET_BUILD_EXAMPLES=ON \
      -DPARQUET_GENERATE_COVERAGE=1 \
      $TRAVIS_BUILD_DIR

pushd $CPP_BUILD_DIR

make -j4 VERBOSE=1 || exit 1
ctest -VV -L unittest || { cat $TRAVIS_BUILD_DIR/parquet-build/Testing/Temporary/LastTest.log; exit 1; }

popd
