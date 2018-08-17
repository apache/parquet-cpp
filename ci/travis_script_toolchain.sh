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
      boost-cpp thrift-cpp=0.11.0 cmake git \
      -c conda-forge

# ----------------------------------------------------------------------

: ${CPP_BUILD_DIR=$TRAVIS_BUILD_DIR/parquet-build}
export PARQUET_TEST_DATA=$TRAVIS_BUILD_DIR/data
export PARQUET_BUILD_TOOLCHAIN=$CPP_TOOLCHAIN
export LD_LIBRARY_PATH=$CPP_TOOLCHAIN/lib:$LD_LIBRARY_PATH
export BOOST_ROOT=$CPP_TOOLCHAIN

CMAKE_COMMON_FLAGS="-DPARQUET_BUILD_WARNING_LEVEL=CHECKIN"

if [ $PARQUET_TRAVIS_VALGRIND == "1" ]; then
  CMAKE_COMMON_FLAGS="$CMAKE_COMMON_FLAGS -DPARQUET_TEST_MEMCHECK=ON"
fi

cmake $CMAKE_COMMON_FLAGS \
      -DPARQUET_CXXFLAGS=-Werror \
      -DPARQUET_GENERATE_COVERAGE=1 \
      -DCMAKE_INSTALL_PREFIX=$CPP_TOOLCHAIN \
      $TRAVIS_BUILD_DIR

pushd $CPP_BUILD_DIR

make -j4
make install
ctest -j2 -VV -L unittest

popd

# Build and run the parquet::arrow example. This also tests the usage of parquet-cpp as a library.

pushd $TRAVIS_BUILD_DIR/examples/parquet-arrow
mkdir build
pushd build

export ARROW_HOME=$CPP_TOOLCHAIN
export PARQUET_HOME=$CPP_TOOLCHAIN

cmake ..
make VERBOSE=1
./parquet-arrow-reader-writer

popd
popd
