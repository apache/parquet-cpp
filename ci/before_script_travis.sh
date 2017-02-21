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

if [ $TRAVIS_OS_NAME == "osx" ]; then
  brew update > /dev/null
  brew install boost
else
  # Use a C++11 compiler on Linux
  export CC="gcc-4.9"
  export CXX="g++-4.9"
fi

export PARQUET_TEST_DATA=$TRAVIS_BUILD_DIR/data

if [ $TRAVIS_OS_NAME == "linux" ]; then
    cmake -DCMAKE_CXX_FLAGS="-Werror" \
          -DPARQUET_TEST_MEMCHECK=ON \
          -DPARQUET_BUILD_BENCHMARKS=ON \
          -DPARQUET_ARROW=ON \
          -DPARQUET_ARROW_LINKAGE=static \
          -DPARQUET_GENERATE_COVERAGE=1 \
          $TRAVIS_BUILD_DIR
else
    cmake -DCMAKE_CXX_FLAGS="-Werror" \
          -DPARQUET_ARROW=ON \
          -DPARQUET_ARROW_LINKAGE=static \
          $TRAVIS_BUILD_DIR
fi
