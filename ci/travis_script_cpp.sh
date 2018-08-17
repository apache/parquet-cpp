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

: ${CPP_BUILD_DIR=$TRAVIS_BUILD_DIR/parquet-build}

# Check licenses according to Apache policy
pushd $TRAVIS_BUILD_DIR
git archive HEAD -o parquet-cpp-src.tar.gz
$TRAVIS_BUILD_DIR/dev/release/run-rat.sh parquet-cpp-src.tar.gz
popd

pushd $CPP_BUILD_DIR

make lint

# PARQUET-626: disabled check for now
# if [ $TRAVIS_OS_NAME == "linux" ]; then
#   make check-format
#   make check-clang-tidy
# fi

if [ $TRAVIS_OS_NAME == "linux" ]; then
  make -j4
  ctest -j2 -VV -L unittest
# Current cpp-coveralls version 0.4 throws an error (PARQUET-1075) on Travis CI. Pin to last working version
  sudo pip install cpp_coveralls==0.3.12
  export PARQUET_ROOT=$TRAVIS_BUILD_DIR
  $TRAVIS_BUILD_DIR/ci/upload_coverage.sh
else
  make -j4
  BUILD_TYPE=debug
  EXECUTABLE_DIR=$CPP_BUILD_DIR/$BUILD_TYPE
  export LD_LIBRARY_PATH=$EXECUTABLE_DIR:$LD_LIBRARY_PATH
  ctest -j2 -VV -L unittest
fi

popd
