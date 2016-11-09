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
git archive HEAD -o parquet-cpp-src.tar.gz "--remote=file://$TRAVIS_BUILD_DIR"
$TRAVIS_BUILD_DIR/dev/release/run-rat.sh parquet-cpp-src.tar.gz

pushd $CPP_BUILD_DIR

make lint

# PARQUET-626: disabled check for now
# if [ $TRAVIS_OS_NAME == "linux" ]; then
#   make check-format
#   make check-clang-tidy
# fi

if [ $TRAVIS_OS_NAME == "linux" ]; then
  make -j4 || exit 1
  ctest -VV -L unittest || { cat $TRAVIS_BUILD_DIR/parquet-build/Testing/Temporary/LastTest.log; exit 1; }
  sudo pip install cpp_coveralls
  export PARQUET_ROOT=$TRAVIS_BUILD_DIR
  $TRAVIS_BUILD_DIR/ci/upload_coverage.sh
else
  make -j4 || exit 1
  ctest -VV -L unittest || { cat $TRAVIS_BUILD_DIR/parquet-build/Testing/Temporary/LastTest.log; exit 1; }
fi

popd
