#!/usr/bin/env bash

set -e

: ${CPP_BUILD_DIR=$TRAVIS_BUILD_DIR/parquet-build}

pushd $CPP_BUILD_DIR

make lint
if [ $TRAVIS_OS_NAME == "linux" ]; then
  make check-format
  make check-clang-tidy
fi

if [ $TRAVIS_OS_NAME == "linux" ]; then
  make -j4 || exit 1
  ctest || { cat $TRAVIS_BUILD_DIR/parquet-build/Testing/Temporary/LastTest.log; exit 1; }
  sudo pip install cpp_coveralls
  export PARQUET_ROOT=$TRAVIS_BUILD_DIR
  $TRAVIS_BUILD_DIR/ci/upload_coverage.sh
else
  make -j4 || exit 1
  ctest || { cat $TRAVIS_BUILD_DIR/parquet-build/Testing/Temporary/LastTest.log; exit 1; }
fi

popd
