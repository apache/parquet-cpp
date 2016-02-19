#!/usr/bin/env bash

set -e
set -x

ROOT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)

mkdir coverage_artifacts
python $ROOT_DIR/../build-support/collect_coverage.py CMakeFiles/parquet.dir/src/ coverage_artifacts

cd coverage_artifacts

ls -l

echo $PARQUET_ROOT

coveralls --gcov-options '\-l' -r $PARQUET_ROOT --exclude $PARQUET_ROOT/parquet-build/thirdparty --exclude $PARQUET_ROOT/build --exclude /usr --exclude $PARQUET_ROOT/src/parquet/thrift
