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

set -e
set -x

ROOT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)

mkdir coverage_artifacts
python $ROOT_DIR/../build-support/collect_coverage.py CMakeFiles/parquet.dir/src/ coverage_artifacts

cd coverage_artifacts

ls -l

echo $PARQUET_ROOT

coveralls --gcov $(which gcov-4.9) \
    --gcov-options '\-l' --root '' \
    --include $PARQUET_ROOT \
    --exclude $PARQUET_ROOT/parquet-build/thirdparty \
    --exclude $PARQUET_ROOT/parquet-build/arrow_ep \
    --exclude $PARQUET_ROOT/parquet-build/brotli_ep \
    --exclude $PARQUET_ROOT/parquet-build/brotli_ep-prefix \
    --exclude $PARQUET_ROOT/parquet-build/gbenchmark_ep \
    --exclude $PARQUET_ROOT/parquet-build/googletest_ep-prefix \
    --exclude $PARQUET_ROOT/parquet-build/snappy_ep \
    --exclude $PARQUET_ROOT/parquet-build/snappy_ep-prefix \
    --exclude $PARQUET_ROOT/parquet-build/zlib_ep \
    --exclude $PARQUET_ROOT/parquet-build/zlib_ep-prefix \
    --exclude $PARQUET_ROOT/build \
    --exclude $PARQUET_ROOT/src/parquet/thrift \
    --exclude /usr
