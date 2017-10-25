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

coveralls --gcov 'gcov-4.9' --gcov-options '\-lp' \
          -r $PARQUET_ROOT \
          -i '/src' \
          -i '$PARQUET_ROOT/src' \
          -e '/parquet-build' \
          -e '$PARQUET_ROOT/parquet-build' \
          -e '/usr' \
          -e '$PARQUET_ROOT/src/parquet/thrift'
