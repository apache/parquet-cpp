# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

ARROW_VERSION="d946e7917d55cb220becd6469ae93430f2e60764"
ARROW_URL="https://github.com/apache/arrow/archive/${ARROW_VERSION}.tar.gz"
ARROW_BASEDIR="arrow-${ARROW_VERSION}"

BROTLI_VERSION="5db62dcc9d386579609540cdf8869e95ad334bbd"
BROTLI_URL="https://github.com/google/brotli/archive/${BROTLI_VERSION}.tar.gz"
BROTLI_BASEDIR="brotli-${BROTLI_VERSION}"

SNAPPY_VERSION=1.1.3
SNAPPY_URL="https://github.com/google/snappy/releases/download/${SNAPPY_VERSION}/snappy-${SNAPPY_VERSION}.tar.gz"
SNAPPY_BASEDIR=snappy-$SNAPPY_VERSION

THRIFT_VERSION=0.9.1
THRIFT_URL="http://archive.apache.org/dist/thrift/${THRIFT_VERSION}/thrift-${THRIFT_VERSION}.tar.gz"
THRIFT_BASEDIR=thrift-$THRIFT_VERSION

GBENCHMARK_VERSION=1.0.0
GBENCHMARK_URL="https://github.com/google/benchmark/archive/v${GBENCHMARK_VERSION}.tar.gz"
GBENCHMARK_BASEDIR=benchmark-$GBENCHMARK_VERSION

GTEST_VERSION=1.7.0
GTEST_URL="https://github.com/google/googletest/archive/release-${GTEST_VERSION}.tar.gz"
GTEST_BASEDIR=googletest-release-$GTEST_VERSION

ZLIB_VERSION=1.2.8
ZLIB_URL=http://zlib.net/zlib-${ZLIB_VERSION}.tar.gz
ZLIB_BASEDIR=zlib-${ZLIB_VERSION}
