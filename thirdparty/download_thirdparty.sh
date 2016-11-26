#!/usr/bin/env bash

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

set -x
set -e

TP_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

source $TP_DIR/versions.sh

: ${PARQUET_INSECURE_CURL=0}

download_extract_and_cleanup() {
    filename=$TP_DIR/$(basename "$1")
    if [ "$PARQUET_INSECURE_CURL" == "1" ]; then
        curl -L -k "$1" -o $filename
    else
        curl -#LC - "$1" -o $filename
    fi
    tar xzf $filename -C $TP_DIR
    rm $filename
}

if [ ! -d ${ARROW_BASEDIR} ]; then
  echo "Fetching arrow"
  download_extract_and_cleanup $ARROW_URL
fi

if [ ! -d ${BROTLI_BASEDIR} ]; then
  echo "Fetching brotli"
  download_extract_and_cleanup $BROTLI_URL
fi

if [ ! -d ${SNAPPY_BASEDIR} ]; then
  echo "Fetching snappy"
  download_extract_and_cleanup $SNAPPY_URL
fi

if [ ! -d ${GTEST_BASEDIR} ]; then
  echo "Fetching gtest"
  download_extract_and_cleanup $GTEST_URL
fi

if [ ! -d ${GBENCHMARK_BASEDIR} ]; then
  echo "Fetching gtest"
  download_extract_and_cleanup $GBENCHMARK_URL
fi

if [ ! -d ${THRIFT_BASEDIR} ]; then
  echo "Fetching thrift"
  download_extract_and_cleanup $THRIFT_URL
fi

if [ ! -d ${ZLIB_BASEDIR} ]; then
  echo "Fetching zlib"
  download_extract_and_cleanup $ZLIB_URL
fi
