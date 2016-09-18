#!/usr/bin/env bash

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
