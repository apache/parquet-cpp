#!/usr/bin/env bash

set -x
set -e

TP_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)

source $TP_DIR/versions.sh

download_extract_and_cleanup() {
	filename=$TP_DIR/$(basename "$1")
	curl -#LC - "$1" -o $filename
	tar xzf $filename -C $TP_DIR
	rm $filename
}

if [ ! -d ${LZ4_BASEDIR} ]; then
  echo "Fetching lz4"
  download_extract_and_cleanup $LZ4_URL
fi

if [ ! -d ${SNAPPY_BASEDIR} ]; then
  echo "Fetching snappy"
  download_extract_and_cleanup $SNAPPY_URL
fi

if [ ! -d ${GTEST_BASEDIR} ]; then
  echo "Fetching gtest"
  download_extract_and_cleanup $GTEST_URL
fi

if [ ! -d ${THRIFT_BASEDIR} ]; then
  echo "Fetching thrift"
  download_extract_and_cleanup $THRIFT_URL
fi

if [ ! -d ${ZLIB_BASEDIR} ]; then
  echo "Fetching zlib"
  download_extract_and_cleanup $ZLIB_URL
fi
