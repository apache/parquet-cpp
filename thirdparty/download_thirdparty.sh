#!/bin/bash

set -x
set -e

TP_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
cd $TP_DIR

source versions.sh

if [ ! -d snappy-${SNAPPY_VERSION} ]; then
  echo "Fetching snappy"
  curl -OC - http://snappy.googlecode.com/files/snappy-${SNAPPY_VERSION}.tar.gz
  tar xzf snappy-${SNAPPY_VERSION}.tar.gz
  rm snappy-${SNAPPY_VERSION}.tar.gz
fi

if [ ! -d snappy-${LZ4_VERSION} ]; then
  echo "Fetching lz4"
  wget https://github.com/Cyan4973/lz4/archive/${LZ4_VERSION}.tar.gz
  mv ${LZ4_VERSION}.tar.gz lz4-${LZ4_VERSION}.tar.gz
  tar xvzf lz4-${LZ4_VERSION}.tar.gz
  rm lz4-${LZ4_VERSION}.tar.gz
fi


