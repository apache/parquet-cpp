#!/bin/bash

set -x
set -e
TP_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)

source $TP_DIR/versions.sh
PREFIX=$TP_DIR/installed

################################################################################

if [ "$#" = "0" ]; then
  F_ALL=1
else
  # Allow passing specific libs to build on the command line
  for arg in "$*"; do
    case $arg in
      "lz4")        F_LZ4=1 ;;
      "snappy")     F_SNAPPY=1 ;;
      "thrift")     F_THRIFT=1 ;;
      *)            echo "Unknown module: $arg"; exit 1 ;;
    esac
  done
fi

################################################################################

# Determine how many parallel jobs to use for make based on the number of cores
if [[ "$OSTYPE" =~ ^linux ]]; then
  PARALLEL=$(grep -c processor /proc/cpuinfo)
elif [[ "$OSTYPE" == "darwin"* ]]; then
  PARALLEL=$(sysctl -n hw.ncpu)
else
  echo Unsupported platform $OSTYPE
  exit 1
fi

mkdir -p "$PREFIX/include"
mkdir -p "$PREFIX/lib"

# On some systems, autotools installs libraries to lib64 rather than lib.  Fix
# this by setting up lib64 as a symlink to lib.  We have to do this step first
# to handle cases where one third-party library depends on another.
ln -sf lib "$PREFIX/lib64"

# use the compiled tools
export PATH=$PREFIX/bin:$PATH

# build snappy
if [ -n "$F_ALL" -o -n "$F_SNAPPY" ]; then
  cd $TP_DIR/$SNAPPY_BASEDIR
  ./configure --with-pic --prefix=$PREFIX
  make -j$PARALLEL install
fi

# build lz4
if [ -n "$F_ALL" -o -n "$F_LZ4" ]; then
  cd $TP_DIR/$LZ4_BASEDIR/cmake_unofficial
  CFLAGS=-fPIC cmake -DCMAKE_INSTALL_PREFIX:PATH=$PREFIX $LZ4_DIR
  make -j$PARALLEL install
fi

# build thrift
if [ -n "$F_ALL" -o -n "$F_THRIFT" ]; then
  if [ "$(uname)" == "Darwin" ]; then
      echo "thrift compilation under OSX is not currently supported."

      # exit with an error if thrift was specified explicitly otherwise it is
      # just a warning
      if [ -n "$F_THRIFT" ]; then
        exit 1
      fi
  else
    # linux build
    # this expects all of the depedencies for thrift to already be installed in
    # such a way that ./configure can find them
    cd $TP_DIR/$THRIFT_BASEDIR
    ./configure CXXFLAGS='-fPIC' --without-qt4 --without-c_glib --without-csharp --without-java --without-erlang --without-nodejs --without-lua --without-python --without-perl --without-php --without-php_extension --without-ruby --without-haskell --without-go --without-d --with-cpp --prefix=$PREFIX
    make install
  fi
fi

echo "---------------------"
echo "Thirdparty dependencies built and installed into $PREFIX successfully"
