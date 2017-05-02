<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

## parquet-cpp: a C++ library to read and write the Apache Parquet columnar data format.

<table>
  <tr>
    <td>Build Status</td>
    <td>
    <a href="https://travis-ci.org/apache/parquet-cpp">
    <img src="https://travis-ci.org/apache/parquet-cpp.svg?branch=master" alt="travis build status" />
    </a>
    </td>
  </tr>
  <tr>
    <td>Test coverage</td>
    <td>
      <a href='https://coveralls.io/github/apache/parquet-cpp?branch=master'><img src='https://coveralls.io/repos/github/apache/parquet-cpp/badge.svg?branch=master' alt='Coverage Status' /></a>
    </td>
  </tr>
</table>

## System Dependencies

### Linux

parquet-cpp requires gcc 4.8 or higher on Linux.

To build parquet-cpp out of the box, you must install some build prerequisites
for the thirdparty dependencies. On Debian/Ubuntu, these can be installed with:

```
sudo apt-get install libboost-dev libboost-filesystem-dev \
                     libboost-program-options-dev libboost-regex-dev \
                     libboost-system-dev libboost-test-dev \
                     libssl-dev libtool bison flex pkg-config
```

### OS X

You must use XCode 6 or higher. We recommend using Homebrew to install Boost,
which is required for Thrift:

```
brew install boost
```

### Windows

Check [Windows developer guide][1] for instructions to build parquet-cpp on Windows.

## Third Party Dependencies

- Apache Arrow (memory management, built-in IO, optional Array adapters)
- snappy
- zlib
- Thrift 0.7+ [install instructions](https://thrift.apache.org/docs/install/)
- googletest 1.7.0 (cannot be installed with package managers)
- Google Benchmark (only required if building benchmarks)

You can either install these dependencies separately, otherwise they will be
built automatically as part of the build.

Symbols from Thrift, Snappy, and ZLib are statically-linked into the
`libparquet` shared library, so these dependencies must be built with `-fPIC`
on Linux and OS X. Since Linux package managers do not consistently compile the
static libraries for these components with `-fPIC`, you may have issues with
Linux packages such as `libsnappy-dev`. It may be easier to depend on the
thirdparty toolchain that parquet-cpp builds automatically.

## Build

- `cmake .`

  - You can customize build dependency locations through various environment variables:
    - ARROW_HOME customizes the Apache Arrow installed location.
    - THRIFT_HOME customizes the Apache Thrift (C++ libraries and compiler
      installed location.
    - SNAPPY_HOME customizes the Snappy installed location.
    - ZLIB_HOME customizes the zlib installed location.
    - BROTLI_HOME customizes the Brotli installed location.
    - GTEST_HOME customizes the googletest installed location (if you are
      building the unit tests).
    - GBENCHMARK_HOME customizes the Google Benchmark installed location (if
      you are building the benchmarks).

- `make`

The binaries will be built to ./debug which contains the libraries to link against as
well as a few example executables.

To disable the testing (which requires `googletest`), pass
`-DPARQUET_BUILD_TESTS=Off` to `cmake`.

For release-level builds (enable optimizations and disable debugging), pass
`-DCMAKE_BUILD_TYPE=Release` to `cmake`.

To build only the library with minimal dependencies, pass
`-DPARQUET_MINIMAL_DEPENDENCY=ON` to `cmake`.
Note that the executables, tests, and benchmarks should be disabled as well.

Incremental builds can be done afterwords with just `make`.

## Using with Apache Arrow

Arrow provides some of the memory management and IO interfaces that we use in
parquet-cpp. By default, Parquet links to Arrow's shared libraries. If you wish
to statically-link the Arrow symbols instead, pass
`-DPARQUET_ARROW_LINKAGE=static`.

## Testing

This library uses Google's `googletest` unit test framework. After building
with `make`, you can run the test suite by running

```
make unittest
```

The test suite relies on an environment variable `PARQUET_TEST_DATA` pointing
to the `data` directory in the source checkout, for example:

```
export PARQUET_TEST_DATA=`pwd`/data
```

See `ctest --help` for configuration details about ctest. On GNU/Linux systems,
you can use valgrind with ctest to look for memory leaks:

```
valgrind --tool=memcheck --leak-check=yes ctest
```

## Building/Running benchmarks

Follow the directions for simple build except run cmake
with the `--PARQUET_BUILD_BENCHMARKS` parameter set correctly:

    cmake -DPARQUET_BUILD_BENCHMARKS=ON ..

and instead of make unittest run either `make; ctest` to run both unit tests
and benchmarks or `make runbenchmark` to run only the benchmark tests.

Benchmark logs will be placed in the build directory under `build/benchmark-logs`.


## Out-of-source builds

parquet-cpp supports out of source builds. For example:

```
mkdir test-build
cd test-build
cmake ..
make
ctest -L unittest
```

By using out-of-source builds you can preserve your current build state in case
you need to switch to another git branch.

Design
========
The library consists of 3 layers that map to the 3 units in the parquet format.

The first is the encodings which correspond to data pages. The APIs at this level
return single values.

The second layer is the column reader which corresponds to column chunks. The APIs at
this level return a triple: definition level, repetition level and value. It also handles
reading pages, compression and managing encodings.

The 3rd layer would handle reading/writing records.

Developer Notes
========
The project adheres to the google coding convention:
http://google-styleguide.googlecode.com/svn/trunk/cppguide.xml
with two notable exceptions. We do not encourage anonymous namespaces and the line
length is 90 characters.

You can run `cpplint` through the build system with

```
make lint
```

The project prefers the use of C++ style memory management. new/delete should be used
over malloc/free. new/delete should be avoided whenever possible by using stl/boost
where possible. For example, scoped_ptr instead of explicit new/delete and using
std::vector instead of allocated buffers. Currently, c++11 features are not used.

For error handling, this project uses exceptions.

In general, many of the APIs at the layers are interface based for extensibility. To
minimize the cost of virtual calls, the APIs should be batch-centric. For example,
encoding should operate on batches of values rather than a single value.

## Using clang with a custom gcc toolchain

Suppose you are building libraries with a thirdparty gcc toolchain (not a
built-in system one) on Linux. To use clang for development while linking to
the proper toolchain, you can do (for out of source builds):

```shell
export CMAKE_CLANG_OPTIONS=--gcc-toolchain=$TOOLCHAIN/gcc-4.9.2

export CC=$TOOLCHAIN/llvm-3.7.0/bin/clang
export CXX=$TOOLCHAIN/llvm-3.7.0/bin/clang++

cmake -DCMAKE_CLANG_OPTIONS=$CMAKE_CLANG_OPTIONS \
	  -DCMAKE_CXX_FLAGS="-Werror" ..
```

## Code Coverage

To build with `gcov` code coverage and upload results to http://coveralls.io or
http://codecov.io, here are some instructions.

First, build the project with coverage and run the test suite

```
cd $PARQUET_HOME
mkdir coverage-build
cd coverage-build
cmake -DPARQUET_GENERATE_COVERAGE=1
make -j$PARALLEL
ctest -L unittest
```

The `gcov` artifacts are not located in a place that works well with either
coveralls or codecov, so there is a helper script you need to run

```
mkdir coverage_artifacts
python ../build-support/collect_coverage.py CMakeFiles/parquet.dir/src/ coverage_artifacts
```

For codecov.io (using the provided project token -- be sure to keep this
private):

```
cd coverage_artifacts
codecov --token $PARQUET_CPP_CODECOV_TOKEN --gcov-args '\-l' --root $PARQUET_ROOT
```

For coveralls, install `cpp_coveralls`:

```
pip install cpp_coveralls
```

And the coveralls upload script:

```
coveralls -t $PARQUET_CPP_COVERAGE_TOKEN --gcov-options '\-l' -r $PARQUET_ROOT --exclude $PARQUET_ROOT/thirdparty --exclude $PARQUET_ROOT/build --exclude $NATIVE_TOOLCHAIN --exclude $PARQUET_ROOT/src/parquet/thrift
```

Note that `gcov` throws off artifacts from the STL, so I excluded my toolchain
root stored in `$NATIVE_TOOLCHAIN` to avoid a cluttered coverage report.

[1]: https://github.com/apache/parquet-cpp/blob/master/docs/Windows.md
