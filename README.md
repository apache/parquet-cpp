Parquet-cpp [![Build Status](https://travis-ci.org/apache/parquet-cpp.svg)](https://travis-ci.org/apache/parquet-cpp)
===========
A C++ library to read parquet files.

## Third Party Dependencies
- snappy
- lz4
- thrift 0.7+ [install instructions](https://thrift.apache.org/docs/install/)

Many package managers support some or all of these dependencies. E.g.:
```shell
ubuntu$ sudo apt-get install libboost-dev libsnappy-dev liblz4-dev
```
```shell
mac$ brew install snappy lz4 thrift
```

`setup_build_env.sh` tries to automate setting up a build environment for you
with third party dependencies.  You use it by running `source
setup_build_env.sh`.  By default, it will create a build directory `build/`.
You can override the build directory by setting the BUILD_DIR env variable to
another location.

Also feel free to take a look at our [.travis.yml](.travis.yml) to see how that build env is set up.


## Build
- `cmake .`
  - You can customize dependent library locations through various environment variables:
    - THRIFT_HOME customizes the thrift installed location.
    - SNAPPY_HOME customizes the snappy installed location.
    - LZ4_HOME customizes the lz4 installed location.
- `make`

The binaries will be built to ./debug which contains the libraries to link against as
well as a few example executables.

Incremental builds can be done afterwords with just `make`.

## Testing

This library uses Google's `googletest` unit test framework. After building
with `make`, you can run the test suite by running

```
ctest
```

The test suite relies on an environment variable `PARQUET_TEST_DATA` pointing
to the `data` directory in the source checkout, for example:

```
export PARQUET_TEST_DATA=`pwd`/data
```

If you run `source setup_build_env.sh` it will set this variable automatically,
but you may also wish to put it in your `.bashrc` or somewhere else.

See `ctest --help` for configuration details about ctest. On GNU/Linux systems,
you can use valgrind with ctest to look for memory leaks:

```
valgrind --tool=memcheck --leak-check=yes ctest
```

## Out-of-source builds

parquet-cpp supports out of source builds. For example:

```
mkdir test-build
cd test-build
cmake ..
make
ctest
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

## Code Coverage

To build with `gcov` code coverage and upload results to http://coveralls.io or
http://codecov.io, here are some instructions.

First, build the project with coverage and run the test suite

```
cd $PARQUET_HOME
mkdir coverage-build
cd coverage-build
cmake -DPARQUET_GENERATE_COVERAGE=1
make -j4
ctest
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