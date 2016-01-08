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
mac$ brew install boost snappy lz4 thrift
```

The thirdparty directory contains helper scripts to manually install third-party dependencies on Linux. You can use them like so.
- `mkdir <build-dir>`
- `cd <build-dir>`
- `cp -r <src-dir>/thirdparty .`
- `./thirdparty/download_thirdparty.sh`
- `./thirdparty/build_thirdparty.sh`
- Use the thirdparty/installed directory when configuring HOME dirs for dependencies.
Ex., `THRIFT_HOME=$PWD/thirdparty/installed SNAPPY_HOME=$PWD/thirdparty/installed LZ4_HOME=$PWD/thirdparty/installed cmake <source_dir>`

You can also take a look at our [.travis.yml](.travis.yml) to see how that build env is set up.


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

The project prefers the use of C++ style memory management. new/delete should be used 
over malloc/free. new/delete should be avoided whenever possible by using stl/boost 
where possible. For example, scoped_ptr instead of explicit new/delete and using 
std::vector instead of allocated buffers. Currently, c++11 features are not used.

For error handling, this project uses exceptions.

In general, many of the APIs at the layers are interface based for extensibility. To 
minimize the cost of virtual calls, the APIs should be batch-centric. For example, 
encoding should operate on batches of values rather than a single value.
