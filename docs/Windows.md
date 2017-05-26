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

# Building parquet-cpp on Windows

## Fast setup of building requirements with conda and conda-forge

A convenient and tested way to set up thirdparty dependencies is to use the
conda package manager.
Please feel free to extend this document with others ways to setup
development environment for parquet-cpp.

### conda and package toolchain

[Miniconda][1] is a minimal Python distribution including the conda package
manager. To get started, download and install a 64-bit distribution.

We recommend using packages from [conda-forge][2].
Launch cmd.exe and run following to bootstrap a build environment:

```shell
conda create -n parquet-dev cmake git boost-cpp curl zlib snappy brotli thrift-cpp -c conda-forge
```

To allow cmake to pick up 3rd party dependencies, you should set
`PARQUET_BUILD_TOOLCHAIN` environment variable to contain `Library` folder
path of new created on previous step `parquet-dev` conda environment.
For instance, if `Miniconda` was installed to default destination, `Library`
folder path for `parquet-dev` conda environment will be as following:

```shell
C:\Users\YOUR_USER_NAME\Miniconda3\envs\parquet-dev\Library
```

As alternative to `PARQUET_BUILD_TOOLCHAIN`, it's possible to configure path
to each 3rd party dependency separately by setting appropriate environment
variable:

`BOOST_ROOT` variable with path to `boost` installation  
`THRIFT_HOME` variable with path to `thrift-cpp` installation  
`SNAPPY_HOME` variable with path to `snappy` installation  
`ZLIB_HOME` variable with path to `zlib` installation  
`BROTLI_HOME` variable with path to `brotli` installation  
`ARROW_HOME` variable with path to `arrow` installation

### Customize static libraries names lookup of 3rd party dependencies 

If you decided to use pre-built 3rd party dependencies libs, it's possible to
configure parquet-cpp cmake build script to search for customized names of 3rd
party static libs.

`zlib`. Pass `-DPARQUET_ZLIB_VENDORED=OFF` to enable lookup of custom zlib
build. Set `ZLIB_HOME` environment variable. Pass
`-DZLIB_MSVC_STATIC_LIB_SUFFIX=%ZLIB_SUFFIX%` to link with z%ZLIB_SUFFIX%.lib

`arrow`. Set `ARROW_HOME` environment variable. Pass
`-DARROW_MSVC_STATIC_LIB_SUFFIX=%ARROW_SUFFIX%` to link with
arrow%ARROW_SUFFIX%.lib

`brotli`. Set `BROTLY_HOME` environment variable. Pass
`-DBROTLI_MSVC_STATIC_LIB_SUFFIX=%BROTLI_SUFFIX%` to link with
brotli*%BROTLI_SUFFIX%.lib.

`snappy`. Set `SNAPPY_HOME` environment variable. Pass
`-DSNAPPY_MSVC_STATIC_LIB_SUFFIX=%SNAPPY_SUFFIX%` to link with
snappy%SNAPPY_SUFFIX%.lib.

`thrift`. Set `THRIFT_HOME` environment variable. Pass
`-DTHRIFT_MSVC_STATIC_LIB_SUFFIX=%THRIFT_SUFFIX%` to link with
thrift*%THRIFT_SUFFIX%.lib.

### Visual Studio

Microsoft provides the free Visual Studio Community edition. Once you have
Visual Studio installed, you should configure cmd.exe environment to be able
to find Visual Studio's build toolchain by running following commands:

#### Visual Studio 2015

```"C:\Program Files (x86)\Microsoft Visual Studio 14.0\VC\vcvarsall.bat" amd64```

#### Visual Studio 2017

```"C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\Common7\Tools\VsDevCmd.bat" -arch=amd64```

It's easiest to configure a console emulator like [cmder][3] to automatically
launch this when starting a new development console.

## Building with NMake

Activate your conda build environment:

```
activate parquet-dev
```

Change working directory in cmd.exe to the root directory of parquet-cpp and
do an out of source build using `nmake`:

```
cd %PARQUET_ROOT_SOURCES_DIRECTORY%
mkdir build
cd build
cmake -G "NMake Makefiles" -DCMAKE_BUILD_TYPE=Release ..
nmake
```

## Building with Visual Studio cmake generator

Activate your conda build environment:

```
activate parquet-dev
```

Change working directory in cmd.exe to the root directory of parquet-cpp and
do an out of source build:

```
cd %PARQUET_ROOT_SOURCES_DIRECTORY%
mkdir build
cd build
cmake -G "Visual Studio 14 2015 Win64" -DCMAKE_BUILD_TYPE=Release ..
cmake --build . --config Release
```

When using conda, only release builds are currently supported.

[1]: https://conda.io/miniconda.html
[2]: https://conda-forge.github.io/
[3]: http://cmder.net/