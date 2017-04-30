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
conda create -n parquet-dev cmake git boost-cpp curl zlib snappy -c conda-forge
```

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

When using conda, only release builds are currently supported.

[1]: https://conda.io/miniconda.html
[2]: https://conda-forge.github.io/
[3]: http://cmder.net/