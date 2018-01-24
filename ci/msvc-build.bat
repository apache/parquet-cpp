@rem Licensed to the Apache Software Foundation (ASF) under one
@rem or more contributor license agreements.  See the NOTICE file
@rem distributed with this work for additional information
@rem regarding copyright ownership.  The ASF licenses this file
@rem to you under the Apache License, Version 2.0 (the
@rem "License"); you may not use this file except in compliance
@rem with the License.  You may obtain a copy of the License at
@rem
@rem   http://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing,
@rem software distributed under the License is distributed on an
@rem "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
@rem KIND, either express or implied.  See the License for the
@rem specific language governing permissions and limitations
@rem under the License.

@echo on

mkdir build
cd build

SET PARQUET_TEST_DATA=%APPVEYOR_BUILD_FOLDER%\data
set PARQUET_CXXFLAGS=/MP

set PARQUET_USE_STATIC_CRT_OPTION=OFF
if "%USE_STATIC_CRT%" == "ON" (
      set PARQUET_USE_STATIC_CRT_OPTION=ON
)

if NOT "%CONFIGURATION%" == "Debug" (
  set PARQUET_CXXFLAGS="%PARQUET_CXXFLAGS% /WX"
)

if "%CONFIGURATION%" == "Toolchain" (
  conda install -y boost-cpp=1.63 thrift-cpp=0.10.0 ^
      brotli=0.6.0 zlib=1.2.11 snappy=1.1.6 lz4-c=1.7.5 zstd=1.2.0 ^
      -c conda-forge

  set ARROW_BUILD_TOOLCHAIN=%MINICONDA%/Library
  set PARQUET_BUILD_TOOLCHAIN=%MINICONDA%/Library

  cmake -G "%GENERATOR%" ^
      -DCMAKE_BUILD_TYPE=Release ^
      -DPARQUET_BOOST_USE_SHARED=OFF ^
      -DPARQUET_CXXFLAGS=%PARQUET_CXXFLAGS% ^
      .. || exit /B

  cmake --build . --config Release || exit /B
  ctest -VV || exit /B
)

if NOT "%CONFIGURATION%" == "Toolchain" (
  cmake -G "%GENERATOR%" ^
        -DCMAKE_BUILD_TYPE=%CONFIGURATION% ^
        -DPARQUET_BOOST_USE_SHARED=OFF ^
        -DPARQUET_CXXFLAGS=%PARQUET_CXXFLAGS% ^
        -DPARQUET_USE_STATIC_CRT=%PARQUET_USE_STATIC_CRT_OPTION% ^
        .. || exit /B

  cmake --build . --config %CONFIGURATION% || exit /B

  if "%CONFIGURATION%" == "Release" (
    ctest -VV || exit /B
  )
)
