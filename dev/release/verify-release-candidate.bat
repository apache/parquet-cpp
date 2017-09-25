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

@rem Then run from the directory containing the RC tarball
@rem
@rem verify-release-candidate.bat apache-parquet-cpp-%VERSION%

@echo on

if not exist "C:\tmp\" mkdir C:\tmp
if exist "C:\tmp\parquet-verify-release" rd C:\tmp\parquet-verify-release /s /q
if not exist "C:\tmp\parquet-verify-release" mkdir C:\tmp\parquet-verify-release

tar xvf %1.tar.gz -C "C:/tmp/"

set GENERATOR=Visual Studio 14 2015 Win64
set CONFIGURATION=release
set PARQUET_SOURCE=C:\tmp\%1
set INSTALL_DIR=C:\tmp\%1\install

pushd %PARQUET_SOURCE%

set PARQUET_TEST_DATA=%PARQUET_SOURCE%\data

set PARQUET_HOME=%INSTALL_DIR%
set PATH=%INSTALL_DIR%\bin;%PATH%

mkdir build
pushd build

cmake -G "%GENERATOR%" ^
      -DCMAKE_INSTALL_PREFIX=%PARQUET_HOME% ^
      -DPARQUET_BOOST_USE_SHARED=OFF ^
      -DCMAKE_BUILD_TYPE=%CONFIGURATION% ^
      -DPARQUET_CXXFLAGS="/WX /MP" ^
      ..  || exit /B
cmake --build . --target INSTALL --config %CONFIGURATION%  || exit /B

ctest -VV  || exit /B
popd
