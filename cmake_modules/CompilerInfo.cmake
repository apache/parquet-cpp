# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Sets COMPILER_FAMILY to 'clang' or 'gcc' or 'msvc'
# Sets COMPILER_VERSION to the version
if(NOT MSVC)
  set(COMPILER_GET_VERSION_SWITCH "-v")
endif()

execute_process(COMMAND "${CMAKE_CXX_COMPILER}" ${COMPILER_GET_VERSION_SWITCH}
                OUTPUT_VARIABLE COMPILER_VERSION_FULL
                ERROR_VARIABLE COMPILER_VERSION_FULL)
message(STATUS "Compiler id: ${CMAKE_CXX_COMPILER_ID}")
string(TOLOWER "${COMPILER_VERSION_FULL}" COMPILER_VERSION_FULL_LOWER)

if(MSVC)
  set(COMPILER_FAMILY "msvc")
  if ("${COMPILER_VERSION_FULL}" MATCHES ".*Microsoft \\(R\\) C/C\\+\\+ Optimizing Compiler Version 19.*x64")
    string(REGEX REPLACE ".*Optimizing Compiler Version ([0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+).*" "\\1"
      COMPILER_VERSION "${COMPILER_VERSION_FULL}")
  elseif(NOT "${COMPILER_VERSION_FULL}" STREQUAL "")
    message(FATAL_ERROR "Not supported MSVC compiler:\n${COMPILER_VERSION_FULL}\n"
      "Supported MSVC versions: Visual Studio 2015 2017 x64")
  endif()

# clang on Linux and Mac OS X before 10.9
elseif("${COMPILER_VERSION_FULL}" MATCHES ".*clang version.*")
  set(COMPILER_FAMILY "clang")
  string(REGEX REPLACE ".*clang version ([0-9]+\\.[0-9]+).*" "\\1"
    COMPILER_VERSION "${COMPILER_VERSION_FULL}")
# clang on Mac OS X 10.9 and later
elseif("${COMPILER_VERSION_FULL}" MATCHES ".*based on LLVM.*")
  set(COMPILER_FAMILY "clang")
  string(REGEX REPLACE ".*based on LLVM ([0-9]+\\.[0.9]+).*" "\\1"
    COMPILER_VERSION "${COMPILER_VERSION_FULL}")

# clang on Mac OS X, XCode 7+.
elseif("${COMPILER_VERSION_FULL}" MATCHES ".*clang-.*")
  set(COMPILER_FAMILY "clang")

# gcc
elseif("${COMPILER_VERSION_FULL_LOWER}" MATCHES ".*gcc[ -]version.*")
  set(COMPILER_FAMILY "gcc")
  string(REGEX REPLACE ".*gcc[ -]version ([0-9\\.]+).*" "\\1"
      COMPILER_VERSION "${COMPILER_VERSION_FULL_LOWER}")
else()
  message(FATAL_ERROR "Unknown compiler. Version info:\n${COMPILER_VERSION_FULL}")
endif()
message("Selected compiler ${COMPILER_FAMILY} ${COMPILER_VERSION}")
