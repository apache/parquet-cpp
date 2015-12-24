# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


cmake_minimum_required(VERSION 2.6)

# generate CTest input files
enable_testing()

# where to find cmake modules
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")

# Bootstrap thirdparty dependencies
if ("$ENV{DISABLE_NATIVE_TOOLCHAIN}" STREQUAL "")
  # Enable toolchain variable if the environment is setup
  set(NATIVE_TOOLCHAIN ON)
  message(STATUS "Toolchain build.")

  # If toolchain is not set, pick a directory
  if ("$ENV{NATIVE_TOOLCHAIN}" STREQUAL "")
    set(ENV{NATIVE_TOOLCHAIN} "${CMAKE_CURRENT_SOURCE_DIR}/toolchain")
  endif()

  # Set the environment variables for dependent versions
  set(ENV{GCC_VERSION} "4.9.2")
  set(ENV{THRIFT_VERSION} "0.9.0-p2")
  set(ENV{LZ4_VERSION} "svn")
  set(ENV{SNAPPY_VERSION} "1.0.5")
  set(ENV{GPERFTOOLS_VERSION} "2.3")
  set(ENV{GOOGLETEST_VERSION} "20151222")

  set(THRIFT_HOME $ENV{NATIVE_TOOLCHAIN}/thrift-$ENV{THRIFT_VERSION})

  # Setting SYSTEM_GCC will use the toolchain dependencies compiled with the original
  # host's compiler.
  if ("$ENV{SYSTEM_GCC}" STREQUAL "")
    set(GCC_ROOT $ENV{NATIVE_TOOLCHAIN}/gcc-$ENV{GCC_VERSION})
    set(CMAKE_C_COMPILER ${GCC_ROOT}/bin/gcc)
    set(CMAKE_CXX_COMPILER ${GCC_ROOT}/bin/g++)
  endif()

  # If the toolchain directory does not yet exists, we assume that the dependencies
  # should be downloaded. If the download script is not available fail the
  # configuration.
  if (NOT IS_DIRECTORY $ENV{NATIVE_TOOLCHAIN})
    set(BOOTSTRAP_CMD "${BUILD_SUPPORT_DIR}/bootstrap_toolchain.py")
    # Download and unpack the dependencies
    message(STATUS "Downloading and extracting dependencies.")
    execute_process(COMMAND ${BOOTSTRAP_CMD} RESULT_VARIABLE BOOTSTRAP_RESULT)
    if (${BOOTSTRAP_RESULT} EQUAL 0)
      message(STATUS "Toolchain bootstrap complete.")
    else()
      message(FATAL_ERROR "Toolchain bootstrap failed.")
    endif()
  else()
    message(STATUS "Native toolchain picked up at $ENV{NATIVE_TOOLCHAIN}")
  endif()
else()
  set(NATIVE_TOOLCHAIN OFF)
  message(STATUS "Native toolchain was explicitly disabled using DISABLE_NATIVE_TOOLCHAIN.")
  message(STATUS "Assuming system search path for dependencies.")
endif()

project(parquet-cpp)

# find boost headers and libs
set(Boost_DEBUG TRUE)
set(Boost_USE_MULTITHREADED ON)
find_package(Boost REQUIRED)
include_directories(SYSTEM ${Boost_INCLUDE_DIRS})
set(LIBS ${LIBS} ${Boost_LIBRARIES})
message(STATUS "Boost include dir: " ${Boost_INCLUDE_DIRS})
message(STATUS "Boost libraries: " ${Boost_LIBRARIES})

# find thrift headers and libs
find_package(Thrift REQUIRED)
include_directories(SYSTEM ${THRIFT_INCLUDE_DIR})
set(LIBS ${LIBS} ${THRIFT_LIBS})
message(STATUS "Thrift include dir: ${THRIFT_INCLUDE_DIR}")
message(STATUS "Thrift contrib dir: ${THRIFT_CONTRIB_DIR}")
message(STATUS "Thrift library path: ${THRIFT_LIBS}")
message(STATUS "Thrift static library: ${THRIFT_STATIC_LIB}")
message(STATUS "Thrift compiler: ${THRIFT_COMPILER}")
# for static linking with Thrift, THRIFT_STATIC_LIB is set in FindThrift.cmake
add_library(thriftstatic STATIC IMPORTED)
set_target_properties(thriftstatic PROPERTIES IMPORTED_LOCATION ${THRIFT_STATIC_LIB})

## Snappy
find_package(Snappy REQUIRED)
include_directories(SYSTEM ${SNAPPY_INCLUDE_DIR})
add_library(snappystatic STATIC IMPORTED)
set_target_properties(snappystatic PROPERTIES IMPORTED_LOCATION ${SNAPPY_STATIC_LIB})

## LZ4
find_package(Lz4 REQUIRED)
include_directories(SYSTEM ${LZ4_INCLUDE_DIR})
add_library(lz4static STATIC IMPORTED)
set_target_properties(lz4static PROPERTIES IMPORTED_LOCATION ${LZ4_STATIC_LIB})

SET(CMAKE_CXX_FLAGS "-msse4.2 -Wall -Wno-unused-value -Wno-unused-variable -Wno-sign-compare -Wno-unknown-pragmas")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -ggdb")

# Thrift requires these definitions for some types that we use
add_definitions(-DHAVE_INTTYPES_H -DHAVE_NETINET_IN_H -DHAVE_NETDB_H)
add_definitions(-fPIC)

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${CMAKE_CURRENT_SOURCE_DIR}/build")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${CMAKE_CURRENT_SOURCE_DIR}/bin")

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src)

add_subdirectory(src)
add_subdirectory(example)

add_custom_target(clean-all
   COMMAND ${CMAKE_BUILD_TOOL} clean
   COMMAND ${CMAKE_COMMAND} -P cmake_modules/clean-all.cmake
)
