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

set(BUILD_SUPPORT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/build-support)

# Only add these options if this is the top level CMakeLists.txt
if("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_CURRENT_SOURCE_DIR}")
################################################
# Some options configurable from the CMAKE command execution
#
# -DPARQUET_SHARED_LIB=ON/OFF, whether to build a shared or a static library.
    option(PARQUET_SHARED_LIB
        "Build a libparquet shared library instead of a static library"
        ON)
# -DPARQUET_BUILD_TESTS=ON/OFF, whether to build the googletest unit tests.
    option(PARQUET_BUILD_TESTS
        "Build the googletest unit tests for libparquet"
        ON)
# -DPARQUET_BUILD_BENCHMARKS=ON/OFF, whether to build the Google benchmarks
    option(PARQUET_BUILD_BENCHMARKS
        "Build the Google benchmarks for libparquet"
        ON)
################################################
endif()

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
  set(ENV{BOOST_VERSION} "1.57.0")
  set(ENV{THRIFT_VERSION} "0.9.0-p2")
  set(ENV{LZ4_VERSION} "svn")
  set(ENV{SNAPPY_VERSION} "1.0.5")
  set(ENV{GPERFTOOLS_VERSION} "2.3")
  set(ENV{GOOGLETEST_VERSION} "20151222")

  set(ENV{THRIFT_HOME} $ENV{NATIVE_TOOLCHAIN}/thrift-$ENV{THRIFT_VERSION})

  message(STATUS "THRIFT_HOME: " $ENV{THRIFT_HOME})

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

if (NATIVE_TOOLCHAIN)
  message(STATUS "Found native toolchain, enabling toolchain boost")
  set(ENV{BOOST_ROOT} "$ENV{NATIVE_TOOLCHAIN}/boost-$ENV{BOOST_VERSION}")
  set(Boost_USE_STATIC_LIBS NOT ${BUILD_SHARED_LIBS})
  set(Boost_USE_STATIC_RUNTIME ON)

  # Newer versions of boost (including the version in toolchain) don't build
  # separate multithreaded versions (they always are). Make sure to pick those
  # up.
  set(Boost_USE_MULTITHREADED OFF)
endif()

# find boost headers and libs
IF (DEFINED ENV{BOOST_ROOT})
    # The casing and underscoring expected for these properties varies between
    # versions of CMake. Multiple inconsistent versions may be present here
    # intentionally to provide what a wide range of versions expects.
    set(Boost_NO_SYSTEM_PATHS true)
    set(BOOST_ROOT $ENV{BOOST_ROOT})
    set(BOOST_LIBRARYDIR $ENV{BOOST_ROOT}/lib)
    set(BOOST_INCLUDEDIR $ENV{BOOST_ROOT}/include)
    set(Boost_INCLUDE_DIR ${BOOST_INCLUDEDIR})
ENDIF (DEFINED ENV{BOOST_ROOT})

if (CMAKE_DEBUG)
  set(Boost_DEBUG TRUE)
endif()

# find boost headers and libs
find_package(Boost REQUIRED
  COMPONENTS thread regex system filesystem date_time)

# Boost required by Thrift
include_directories(${Boost_INCLUDE_DIRS})

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

## Snappy
find_package(Snappy REQUIRED)
include_directories(SYSTEM ${SNAPPY_INCLUDE_DIR})
message(STATUS "SNAPPY include dir: ${SNAPPY_INCLUDE_DIR}")
message(STATUS "SNAPPY static library: ${SNAPPY_STATIC_LIB}")


## LZ4
find_package(Lz4 REQUIRED)
include_directories(SYSTEM ${LZ4_INCLUDE_DIR})
message(STATUS "LZ4 include dir: ${LZ4_INCLUDE_DIR}")
message(STATUS "LZ4 static library: ${LZ4_STATIC_LIB}")

SET(CMAKE_CXX_FLAGS "-std=c++11 -msse4.2 -Wall -Wno-unused-value -Wno-unused-variable -Wno-sign-compare -Wno-unknown-pragmas")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -ggdb")

# for static linking with Thrift, THRIFT_STATIC_LIB is set in FindThrift.cmake
add_library(snappystatic STATIC IMPORTED)
set_target_properties(snappystatic PROPERTIES IMPORTED_LOCATION ${SNAPPY_STATIC_LIB})

add_library(thriftstatic STATIC IMPORTED)
set_target_properties(thriftstatic PROPERTIES IMPORTED_LOCATION ${THRIFT_STATIC_LIB})
add_library(lz4static STATIC IMPORTED)
set_target_properties(lz4static PROPERTIES IMPORTED_LOCATION ${LZ4_STATIC_LIB})

# Thrift requires these definitions for some types that we use
add_definitions(-DHAVE_INTTYPES_H -DHAVE_NETINET_IN_H -DHAVE_NETDB_H)
add_definitions(-fPIC)

# where to put generated archives (.a files)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")
set(ARCHIVE_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")

# where to put generated libraries (.so files)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")
set(LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}")

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src)

############################################################
# "make lint" target
############################################################
if (UNIX)
  # Full lint
  add_custom_target(lint ${BUILD_SUPPORT_DIR}/cpplint.py
  --verbose=4
  --filter=-whitespace/comments,-readability/todo,-build/header_guard,-build/include_order
    `find ${CMAKE_CURRENT_SOURCE_DIR}/src -name \\*.cc -or -name \\*.h | sed -e '/parquet\\/thrift/g'`)
endif (UNIX)

############################################################
# Library config

set(LIBPARQUET_SRCS
  src/parquet.cc
)

set(LIBPARQUET_LINK_LIBS
  parquet_compression
  parquet_thrift
  lz4static
)

if (PARQUET_SHARED_LIB)
  set(LIBPARQUET_LINKAGE "SHARED")
else()
  set(LIBPARQUET_LINKAGE "STATIC")
endif()

add_library(parquet
  ${LIBPARQUET_LINKAGE}
  ${LIBPARQUET_SRCS})
target_link_libraries(parquet ${LIBPARQUET_LINK_LIBS})

add_subdirectory(src/parquet)
add_subdirectory(src/parquet/compression)
add_subdirectory(src/parquet/encodings)
add_subdirectory(src/parquet/thrift)
add_subdirectory(src/parquet/util)

add_subdirectory(example)

add_custom_target(clean-all
   COMMAND ${CMAKE_BUILD_TOOL} clean
   COMMAND ${CMAKE_COMMAND} -P cmake_modules/clean-all.cmake
)

# installation

install(TARGETS parquet
  LIBRARY DESTINATION lib)
