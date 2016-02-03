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

cmake_minimum_required(VERSION 2.6)
project(parquet-cpp)

if (NOT "$ENV{PARQUET_GCC_ROOT}" STREQUAL "")
  set(GCC_ROOT $ENV{PARQUET_GCC_ROOT})
  set(CMAKE_C_COMPILER ${GCC_ROOT}/bin/gcc)
  set(CMAKE_CXX_COMPILER ${GCC_ROOT}/bin/g++)
endif()

# generate CTest input files
enable_testing()

# where to find cmake modules
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")
set(BUILD_SUPPORT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/build-support)

if(APPLE)
  set(CMAKE_MACOSX_RPATH 1)
endif()

if (NOT PARQUET_LINK)
  set(PARQUET_LINK "a")
elseif(NOT ("auto" MATCHES "^${PARQUET_LINK}" OR
            "dynamic" MATCHES "^${PARQUET_LINK}" OR
            "static" MATCHES "^${PARQUET_LINK}"))
  message(FATAL_ERROR "Unknown value for PARQUET_LINK, must be auto|dynamic|static")
else()
  # Remove all but the first letter.
  string(SUBSTRING "${PARQUET_LINK}" 0 1 PARQUET_LINK)
endif()

# if no build build type is specified, default to debug builds
if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif(NOT CMAKE_BUILD_TYPE)

# set compile output directory
string (TOLOWER ${CMAKE_BUILD_TYPE} BUILD_SUBDIR_NAME)

# If build in-source, create the latest symlink. If build out-of-source, which is
# preferred, simply output the binaries in the build folder
if (${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_CURRENT_BINARY_DIR})
  set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/build/${BUILD_SUBDIR_NAME}")
  # Link build/latest to the current build directory, to avoid developers
  # accidentally running the latest debug build when in fact they're building
  # release builds.
  FILE(MAKE_DIRECTORY ${BUILD_OUTPUT_ROOT_DIRECTORY})
  if (NOT APPLE)
    set(MORE_ARGS "-T")
  endif()
EXECUTE_PROCESS(COMMAND ln ${MORE_ARGS} -sf ${BUILD_OUTPUT_ROOT_DIRECTORY}
  ${CMAKE_CURRENT_BINARY_DIR}/build/latest)
else()
  set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/${BUILD_SUBDIR_NAME}")
endif()

############################################################
# Testing
############################################################

# Add a new test case, with or without an executable that should be built.
#
# REL_TEST_NAME is the name of the test. It may be a single component
# (e.g. monotime-test) or contain additional components (e.g.
# net/net_util-test). Either way, the last component must be a globally
# unique name.
#
# Arguments after the test name will be passed to set_tests_properties().
function(ADD_PARQUET_TEST REL_TEST_NAME)
  if(NO_TESTS)
    return()
  endif()
  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)

  if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${REL_TEST_NAME}.cc)
    # This test has a corresponding .cc file, set it up as an executable.
    set(TEST_PATH "${EXECUTABLE_OUTPUT_PATH}/${TEST_NAME}")
    add_executable(${TEST_NAME} "${REL_TEST_NAME}.cc")
    target_link_libraries(${TEST_NAME} ${PARQUET_TEST_LINK_LIBS})
  else()
    # No executable, just invoke the test (probably a script) directly.
    set(TEST_PATH ${CMAKE_CURRENT_SOURCE_DIR}/${REL_TEST_NAME})
  endif()

  add_test(${TEST_NAME}
    ${BUILD_SUPPORT_DIR}/run-test.sh ${TEST_PATH})
  if(ARGN)
    set_tests_properties(${TEST_NAME} PROPERTIES ${ARGN})
  endif()
endfunction()

# A wrapper for add_dependencies() that is compatible with NO_TESTS.
function(ADD_PARQUET_TEST_DEPENDENCIES REL_TEST_NAME)
  if(NO_TESTS)
    return()
  endif()
  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)

  add_dependencies(${TEST_NAME} ${ARGN})
endfunction()

enable_testing()

############################################################
# Dependencies
############################################################

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
include_directories(SYSTEM ${THRIFT_INCLUDE_DIR} ${THRIFT_INCLUDE_DIR}/thrift)
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

## GTest
find_package(GTest REQUIRED)
include_directories(SYSTEM ${GTEST_INCLUDE_DIR})
add_library(gtest STATIC IMPORTED)
set_target_properties(gtest PROPERTIES IMPORTED_LOCATION ${GTEST_STATIC_LIB})

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

SET(CMAKE_CXX_FLAGS "-std=c++11 -msse3 -Wall -Wno-unused-value -Wno-unused-variable -Wno-sign-compare -Wno-unknown-pragmas")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -ggdb")

if (APPLE)
  # Use libc++ to avoid linker errors on some platforms
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
endif()


include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}/src
)

############################################################
# "make lint" target
############################################################
if (UNIX)
  # Full lint
  add_custom_target(lint ${BUILD_SUPPORT_DIR}/cpplint.py
  --verbose=2
  --linelength=90
  --filter=-whitespace/comments,-readability/todo,-build/header_guard,-build/include_order,-runtime/references,-readability/check
    `find ${CMAKE_CURRENT_SOURCE_DIR}/src -name \\*.cc -or -name \\*.h | sed -e '/parquet\\/thrift/g'`)
endif (UNIX)

#############################################################
# Test linking

set(PARQUET_MIN_TEST_LIBS
  parquet_test_main
  parquet)
set(PARQUET_TEST_LINK_LIBS ${PARQUET_MIN_TEST_LIBS})

############################################################
# Library config

set(LIBPARQUET_SRCS
  src/parquet/column/serialized-page.cc
  src/parquet/column/reader.cc
  src/parquet/column/scanner.cc
  src/parquet/reader.cc
)

set(LIBPARQUET_LINK_LIBS
  parquet_compression
  parquet_thrift
  parquet_util
  thriftstatic
)

if ("${PARQUET_LINK}" STREQUAL "d" OR "${PARQUET_LINK}" STREQUAL "a")
  set(LIBPARQUET_LINKAGE "SHARED")
else()
  set(LIBPARQUET_LINKAGE "STATIC")
endif()

add_library(parquet
  ${LIBPARQUET_LINKAGE}
  ${LIBPARQUET_SRCS}
)
set_target_properties(parquet
  PROPERTIES
  LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")
target_link_libraries(parquet ${LIBPARQUET_LINK_LIBS})

if(APPLE)
    set_target_properties(parquet PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
endif()

add_subdirectory(src/parquet)
add_subdirectory(src/parquet/column)
add_subdirectory(src/parquet/compression)
add_subdirectory(src/parquet/encodings)
add_subdirectory(src/parquet/schema)
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
