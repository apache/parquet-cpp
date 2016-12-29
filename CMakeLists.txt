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

include(ExternalProject)

if (NOT "$ENV{PARQUET_GCC_ROOT}" STREQUAL "")
  set(GCC_ROOT $ENV{PARQUET_GCC_ROOT})
  set(CMAKE_C_COMPILER ${GCC_ROOT}/bin/gcc)
  set(GCOV_PATH ${GCC_ROOT}/bin/gcov)
  set(CMAKE_CXX_COMPILER ${GCC_ROOT}/bin/g++)
endif()

# generate CTest input files
enable_testing()

# where to find cmake modules
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")
set(BUILD_SUPPORT_DIR "${CMAKE_SOURCE_DIR}/build-support")

find_package(ClangTools)
if ("$ENV{CMAKE_EXPORT_COMPILE_COMMANDS}" STREQUAL "1" OR CLANG_TIDY_FOUND)
  # Generate a Clang compile_commands.json "compilation database" file for use
  # with various development tools, such as Vim's YouCompleteMe plugin.
  # See http://clang.llvm.org/docs/JSONCompilationDatabase.html
  set(CMAKE_EXPORT_COMPILE_COMMANDS 1)
endif()

find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif(CCACHE_FOUND)

if(APPLE)
  set(CMAKE_MACOSX_RPATH 1)
endif()

option(PARQUET_BUILD_SHARED
    "Build the shared version of libparquet"
    ON)
option(PARQUET_BUILD_STATIC
    "Build the static version of libparquet"
    ON)

# if no build build type is specified, default to debug builds
if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif(NOT CMAKE_BUILD_TYPE)

# set compile output directory
string (TOLOWER ${CMAKE_BUILD_TYPE} BUILD_SUBDIR_NAME)

# Top level cmake file, set options
if ("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_CURRENT_SOURCE_DIR}")
  option(PARQUET_USE_SSE
    "Build with SSE4 optimizations"
    OFF)
  option(PARQUET_BUILD_BENCHMARKS
    "Build the libparquet benchmark suite"
    OFF)
  option(PARQUET_BUILD_TESTS
    "Build the libparquet test suite"
    ON)
  option(PARQUET_TEST_MEMCHECK
    "Run the test suite using valgrind --tool=memcheck"
    OFF)
  option(PARQUET_BUILD_EXECUTABLES
    "Build the libparquet executable CLI tools"
    ON)
  option(PARQUET_ARROW
    "Build the Arrow support"
    OFF)
endif()

# If build in-source, create the latest symlink. If build out-of-source, which is
# preferred, simply output the binaries in the build folder
if (${CMAKE_SOURCE_DIR} STREQUAL "${CMAKE_CURRENT_BINARY_DIR}")
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
# Benchmarking
############################################################
# Add a new micro benchmark, with or without an executable that should be built.
# If benchmarks are enabled then they will be run along side unit tests with ctest.
# 'make runbenchmark' and 'make unittest' to build/run only benchmark or unittests,
# respectively.
#
# REL_BENCHMARK_NAME is the name of the benchmark app. It may be a single component
# (e.g. monotime-benchmark) or contain additional components (e.g.
# net/net_util-benchmark). Either way, the last component must be a globally
# unique name.

# The benchmark will registered as unit test with ctest with a label
# of 'benchmark'.
#
# Arguments after the test name will be passed to set_tests_properties().
function(ADD_PARQUET_BENCHMARK REL_BENCHMARK_NAME)
  if(NOT PARQUET_BUILD_BENCHMARKS)
    return()
  endif()
  get_filename_component(BENCHMARK_NAME ${REL_BENCHMARK_NAME} NAME_WE)

  if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${REL_BENCHMARK_NAME}.cc)
    # This benchmark has a corresponding .cc file, set it up as an executable.
    set(BENCHMARK_PATH "${EXECUTABLE_OUTPUT_PATH}/${BENCHMARK_NAME}")
    add_executable(${BENCHMARK_NAME} "${REL_BENCHMARK_NAME}.cc")

    if(APPLE)
      # On OS X / Thrift >= 0.9.2, tr1/tuple.h is not in libc++
      SET_TARGET_PROPERTIES(${TEST_NAME} PROPERTIES COMPILE_FLAGS
        -DGTEST_USE_OWN_TR1_TUPLE=1)
    else()
      # Linux, for Thrift >= 0.9.2
      SET_TARGET_PROPERTIES(${TEST_NAME} PROPERTIES COMPILE_FLAGS
        -DGTEST_USE_OWN_TR1_TUPLE=0)
    endif()

    target_link_libraries(${BENCHMARK_NAME} ${PARQUET_BENCHMARK_LINK_LIBS})
    add_dependencies(runbenchmark ${BENCHMARK_NAME})
    set(NO_COLOR "--color_print=false")
  else()
    # No executable, just invoke the benchmark (probably a script) directly.
    set(BENCHMARK_PATH ${CMAKE_CURRENT_SOURCE_DIR}/${REL_BENCHMARK_NAME})
    set(NO_COLOR "")
  endif()

  add_test(${BENCHMARK_NAME}
    ${BUILD_SUPPORT_DIR}/run-test.sh ${CMAKE_BINARY_DIR} benchmark ${BENCHMARK_PATH} ${NO_COLOR})
  set_tests_properties(${BENCHMARK_NAME} PROPERTIES LABELS "benchmark")
  if(ARGN)
    set_tests_properties(${BENCHMARK_NAME} PROPERTIES ${ARGN})
  endif()
endfunction()

# A wrapper for add_dependencies() that is compatible with NO_BENCHMARKS.
function(ADD_PARQUET_BENCHMARK_DEPENDENCIES REL_BENCHMARK_NAME)
  if(NOT PARQUET_BUILD_BENCHMARKS)
    return()
  endif()
  get_filename_component(BENCMARK_NAME ${REL_BENCHMARK_NAME} NAME_WE)

  add_dependencies(${BENCHMARK_NAME} ${ARGN})
endfunction()

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
# The unit test is added with a label of "unittest" to support filtering with
# ctest.
#
# Arguments after the test name will be passed to set_tests_properties().
function(ADD_PARQUET_TEST REL_TEST_NAME)
  set(options)
  set(one_value_args LINKAGE)
  set(multi_value_args)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})

  if(NOT PARQUET_BUILD_TESTS)
    return()
  endif()

  # TODO(wesm): not very rigorous error checking
  if (ARG_LINKAGE AND "${ARG_LINKAGE}" STREQUAL "shared")
    if(NOT PARQUET_BUILD_SHARED)
      # Skip this test if we are not building the shared library
      return()
    else()
      set(TEST_LINK_LIBS ${PARQUET_TEST_SHARED_LINK_LIBS})
    endif()
  else()
    if(NOT PARQUET_BUILD_STATIC)
      # Skip this test if we are not building the static library
      return()
    else()
      set(TEST_LINK_LIBS ${PARQUET_TEST_LINK_LIBS})
    endif()
  endif()

  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)

  if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${REL_TEST_NAME}.cc)
    # This test has a corresponding .cc file, set it up as an executable.
    set(TEST_PATH "${EXECUTABLE_OUTPUT_PATH}/${TEST_NAME}")
    add_executable(${TEST_NAME} "${REL_TEST_NAME}.cc")
    add_dependencies(unittest ${TEST_NAME})

    if(APPLE)
      # On OS X / Thrift >= 0.9.2, tr1/tuple.h is not in libc++
      SET_TARGET_PROPERTIES(${TEST_NAME} PROPERTIES COMPILE_FLAGS
        -DGTEST_USE_OWN_TR1_TUPLE=1)
    else()
      # Linux, for Thrift >= 0.9.2
      SET_TARGET_PROPERTIES(${TEST_NAME} PROPERTIES COMPILE_FLAGS
        -DGTEST_USE_OWN_TR1_TUPLE=0)
    endif()

    target_link_libraries(${TEST_NAME} ${TEST_LINK_LIBS})
  else()
    # No executable, just invoke the test (probably a script) directly.
    set(TEST_PATH ${CMAKE_CURRENT_SOURCE_DIR}/${REL_TEST_NAME})
  endif()

  if (PARQUET_TEST_MEMCHECK)
    SET_PROPERTY(TARGET ${TEST_NAME}
      APPEND_STRING PROPERTY
      COMPILE_FLAGS " -DPARQUET_VALGRIND")
    add_test(${TEST_NAME}
      valgrind --tool=memcheck --leak-check=full --error-exitcode=1 ${TEST_PATH})
  else()
    add_test(${TEST_NAME}
        ${BUILD_SUPPORT_DIR}/run-test.sh ${CMAKE_BINARY_DIR} test ${TEST_PATH})
  endif()
  set_tests_properties(${TEST_NAME} PROPERTIES LABELS "unittest")
  if(ARGN)
    set_tests_properties(${TEST_NAME} PROPERTIES ${ARGN})
  endif()
endfunction()

# A wrapper for add_dependencies() that is compatible with PARQUET_BUILD_TESTS.
function(ADD_PARQUET_TEST_DEPENDENCIES REL_TEST_NAME)
  if(NOT PARQUET_BUILD_TESTS)
    return()
  endif()
  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)

  add_dependencies(${TEST_NAME} ${ARGN})
endfunction()

# A wrapper for add_dependencies() that is compatible with PARQUET_BUILD_TESTS.
function(ADD_PARQUET_LINK_LIBRARIES REL_TEST_NAME)
  if(NOT PARQUET_BUILD_TESTS)
    return()
  endif()
  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)

  target_link_libraries(${TEST_NAME} ${ARGN})
endfunction()

enable_testing()

############################################################
# Dependencies
############################################################

set(GTEST_VERSION "1.7.0")
set(GBENCHMARK_VERSION "1.0.0")
set(SNAPPY_VERSION "1.1.3")
set(THRIFT_VERSION "0.9.1")

# find boost headers and libs
set(Boost_DEBUG TRUE)
set(Boost_USE_MULTITHREADED ON)
find_package(Boost REQUIRED)
include_directories(SYSTEM ${Boost_INCLUDE_DIRS})
set(LIBS ${LIBS} ${Boost_LIBRARIES})
message(STATUS "Boost include dir: " ${Boost_INCLUDE_DIRS})
message(STATUS "Boost libraries: " ${Boost_LIBRARIES})

# find thrift headers and libs
find_package(Thrift)

if (NOT THRIFT_FOUND)
  if (APPLE)
      message(FATAL_ERROR "thrift compilation under OSX is not currently supported.")
  endif()

  set(THRIFT_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/thrift_ep/src/thrift_ep-install")
  set(THRIFT_HOME "${THRIFT_PREFIX}")
  set(THRIFT_INCLUDE_DIR "${THRIFT_PREFIX}/include")
  set(THRIFT_STATIC_LIB "${THRIFT_PREFIX}/lib/libthrift.a")
  set(THRIFT_COMPILER "${THRIFT_PREFIX}/bin/thrift")
  set(THRIFT_VENDORED 1)

  if (CMAKE_VERSION VERSION_GREATER "3.2")
    # BUILD_BYPRODUCTS is a 3.2+ feature
    ExternalProject_Add(thrift_ep
      CONFIGURE_COMMAND ./configure "CXXFLAGS=-fPIC" --without-qt4 --without-c_glib --without-csharp --without-java --without-erlang --without-nodejs --without-lua --without-python --without-perl --without-php --without-php_extension --without-ruby --without-haskell --without-go --without-d --with-cpp "--prefix=${THRIFT_PREFIX}"
      BUILD_IN_SOURCE 1
      # This is needed for 0.9.1 and can be removed for 0.9.3 again
      BUILD_COMMAND make clean
      INSTALL_COMMAND make install
      INSTALL_DIR ${THRIFT_PREFIX}
      URL "http://archive.apache.org/dist/thrift/${THRIFT_VERSION}/thrift-${THRIFT_VERSION}.tar.gz"
      BUILD_BYPRODUCTS "${THRIFT_STATIC_LIB}" "${THRIFT_COMPILER}"
      )
  else()
    ExternalProject_Add(thrift_ep
      CONFIGURE_COMMAND ./configure "CXXFLAGS=-fPIC" --without-qt4 --without-c_glib --without-csharp --without-java --without-erlang --without-nodejs --without-lua --without-python --without-perl --without-php --without-php_extension --without-ruby --without-haskell --without-go --without-d --with-cpp "--prefix=${THRIFT_PREFIX}"
      BUILD_IN_SOURCE 1
      # This is needed for 0.9.1 and can be removed for 0.9.3 again
      BUILD_COMMAND make clean
      INSTALL_COMMAND make install
      INSTALL_DIR ${THRIFT_PREFIX}
      URL "http://archive.apache.org/dist/thrift/${THRIFT_VERSION}/thrift-${THRIFT_VERSION}.tar.gz"
      )
  endif()
    set(THRIFT_VENDORED 1)
else()
    set(THRIFT_VENDORED 0)
endif()

include_directories(SYSTEM ${THRIFT_INCLUDE_DIR} ${THRIFT_INCLUDE_DIR}/thrift)
message(STATUS "Thrift include dir: ${THRIFT_INCLUDE_DIR}")
message(STATUS "Thrift static library: ${THRIFT_STATIC_LIB}")
message(STATUS "Thrift compiler: ${THRIFT_COMPILER}")
add_library(thriftstatic STATIC IMPORTED)
set_target_properties(thriftstatic PROPERTIES IMPORTED_LOCATION ${THRIFT_STATIC_LIB})

if (THRIFT_VENDORED)
  add_dependencies(thriftstatic thrift_ep)
endif()

## Snappy
find_package(Snappy)
if (NOT SNAPPY_FOUND)
  set(SNAPPY_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/snappy_ep/src/snappy_ep-install")
  set(SNAPPY_HOME "${SNAPPY_PREFIX}")
  set(SNAPPY_INCLUDE_DIR "${SNAPPY_PREFIX}/include")
  set(SNAPPY_STATIC_LIB "${SNAPPY_PREFIX}/lib/libsnappy.a")
  set(SNAPPY_VENDORED 1)

  if (CMAKE_VERSION VERSION_GREATER "3.2")
    # BUILD_BYPRODUCTS is a 3.2+ feature
    ExternalProject_Add(snappy_ep
      CONFIGURE_COMMAND ./configure --with-pic "--prefix=${SNAPPY_PREFIX}"
      BUILD_IN_SOURCE 1
      BUILD_COMMAND ${MAKE}
      INSTALL_DIR ${SNAPPY_PREFIX}
      URL "https://github.com/google/snappy/releases/download/${SNAPPY_VERSION}/snappy-${SNAPPY_VERSION}.tar.gz"
      BUILD_BYPRODUCTS "${SNAPPY_STATIC_LIB}"
      )
  else()
    ExternalProject_Add(snappy_ep
      CONFIGURE_COMMAND ./configure --with-pic "--prefix=${SNAPPY_PREFIX}"
      BUILD_IN_SOURCE 1
      BUILD_COMMAND ${MAKE}
      INSTALL_DIR ${SNAPPY_PREFIX}
      URL "https://github.com/google/snappy/releases/download/${SNAPPY_VERSION}/snappy-${SNAPPY_VERSION}.tar.gz"
      )
  endif()
else()
    set(SNAPPY_VENDORED 0)
endif()

include_directories(SYSTEM ${SNAPPY_INCLUDE_DIR})
add_library(snappystatic STATIC IMPORTED)
set_target_properties(snappystatic PROPERTIES IMPORTED_LOCATION ${SNAPPY_STATIC_LIB})

if (SNAPPY_VENDORED)
  add_dependencies(snappystatic snappy_ep)
endif()

## Brotli
find_package(Brotli)
if (NOT BROTLI_FOUND)
  set(BROTLI_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/brotli_ep/src/brotli_ep-install")
  set(BROTLI_HOME "${BROTLI_PREFIX}")
  set(BROTLI_INCLUDE_DIR "${BROTLI_PREFIX}/include")
  set(BROTLI_LIBRARY_ENC "${BROTLI_PREFIX}/lib/${CMAKE_LIBRARY_ARCHITECTURE}/libbrotlienc.a")
  set(BROTLI_LIBRARY_DEC "${BROTLI_PREFIX}/lib/${CMAKE_LIBRARY_ARCHITECTURE}/libbrotlidec.a")
  set(BROTLI_LIBRARY_COMMON "${BROTLI_PREFIX}/lib/${CMAKE_LIBRARY_ARCHITECTURE}/libbrotlicommon.a")
  set(BROTLI_VENDORED 1)
  set(BROTLI_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
                        -DCMAKE_INSTALL_PREFIX=${BROTLI_PREFIX}
                        -DCMAKE_INSTALL_LIBDIR=lib/${CMAKE_LIBRARY_ARCHITECTURE}
                        -DBUILD_SHARED_LIBS=OFF)

  if (CMAKE_VERSION VERSION_GREATER "3.2")
    # BUILD_BYPRODUCTS is a 3.2+ feature
    ExternalProject_Add(brotli_ep
      GIT_REPOSITORY https://github.com/google/brotli.git
      GIT_TAG 5db62dcc9d386579609540cdf8869e95ad334bbd
      BUILD_BYPRODUCTS "${BROTLI_LIBRARY_ENC}" "${BROTLI_LIBRARY_DEC}" "${BROTLI_LIBRARY_COMMON}"
      CMAKE_ARGS ${BROTLI_CMAKE_ARGS})
  else()
    ExternalProject_Add(brotli_ep
      GIT_REPOSITORY https://github.com/google/brotli.git
      GIT_TAG 5db62dcc9d386579609540cdf8869e95ad334bbd
      CMAKE_ARGS ${BROTLI_CMAKE_ARGS})
  endif()
else()
  set(BROTLI_VENDORED 0)
endif()

include_directories(SYSTEM ${BROTLI_INCLUDE_DIR})
add_library(brotlistatic_enc STATIC IMPORTED)
set_target_properties(brotlistatic_enc PROPERTIES IMPORTED_LOCATION ${BROTLI_LIBRARY_ENC})
add_library(brotlistatic_dec STATIC IMPORTED)
set_target_properties(brotlistatic_dec PROPERTIES IMPORTED_LOCATION ${BROTLI_LIBRARY_DEC})
add_library(brotlistatic_common STATIC IMPORTED)
set_target_properties(brotlistatic_common PROPERTIES IMPORTED_LOCATION ${BROTLI_LIBRARY_COMMON})

if (BROTLI_VENDORED)
  add_dependencies(brotlistatic_enc brotli_ep)
  add_dependencies(brotlistatic_dec brotli_ep)
  add_dependencies(brotlistatic_common brotli_ep)
endif()

## ZLIB
# For now: Always build zlib so that the shared lib has -fPIC
# find_package(ZLIB)

if (NOT ZLIB_FOUND)
  set(ZLIB_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/zlib_ep/src/zlib_ep-install")
  set(ZLIB_HOME "${ZLIB_PREFIX}")
  set(ZLIB_INCLUDE_DIR "${ZLIB_PREFIX}/include")
  set(ZLIB_STATIC_LIB "${ZLIB_PREFIX}/lib/libz.a")
  set(ZLIB_VENDORED 1)
  set(ZLIB_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
                      -DCMAKE_INSTALL_PREFIX=${ZLIB_PREFIX}
                      -DCMAKE_C_FLAGS=-fPIC
                      -DBUILD_SHARED_LIBS=OFF)

  if (CMAKE_VERSION VERSION_GREATER "3.2")
    # BUILD_BYPRODUCTS is a 3.2+ feature
    ExternalProject_Add(zlib_ep
      URL "http://zlib.net/zlib-1.2.8.tar.gz"
      BUILD_BYPRODUCTS "${ZLIB_STATIC_LIB}"
      CMAKE_ARGS ${ZLIB_CMAKE_ARGS})
  else()
    ExternalProject_Add(zlib_ep
      URL "http://zlib.net/zlib-1.2.8.tar.gz"
      CMAKE_ARGS ${ZLIB_CMAKE_ARGS})
  endif()
else()
    set(ZLIB_VENDORED 0)
endif()

include_directories(SYSTEM ${ZLIB_INCLUDE_DIRS})
add_library(zlibstatic STATIC IMPORTED)
set_target_properties(zlibstatic PROPERTIES IMPORTED_LOCATION ${ZLIB_STATIC_LIB})

if (ZLIB_VENDORED)
  add_dependencies(zlibstatic zlib_ep)
endif()

## GTest
if(PARQUET_BUILD_TESTS)
  add_custom_target(unittest ctest -L unittest)

  if("$ENV{GTEST_HOME}" STREQUAL "")
    if(APPLE)
      set(GTEST_CMAKE_CXX_FLAGS "-fPIC -std=c++11 -stdlib=libc++ -DGTEST_USE_OWN_TR1_TUPLE=1 -Wno-unused-value -Wno-ignored-attributes")
    else()
      set(GTEST_CMAKE_CXX_FLAGS "-fPIC")
    endif()

    set(GTEST_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/googletest_ep-prefix/src/googletest_ep")
    set(GTEST_INCLUDE_DIR "${GTEST_PREFIX}/include")
    set(GTEST_STATIC_LIB "${GTEST_PREFIX}/${CMAKE_CFG_INTDIR}/${CMAKE_STATIC_LIBRARY_PREFIX}gtest${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(GTEST_VENDORED 1)

    if (CMAKE_VERSION VERSION_GREATER "3.2")
      # BUILD_BYPRODUCTS is a 3.2+ feature
      ExternalProject_Add(googletest_ep
        URL "https://github.com/google/googletest/archive/release-${GTEST_VERSION}.tar.gz"
        CMAKE_ARGS -DCMAKE_CXX_FLAGS=${GTEST_CMAKE_CXX_FLAGS} -Dgtest_force_shared_crt=ON
        # googletest doesn't define install rules, so just build in the
        # source dir and don't try to install.  See its README for
        # details.
        BUILD_IN_SOURCE 1
        BUILD_BYPRODUCTS "${GTEST_STATIC_LIB}"
        INSTALL_COMMAND "")
    else()
      ExternalProject_Add(googletest_ep
        URL "https://github.com/google/googletest/archive/release-${GTEST_VERSION}.tar.gz"
        CMAKE_ARGS -DCMAKE_CXX_FLAGS=${GTEST_CMAKE_CXX_FLAGS} -Dgtest_force_shared_crt=ON
        # googletest doesn't define install rules, so just build in the
        # source dir and don't try to install.  See its README for
        # details.
        BUILD_IN_SOURCE 1
        INSTALL_COMMAND "")
    endif()
  else()
    find_package(GTest REQUIRED)
    set(GTEST_VENDORED 0)
  endif()

  message(STATUS "GTest include dir: ${GTEST_INCLUDE_DIR}")
  message(STATUS "GTest static library: ${GTEST_STATIC_LIB}")
  include_directories(SYSTEM ${GTEST_INCLUDE_DIR})
  add_library(gtest STATIC IMPORTED)
  set_target_properties(gtest PROPERTIES IMPORTED_LOCATION ${GTEST_STATIC_LIB})

  if(GTEST_VENDORED)
    add_dependencies(gtest googletest_ep)
  endif()
endif()

## Google Benchmark
if ("$ENV{GBENCHMARK_HOME}" STREQUAL "")
  set(GBENCHMARK_HOME ${THIRDPARTY_DIR}/installed)
endif()

if(PARQUET_BUILD_BENCHMARKS)
  add_custom_target(runbenchmark ctest -L benchmark)

  if("$ENV{GBENCHMARK_HOME}" STREQUAL "")
    if(APPLE)
      set(GBENCHMARK_CMAKE_CXX_FLAGS "-std=c++11 -stdlib=libc++")
    else()
      set(GBENCHMARK_CMAKE_CXX_FLAGS "--std=c++11")
    endif()

    set(GBENCHMARK_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/gbenchmark_ep/src/gbenchmark_ep-install")
    set(GBENCHMARK_INCLUDE_DIR "${GBENCHMARK_PREFIX}/include")
    set(GBENCHMARK_STATIC_LIB "${GBENCHMARK_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}benchmark${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(GBENCHMARK_VENDORED 1)
    set(GBENCHMARK_CMAKE_ARGS
          "-DCMAKE_BUILD_TYPE=Release"
          "-DCMAKE_INSTALL_PREFIX:PATH=${GBENCHMARK_PREFIX}"
          "-DCMAKE_CXX_FLAGS=-fPIC ${GBENCHMARK_CMAKE_CXX_FLAGS}")
    if (CMAKE_VERSION VERSION_GREATER "3.2")
      # BUILD_BYPRODUCTS is a 3.2+ feature
      ExternalProject_Add(gbenchmark_ep
        URL "https://github.com/google/benchmark/archive/v${GBENCHMARK_VERSION}.tar.gz"
        BUILD_BYPRODUCTS "${GBENCHMARK_STATIC_LIB}"
        CMAKE_ARGS ${GBENCHMARK_CMAKE_ARGS})
    else()
      ExternalProject_Add(gbenchmark_ep
        URL "https://github.com/google/benchmark/archive/v${GBENCHMARK_VERSION}.tar.gz"
        CMAKE_ARGS ${GBENCHMARK_CMAKE_ARGS})
    endif()
  else()
    find_package(GBenchmark REQUIRED)
    set(GBENCHMARK_VENDORED 0)
  endif()

  message(STATUS "GBenchmark include dir: ${GBENCHMARK_INCLUDE_DIR}")
  message(STATUS "GBenchmark static library: ${GBENCHMARK_STATIC_LIB}")
  include_directories(SYSTEM ${GBENCHMARK_INCLUDE_DIR})
  add_library(gbenchmark STATIC IMPORTED)
  set_target_properties(gbenchmark PROPERTIES IMPORTED_LOCATION ${GBENCHMARK_STATIC_LIB})

  if(GBENCHMARK_VENDORED)
    add_dependencies(gbenchmark gbenchmark_ep)
  endif()
endif()

# Thrift requires these definitions for some types that we use
add_definitions(-DHAVE_INTTYPES_H -DHAVE_NETINET_IN_H -DHAVE_NETDB_H)
add_definitions(-fPIC)

set(ARROW_VERSION "4733ee876e1fddb8032fce1dc9e486d68904fbea")

## Apache Arrow
find_package(Arrow)
if (NOT ARROW_FOUND)
  set(ARROW_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/arrow_ep/src/arrow_ep-install")
  set(ARROW_HOME "${ARROW_PREFIX}")
  set(ARROW_INCLUDE_DIR "${ARROW_PREFIX}/include")
  set(ARROW_SHARED_LIB "${ARROW_PREFIX}/lib/libarrow${CMAKE_SHARED_LIBRARY_SUFFIX}")
  set(ARROW_IO_SHARED_LIB "${ARROW_PREFIX}/lib/libarrow_io${CMAKE_SHARED_LIBRARY_SUFFIX}")
  set(ARROW_STATIC_LIB "${ARROW_PREFIX}/lib/libarrow.a")
  set(ARROW_IO_STATIC_LIB "${ARROW_PREFIX}/lib/libarrow_io.a")
  set(ARROW_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    -DCMAKE_INSTALL_PREFIX=${ARROW_PREFIX}
    -DARROW_BUILD_TESTS=OFF
    -DARROW_HDFS=ON)

  if (CMAKE_VERSION VERSION_GREATER "3.2")
    # BUILD_BYPRODUCTS is a 3.2+ feature
    ExternalProject_Add(arrow_ep
      GIT_REPOSITORY https://github.com/apache/arrow.git
      GIT_TAG ${ARROW_VERSION}
      BUILD_BYPRODUCTS "${ARROW_SHARED_LIB}" "${ARROW_IO_SHARED_LIB}" "${ARROW_IO_STATIC_LIB}" "${ARROW_STATIC_LIB}"
      # With CMake 3.7.0 there is a SOURCE_SUBDIR argument which we can use
      # to specify that the CMakeLists.txt of Arrow is located in cpp/
      #
      # See https://gitlab.kitware.com/cmake/cmake/commit/a8345d65f359d75efb057d22976cfb92b4d477cf
      CONFIGURE_COMMAND "${CMAKE_COMMAND}" ${ARROW_CMAKE_ARGS} ${CMAKE_CURRENT_BINARY_DIR}/arrow_ep-prefix/src/arrow_ep/cpp
      CMAKE_ARGS ${ARROW_CMAKE_ARGS})
  else()
    ExternalProject_Add(arrow_ep
      GIT_REPOSITORY https://github.com/apache/arrow.git
      GIT_TAG ${ARROW_VERSION}
      CONFIGURE_COMMAND "${CMAKE_COMMAND}" ${ARROW_CMAKE_ARGS} ${CMAKE_CURRENT_BINARY_DIR}/arrow_ep-prefix/src/arrow_ep/cpp
      CMAKE_ARGS ${ARROW_CMAKE_ARGS})
  endif()
  set(ARROW_VENDORED 1)
else()
  set(ARROW_VENDORED 0)
endif()

include_directories(SYSTEM ${ARROW_INCLUDE_DIR})
add_library(arrow SHARED IMPORTED)
set_target_properties(arrow PROPERTIES IMPORTED_LOCATION ${ARROW_SHARED_LIB})
add_library(arrow_io SHARED IMPORTED)
set_target_properties(arrow_io PROPERTIES IMPORTED_LOCATION ${ARROW_IO_SHARED_LIB})
add_library(arrow_static STATIC IMPORTED)
set_target_properties(arrow_static PROPERTIES IMPORTED_LOCATION ${ARROW_STATIC_LIB})
add_library(arrow_io_static STATIC IMPORTED)
set_target_properties(arrow_io_static PROPERTIES IMPORTED_LOCATION ${ARROW_IO_STATIC_LIB})

if (ARROW_VENDORED)
  add_dependencies(arrow arrow_ep)
  add_dependencies(arrow_io arrow_ep)
  add_dependencies(arrow_static arrow_ep)
  add_dependencies(arrow_io_static arrow_ep)
endif()

# where to put generated archives (.a files)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")
set(ARCHIVE_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")

# where to put generated libraries (.so files)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")
set(LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}")

#############################################################
# Compiler flags and release types

# compiler flags for different build types (run 'cmake -DCMAKE_BUILD_TYPE=<type> .')
# For all builds:
# For CMAKE_BUILD_TYPE=Debug
#   -ggdb: Enable gdb debugging
# For CMAKE_BUILD_TYPE=FastDebug
#   Same as DEBUG, except with -O1
# For CMAKE_BUILD_TYPE=Release
#   -O3: Enable all compiler optimizations
#   -g: Enable symbols for profiler tools (TODO: remove for shipping)
set(CXX_FLAGS_DEBUG "-ggdb -O0")
set(CXX_FLAGS_FASTDEBUG "-ggdb -O1")
set(CXX_FLAGS_RELEASE "-O3 -g")

string (TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE)

if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_FLAGS_DEBUG}")

elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "FASTDEBUG")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_FLAGS_FASTDEBUG}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_FLAGS_RELEASE}")
else()
  message(FATAL_ERROR "Unknown build type: ${CMAKE_BUILD_TYPE}")
endif ()

message(STATUS "Build Type: ${CMAKE_BUILD_TYPE}")

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -Wall -fno-strict-aliasing")

if (PARQUET_USE_SSE)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
  add_definitions(-DPARQUET_USE_SSE)
endif()

if (APPLE)
  # Use libc++ to avoid linker errors on some platforms
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
endif()

# Determine compiler version
include(CompilerInfo)

if ("${COMPILER_FAMILY}" STREQUAL "clang")
  # Using Clang with ccache causes a bunch of spurious warnings that are
  # purportedly fixed in the next version of ccache. See the following for details:
  #
  #   http://petereisentraut.blogspot.com/2011/05/ccache-and-clang.html
  #   http://petereisentraut.blogspot.com/2011/09/ccache-and-clang-part-2.html
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Qunused-arguments")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CLANG_OPTIONS}")
endif()

############################################################
# Visibility
############################################################
# For generate_export_header() and add_compiler_export_flags().
include(GenerateExportHeader)

# Sets -fvisibility=hidden for gcc
add_compiler_export_flags()

############################################################
# Includes

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
  --filter=-whitespace/comments,-readability/todo,-build/header_guard,-runtime/references,-readability/check,-build/c++11,-build/include_order
    `find ${CMAKE_CURRENT_SOURCE_DIR}/src ${CMAKE_CURRENT_SOURCE_DIR}/tools ${CMAKE_CURRENT_SOURCE_DIR}/examples  ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks -name \\*.cc -or -name \\*.h | sed -e '/parquet\\/thrift/g'`)
endif (UNIX)

############################################################
# "make format" and "make check-format" targets
############################################################

if (${CLANG_FORMAT_FOUND})
  # runs clang format and updates files in place.
  add_custom_target(format ${BUILD_SUPPORT_DIR}/run-clang-format.sh ${CMAKE_CURRENT_SOURCE_DIR} ${CLANG_FORMAT_BIN} 1
  `find ${CMAKE_CURRENT_SOURCE_DIR}/src ${CMAKE_CURRENT_SOURCE_DIR}/tools  ${CMAKE_CURRENT_SOURCE_DIR}/examples ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks -name \\*.cc -or -name \\*.h | sed -e '/_generated/g'`)

  # runs clang format and exits with a non-zero exit code if any files need to be reformatted
  add_custom_target(check-format ${BUILD_SUPPORT_DIR}/run-clang-format.sh ${CMAKE_CURRENT_SOURCE_DIR} ${CLANG_FORMAT_BIN} 0
  `find ${CMAKE_CURRENT_SOURCE_DIR}/src ${CMAKE_CURRENT_SOURCE_DIR}/tools  ${CMAKE_CURRENT_SOURCE_DIR}/examples ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks -name \\*.cc -or -name \\*.h | sed -e '/_generated/g'`)
endif()


############################################################
# "make clang-tidy" and "make check-clang-tidy" targets
############################################################

if (${CLANG_TIDY_FOUND})
  # runs clang-tidy and attempts to fix any warning automatically
  add_custom_target(clang-tidy ${BUILD_SUPPORT_DIR}/run-clang-tidy.sh ${CLANG_TIDY_BIN} ${CMAKE_BINARY_DIR}/compile_commands.json 1
  `find ${CMAKE_CURRENT_SOURCE_DIR}/src ${CMAKE_CURRENT_SOURCE_DIR}/tools  ${CMAKE_CURRENT_SOURCE_DIR}/examples ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks -name \\*.cc | sed -e '/_types/g' | sed -e '/_constants/g'`)
  # runs clang-tidy and exits with a non-zero exit code if any errors are found.
  add_custom_target(check-clang-tidy ${BUILD_SUPPORT_DIR}/run-clang-tidy.sh ${CLANG_TIDY_BIN} ${CMAKE_BINARY_DIR}/compile_commands.json
  0 `find ${CMAKE_CURRENT_SOURCE_DIR}/src ${CMAKE_CURRENT_SOURCE_DIR}/tools  ${CMAKE_CURRENT_SOURCE_DIR}/examples ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks -name \\*.cc |grep -v -F -f ${CMAKE_CURRENT_SOURCE_DIR}/.clang-tidy-ignore`)

endif()

#############################################################
# Test linking

set(PARQUET_MIN_TEST_LIBS
  parquet_test_main)

set(PARQUET_TEST_LINK_LIBS ${PARQUET_MIN_TEST_LIBS}
  parquet_static)

set(PARQUET_TEST_SHARED_LINK_LIBS ${PARQUET_MIN_TEST_LIBS}
  parquet_shared)

#############################################################
# Benchmark linking

if (PARQUET_BUILD_STATIC)
    set(PARQUET_BENCHMARK_LINK_LIBS
      parquet_benchmark_main
      parquet_static)
else()
    set(PARQUET_BENCHMARK_LINK_LIBS
      parquet_benchmark_main
      parquet_shared)
endif()

#############################################################
# Code coverage

# Adapted from Apache Kudu (incubating)
if ("${PARQUET_GENERATE_COVERAGE}")
  if("${CMAKE_CXX_COMPILER}" MATCHES ".*clang.*")
    # There appears to be some bugs in clang 3.3 which cause code coverage
    # to have link errors, not locating the llvm_gcda_* symbols.
    # This should be fixed in llvm 3.4 with http://llvm.org/viewvc/llvm-project?view=revision&revision=184666
    message(SEND_ERROR "Cannot currently generate coverage with clang")
  endif()
  message(STATUS "Configuring build for gcov")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} --coverage")
  # For coverage to work properly, we need to use static linkage. Otherwise,
  # __gcov_flush() doesn't properly flush coverage from every module.
  # See http://stackoverflow.com/questions/28164543/using-gcov-flush-within-a-library-doesnt-force-the-other-modules-to-yield-gc
  if(NOT PARQUET_BUILD_STATIC)
    message(SEND_ERROR "Coverage requires the static lib to be built")
  endif()
endif()

############################################################
# Library config

set(LIBPARQUET_SRCS
  src/parquet/exception.cc
  src/parquet/types.cc

  src/parquet/column/levels.cc
  src/parquet/column/reader.cc
  src/parquet/column/writer.cc
  src/parquet/column/scanner.cc
  src/parquet/column/scan-all.cc
  src/parquet/column/statistics.cc

  src/parquet/compression/codec.cc
  src/parquet/compression/brotli-codec.cc
  src/parquet/compression/snappy-codec.cc
  src/parquet/compression/gzip-codec.cc

  src/parquet/file/metadata.cc
  src/parquet/file/reader.cc
  src/parquet/file/reader-internal.cc
  src/parquet/file/writer.cc
  src/parquet/file/writer-internal.cc

  src/parquet/schema/converter.cc
  src/parquet/schema/descriptor.cc
  src/parquet/schema/printer.cc
  src/parquet/schema/types.cc

  src/parquet/util/cpu-info.cc
  src/parquet/util/memory.cc
)

set(LIBPARQUET_LINK_LIBS
  arrow
  arrow_io
)

set(LIBPARQUET_PRIVATE_LINK_LIBS
  parquet_thrift
  brotlistatic_dec
  brotlistatic_enc
  brotlistatic_common
  snappystatic
  thriftstatic
  zlibstatic
)

add_library(parquet_objlib OBJECT
  ${LIBPARQUET_SRCS}
)

# Although we don't link parquet_objlib against anything, we need it to depend
# on these libs as we may generate their headers via ExternalProject_Add
add_dependencies(parquet_objlib ${LIBPARQUET_PRIVATE_LINK_LIBS})

set_property(TARGET parquet_objlib PROPERTY POSITION_INDEPENDENT_CODE 1)

if(APPLE)
  set(SHARED_LINK_FLAGS "-undefined dynamic_lookup")
elseif()
  # Localize thirdparty symbols using a linker version script. This hides them
  # from the client application. The OS X linker does not support the
  # version-script option.
  set(SHARED_LINK_FLAGS "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/src/parquet/symbols.map")
endif()

if (PARQUET_BUILD_SHARED)
    add_library(parquet_shared SHARED $<TARGET_OBJECTS:parquet_objlib>)
    set_target_properties(parquet_shared
      PROPERTIES
      LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}"
      LINK_FLAGS "${SHARED_LINK_FLAGS}"
      OUTPUT_NAME "parquet")
    target_link_libraries(parquet_shared
      LINK_PUBLIC ${LIBPARQUET_LINK_LIBS}
      LINK_PRIVATE ${LIBPARQUET_PRIVATE_LINK_LIBS})
    if (APPLE)
      set_target_properties(parquet_shared
        PROPERTIES
        BUILD_WITH_INSTALL_RPATH ON
        INSTALL_NAME_DIR "@rpath")
    endif()
endif()

if (PARQUET_BUILD_STATIC)
    add_library(parquet_static STATIC $<TARGET_OBJECTS:parquet_objlib>)
    set_target_properties(parquet_static
      PROPERTIES
      LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}"
      OUTPUT_NAME "parquet")
    target_link_libraries(parquet_static
      LINK_PUBLIC ${LIBPARQUET_LINK_LIBS}
      LINK_PRIVATE ${LIBPARQUET_PRIVATE_LINK_LIBS})
endif()

add_subdirectory(src/parquet)
add_subdirectory(src/parquet/api)
add_subdirectory(src/parquet/column)
add_subdirectory(src/parquet/compression)
add_subdirectory(src/parquet/encodings)
add_subdirectory(src/parquet/file)
add_subdirectory(src/parquet/schema)
add_subdirectory(src/parquet/thrift)
add_subdirectory(src/parquet/util)

# Ensure that thrift compilation is done before using its generated headers
# in parquet code.
add_dependencies(parquet_objlib parquet_thrift)

add_subdirectory(benchmarks)
add_subdirectory(examples)
add_subdirectory(tools)

# Arrow adapter
if (PARQUET_ARROW)
  add_subdirectory(src/parquet/arrow)
endif()

add_custom_target(clean-all
   COMMAND ${CMAKE_BUILD_TOOL} clean
   COMMAND ${CMAKE_COMMAND} -P cmake_modules/clean-all.cmake
)

# installation

if (PARQUET_BUILD_STATIC)
    install(TARGETS parquet_static
      ARCHIVE DESTINATION lib
      LIBRARY DESTINATION lib)
endif()
if (PARQUET_BUILD_SHARED)
    install(TARGETS parquet_shared
      ARCHIVE DESTINATION lib
      LIBRARY DESTINATION lib)
endif()
