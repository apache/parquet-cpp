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

file(READ "${CMAKE_CURRENT_SOURCE_DIR}/.parquetcppversion" PARQUET_VERSION)
string(REPLACE "\n" "" PARQUET_VERSION "${PARQUET_VERSION}")

include(ExternalProject)
include(FindPkgConfig)

if(CMAKE_MAJOR_VERSION LESS 3)
  set(CMAKE_INSTALL_INCLUDEDIR "include")
  set(CMAKE_INSTALL_LIBDIR "lib")
else()
  include(GNUInstallDirs)
endif()

set(PARQUET_SO_VERSION "1")
set(PARQUET_ABI_VERSION "${PARQUET_SO_VERSION}.0.0")

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

# if no build build type is specified, default to debug builds
if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif(NOT CMAKE_BUILD_TYPE)

# set compile output directory
string (TOLOWER ${CMAKE_BUILD_TYPE} BUILD_SUBDIR_NAME)

# Top level cmake file, set options
if ("${CMAKE_SOURCE_DIR}" STREQUAL "${CMAKE_CURRENT_SOURCE_DIR}")
  option(PARQUET_BUILD_SHARED
    "Build the shared version of libparquet"
    ON)
  option(PARQUET_BUILD_STATIC
    "Build the static version of libparquet. Always ON if building unit tests"
    ON)
  set(PARQUET_ARROW_LINKAGE "shared" CACHE STRING
    "Libraries to link for Apache Arrow. static|shared (default shared)")
  set(PARQUET_CXXFLAGS "" CACHE STRING
    "Compiler flags to use when compiling Parquet")
  option(PARQUET_USE_SSE
    "Build with SSE4 optimizations"
    OFF)
  option(PARQUET_BUILD_BENCHMARKS
    "Build the libparquet benchmark suite"
    OFF)
  option(PARQUET_BOOST_USE_SHARED
    "Rely on boost shared libraries where relevant"
    ON)
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
    ON)
  option(PARQUET_ZLIB_VENDORED
    "Build our own zlib (some libz.a aren't configured for static linking)"
    ON)
  option(PARQUET_RPATH_ORIGIN
    "Build Parquet libraries with RATH set to \$ORIGIN"
    OFF)
  option(PARQUET_MINIMAL_DEPENDENCY
    "Depend only on Thirdparty headers to build libparquet. Always OFF if building binaries"
    OFF)
endif()

if (PARQUET_BUILD_TESTS OR PARQUET_BUILD_EXECUTABLES OR PARQUET_BUILD_BENCHMARKS)
  set(PARQUET_BUILD_STATIC ON)
  set(PARQUET_MINIMAL_DEPENDENCY OFF)
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

# where to put generated archives (.a files)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")
set(ARCHIVE_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")

# where to put generated libraries (.so files)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")
set(LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}")

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
    set(TEST_LINK_LIBS ${PARQUET_TEST_LINK_LIBS})
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

if (PARQUET_MINIMAL_DEPENDENCY)
    set(IGNORE_OPTIONAL_PACKAGES ON)
    message(STATUS "Build using minimal dependencies")
else()
    set(IGNORE_OPTIONAL_PACKAGES OFF)
endif()
include(ThirdpartyToolchain)

# Thrift requires these definitions for some types that we use
add_definitions(-DHAVE_INTTYPES_H -DHAVE_NETINET_IN_H -DHAVE_NETDB_H)
add_definitions(-fPIC)

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
#   Debug symbols are stripped for reduced binary size. Add
#   -DPARQUET_CXXFLAGS="-g" to include them
set(CXX_FLAGS_DEBUG "-ggdb -O0")
set(CXX_FLAGS_FASTDEBUG "-ggdb -O1")
set(CXX_FLAGS_RELEASE "-O3")

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

SET(CMAKE_CXX_FLAGS "${PARQUET_CXXFLAGS} ${CMAKE_CXX_FLAGS} -std=c++11 -Wall -fno-strict-aliasing")

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
    `find ${CMAKE_CURRENT_SOURCE_DIR}/src ${CMAKE_CURRENT_SOURCE_DIR}/tools ${CMAKE_CURRENT_SOURCE_DIR}/examples  ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks -name \\*.cc -or -name \\*.h | sed -e '/parquet\\/parquet_/g'`)
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

#############################################################
# Boost linkage

if (PARQUET_BOOST_USE_SHARED)
  set(BOOST_LINK_LIBS
    boost_shared_regex)
else()
  set(BOOST_LINK_LIBS
    boost_static_regex)
endif()

#############################################################
# Apache Arrow linkage

if ("${PARQUET_ARROW_LINKAGE}" STREQUAL "shared")
  set(ARROW_LINK_LIBS
    arrow)
else()
  set(ARROW_LINK_LIBS
    arrow_static)
endif()

#############################################################
# Test linking

set(PARQUET_MIN_TEST_LIBS
  gtest
  gtest_main)

if (APPLE)
  set(PARQUET_MIN_TEST_LIBS
    ${PARQUET_MIN_TEST_LIBS}
    ${CMAKE_DL_LIBS})
elseif(NOT MSVC)
  set(PARQUET_MIN_TEST_LIBS
    ${PARQUET_MIN_TEST_LIBS}
    pthread
    ${CMAKE_DL_LIBS})
endif()

set(PARQUET_TEST_LINK_LIBS ${PARQUET_MIN_TEST_LIBS}
  ${ARROW_LINK_LIBS}
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

############################################################
# Generated Thrift sources

set(THRIFT_SRCS
  src/parquet/parquet_constants.cpp
  src/parquet/parquet_types.cpp)

set_source_files_properties(src/parquet/parquet_types.cpp PROPERTIES
  COMPILE_FLAGS -Wno-unused-variable)

# List of thrift output targets
set(THRIFT_OUTPUT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src/parquet)
set(THRIFT_OUTPUT_FILES "${THRIFT_OUTPUT_DIR}/parquet_types.cpp")
set(THRIFT_OUTPUT_FILES ${THRIFT_OUTPUT_FILES} "${THRIFT_OUTPUT_DIR}/parquet_types.h")
set(THRIFT_OUTPUT_FILES ${THRIFT_OUTPUT_FILES} "${THRIFT_OUTPUT_DIR}/parquet_constants.cpp")
set(THRIFT_OUTPUT_FILES ${THRIFT_OUTPUT_FILES} "${THRIFT_OUTPUT_DIR}/parquet_constants.h")

set_source_files_properties(${THRIFT_OUTPUT_FILES} PROPERTIES GENERATED TRUE)

get_filename_component(ABS_PARQUET_THRIFT src/parquet/parquet.thrift ABSOLUTE)

add_custom_command(
  OUTPUT ${THRIFT_OUTPUT_FILES}
  COMMAND ${THRIFT_COMPILER} --gen cpp -out ${THRIFT_OUTPUT_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/src/parquet/parquet.thrift
  DEPENDS ${ABS_PARQUET_THRIFT} thriftstatic
  COMMENT "Running thrift compiler on parquet.thrift"
  VERBATIM
)

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

  src/parquet/compression.cc

  src/parquet/file/metadata.cc
  src/parquet/file/printer.cc
  src/parquet/file/reader.cc
  src/parquet/file/reader-internal.cc
  src/parquet/file/writer.cc
  src/parquet/file/writer-internal.cc

  src/parquet/schema.cc

  src/parquet/parquet_constants.cpp
  src/parquet/parquet_types.cpp

  src/parquet/util/cpu-info.cc
  src/parquet/util/memory.cc
)

set(BUNDLED_STATIC_LIBS
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

# # Ensure that thrift compilation is done before using its generated headers
# # in parquet code.
add_custom_target(thrift-deps ALL
  DEPENDS ${THRIFT_OUTPUT_FILES})
add_dependencies(parquet_objlib thrift-deps)

if (NOT PARQUET_MINIMAL_DEPENDENCY)
# These are libraries that we will link privately with parquet_shared (as they
# do not need to be linked transitively by other linkers), but publicly with
# parquet_static (because internal users need to transitively link all
# dependencies)
  set(LIBPARQUET_INTERFACE_LINK_LIBS
    ${ARROW_LINK_LIBS}
    ${BOOST_LINK_LIBS}
    ${BUNDLED_STATIC_LIBS}
  )
# Although we don't link parquet_objlib against anything, we need it to depend
# on these libs as we may generate their headers via ExternalProject_Add
  add_dependencies(parquet_objlib
    ${LIBPARQUET_INTERFACE_LINK_LIBS})
endif()

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
      OUTPUT_NAME "parquet"
      VERSION "${PARQUET_ABI_VERSION}"
      SOVERSION "${PARQUET_SO_VERSION}")
    target_link_libraries(parquet_shared
      LINK_PRIVATE ${LIBPARQUET_INTERFACE_LINK_LIBS})
    if (PARQUET_RPATH_ORIGIN)
        if (APPLE)
            set(_lib_install_rpath "@loader_path")
        else()
            set(_lib_install_rpath "\$ORIGIN")
        endif()
        set_target_properties(parquet_shared PROPERTIES
            INSTALL_RPATH ${_lib_install_rpath})
    elseif (APPLE)
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
      LINK_PUBLIC ${LIBPARQUET_INTERFACE_LINK_LIBS})
endif()

add_subdirectory(src/parquet)
add_subdirectory(src/parquet/api)
add_subdirectory(src/parquet/column)
add_subdirectory(src/parquet/file)
add_subdirectory(src/parquet/util)

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
      ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
      LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()
if (PARQUET_BUILD_SHARED)
    install(TARGETS parquet_shared
      ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
      LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()
