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

# Require cmake that supports BYPRODUCTS in add_custom_command, ExternalProject_Add [1].
cmake_minimum_required(VERSION 3.2.0)

file(READ "${CMAKE_CURRENT_SOURCE_DIR}/.parquetcppversion" PARQUET_VERSION)
string(REPLACE "\n" "" PARQUET_VERSION "${PARQUET_VERSION}")
string(REGEX MATCH "^([0-9]+\.[0-9]+\.[0-9]+(\.[0-9]+)?)" VERSION ${PARQUET_VERSION})
if(NOT VERSION)
  message(FATAL_ERROR "invalid .parquetcppversion")
endif()

project(parquet-cpp VERSION ${VERSION})
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "C++ library to read and write the Apache Parquet columnar data format")
set(CPACK_PACKAGE_VERSION_MAJOR ${PROJECT_VERSION_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${PROJECT_VERSION_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH ${PROJECT_VERSION_PATCH})
set(CPACK_PACKAGE_VENDOR        "Apache Software Foundation")
set(CPACK_PACKAGE_CONTACT       "Apache Parquet Development <dev@parquet.apache.org>")
set(CPACK_STRIP_FILES           TRUE)
set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_SOURCE_DIR}/LICENSE.txt")
set(CPACK_DEBIAN_FILE_NAME      "DEB-DEFAULT")
set(CPACK_RPM_FILE_NAME         "RPM-DEFAULT")
set(CPACK_RPM_PACKAGE_LICENSE   "Apache v2.0")

include(ExternalProject)
include(FindPkgConfig)

# This ensures that things like gnu++11 get passed correctly
set(CMAKE_CXX_STANDARD 11)

# We require a C++11 compliant compiler
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(GNUInstallDirs)

set(PARQUET_SO_VERSION ${PROJECT_VERSION_MAJOR})
set(PARQUET_ABI_VERSION ${PROJECT_VERSION})
if(WIN32)
  set(INSTALL_CMAKE_DIR cmake)
else()
  set(INSTALL_CMAKE_DIR "${CMAKE_INSTALL_DATAROOTDIR}/${PROJECT_NAME}/cmake")
endif()
set(CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION "/usr/${CMAKE_INSTALL_LIBDIR}/pkgconfig")
include(CPack)

include(CMakePackageConfigHelpers)
configure_package_config_file(cmake_modules/${PROJECT_NAME}Config.cmake.in
                              ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake
                              INSTALL_DESTINATION ${INSTALL_CMAKE_DIR}
                              PATH_VARS CMAKE_INSTALL_INCLUDEDIR CMAKE_INSTALL_LIBDIR)
write_basic_package_version_file(${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake
                                 COMPATIBILITY SameMajorVersion)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake
              ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake
DESTINATION ${INSTALL_CMAKE_DIR})

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

set(CLANG_FORMAT_VERSION "6.0")
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

  set(PARQUET_BUILD_WARNING_LEVEL "PRODUCTION" CACHE STRING
    "Levels of compiler warnings for development: PRODUCTION/CHECKIN/EVERYTHING")

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
  option(PARQUET_RPATH_ORIGIN
    "Build Parquet libraries with RPATH set to \$ORIGIN"
    OFF)
  option(PARQUET_MINIMAL_DEPENDENCY
    "Depend only on Thirdparty headers to build libparquet. Always OFF if building binaries"
    OFF)

  option(PARQUET_THRIFT_USE_BOOST
    "Enable if Thirdparty Thrift uses boost::shared_ptr (Apache Thrift < 0.11)"
    OFF)

  if (MSVC)
    set(ARROW_MSVC_STATIC_LIB_SUFFIX "_static" CACHE STRING
      "Arrow static lib suffix used on Windows with MSVC (default _static)")
    set(THRIFT_MSVC_STATIC_LIB_SUFFIX "md" CACHE STRING
      "Thrift static lib suffix used on Windows with MSVC (default md)")
    option(PARQUET_USE_STATIC_CRT
      "Build Parquet with statically linked CRT"
      OFF)
    option(PARQUET_USE_CLCACHE
      "Use clcache if available"
      ON)
  endif()

  option(PARQUET_VERBOSE_THIRDPARTY_BUILD
    "If off, output from ExternalProjects will be logged to files rather than shown"
    OFF)

endif()

if (MSVC AND PARQUET_USE_CLCACHE AND
     (("${CMAKE_GENERATOR}" STREQUAL "NMake Makefiles") OR
      ("${CMAKE_GENERATOR}" STREQUAL "Ninja")))
  find_program(CLCACHE_FOUND clcache)
  if(CLCACHE_FOUND)
    set(CMAKE_CXX_COMPILER ${CLCACHE_FOUND})
  endif(CLCACHE_FOUND)
endif()

include(BuildUtils)

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

if (MSVC)
  # define output directories to overwrite default Visual Studio generator output directories
  FOREACH(BUILD_CONFIG ${CMAKE_CONFIGURATION_TYPES})
    string(TOUPPER ${BUILD_CONFIG} UPPERCASE_BUILD_CONFIG)
    set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_${UPPERCASE_BUILD_CONFIG} ${BUILD_OUTPUT_ROOT_DIRECTORY})
    set(CMAKE_PDB_OUTPUT_DIRECTORY_${UPPERCASE_BUILD_CONFIG} ${BUILD_OUTPUT_ROOT_DIRECTORY})
    set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY_${UPPERCASE_BUILD_CONFIG} ${BUILD_OUTPUT_ROOT_DIRECTORY})
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_${UPPERCASE_BUILD_CONFIG} ${BUILD_OUTPUT_ROOT_DIRECTORY})
  ENDFOREACH()
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

  if(WIN32)
    add_test(${BENCHMARK_NAME} ${BENCHMARK_PATH} ${NO_COLOR})
  else()
    add_test(${BENCHMARK_NAME}
      ${BUILD_SUPPORT_DIR}/run-test.sh ${CMAKE_BINARY_DIR} benchmark ${BENCHMARK_PATH} ${NO_COLOR})
  endif()
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
  elseif(MSVC)
    add_test(${TEST_NAME} ${TEST_PATH})
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

# Determine compiler version
include(CompilerInfo)
include(SetupCxxFlags)

include_directories(${CMAKE_CURRENT_BINARY_DIR}/src)
include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}/src
)

if (PARQUET_MINIMAL_DEPENDENCY)
    set(IGNORE_OPTIONAL_PACKAGES ON)
    message(STATUS "Build using minimal dependencies")
else()
    set(IGNORE_OPTIONAL_PACKAGES OFF)
endif()
include(ThirdpartyToolchain)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_COMMON_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${PARQUET_CXXFLAGS}")

message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")

# Thrift requires these definitions for some types that we use
add_definitions(-DHAVE_INTTYPES_H -DHAVE_NETDB_H)
if (MSVC)
  add_definitions(-DNOMINMAX -D_CRT_SECURE_NO_WARNINGS)
else()
  add_definitions(-DHAVE_NETINET_IN_H -fPIC)
endif()

############################################################
# "make lint" target
############################################################
if (UNIX)
  find_program(CPPLINT_BIN NAMES cpplint cpplint.py HINTS ${BUILD_SUPPORT_DIR})
  message(STATUS "Found cpplint executable at ${CPPLINT_BIN}")
  # Full lint
  add_custom_target(lint ${CPPLINT_BIN}
  --verbose=2
  --linelength=90
  --filter=-whitespace/comments,-readability/todo,-build/header_guard,-runtime/references,-readability/check,-build/c++11,-build/include_order
    `find ${CMAKE_CURRENT_SOURCE_DIR}/src ${CMAKE_CURRENT_SOURCE_DIR}/tools ${CMAKE_CURRENT_SOURCE_DIR}/examples  ${CMAKE_CURRENT_SOURCE_DIR}/benchmarks -name \\*.cc -or -name \\*.h | sed -e '/parquet\\/parquet_/g'`)
endif (UNIX)

############################################################
# "make format" and "make check-format" targets
############################################################

# runs clang format and updates files in place.
add_custom_target(format-example
  COMMAND
  ${BUILD_SUPPORT_DIR}/run_clang_format.py
  ${CLANG_FORMAT_BIN}
  ${BUILD_SUPPORT_DIR}/clang_format_exclusions.txt
  ${CMAKE_CURRENT_SOURCE_DIR}/examples)

add_custom_target(format
  DEPENDS format-example
  COMMAND
  ${BUILD_SUPPORT_DIR}/run_clang_format.py
  ${CLANG_FORMAT_BIN}
  ${BUILD_SUPPORT_DIR}/clang_format_exclusions.txt
  ${CMAKE_CURRENT_SOURCE_DIR}/src)

# runs clang format and exits with a non-zero exit code if any files need to be reformatted

# TODO(wesm): Make this work in run_clang_format.py
add_custom_target(check-format-examples ${BUILD_SUPPORT_DIR}/run_clang_format.py
   ${CLANG_FORMAT_BIN}
   ${BUILD_SUPPORT_DIR}/clang_format_exclusions.txt
   ${CMAKE_CURRENT_SOURCE_DIR}/examples 1)
add_custom_target(check-format
   DEPENDS check-format-examples
   COMMAND
   ${BUILD_SUPPORT_DIR}/run_clang_format.py
   ${CLANG_FORMAT_BIN}
   ${BUILD_SUPPORT_DIR}/clang_format_exclusions.txt
   ${CMAKE_CURRENT_SOURCE_DIR}/src 1)

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
  if(MSVC)
    set(BOOST_LINK_LIBS ${BOOST_LINK_LIBS}
      boost_shared_system)
  endif()
else()
  set(BOOST_LINK_LIBS
    boost_static_regex)
  if(MSVC)
    set(BOOST_LINK_LIBS ${BOOST_LINK_LIBS}
      boost_static_system boost_static_filesystem)
  endif()
endif()

#############################################################
# Apache Arrow linkage

if ("${PARQUET_ARROW_LINKAGE}" STREQUAL "shared")
  set(ARROW_LINK_LIBS
    arrow)
else()
  #############################################################
  # Transitive Library Linkage

  if (NOT DEFINED ENV{BROTLI_STATIC_LIB_ENC} OR
      NOT DEFINED ENV{BROTLI_STATIC_LIB_DEC} OR
      NOT DEFINED ENV{BROTLI_STATIC_LIB_COMMON} OR
      NOT DEFINED ENV{SNAPPY_STATIC_LIB} OR
      NOT DEFINED ENV{ZLIB_STATIC_LIB} OR
      NOT DEFINED ENV{LZ4_STATIC_LIB} OR
      NOT DEFINED ENV{ZSTD_STATIC_LIB})
    message(FATAL_ERROR "Missing transitive dependencies for Arrow static linking")
  endif()

  set(BROTLI_STATIC_LIB_ENC "$ENV{BROTLI_STATIC_LIB_ENC}")
  set(BROTLI_STATIC_LIB_DEC "$ENV{BROTLI_STATIC_LIB_DEC}")
  set(BROTLI_STATIC_LIB_COMMON "$ENV{BROTLI_STATIC_LIB_COMMON}")
  set(SNAPPY_STATIC_LIB "$ENV{SNAPPY_STATIC_LIB}")
  set(ZLIB_STATIC_LIB "$ENV{ZLIB_STATIC_LIB}")
  set(LZ4_STATIC_LIB "$ENV{LZ4_STATIC_LIB}")
  set(ZSTD_STATIC_LIB "$ENV{ZSTD_STATIC_LIB}")

  add_library(brotli_enc STATIC IMPORTED)
  set_target_properties(brotli_enc PROPERTIES IMPORTED_LOCATION ${BROTLI_STATIC_LIB_ENC})
  add_library(brotli_dec STATIC IMPORTED)
  set_target_properties(brotli_dec PROPERTIES IMPORTED_LOCATION ${BROTLI_STATIC_LIB_DEC})
  add_library(brotli_common STATIC IMPORTED)
  set_target_properties(brotli_common PROPERTIES IMPORTED_LOCATION ${BROTLI_STATIC_LIB_COMMON})
  add_library(snappy STATIC IMPORTED)
  set_target_properties(snappy PROPERTIES IMPORTED_LOCATION ${SNAPPY_STATIC_LIB})
  add_library(zlib STATIC IMPORTED)
  set_target_properties(zlib PROPERTIES IMPORTED_LOCATION ${ZLIB_STATIC_LIB})
  add_library(lz4 STATIC IMPORTED)
  set_target_properties(lz4 PROPERTIES IMPORTED_LOCATION ${LZ4_STATIC_LIB})
  add_library(zstd STATIC IMPORTED)
  set_target_properties(zstd PROPERTIES IMPORTED_LOCATION ${ZSTD_STATIC_LIB})

  set(TRANSITIVE_LINK_LIBS
    snappy
    zlib
    brotli_enc
    brotli_dec
    brotli_common
    lz4
    zstd
  )

  add_definitions(-DARROW_STATIC)

  set(ARROW_LINK_LIBS
    arrow_static
    ${TRANSITIVE_LINK_LIBS})
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

if (NOT MSVC)
  set_source_files_properties(src/parquet/parquet_types.cpp PROPERTIES
    COMPILE_FLAGS -Wno-unused-variable)
endif()

# List of thrift output targets
set(THRIFT_OUTPUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/src/parquet)
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
  src/parquet/arrow/reader.cc
  src/parquet/arrow/record_reader.cc
  src/parquet/arrow/schema.cc
  src/parquet/arrow/writer.cc
  src/parquet/bloom_filter.cc
  src/parquet/column_reader.cc
  src/parquet/column_scanner.cc
  src/parquet/column_writer.cc
  src/parquet/exception.cc
  src/parquet/file_reader.cc
  src/parquet/file_writer.cc
  src/parquet/metadata.cc
  src/parquet/murmur3.cc
  src/parquet/parquet_constants.cpp
  src/parquet/parquet_types.cpp
  src/parquet/printer.cc
  src/parquet/schema.cc
  src/parquet/statistics.cc
  src/parquet/types.cc
  src/parquet/util/comparison.cc
  src/parquet/util/memory.cc
)

# # Ensure that thrift compilation is done before using its generated headers
# # in parquet code.
add_custom_target(thrift-deps ALL
  DEPENDS ${THRIFT_OUTPUT_FILES})
set(PARQUET_DEPENDENCIES ${PARQUET_DEPENDENCIES} thrift-deps)

if (NOT PARQUET_MINIMAL_DEPENDENCY)
# These are libraries that we will link privately with parquet_shared (as they
# do not need to be linked transitively by other linkers), but publicly with
# parquet_static (because internal users need to transitively link all
# dependencies)
  set(LIBPARQUET_INTERFACE_LINK_LIBS
    ${ARROW_LINK_LIBS}
    ${BOOST_LINK_LIBS}
    thriftstatic
  )
# Although we don't link parquet_objlib against anything, we need it to depend
# on these libs as we may generate their headers via ExternalProject_Add
  set(PARQUET_DEPENDENCIES ${PARQUET_DEPENDENCIES} ${LIBPARQUET_INTERFACE_LINK_LIBS})
endif()

if(NOT APPLE AND NOT MSVC)
  # Localize thirdparty symbols using a linker version script. This hides them
  # from the client application. The OS X linker does not support the
  # version-script option.
  set(SHARED_LINK_FLAGS "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/src/parquet/symbols.map")
endif()

ADD_LIB(parquet
        SOURCES ${LIBPARQUET_SRCS}
        LIB_BUILD_SHARED ${PARQUET_BUILD_SHARED}
        LIB_BUILD_STATIC ${PARQUET_BUILD_STATIC}
        DEPENDENCIES ${PARQUET_DEPENDENCIES}
        SHARED_LINK_FLAGS ${SHARED_LINK_FLAGS}
        SHARED_PRIVATE_LINK_LIBS ${LIBPARQUET_INTERFACE_LINK_LIBS}
        STATIC_LINK_LIBS ${LIBPARQUET_INTERFACE_LINK_LIBS}
        ABI_VERSION ${PARQUET_ABI_VERSION}
        SO_VERSION ${PARQUET_SO_VERSION}
)

############################################################
# Visibility
############################################################
# For generate_export_header() and add_compiler_export_flags().
include(GenerateExportHeader)

# Adapted from Apache Kudu: https://github.com/apache/kudu/commit/bd549e13743a51013585
# Honor visibility properties for all target types. See
# "cmake --help-policy CMP0063" for details.
#
# This policy was only added to cmake in version 3.3, so until the cmake in
# thirdparty is updated, we must check if the policy exists before setting it.
if(POLICY CMP0063)
  cmake_policy(SET CMP0063 NEW)
endif()

if (PARQUET_BUILD_SHARED)
  if (POLICY CMP0063)
    set_target_properties(parquet_shared
      PROPERTIES C_VISIBILITY_PRESET hidden)
    set_target_properties(parquet_shared
      PROPERTIES CXX_VISIBILITY_PRESET hidden)
    set_target_properties(parquet_shared
      PROPERTIES VISIBILITY_INLINES_HIDDEN 1)
  else()
    # Sets -fvisibility=hidden for gcc
    add_compiler_export_flags()
  endif()
endif()

add_subdirectory(src/parquet)
add_subdirectory(src/parquet/api)
add_subdirectory(src/parquet/arrow)
add_subdirectory(src/parquet/util)

if (NOT MSVC)
  add_subdirectory(benchmarks)
endif()
add_subdirectory(examples/low-level-api)
add_subdirectory(tools)

add_custom_target(clean-all
   COMMAND ${CMAKE_BUILD_TOOL} clean
   COMMAND ${CMAKE_COMMAND} -P cmake_modules/clean-all.cmake
)
