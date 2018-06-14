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

set(GTEST_VERSION "1.8.0")
set(GBENCHMARK_VERSION "1.1.0")
set(THRIFT_VENDOR_VERSION "0.11.0")

string(TOUPPER ${CMAKE_BUILD_TYPE} UPPERCASE_BUILD_TYPE)

set(EP_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}}")
set(EP_C_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_${UPPERCASE_BUILD_TYPE}}")

if (NOT MSVC)
  # Set -fPIC on all external projects
  set(EP_CXX_FLAGS "${EP_CXX_FLAGS} -fPIC")
  set(EP_C_FLAGS "${EP_C_FLAGS} -fPIC")
endif()

if (NOT PARQUET_VERBOSE_THIRDPARTY_BUILD)
  set(EP_LOG_OPTIONS
    LOG_CONFIGURE 1
    LOG_BUILD 1
    LOG_INSTALL 1
    LOG_DOWNLOAD 1)
  set(Boost_DEBUG FALSE)
else()
  set(EP_LOG_OPTIONS)
  set(Boost_DEBUG TRUE)
endif()

# ----------------------------------------------------------------------
# Configure toolchain with environment variables, if the exist

if (NOT "$ENV{PARQUET_BUILD_TOOLCHAIN}" STREQUAL "")
  set(THRIFT_HOME "$ENV{PARQUET_BUILD_TOOLCHAIN}")
  set(ARROW_HOME "$ENV{PARQUET_BUILD_TOOLCHAIN}")

  if (NOT DEFINED ENV{BOOST_ROOT})
    # Since we have to set this in the environment, we check whether
    # $BOOST_ROOT is defined inside here
    set(ENV{BOOST_ROOT} "$ENV{PARQUET_BUILD_TOOLCHAIN}")
  endif()
endif()

if (DEFINED ENV{THRIFT_HOME})
  set(THRIFT_HOME "$ENV{THRIFT_HOME}")
endif()

# ----------------------------------------------------------------------
# Boost

# find boost headers and libs
set(Boost_USE_MULTITHREADED ON)
if (MSVC AND PARQUET_USE_STATIC_CRT)
  set(Boost_USE_STATIC_RUNTIME ON)
endif()
if (MSVC)
  # disable autolinking in boost
  add_definitions(-DBOOST_ALL_NO_LIB)
endif()
if (PARQUET_BOOST_USE_SHARED)
  # Find shared Boost libraries.
  set(Boost_USE_STATIC_LIBS OFF)

  if (MSVC)
    # force all boost libraries to dynamic link
    add_definitions(-DBOOST_ALL_DYN_LINK)
    find_package(Boost COMPONENTS regex system REQUIRED)
  else()
    find_package(Boost COMPONENTS regex REQUIRED)
  endif()

  if ("${UPPERCASE_BUILD_TYPE}" STREQUAL "DEBUG")
    set(BOOST_SHARED_REGEX_LIBRARY ${Boost_REGEX_LIBRARY_DEBUG})
    if (MSVC)
      set(BOOST_SHARED_SYSTEM_LIBRARY ${Boost_SYSTEM_LIBRARY_DEBUG})
    endif()
  else()
    set(BOOST_SHARED_REGEX_LIBRARY ${Boost_REGEX_LIBRARY_RELEASE})
    if (MSVC)
      set(BOOST_SHARED_SYSTEM_LIBRARY ${Boost_SYSTEM_LIBRARY_RELEASE})
    endif()
  endif()
else()
  # Find static Boost libraries.
  set(Boost_USE_STATIC_LIBS ON)
  if (MSVC)
    find_package(Boost COMPONENTS regex system filesystem REQUIRED)
  else()
    find_package(Boost COMPONENTS regex REQUIRED)
  endif()
  if ("${UPPERCASE_BUILD_TYPE}" STREQUAL "DEBUG")
    set(BOOST_STATIC_REGEX_LIBRARY ${Boost_REGEX_LIBRARY_DEBUG})
    if (MSVC)
      set(BOOST_STATIC_SYSTEM_LIBRARY ${Boost_SYSTEM_LIBRARY_DEBUG})
      set(BOOST_STATIC_FILESYSTEM_LIBRARY ${Boost_FILESYSTEM_LIBRARY_DEBUG})
    endif()
  else()
    set(BOOST_STATIC_REGEX_LIBRARY ${Boost_REGEX_LIBRARY_RELEASE})
    if (MSVC)
      set(BOOST_STATIC_SYSTEM_LIBRARY ${Boost_SYSTEM_LIBRARY_RELEASE})
      set(BOOST_STATIC_FILESYSTEM_LIBRARY ${Boost_FILESYSTEM_LIBRARY_RELEASE})
    endif()
  endif()
endif()

message(STATUS "Boost include dir: " ${Boost_INCLUDE_DIRS})
message(STATUS "Boost libraries: " ${Boost_LIBRARIES})
if (PARQUET_BOOST_USE_SHARED)
  add_library(boost_shared_regex SHARED IMPORTED)
  if (MSVC)
    set_target_properties(boost_shared_regex
                          PROPERTIES IMPORTED_IMPLIB "${BOOST_SHARED_REGEX_LIBRARY}")
  else()
    set_target_properties(boost_shared_regex
                          PROPERTIES IMPORTED_LOCATION "${BOOST_SHARED_REGEX_LIBRARY}")
  endif()
  if (MSVC)
    add_library(boost_shared_system SHARED IMPORTED)
    set_target_properties(boost_shared_system
                          PROPERTIES IMPORTED_IMPLIB "${BOOST_SHARED_SYSTEM_LIBRARY}")
  endif()
else()
  add_library(boost_static_regex STATIC IMPORTED)
  set_target_properties(boost_static_regex PROPERTIES IMPORTED_LOCATION ${BOOST_STATIC_REGEX_LIBRARY})
  if (MSVC)
    add_library(boost_static_system STATIC IMPORTED)
    set_target_properties(boost_static_system PROPERTIES IMPORTED_LOCATION ${BOOST_STATIC_SYSTEM_LIBRARY})
    add_library(boost_static_filesystem STATIC IMPORTED)
    set_target_properties(boost_static_filesystem PROPERTIES IMPORTED_LOCATION ${BOOST_STATIC_FILESYSTEM_LIBRARY})
  endif()
endif()

include_directories(SYSTEM ${Boost_INCLUDE_DIRS})
set(LIBS ${LIBS} ${Boost_LIBRARIES})

# ----------------------------------------------------------------------
# ZLIB

# ----------------------------------------------------------------------
# Thrift

# find thrift headers and libs
find_package(Thrift)

if (NOT THRIFT_FOUND)
  set(ZLIB_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/zlib_ep/src/zlib_ep-install")
  set(ZLIB_HOME "${ZLIB_PREFIX}")
  set(ZLIB_INCLUDE_DIR "${ZLIB_PREFIX}/include")
  if (MSVC)
    if (${UPPERCASE_BUILD_TYPE} STREQUAL "DEBUG")
      set(ZLIB_STATIC_LIB_NAME zlibstaticd.lib)
    else()
      set(ZLIB_STATIC_LIB_NAME zlibstatic.lib)
    endif()
  else()
    set(ZLIB_STATIC_LIB_NAME libz.a)
  endif()
  set(ZLIB_STATIC_LIB "${ZLIB_PREFIX}/lib/${ZLIB_STATIC_LIB_NAME}")
  set(ZLIB_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    -DCMAKE_INSTALL_PREFIX=${ZLIB_PREFIX}
    -DCMAKE_C_FLAGS=${EP_C_FLAGS}
    -DBUILD_SHARED_LIBS=OFF)
  ExternalProject_Add(zlib_ep
    URL "http://zlib.net/fossils/zlib-1.2.8.tar.gz"
    BUILD_BYPRODUCTS "${ZLIB_STATIC_LIB}"
    ${ZLIB_BUILD_BYPRODUCTS}
    ${EP_LOG_OPTIONS}
    CMAKE_ARGS ${ZLIB_CMAKE_ARGS})

  set(THRIFT_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/thrift_ep/src/thrift_ep-install")
  set(THRIFT_HOME "${THRIFT_PREFIX}")
  set(THRIFT_INCLUDE_DIR "${THRIFT_PREFIX}/include")
  set(THRIFT_COMPILER "${THRIFT_PREFIX}/bin/thrift")
  set(THRIFT_CMAKE_ARGS "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
                        "-DCMAKE_CXX_FLAGS=${EP_CXX_FLAGS}"
                        "-DCMAKE_C_FLAGS=${EP_C_FLAGS}"
                        "-DCMAKE_INSTALL_PREFIX=${THRIFT_PREFIX}"
                        "-DCMAKE_INSTALL_RPATH=${THRIFT_PREFIX}/lib"
                        "-DBUILD_SHARED_LIBS=OFF"
                        "-DBUILD_TESTING=OFF"
                        "-DBUILD_EXAMPLES=OFF"
                        "-DBUILD_TUTORIALS=OFF"
                        "-DWITH_QT4=OFF"
                        "-DWITH_C_GLIB=OFF"
                        "-DWITH_JAVA=OFF"
                        "-DWITH_PYTHON=OFF"
                        "-DWITH_HASKELL=OFF"
                        "-DWITH_CPP=ON"
                        "-DWITH_STATIC_LIB=ON"
                        "-DWITH_LIBEVENT=OFF"
                        )

  # Thrift also uses boost. Forward important boost settings if there were ones passed.
  if (DEFINED BOOST_ROOT)
    set(THRIFT_CMAKE_ARGS ${THRIFT_CMAKE_ARGS} "-DBOOST_ROOT=${BOOST_ROOT}")
  endif()
  if (DEFINED Boost_NAMESPACE)
    set(THRIFT_CMAKE_ARGS ${THRIFT_CMAKE_ARGS} "-DBoost_NAMESPACE=${Boost_NAMESPACE}")
  endif()

  set(THRIFT_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}thrift")
  if (MSVC)
    if (PARQUET_USE_STATIC_CRT)
      set(THRIFT_STATIC_LIB_NAME "${THRIFT_STATIC_LIB_NAME}mt")
      set(THRIFT_CMAKE_ARGS ${THRIFT_CMAKE_ARGS} "-DWITH_MT=ON")
    else()
      set(THRIFT_STATIC_LIB_NAME "${THRIFT_STATIC_LIB_NAME}md")
      set(THRIFT_CMAKE_ARGS ${THRIFT_CMAKE_ARGS} "-DWITH_MT=OFF")
    endif()
  endif()
  if (${UPPERCASE_BUILD_TYPE} STREQUAL "DEBUG")
    set(THRIFT_STATIC_LIB_NAME "${THRIFT_STATIC_LIB_NAME}d")
  endif()
  set(THRIFT_STATIC_LIB "${THRIFT_PREFIX}/lib/${THRIFT_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")

  if (MSVC)
    set(WINFLEXBISON_VERSION 2.4.9)
    set(WINFLEXBISON_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/winflexbison_ep/src/winflexbison_ep-install")
    ExternalProject_Add(winflexbison_ep
      URL https://github.com/lexxmark/winflexbison/releases/download/v.${WINFLEXBISON_VERSION}/win_flex_bison-${WINFLEXBISON_VERSION}.zip
      URL_HASH MD5=a2e979ea9928fbf8567e995e9c0df765
      SOURCE_DIR ${WINFLEXBISON_PREFIX}
      CONFIGURE_COMMAND ""
      BUILD_COMMAND ""
      INSTALL_COMMAND ""
      ${EP_LOG_OPTIONS})
    set(THRIFT_DEPENDENCIES ${THRIFT_DEPENDENCIES} winflexbison_ep)

    set(THRIFT_CMAKE_ARGS "-DFLEX_EXECUTABLE=${WINFLEXBISON_PREFIX}/win_flex.exe"
                          "-DBISON_EXECUTABLE=${WINFLEXBISON_PREFIX}/win_bison.exe"
                          "-DZLIB_INCLUDE_DIR=${ZLIB_INCLUDE_DIR}"
                          "-DZLIB_LIBRARY=${ZLIB_STATIC_LIB}"
                          "-DWITH_SHARED_LIB=OFF"
                          "-DWITH_PLUGIN=OFF"
                          ${THRIFT_CMAKE_ARGS})
    set(THRIFT_DEPENDENCIES ${THRIFT_DEPENDENCIES} zlib_ep)
  elseif (APPLE)
    if (DEFINED BISON_EXECUTABLE)
      set(THRIFT_CMAKE_ARGS "-DBISON_EXECUTABLE=${BISON_EXECUTABLE}"
                            ${THRIFT_CMAKE_ARGS})
    endif()
  endif()

  ExternalProject_Add(thrift_ep
    URL "http://archive.apache.org/dist/thrift/${THRIFT_VENDOR_VERSION}/thrift-${THRIFT_VENDOR_VERSION}.tar.gz"
    BUILD_BYPRODUCTS "${THRIFT_STATIC_LIB}" "${THRIFT_COMPILER}"
    CMAKE_ARGS ${THRIFT_CMAKE_ARGS}
    DEPENDS ${THRIFT_DEPENDENCIES}
    ${EP_LOG_OPTIONS})

  set(THRIFT_VENDORED 1)
else()
  set(THRIFT_VENDORED 0)
endif()

include_directories(SYSTEM ${THRIFT_INCLUDE_DIR} ${THRIFT_INCLUDE_DIR}/thrift)
message(STATUS "Thrift include dir: ${THRIFT_INCLUDE_DIR}")
message(STATUS "Thrift static library: ${THRIFT_STATIC_LIB}")
message(STATUS "Thrift compiler: ${THRIFT_COMPILER}")
message(STATUS "Thrift version: ${THRIFT_VERSION}")
add_library(thriftstatic STATIC IMPORTED)
set_target_properties(thriftstatic PROPERTIES IMPORTED_LOCATION ${THRIFT_STATIC_LIB})

if (THRIFT_VENDORED)
  add_dependencies(thriftstatic thrift_ep)
endif()

if (PARQUET_THRIFT_USE_BOOST)
  add_definitions(-DPARQUET_THRIFT_USE_BOOST)
  message(STATUS "Using Boost in Thrift header")
endif()

## GTest
if(PARQUET_BUILD_TESTS AND NOT IGNORE_OPTIONAL_PACKAGES)
  add_custom_target(unittest ctest -L unittest)

  if("$ENV{GTEST_HOME}" STREQUAL "")
    if(APPLE)
      set(GTEST_CMAKE_CXX_FLAGS "${EP_CXX_FLAGS} -DGTEST_USE_OWN_TR1_TUPLE=1 -Wno-unused-value -Wno-ignored-attributes")
    elseif(MSVC)
      # Workaround https://github.com/google/googletest/issues/1111 until
      # a new gtest version is released
      set(GTEST_CMAKE_CXX_FLAGS "${EP_CXX_FLAGS} -D_SILENCE_TR1_NAMESPACE_DEPRECATION_WARNING")
    else()
      set(GTEST_CMAKE_CXX_FLAGS "${EP_CXX_FLAGS}")
    endif()

    set(GTEST_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/googletest_ep-prefix/src/googletest_ep")
    set(GTEST_INCLUDE_DIR "${GTEST_PREFIX}/include")
    set(GTEST_STATIC_LIB
      "${GTEST_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}gtest${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(GTEST_MAIN_STATIC_LIB
      "${GTEST_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}gtest_main${CMAKE_STATIC_LIBRARY_SUFFIX}")

    set(GTEST_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
                         -DCMAKE_INSTALL_PREFIX=${GTEST_PREFIX}
                         -DCMAKE_CXX_FLAGS=${GTEST_CMAKE_CXX_FLAGS})

    if (MSVC AND NOT PARQUET_USE_STATIC_CRT)
      set(GTEST_CMAKE_ARGS ${GTEST_CMAKE_ARGS} -Dgtest_force_shared_crt=ON)
    endif()

    ExternalProject_Add(googletest_ep
      URL "https://github.com/google/googletest/archive/release-${GTEST_VERSION}.tar.gz"
      BUILD_BYPRODUCTS "${GTEST_STATIC_LIB}" "${GTEST_MAIN_STATIC_LIB}"
      CMAKE_ARGS ${GTEST_CMAKE_ARGS}
      ${EP_LOG_OPTIONS})
    set(GTEST_VENDORED 1)
  else()
    find_package(GTest REQUIRED)
    set(GTEST_VENDORED 0)
  endif()

  message(STATUS "GTest include dir: ${GTEST_INCLUDE_DIR}")
  message(STATUS "GTest static library: ${GTEST_STATIC_LIB}")
  include_directories(SYSTEM ${GTEST_INCLUDE_DIR})

  add_library(gtest STATIC IMPORTED)
  set_target_properties(gtest PROPERTIES IMPORTED_LOCATION ${GTEST_STATIC_LIB})

  add_library(gtest_main STATIC IMPORTED)
  set_target_properties(gtest_main PROPERTIES IMPORTED_LOCATION
    ${GTEST_MAIN_STATIC_LIB})

  if(GTEST_VENDORED)
    add_dependencies(gtest googletest_ep)
    add_dependencies(gtest_main googletest_ep)
  endif()
endif()

## Google Benchmark
if ("$ENV{GBENCHMARK_HOME}" STREQUAL "")
  set(GBENCHMARK_HOME ${THIRDPARTY_DIR}/installed)
endif()

if(PARQUET_BUILD_BENCHMARKS AND NOT IGNORE_OPTIONAL_PACKAGES)
  add_custom_target(runbenchmark ctest -L benchmark)

  if("$ENV{GBENCHMARK_HOME}" STREQUAL "")
    set(GBENCHMARK_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/gbenchmark_ep/src/gbenchmark_ep-install")
    set(GBENCHMARK_INCLUDE_DIR "${GBENCHMARK_PREFIX}/include")
    set(GBENCHMARK_STATIC_LIB "${GBENCHMARK_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}benchmark${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(GBENCHMARK_CMAKE_ARGS
          "-DCMAKE_INSTALL_PREFIX:PATH=${GBENCHMARK_PREFIX}"
          "-DBENCHMARK_ENABLE_TESTING=OFF"
          "-DCMAKE_CXX_FLAGS=${EP_CXX_FLAGS}")
    if (MSVC)
      set(GBENCHMARK_CMAKE_ARGS ${GBENCHMARK_CMAKE_ARGS} "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}")
    else()
      set(GBENCHMARK_CMAKE_ARGS ${GBENCHMARK_CMAKE_ARGS} "-DCMAKE_BUILD_TYPE=Release")
    endif()
    if (APPLE)
      set(GBENCHMARK_CMAKE_ARGS ${GBENCHMARK_CMAKE_ARGS} "-DBENCHMARK_USE_LIBCXX=ON")
    endif()

    ExternalProject_Add(gbenchmark_ep
      URL "https://github.com/google/benchmark/archive/v${GBENCHMARK_VERSION}.tar.gz"
      BUILD_BYPRODUCTS "${GBENCHMARK_STATIC_LIB}"
      CMAKE_ARGS ${GBENCHMARK_CMAKE_ARGS}
      ${EP_LOG_OPTIONS})
    set(GBENCHMARK_VENDORED 1)
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

# ----------------------------------------------------------------------
# Apache Arrow

find_package(Arrow)
if (NOT ARROW_FOUND)
  include(ArrowExternalProject)
  set(ARROW_VENDORED 1)
else()
  set(ARROW_VENDORED 0)
endif()

include_directories(SYSTEM ${ARROW_INCLUDE_DIR})
add_library(arrow SHARED IMPORTED)
if(MSVC)
  set_target_properties(arrow
                        PROPERTIES IMPORTED_IMPLIB "${ARROW_SHARED_IMPLIB}")
else()
  set_target_properties(arrow
                        PROPERTIES IMPORTED_LOCATION "${ARROW_SHARED_LIB}")
endif()
add_library(arrow_static STATIC IMPORTED)
set_target_properties(arrow_static PROPERTIES IMPORTED_LOCATION ${ARROW_STATIC_LIB})

if (ARROW_VENDORED)
  add_dependencies(arrow arrow_ep)
  add_dependencies(arrow_static arrow_ep)
endif()
