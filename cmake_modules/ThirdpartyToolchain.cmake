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

set(GTEST_VERSION "1.7.0")
set(GBENCHMARK_VERSION "1.0.0")
set(SNAPPY_VERSION "1.1.3")
set(THRIFT_VERSION "0.9.1")

# Brotli 0.5.2 does not install headers/libraries yet, but 0.6.0.dev does
set(BROTLI_VERSION "5db62dcc9d386579609540cdf8869e95ad334bbd")
set(ARROW_VERSION "fa8d27f314b7c21c611d1c5caaa9b32ae0cb2b06")

# find boost headers and libs
set(Boost_DEBUG TRUE)
set(Boost_USE_MULTITHREADED ON)
find_package(Boost REQUIRED)
include_directories(SYSTEM ${Boost_INCLUDE_DIRS})
set(LIBS ${LIBS} ${Boost_LIBRARIES})
message(STATUS "Boost include dir: " ${Boost_INCLUDE_DIRS})
message(STATUS "Boost libraries: " ${Boost_LIBRARIES})

string(TOUPPER ${CMAKE_BUILD_TYPE} UPPERCASE_BUILD_TYPE)
# Set -fPIC on all external projects and include the main CXX_FLAGS
set(EP_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}} -fPIC")
set(EP_C_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_${UPPERCASE_BUILD_TYPE}} -fPIC")

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
  set(THRIFT_CONFIGURE_COMMAND
      ./configure "CFLAGS=${EP_C_FLAGS}" "CXXFLAGS=${EP_CXX_FLAGS}" --without-qt4 --without-c_glib --without-csharp --without-java --without-erlang --without-nodejs --without-lua --without-python --without-perl --without-php --without-php_extension --without-ruby --without-haskell --without-go --without-d --with-cpp "--prefix=${THRIFT_PREFIX}")

  if (CMAKE_VERSION VERSION_GREATER "3.2")
    # BUILD_BYPRODUCTS is a 3.2+ feature
    ExternalProject_Add(thrift_ep
      CONFIGURE_COMMAND ${THRIFT_CONFIGURE_COMMAND}
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
      CONFIGURE_COMMAND ${THRIFT_CONFIGURE_COMMAND}
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

  if (${UPPERCASE_BUILD_TYPE} EQUAL "RELEASE")
    if (APPLE)
      set(SNAPPY_CXXFLAGS "CXXFLAGS='-DNDEBUG -O1'")
    else()
      set(SNAPPY_CXXFLAGS "CXXFLAGS='-DNDEBUG -O2'")
    endif()
  endif()

  if (CMAKE_VERSION VERSION_GREATER "3.2")
    # BUILD_BYPRODUCTS is a 3.2+ feature
    ExternalProject_Add(snappy_ep
      CONFIGURE_COMMAND ./configure --with-pic "--prefix=${SNAPPY_PREFIX}" ${SNAPPY_CXXFLAGS}
      BUILD_IN_SOURCE 1
      BUILD_COMMAND ${MAKE}
      INSTALL_DIR ${SNAPPY_PREFIX}
      URL "https://github.com/google/snappy/releases/download/${SNAPPY_VERSION}/snappy-${SNAPPY_VERSION}.tar.gz"
      BUILD_BYPRODUCTS "${SNAPPY_STATIC_LIB}"
      )
  else()
    ExternalProject_Add(snappy_ep
      CONFIGURE_COMMAND ./configure --with-pic "--prefix=${SNAPPY_PREFIX}" ${SNAPPY_CXXFLAGS}
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
                        "-DCMAKE_CXX_FLAGS=${EP_CXX_FLAGS}"
                        "-DCMAKE_C_FLAGS=${EX_C_FLAGS}"
                        -DCMAKE_INSTALL_PREFIX=${BROTLI_PREFIX}
                        -DCMAKE_INSTALL_LIBDIR=lib/${CMAKE_LIBRARY_ARCHITECTURE}
                        -DBUILD_SHARED_LIBS=OFF)

  if (CMAKE_VERSION VERSION_GREATER "3.2")
    # BUILD_BYPRODUCTS is a 3.2+ feature
    ExternalProject_Add(brotli_ep
      URL "https://github.com/google/brotli/archive/${BROTLI_VERSION}.tar.gz"
      BUILD_BYPRODUCTS "${BROTLI_LIBRARY_ENC}" "${BROTLI_LIBRARY_DEC}" "${BROTLI_LIBRARY_COMMON}"
      CMAKE_ARGS ${BROTLI_CMAKE_ARGS})
  else()
    ExternalProject_Add(brotli_ep
      URL "https://github.com/google/brotli/archive/${BROTLI_VERSION}.tar.gz"
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
if (NOT PARQUET_ZLIB_VENDORED)
  find_package(ZLIB)
endif()

if (NOT ZLIB_FOUND)
  set(ZLIB_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/zlib_ep/src/zlib_ep-install")
  set(ZLIB_HOME "${ZLIB_PREFIX}")
  set(ZLIB_INCLUDE_DIR "${ZLIB_PREFIX}/include")
  set(ZLIB_STATIC_LIB "${ZLIB_PREFIX}/lib/libz.a")
  set(ZLIB_VENDORED 1)
  set(ZLIB_CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
                      -DCMAKE_INSTALL_PREFIX=${ZLIB_PREFIX}
                      -DCMAKE_C_FLAGS=${EP_C_FLAGS}
                      -DBUILD_SHARED_LIBS=OFF)

  if (CMAKE_VERSION VERSION_GREATER "3.2")
    # BUILD_BYPRODUCTS is a 3.2+ feature
    ExternalProject_Add(zlib_ep
      URL "http://zlib.net/fossils/zlib-1.2.8.tar.gz"
      BUILD_BYPRODUCTS "${ZLIB_STATIC_LIB}"
      CMAKE_ARGS ${ZLIB_CMAKE_ARGS})
  else()
    ExternalProject_Add(zlib_ep
      URL "http://zlib.net/fossils/zlib-1.2.8.tar.gz"
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
      set(GTEST_CMAKE_CXX_FLAGS "${EP_CXX_FLAGS} -DGTEST_USE_OWN_TR1_TUPLE=1 -Wno-unused-value -Wno-ignored-attributes")
    else()
      set(GTEST_CMAKE_CXX_FLAGS "${EP_CXX_FLAGS}")
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
    set(GBENCHMARK_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/gbenchmark_ep/src/gbenchmark_ep-install")
    set(GBENCHMARK_INCLUDE_DIR "${GBENCHMARK_PREFIX}/include")
    set(GBENCHMARK_STATIC_LIB "${GBENCHMARK_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}benchmark${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(GBENCHMARK_VENDORED 1)
    set(GBENCHMARK_CMAKE_ARGS
          "-DCMAKE_BUILD_TYPE=Release"
          "-DCMAKE_INSTALL_PREFIX:PATH=${GBENCHMARK_PREFIX}"
          "-DCMAKE_CXX_FLAGS=${EP_CXX_FLAGS}")
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
    -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
    -DCMAKE_C_FLAGS=${CMAKE_C_FLAGS}
    -DCMAKE_INSTALL_PREFIX=${ARROW_PREFIX}
    -DARROW_JEMALLOC=OFF
    -DARROW_BUILD_TESTS=OFF)

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
