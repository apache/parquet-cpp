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

set(ARROW_PREFIX "${BUILD_OUTPUT_ROOT_DIRECTORY}")
set(ARROW_INCLUDE_DIR "${ARROW_PREFIX}/include")
set(ARROW_LIB_DIR "${ARROW_PREFIX}")
if (MSVC)
  set(ARROW_SHARED_LIB "${ARROW_PREFIX}/bin/arrow.dll")
  set(ARROW_SHARED_IMPLIB "${ARROW_LIB_DIR}/arrow.lib")
  set(ARROW_STATIC_LIB "${ARROW_LIB_DIR}/arrow_static.lib")
else()
  set(ARROW_SHARED_LIB "${ARROW_LIB_DIR}/libarrow${CMAKE_SHARED_LIBRARY_SUFFIX}")
  set(ARROW_STATIC_LIB "${ARROW_LIB_DIR}/libarrow.a")
endif()

set(ARROW_CMAKE_ARGS
  -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
  -DCMAKE_CXX_FLAGS=${EP_CXX_FLAGS}
  -DCMAKE_C_FLAGS=${EP_C_FLAGS}
  -DCMAKE_INSTALL_PREFIX=${ARROW_PREFIX}
  -DCMAKE_INSTALL_LIBDIR=${ARROW_LIB_DIR}
  -DARROW_JEMALLOC=OFF
  -DARROW_IPC=OFF
  -DARROW_WITH_LZ4=ON
  -DARROW_WITH_ZSTD=ON
  -DARROW_BUILD_SHARED=${PARQUET_BUILD_SHARED}
  -DARROW_BOOST_USE_SHARED=${PARQUET_BOOST_USE_SHARED}
  -DARROW_BUILD_TESTS=OFF)

if (MSVC AND PARQUET_USE_STATIC_CRT)
  set(ARROW_CMAKE_ARGS ${ARROW_CMAKE_ARGS} -DARROW_USE_STATIC_CRT=ON)
endif()

if ("$ENV{PARQUET_ARROW_VERSION}" STREQUAL "")
  set(ARROW_VERSION "501d60e918bd4d10c429ab34e0b8e8a87dffb732")
else()
  set(ARROW_VERSION "$ENV{PARQUET_ARROW_VERSION}")
endif()
message(STATUS "Building Apache Arrow from commit: ${ARROW_VERSION}")

set(ARROW_URL "https://github.com/apache/arrow/archive/${ARROW_VERSION}.tar.gz")

if (CMAKE_VERSION VERSION_GREATER "3.7")
  set(ARROW_CONFIGURE SOURCE_SUBDIR "cpp" CMAKE_ARGS ${ARROW_CMAKE_ARGS})
else()
  set(ARROW_CONFIGURE CONFIGURE_COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}"
    ${ARROW_CMAKE_ARGS} "${CMAKE_CURRENT_BINARY_DIR}/arrow_ep-prefix/src/arrow_ep/cpp")
endif()

ExternalProject_Add(arrow_ep
  URL ${ARROW_URL}
  ${ARROW_CONFIGURE}
  BUILD_BYPRODUCTS "${ARROW_SHARED_LIB}" "${ARROW_STATIC_LIB}")

if (MSVC)
  ExternalProject_Add_Step(arrow_ep copy_dll_step
    DEPENDEES install
    COMMAND ${CMAKE_COMMAND} -E make_directory ${BUILD_OUTPUT_ROOT_DIRECTORY}
    COMMAND ${CMAKE_COMMAND} -E copy ${ARROW_SHARED_LIB} ${BUILD_OUTPUT_ROOT_DIRECTORY})
endif()
