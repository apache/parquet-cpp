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

include(toolchain)

project(parquet-cpp)

# if no build build type is specified, default to debug builds
if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif(NOT CMAKE_BUILD_TYPE)

# set compile output directory
string (TOLOWER ${CMAKE_BUILD_TYPE} BUILD_SUBDIR_NAME)

SET(CMAKE_CXX_FLAGS "-std=c++11 -msse4.2 -Wall -Wno-unused-value -Wno-unused-variable -Wno-sign-compare -Wno-unknown-pragmas")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -ggdb")

# If build in-source, create the latest symlink. If build out-of-source, which is
# preferred, simply output the binaries in the build folder
if (${CMAKE_SOURCE_DIR} STREQUAL ${CMAKE_CURRENT_BINARY_DIR})
  set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/build/${BUILD_SUBDIR_NAME}/")
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
  set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/${BUILD_SUBDIR_NAME}/")
endif()

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

############################################################
# Dependencies
############################################################

# ADD_THIRDPARTY_LIB function used in Kudu
function(ADD_THIRDPARTY_LIB LIB_NAME)
  set(options)
  set(one_value_args SHARED_LIB STATIC_LIB)
  set(multi_value_args DEPS)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  if(("${PARQUET_LINK}" STREQUAL "s" AND ARG_STATIC_LIB) OR (NOT ARG_SHARED_LIB))
    if(NOT ARG_STATIC_LIB)
      message(FATAL_ERROR "No static or shared library provided for ${LIB_NAME}")
    endif()
    add_library(${LIB_NAME} STATIC IMPORTED)
    set_target_properties(${LIB_NAME}
      PROPERTIES IMPORTED_LOCATION "${ARG_STATIC_LIB}")
    message("Added static library dependency ${LIB_NAME}: ${ARG_STATIC_LIB}")
  else()
    add_library(${LIB_NAME} SHARED IMPORTED)
    set_target_properties(${LIB_NAME}
      PROPERTIES IMPORTED_LOCATION "${ARG_SHARED_LIB}")
    message("Added shared library dependency ${LIB_NAME}: ${ARG_SHARED_LIB}")
  endif()

  if(ARG_DEPS)
    set_target_properties(${LIB_NAME}
      PROPERTIES IMPORTED_LINK_INTERFACE_LIBRARIES "${ARG_DEPS}")
  endif()

  # Set up an "exported variant" for this thirdparty library (see "Visibility"
  # above). It's the same as the real target, just with an "_exported" suffix.
  # We prefer the static archive if it exists (as it's akin to an "internal"
  # library), but we'll settle for the shared object if we must.
  #
  # A shared object exported variant will force any "leaf" library that
  # transitively depends on it to also depend on it at runtime; this is
  # desirable for some libraries (e.g. cyrus_sasl).
  set(LIB_NAME_EXPORTED ${LIB_NAME}_exported)
  if(ARG_STATIC_LIB)
    add_library(${LIB_NAME_EXPORTED} STATIC IMPORTED)
    set_target_properties(${LIB_NAME_EXPORTED}
      PROPERTIES IMPORTED_LOCATION "${ARG_STATIC_LIB}")
  else()
    add_library(${LIB_NAME_EXPORTED} SHARED IMPORTED)
    set_target_properties(${LIB_NAME_EXPORTED}
      PROPERTIES IMPORTED_LOCATION "${ARG_SHARED_LIB}")
  endif()
  if(ARG_DEPS)
    set_target_properties(${LIB_NAME_EXPORTED}
      PROPERTIES IMPORTED_LINK_INTERFACE_LIBRARIES "${ARG_DEPS}")
  endif()
endfunction()

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
