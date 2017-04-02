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

# - Find Thrift (a cross platform RPC lib/tool)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  Thrift_HOME - When set, this path is inspected instead of standard library
#                locations as the root of the Thrift installation.
#                The environment variable THRIFT_HOME overrides this variable.
#
# This module defines
#  THRIFT_VERSION, version string of ant if found
#  THRIFT_INCLUDE_DIR, where to find THRIFT headers
#  THRIFT_CONTRIB_DIR, where contrib thrift files (e.g. fb303.thrift) are installed
#  THRIFT_LIBS, THRIFT libraries
#  THRIFT_FOUND, If false, do not try to use ant

# prefer the thrift version supplied in THRIFT_HOME
if( NOT "$ENV{THRIFT_HOME}" STREQUAL "")
    file( TO_CMAKE_PATH "$ENV{THRIFT_HOME}" _native_path )
    list( APPEND _thrift_roots ${_native_path} )
elseif ( Thrift_HOME )
    list( APPEND _thrift_roots ${Thrift_HOME} )
endif()

message(STATUS "THRIFT_HOME: $ENV{THRIFT_HOME}")
find_path(THRIFT_INCLUDE_DIR thrift/Thrift.h HINTS
  ${_thrift_roots}
  NO_DEFAULT_PATH
  PATH_SUFFIXES "include"
)

find_path(THRIFT_CONTRIB_DIR share/fb303/if/fb303.thrift HINTS
  ${_thrift_roots}
  NO_DEFAULT_PATH
)

find_library(THRIFT_STATIC_LIB
  ${CMAKE_STATIC_LIBRARY_PREFIX}thrift${CMAKE_STATIC_LIBRARY_SUFFIX}
  HINTS ${_thrift_roots}
  NO_DEFAULT_PATH
  PATH_SUFFIXES "lib/${CMAKE_LIBRARY_ARCHITECTURE}" "lib"
)

find_library(THRIFT_LIB NAMES
  ${CMAKE_SHARED_LIBRARY_PREFIX}thrift${CMAKE_SHARED_LIBRARY_SUFFIX}
  HINTS ${_thrift_roots}
  NO_DEFAULT_PATH
  PATH_SUFFIXES "lib/${CMAKE_LIBRARY_ARCHITECTURE}" "lib"
)

find_program(THRIFT_COMPILER thrift HINTS
  ${_thrift_roots}
  NO_DEFAULT_PATH
  PATH_SUFFIXES "bin"
)

if (THRIFT_LIB)
  set(THRIFT_FOUND TRUE)
  set(THRIFT_LIBS ${THRIFT_LIB})
  exec_program(${THRIFT_COMPILER}
    ARGS -version OUTPUT_VARIABLE THRIFT_VERSION RETURN_VALUE THRIFT_RETURN)
else ()
  set(THRIFT_FOUND FALSE)
endif ()

if (THRIFT_FOUND)
  if (NOT THRIFT_FIND_QUIETLY)
    message(STATUS "Thrift version: ${THRIFT_VERSION}")
  endif ()
else ()
  message(STATUS "Thrift compiler/libraries NOT found. "
          "Thrift support will be disabled (${THRIFT_RETURN}, "
          "${THRIFT_INCLUDE_DIR}, ${THRIFT_LIB})")
endif ()


mark_as_advanced(
  THRIFT_LIB
  THRIFT_COMPILER
  THRIFT_INCLUDE_DIR
)
