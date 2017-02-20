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
# This module defines
#  THRIFT_VERSION, version string of ant if found
#  THRIFT_INCLUDE_DIR, where to find THRIFT headers
#  THRIFT_CONTRIB_DIR, where contrib thrift files (e.g. fb303.thrift) are installed
#  THRIFT_LIBS, THRIFT libraries
#  THRIFT_FOUND, If false, do not try to use ant

# prefer the thrift version supplied in THRIFT_HOME
message(STATUS "THRIFT_HOME: $ENV{THRIFT_HOME}")
find_path(THRIFT_INCLUDE_DIR thrift/Thrift.h HINTS
  $ENV{THRIFT_HOME}/include/
  NO_DEFAULT_PATH
)

find_path(THRIFT_CONTRIB_DIR share/fb303/if/fb303.thrift HINTS
  $ENV{THRIFT_HOME}
  NO_DEFAULT_PATH
)

set(THRIFT_LIB_PATHS
  $ENV{THRIFT_HOME}/lib
  NO_DEFAULT_PATH)

find_path(THRIFT_STATIC_LIB_PATH libthrift.a PATHS ${THRIFT_LIB_PATHS})

# prefer the thrift version supplied in THRIFT_HOME
find_library(THRIFT_LIB NAMES thrift HINTS ${THRIFT_LIB_PATHS})

find_program(THRIFT_COMPILER thrift
  $ENV{THRIFT_HOME}/bin
  NO_DEFAULT_PATH
)

if (THRIFT_LIB)
  set(THRIFT_FOUND TRUE)
  set(THRIFT_LIBS ${THRIFT_LIB})
  set(THRIFT_STATIC_LIB ${THRIFT_STATIC_LIB_PATH}/libthrift.a)
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
