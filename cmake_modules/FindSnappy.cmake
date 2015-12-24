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

# - Find SNAPPY (snappy.h, libsnappy.a, libsnappy.so, and libsnappy.so.1)
# This module defines
#  SNAPPY_INCLUDE_DIR, directory containing headers
#  SNAPPY_LIBS, directory containing snappy libraries
#  SNAPPY_STATIC_LIB, path to libsnappy.a
#  SNAPPY_FOUND, whether snappy has been found

set(SNAPPY_SEARCH_HEADER_PATHS
  $ENV{NATIVE_TOOLCHAIN}/snappy-$ENV{SNAPPY_VERSION}/include
)

set(SNAPPY_SEARCH_LIB_PATH
  $ENV{NATIVE_TOOLCHAIN}/snappy-$ENV{SNAPPY_VERSION}/lib
)

find_path(SNAPPY_INCLUDE_DIR snappy.h PATHS
  ${SNAPPY_SEARCH_HEADER_PATHS}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH
)

find_library(SNAPPY_LIB_PATH NAMES snappy PATHS ${SNAPPY_SEARCH_LIB_PATH} NO_DEFAULT_PATH)

if (SNAPPY_INCLUDE_DIR AND SNAPPY_LIB_PATH)
  set(SNAPPY_FOUND TRUE)
  set(SNAPPY_LIBS ${SNAPPY_SEARCH_LIB_PATH})
  set(SNAPPY_STATIC_LIB ${SNAPPY_SEARCH_LIB_PATH}/libsnappy.a)
else ()
  set(SNAPPY_FOUND FALSE)
endif ()

if (SNAPPY_FOUND)
  if (NOT Snappy_FIND_QUIETLY)
    message(STATUS "Found the Snappy library: ${SNAPPY_LIB_PATH}")
  endif ()
else ()
  if (NOT Snappy_FIND_QUIETLY)
    set(SNAPPY_ERR_MSG "Could not find the Snappy library. Looked for headers")
    set(SNAPPY_ERR_MSG "${SNAPPY_ERR_MSG} in ${SNAPPY_SEARCH_HEADER_PATHS}, and for libs")
    set(SNAPPY_ERR_MSG "${SNAPPY_ERR_MSG} in ${SNAPPY_SEARCH_LIB_PATH}")
    if (Snappy_FIND_REQUIRED)
      message(FATAL_ERROR "${SNAPPY_ERR_MSG}")
    else (Snappy_FIND_REQUIRED)
      message(STATUS "${SNAPPY_ERR_MSG}")
    endif (Snappy_FIND_REQUIRED)
  endif ()
endif ()

mark_as_advanced(
  SNAPPY_INCLUDE_DIR
  SNAPPY_LIBS
  SNAPPY_STATIC_LIB
)
