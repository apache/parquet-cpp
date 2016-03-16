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
#
# Tries to find Lz4 headers and libraries.
#
# Usage of this module as follows:
#
#  find_package(Lz4)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  Lz4_HOME - When set, this path is inspected instead of standard library
#             locations as the root of the Lz4 installation.
#             The environment variable LZ4_HOME overrides this veriable.
#
# - Find LZ4 (lz4.h, liblz4.a, liblz4.so, and liblz4.so.1)
# This module defines
#  LZ4_INCLUDE_DIR, directory containing headers
#  LZ4_LIBS, directory containing lz4 libraries
#  LZ4_STATIC_LIB, path to liblz4.a
#  LZ4_SHARED_LIB, path to liblz4's shared library
#  LZ4_FOUND, whether lz4 has been found

if( NOT "$ENV{LZ4_HOME}" STREQUAL "")
    file( TO_CMAKE_PATH "$ENV{LZ4_HOME}" _native_path )
    list( APPEND _lz4_roots ${_native_path} )
elseif ( Lz4_HOME )
    list( APPEND _lz4_roots ${Lz4_HOME} )
endif()

# Try the parameterized roots, if they exist
if ( _lz4_roots )
    find_path( LZ4_INCLUDE_DIR NAMES lz4.h
        PATHS ${_lz4_roots} NO_DEFAULT_PATH
        PATH_SUFFIXES "include" )
    find_library( LZ4_LIBRARIES NAMES lz4
        PATHS ${_lz4_roots} NO_DEFAULT_PATH
        PATH_SUFFIXES "lib" )
else ()
    find_path( LZ4_INCLUDE_DIR NAMES lz4.h )
    find_library( LZ4_LIBRARIES NAMES lz4 )
endif ()


if (LZ4_INCLUDE_DIR AND LZ4_LIBRARIES)
  set(LZ4_FOUND TRUE)
  get_filename_component( LZ4_LIBS ${LZ4_LIBRARIES} PATH )
  set(LZ4_LIB_NAME liblz4)
  set(LZ4_STATIC_LIB ${LZ4_LIBS}/${LZ4_LIB_NAME}.a)
  set(LZ4_SHARED_LIB ${LZ4_LIBS}/${LZ4_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
else ()
  set(LZ4_FOUND FALSE)
endif ()

if (LZ4_FOUND)
  if (NOT Lz4_FIND_QUIETLY)
    message(STATUS "Found the Lz4 library: ${LZ4_LIBRARIES}")
  endif ()
else ()
  if (NOT Lz4_FIND_QUIETLY)
    set(LZ4_ERR_MSG "Could not find the Lz4 library. Looked in ")
    if ( _lz4_roots )
      set(LZ4_ERR_MSG "${LZ4_ERR_MSG} in ${_lz4_roots}.")
    else ()
      set(LZ4_ERR_MSG "${LZ4_ERR_MSG} system search paths.")
    endif ()
    if (Lz4_FIND_REQUIRED)
      message(FATAL_ERROR "${LZ4_ERR_MSG}")
    else (Lz4_FIND_REQUIRED)
      message(STATUS "${LZ4_ERR_MSG}")
    endif (Lz4_FIND_REQUIRED)
  endif ()
endif ()

mark_as_advanced(
  LZ4_INCLUDE_DIR
  LZ4_LIBS
  LZ4_LIBRARIES
  LZ4_STATIC_LIB
  LZ4_SHARED_LIB
)
