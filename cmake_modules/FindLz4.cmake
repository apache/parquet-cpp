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
# Tries to find LZ4 headers and libraries.
#
# Usage of this module as follows:
#
#  find_package(Lz4)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  Lz4_HOME - The root of the Lz4 installation.  This is considered before
#             standard locations. The environment variable LZ4_HOME is also
#             considered if this variable is not set.
#  Lz4_USE_STATIC_LIBS - Set to ON to force the use of the static
#                        libraries.  Default is OFF
#
# Variables defined by this module:
#
#  LZ4_FOUND              System has Lz4 libs/headers
#  LZ4_LIBRARIES          The Lz4 libraries
#  LZ4_INCLUDE_DIR        The location of Lz4 headers

# Determine parameterized roots
if( Lz4_HOME )
    list( APPEND _lz4_roots ${Lz4_HOME} )
endif()
if( NOT "$ENV{LZ4_HOME}" STREQUAL "")
    file( TO_CMAKE_PATH "$ENV{LZ4_HOME}" _native_path )
    list( APPEND _lz4_roots ${_native_path} )
endif()

# Try the parameterized roots first
find_path( LZ4_INCLUDE_DIR NAMES lz4.h
    PATHS ${_lz4_roots} NO_DEFAULT_PATH
    PATH_SUFFIXES "include" )
find_path( LZ4_INCLUDE_DIR NAMES lz4.h )

# Support preference of static libs by adjusting CMAKE_FIND_LIBRARY_SUFFIXES.
# Patterned after FindBoost.cmake
if( Lz4_USE_STATIC_LIBS )
    set( _lz4_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES} )
    if(WIN32)
        set( CMAKE_FIND_LIBRARY_SUFFIXES .lib .a ${CMAKE_FIND_LIBRARY_SUFFIXES} )
    else()
        set( CMAKE_FIND_LIBRARY_SUFFIXES .a )
    endif()

    # customize the cached variable depending on the search
    set( _lz4_LIBRARIES LZ4_LIBRARIES_STATIC )
else()
    set( _lz4_LIBRARIES LZ4_LIBRARIES_DEFAULT )
endif()

# Try the parameterized roots first
find_library( ${_lz4_LIBRARIES} NAMES lz4
    PATHS ${_lz4_roots} NO_DEFAULT_PATH
    PATH_SUFFIXES "lib" )
find_library( ${_lz4_LIBRARIES} NAMES lz4 )

set( LZ4_LIBRARIES ${${_lz4_LIBRARIES}} )

# Restore the original find library ordering
if( Lz4_USE_STATIC_LIBS )
  set( CMAKE_FIND_LIBRARY_SUFFIXES ${_lz4_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES} )
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args( Lz4 DEFAULT_MSG
    ${_lz4_LIBRARIES}
    LZ4_INCLUDE_DIR )

mark_as_advanced(
    LZ4_LIBRARIES_STATIC
    LZ4_LIBRARIES_DEFAULT
    LZ4_INCLUDE_DIR )

