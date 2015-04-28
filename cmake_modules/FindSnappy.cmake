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
# Tries to find Snappy headers and libraries.
#
# Usage of this module as follows:
#
#  find_package(Snappy)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  Snappy_HOME - The root of the Snappy installation.  This is considered before
#                standard locations. The environment variable SNAPPY_HOME is also
#                considered if this variable is not set.
#  Snappy_USE_STATIC_LIBS - Set to ON to force the use of the static
#                           libraries.  Default is OFF
#
# Variables defined by this module:
#
#  SNAPPY_FOUND              System has Snappy libs/headers
#  SNAPPY_LIBRARIES          The Snappy libraries
#  SNAPPY_INCLUDE_DIR        The location of Snappy headers

# Determine parameterized roots
if( Snappy_HOME )
    list( APPEND _snappy_roots ${Snappy_HOME} )
endif()
if( NOT "$ENV{SNAPPY_HOME}" STREQUAL "")
    file( TO_CMAKE_PATH "$ENV{SNAPPY_HOME}" _native_path )
    list( APPEND _snappy_roots ${_native_path} )
endif()

# Try the parameterized roots first
find_path( SNAPPY_INCLUDE_DIR NAMES snappy.h
    PATHS ${_snappy_roots} NO_DEFAULT_PATH
    PATH_SUFFIXES "include" )
find_path( SNAPPY_INCLUDE_DIR NAMES snappy.h )

# Support preference of static libs by adjusting CMAKE_FIND_LIBRARY_SUFFIXES.
# Patterned after FindBoost.cmake
if( Snappy_USE_STATIC_LIBS )
    set( _snappy_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_FIND_LIBRARY_SUFFIXES} )
    if(WIN32)
        set( CMAKE_FIND_LIBRARY_SUFFIXES .lib .a ${CMAKE_FIND_LIBRARY_SUFFIXES} )
    else()
        set( CMAKE_FIND_LIBRARY_SUFFIXES .a )
    endif()

    # customize the cached variable depending on the search
    set( _snappy_LIBRARIES SNAPPY_LIBRARIES_STATIC )
else()
    set( _snappy_LIBRARIES SNAPPY_LIBRARIES_DEFAULT )
endif()

# Try the parameterized roots first
find_library( ${_snappy_LIBRARIES} NAMES snappy
    PATHS ${_snappy_roots} NO_DEFAULT_PATH
    PATH_SUFFIXES "lib" )
find_library( ${_snappy_LIBRARIES} NAMES snappy )

set( SNAPPY_LIBRARIES ${${_snappy_LIBRARIES}} )

# Restore the original find library ordering
if( Snappy_USE_STATIC_LIBS )
  set( CMAKE_FIND_LIBRARY_SUFFIXES ${_snappy_ORIG_CMAKE_FIND_LIBRARY_SUFFIXES} )
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args( Snappy DEFAULT_MSG
    ${_snappy_LIBRARIES}
    SNAPPY_INCLUDE_DIR )

mark_as_advanced(
    SNAPPY_LIBRARIES_STATIC
    SNAPPY_LIBRARIES_DEFAULT
    SNAPPY_INCLUDE_DIR )
