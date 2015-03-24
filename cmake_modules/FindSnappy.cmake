# Find the Snappy libraries
#
#
# The following are set after configuration is done:
#  SNAPPY_FOUND
#  Snappy_INCLUDE_DIR
#  Snappy_LIBRARIES

find_path(Snappy_INCLUDE_DIR NAMES snappy.h
                             PATHS /usr/local/include /usr/include)

find_library(Snappy_LIBRARIES NAMES snappy
                              PATHS /usr/local/lib /usr/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Snappy DEFAULT_MSG Snappy_INCLUDE_DIR Snappy_LIBRARIES)

if(SNAPPY_FOUND)
  message(STATUS "Found Snappy  (include: ${Snappy_INCLUDE_DIR}, library: ${Snappy_LIBRARIES})")
  mark_as_advanced(Snappy_INCLUDE_DIR Snappy_LIBRARIES)
endif()
