# Find the Lz4 libraries
#
#
# The following are set after configuration is done:
#  LZ4_FOUND
#  LZ4_INCLUDE_DIR
#  LZ4_LIBRARIES


find_path(LZ4_INCLUDE_DIR NAMES lz4.h PATHS
  /usr/local/include
  /usr/include
)

find_library(LZ4_LIBRARIES NAMES lz4 PATHS
  /usr/local/lib
  /usr/lib
)
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(LZ4 DEFAULT_MSG LZ4_INCLUDE_DIR LZ4_LIBRARIES)

if (LZ4_FOUND)
  message(STATUS "Found Lz4  (include: ${LZ4_INCLUDE_DIR}, library: ${LZ4_LIBRARIES})")
  mark_as_advanced(LZ4_INCLUDE_DIR LZ4_LIBRARIES)
endif()
