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


add_library(Example STATIC
  example_util.cc
)

SET(LINK_LIBS
  Parquet
  ParquetCompression
  Example
  rt
  ThriftParquet
  thriftstatic
  lz4static
  snappystatic)

add_executable(compute_stats compute_stats.cc)
target_link_libraries(compute_stats ${LINK_LIBS})

add_executable(decode_benchmark decode_benchmark.cc)
target_link_libraries(decode_benchmark ${LINK_LIBS})

add_executable(parquet_reader parquet_reader.cc)
target_link_libraries(parquet_reader ${LINK_LIBS})
