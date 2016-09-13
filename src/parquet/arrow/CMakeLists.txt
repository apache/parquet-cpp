# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# ----------------------------------------------------------------------
# parquet_arrow : Arrow <-> Parquet adapter

set(PARQUET_ARROW_SRCS
  io.cc
  reader.cc
  schema.cc
  writer.cc
)

set(PARQUET_ARROW_LIBS
  arrow
  arrow_io
  parquet_shared
)

add_library(parquet_arrow SHARED
  ${PARQUET_ARROW_SRCS}
)
target_link_libraries(parquet_arrow ${PARQUET_ARROW_LIBS})
SET_TARGET_PROPERTIES(parquet_arrow PROPERTIES LINKER_LANGUAGE CXX)

if (APPLE)
  set_target_properties(parquet_arrow
    PROPERTIES
    BUILD_WITH_INSTALL_RPATH ON
    INSTALL_NAME_DIR "@rpath")
endif()

ADD_PARQUET_TEST(arrow-schema-test)
ADD_PARQUET_LINK_LIBRARIES(arrow-schema-test parquet_arrow arrow)

ADD_PARQUET_TEST(arrow-io-test)
ADD_PARQUET_LINK_LIBRARIES(arrow-io-test parquet_arrow)

ADD_PARQUET_TEST(arrow-reader-writer-test)
ADD_PARQUET_LINK_LIBRARIES(arrow-reader-writer-test parquet_arrow)

# Headers: top level
install(FILES
  io.h
  reader.h
  schema.h
  utils.h
  writer.h
  DESTINATION include/parquet/arrow)

install(TARGETS parquet_arrow
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib)

