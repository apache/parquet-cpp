// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>

#include <gtest/gtest.h>

#include "parquet/column/reader.h"
#include "parquet/util/test-common.h"

using std::string;

namespace parquet_cpp {

namespace test {

  TEST_P(TestReader, Metadata) {
    reader_.ParseMetaData();
  }

  TEST_P(TestReader, DebugPrintWorks) {
    std::stringstream ss;
    reader_.DebugPrint(ss);
    std::string result = ss.str();
    ASSERT_TRUE(result.length() > 0);
  }

  TEST_P(TestReader, BatchReadInt32) {
    ssize_t c = findColumn(Type::INT32);
    if (c >= 0) {
      RowGroupReader* group = reader_.RowGroup(0);
      const parquet::ColumnMetaData* metadata = group->column_metadata(c);
      int64_t rows = metadata->num_values;

      std::shared_ptr<Int32Reader> col =
          std::dynamic_pointer_cast<Int32Reader>(group->Column(c));

      int16_t def_levels[rows];
      int16_t rep_levels[rows];
      int32_t values[rows];

      // Read all but last rows
      ASSERT_TRUE(col->HasNext());
      size_t values_read;
      size_t levels_read =
          col->ReadBatch(rows-1, def_levels, rep_levels, values, &values_read);
      ASSERT_EQ(rows-1, levels_read);
      size_t total_values_read = values_read;

      // Now read past the end of the file
      ASSERT_TRUE(col->HasNext());
      levels_read = col->ReadBatch(2, def_levels, rep_levels, values, &values_read);
      ASSERT_EQ(1, levels_read);
      total_values_read += values_read;

      ASSERT_FALSE(col->HasNext());

      ASSERT_EQ(total_values_read, rows - metadata->statistics.null_count);
    }
  }

  INSTANTIATE_TEST_CASE_P(ReaderTest, TestReader,
      testing::Values(
          TestFileInfo("alltypes_plain.parquet"),
          TestFileInfo("alltypes_plain.snappy.parquet"),
          TestFileInfo("alltypes_dictionary.parquet")));

} // namespace test

} // namespace parquet_cpp
