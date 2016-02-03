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

#include "parquet/column/scanner.h"
#include "parquet/util/test-common.h"

using std::string;

namespace parquet_cpp {

namespace test {

  TEST_P(TestReader, FlatScannerInt32) {
    ssize_t c = findColumn(Type::INT32);
    if (c >= 0) {
      RowGroupReader* group = reader_.RowGroup(0);
      int64_t rows = group->column_metadata(c)->num_values;

      std::shared_ptr<Int32Scanner> scanner =
          std::make_shared<Int32Scanner>(group->Column(c));

      int32_t val;
      bool is_null;
      for (; rows > 0; rows--) {
        scanner->NextValue(&val, &is_null);
      }
      ASSERT_FALSE(scanner->HasNext());
      ASSERT_FALSE(scanner->NextValue(&val, &is_null));
    }
  }

  TEST_P(TestReader, ScannerBatchSize) {
    ssize_t c = findColumn(Type::INT32);
    if (c >= 0) {
      RowGroupReader* group = reader_.RowGroup(0);
      std::shared_ptr<Int32Scanner> scanner =
          std::make_shared<Int32Scanner>(group->Column(c));

      ASSERT_EQ(128, scanner->batch_size());
      scanner->SetBatchSize(1024);
      ASSERT_EQ(1024, scanner->batch_size());
    }
  }


  INSTANTIATE_TEST_CASE_P(ReaderTest, TestReader,
      testing::Values(
          TestFileInfo("alltypes_plain.parquet"),
          TestFileInfo("alltypes_plain.snappy.parquet"),
          TestFileInfo("alltypes_dictionary.parquet")));

} // namespace test

} // namespace parquet_cpp
