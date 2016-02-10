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

#include <cstdint>
#include <cstdlib>
#include <vector>
#include <limits>

#include <gtest/gtest.h>

#include "parquet/types.h"
#include "parquet/column/page.h"
#include "parquet/column/reader.h"
#include "parquet/column/scanner.h"
#include "parquet/column/test-util.h"

#include "parquet/util/test-common.h"

namespace parquet_cpp {

using schema::NodePtr;

namespace test {

template <int TYPE>
class TestPrimitiveScanner : public ::testing::Test {
 public:
  void SetUp() {}

  void TearDown() {}

  typedef typename type_traits<TYPE>::value_type T;

  void AddPage(std::vector<T>& values, std::vector<int16_t>& def_levels,
        int16_t max_def_level) {
    pages_.push_back(
        MakeDataPage<TYPE>(values, def_levels, max_def_level, {}, 0, &buffer));
  }

  void InitScanner(Repetition::type rep, int16_t max_def_level) {
    NodePtr type = schema::PrimitiveNode::Make("c1", rep, static_cast<Type::type>(TYPE));
    descr_.reset(new ColumnDescriptor(type, max_def_level, 0));

    std::unique_ptr<PageReader> pager(new test::MockPageReader(pages_));
    scanner_ = Scanner::Make(ColumnReader::Make(descr_.get(), std::move(pager)));
  }

 protected:
  std::vector<uint8_t> buffer;
  std::unique_ptr<ColumnDescriptor> descr_;
  std::vector< std::shared_ptr<Page> > pages_;
  std::shared_ptr<Scanner> scanner_;
};

typedef TestPrimitiveScanner<Type::INT32> TestPrimitiveInt32Scanner;
typedef TestPrimitiveScanner<Type::INT64> TestPrimitiveInt64Scanner;
typedef TestPrimitiveScanner<Type::INT96> TestPrimitiveInt96Scanner;
typedef TestPrimitiveScanner<Type::BOOLEAN> TestPrimitiveBoolScanner;
typedef TestPrimitiveScanner<Type::FLOAT> TestPrimitiveFloatScanner;
typedef TestPrimitiveScanner<Type::DOUBLE> TestPrimitiveDoubleScanner;


TEST_F(TestPrimitiveInt32Scanner, TestBatchSize) {
  std::vector<int32_t> values = {1, -2, 0, 123456789, -987654321,
      std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max()};
  std::vector<int16_t> def_levels = {};
  int16_t max_def_level = 0;
  AddPage(values, def_levels, max_def_level);
  InitScanner(Repetition::REQUIRED, max_def_level);

  ASSERT_EQ(DEFAULT_SCANNER_BATCH_SIZE, scanner_->batch_size());

  scanner_->SetBatchSize(1);
  ASSERT_EQ(1, scanner_->batch_size());

  scanner_->SetBatchSize(1000000);
  ASSERT_EQ(1000000, scanner_->batch_size());
}

// PARQUET-502
TEST_F(TestPrimitiveInt32Scanner, TestSmallBatch) {
  std::vector<int32_t> values = {1, -2, 0, 123456789, -987654321,
      std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max()};
  std::vector<int16_t> def_levels = {};
  int16_t max_def_level = 0;
  AddPage(values, def_levels, max_def_level);
  InitScanner(Repetition::REQUIRED, max_def_level);

  Int32Scanner* scanner = reinterpret_cast<Int32Scanner*>(scanner_.get());

  scanner->SetBatchSize(1);

  int32_t val;
  bool is_null;
  while (scanner->HasNext()) {
    scanner->NextValue(&val, &is_null);
  }
}

TEST_F(TestPrimitiveInt32Scanner, TestPrimitiveRequiredColumn) {
  std::vector<int32_t> values = {1, -2, 0, 123456789, -987654321,
      std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max()};
  std::vector<int16_t> def_levels = {};
  int16_t max_def_level = 0;
  AddPage(values, def_levels, max_def_level);
  InitScanner(Repetition::REQUIRED, max_def_level);

  Int32Scanner* scanner = reinterpret_cast<Int32Scanner*>(scanner_.get());
  int32_t val;
  bool is_null;
  for (size_t i = 0; i < values.size(); i++) {
    ASSERT_TRUE(scanner_->HasNext());
    ASSERT_TRUE(scanner->NextValue(&val, &is_null));
    ASSERT_FALSE(is_null);
    ASSERT_EQ(values[i], val);
  }
  ASSERT_FALSE(scanner_->HasNext());
}

TEST_F(TestPrimitiveInt32Scanner, TestPrimitiveOptionalColumn) {
  std::vector<int32_t> values = {1, -2, 0, 123456789, -987654321,
      std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max()};
  std::vector<int16_t> def_levels = {1, 1, 0, 1, 0, 0, 1, 0, 1, 1, 1};
  int16_t max_def_level = 1;
  AddPage(values, def_levels, max_def_level);
  InitScanner(Repetition::OPTIONAL, max_def_level);

  Int32Scanner* scanner = reinterpret_cast<Int32Scanner*>(scanner_.get());
  int32_t val;
  bool is_null;
  size_t j = 0;
  for (size_t i = 0; i < def_levels.size(); i++) {
    ASSERT_TRUE(scanner_->HasNext());
    ASSERT_TRUE(scanner->NextValue(&val, &is_null));
    ASSERT_EQ(def_levels[i] == 0, is_null);
    if (!is_null)
      ASSERT_EQ(values[j++], val);
  }
  ASSERT_FALSE(scanner_->HasNext());
}

TEST_F(TestPrimitiveInt64Scanner, TestPrimitiveRequiredColumn) {
  std::vector<int64_t> values = {1, -2, 0, 123456789, -987654321,
      std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max()};
  std::vector<int16_t> def_levels = {};
  int16_t max_def_level = 0;
  AddPage(values, def_levels, max_def_level);
  InitScanner(Repetition::REQUIRED, max_def_level);

  Int64Scanner* scanner = reinterpret_cast<Int64Scanner*>(scanner_.get());
  int64_t val;
  bool is_null;
  for (size_t i = 0; i < values.size(); i++) {
    ASSERT_TRUE(scanner_->HasNext());
    ASSERT_TRUE(scanner->NextValue(&val, &is_null));
    ASSERT_FALSE(is_null);
    ASSERT_EQ(values[i], val);
  }
  ASSERT_FALSE(scanner_->HasNext());
}

TEST_F(TestPrimitiveInt64Scanner, TestPrimitiveOptionalColumn) {
  std::vector<int64_t> values = {1, -2, 0, 123456789, -987654321,
      std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max()};
  std::vector<int16_t> def_levels = {1, 1, 0, 1, 0, 0, 1, 0, 1, 1, 1};
  int16_t max_def_level = 1;
  AddPage(values, def_levels, max_def_level);
  InitScanner(Repetition::OPTIONAL, max_def_level);

  Int64Scanner* scanner = reinterpret_cast<Int64Scanner*>(scanner_.get());
  int64_t val;
  bool is_null;
  size_t j = 0;
  for (size_t i = 0; i < def_levels.size(); i++) {
    ASSERT_TRUE(scanner_->HasNext());
    ASSERT_TRUE(scanner->NextValue(&val, &is_null));
    ASSERT_EQ(def_levels[i] == 0, is_null);
    if (!is_null)
      ASSERT_EQ(values[j++], val);
  }
  ASSERT_FALSE(scanner_->HasNext());
}

TEST_F(TestPrimitiveInt96Scanner, TestPrimitiveRequiredColumn) {
  uint32_t m = std::numeric_limits<uint32_t>::max();
  std::vector<Int96> values = { Int96({1, 0, 123456789}), Int96({m, 0, m-1}) };
  std::vector<int16_t> def_levels = {};
  int16_t max_def_level = 0;
  AddPage(values, def_levels, max_def_level);
  InitScanner(Repetition::REQUIRED, max_def_level);

  Int96Scanner* scanner = reinterpret_cast<Int96Scanner*>(scanner_.get());
  Int96 val;
  bool is_null;
  for (size_t i = 0; i < values.size(); i++) {
    ASSERT_TRUE(scanner_->HasNext());
    ASSERT_TRUE(scanner->NextValue(&val, &is_null));
    ASSERT_FALSE(is_null);
    ASSERT_EQ(values[i].value[0], val.value[0]);
    ASSERT_EQ(values[i].value[1], val.value[1]);
    ASSERT_EQ(values[i].value[2], val.value[2]);
  }
  ASSERT_FALSE(scanner_->HasNext());
}

TEST_F(TestPrimitiveInt96Scanner, TestPrimitiveOptionalColumn) {
  uint32_t m = std::numeric_limits<uint32_t>::max();
  std::vector<Int96> values = { Int96({1, 0, 123456789}), Int96({m, 0, m-1}) };
  std::vector<int16_t> def_levels = {1, 0, 0, 1};
  int16_t max_def_level = 1;
  AddPage(values, def_levels, max_def_level);
  InitScanner(Repetition::OPTIONAL, max_def_level);

  Int96Scanner* scanner = reinterpret_cast<Int96Scanner*>(scanner_.get());
  Int96 val;
  bool is_null;
  size_t j = 0;
  for (size_t i = 0; i < def_levels.size(); i++) {
    ASSERT_TRUE(scanner_->HasNext());
    ASSERT_TRUE(scanner->NextValue(&val, &is_null));
    ASSERT_EQ(def_levels[i] == 0, is_null);
    if (!is_null) {
      ASSERT_EQ(values[j].value[0], val.value[0]);
      ASSERT_EQ(values[j].value[1], val.value[1]);
      ASSERT_EQ(values[j++].value[2], val.value[2]);
    }
  }
  ASSERT_FALSE(scanner_->HasNext());
}

TEST_F(TestPrimitiveBoolScanner, TestPrimitiveRequiredColumn) {
  std::vector<bool> values = { true, false, false, true, true, false, true};
  std::vector<int16_t> def_levels = {};
  int16_t max_def_level = 0;
  AddPage(values, def_levels, max_def_level);
  InitScanner(Repetition::REQUIRED, max_def_level);

  BoolScanner* scanner = reinterpret_cast<BoolScanner*>(scanner_.get());
  bool val;
  bool is_null;
  for (size_t i = 0; i < values.size(); i++) {
    ASSERT_TRUE(scanner_->HasNext());
    ASSERT_TRUE(scanner->NextValue(&val, &is_null));
    ASSERT_FALSE(is_null);
    ASSERT_EQ(values[i], val);
  }
  ASSERT_FALSE(scanner_->HasNext());
}


TEST_F(TestPrimitiveBoolScanner, TestPrimitiveOptionalColumn) {
  std::vector<bool> values = { true, false, false, true, true, false, true};
  std::vector<int16_t> def_levels = {1, 1, 0, 1, 0, 0, 1, 0, 1, 1, 1};
  int16_t max_def_level = 1;
  AddPage(values, def_levels, max_def_level);
  InitScanner(Repetition::OPTIONAL, max_def_level);

  BoolScanner* scanner = reinterpret_cast<BoolScanner*>(scanner_.get());
  bool val;
  bool is_null;
  size_t j = 0;
  for (size_t i = 0; i < def_levels.size(); i++) {
    ASSERT_TRUE(scanner_->HasNext());
    ASSERT_TRUE(scanner->NextValue(&val, &is_null));
    ASSERT_EQ(def_levels[i] == 0, is_null);
    if (!is_null)
      ASSERT_EQ(values[j++], val);
  }
  ASSERT_FALSE(scanner_->HasNext());
}

TEST_F(TestPrimitiveFloatScanner, TestPrimitiveRequiredColumn) {
  std::vector<float> values = { 123.456, 78.910, 0, 000000001.000001, -123456789.1234};
  std::vector<int16_t> def_levels = {};
  int16_t max_def_level = 0;
  AddPage(values, def_levels, max_def_level);
  InitScanner(Repetition::REQUIRED, max_def_level);

  FloatScanner* scanner = reinterpret_cast<FloatScanner*>(scanner_.get());
  float val;
  bool is_null;
  for (size_t i = 0; i < values.size(); i++) {
    ASSERT_TRUE(scanner_->HasNext());
    ASSERT_TRUE(scanner->NextValue(&val, &is_null));
    ASSERT_FALSE(is_null);
    ASSERT_EQ(values[i], val);
  }
  ASSERT_FALSE(scanner_->HasNext());
}


TEST_F(TestPrimitiveFloatScanner, TestPrimitiveOptionalColumn) {
  std::vector<float> values = { 123.456, 78.910, 0, 000000001.000001, -123456789.1234};
  std::vector<int16_t> def_levels = {1, 1, 0, 1, 0, 0, 1, 0, 1};
  int16_t max_def_level = 1;
  AddPage(values, def_levels, max_def_level);
  InitScanner(Repetition::OPTIONAL, max_def_level);

  FloatScanner* scanner = reinterpret_cast<FloatScanner*>(scanner_.get());
  float val;
  bool is_null;
  size_t j = 0;
  for (size_t i = 0; i < def_levels.size(); i++) {
    ASSERT_TRUE(scanner_->HasNext());
    ASSERT_TRUE(scanner->NextValue(&val, &is_null));
    ASSERT_EQ(def_levels[i] == 0, is_null);
    if (!is_null)
      ASSERT_EQ(values[j++], val);
  }
  ASSERT_FALSE(scanner_->HasNext());
}

TEST_F(TestPrimitiveDoubleScanner, TestPrimitiveRequiredColumn) {
  std::vector<double> values = { 123.456, -78.910, 0, 000000001.000001, -123456789.1234};
  std::vector<int16_t> def_levels = {};
  int16_t max_def_level = 0;
  AddPage(values, def_levels, max_def_level);
  InitScanner(Repetition::REQUIRED, max_def_level);

  DoubleScanner* scanner = reinterpret_cast<DoubleScanner*>(scanner_.get());
  double val;
  bool is_null;
  for (size_t i = 0; i < values.size(); i++) {
    ASSERT_TRUE(scanner_->HasNext());
    ASSERT_TRUE(scanner->NextValue(&val, &is_null));
    ASSERT_FALSE(is_null);
    ASSERT_EQ(values[i], val);
  }
  ASSERT_FALSE(scanner_->HasNext());
}

TEST_F(TestPrimitiveDoubleScanner, TestPrimitiveOptionalColumn) {
  std::vector<double> values = { 123.456, -78.910, 0, 000000001.000001, -123456789.1234};
  std::vector<int16_t> def_levels = {1, 1, 0, 1, 0, 0, 1, 0, 1};
  int16_t max_def_level = 1;
  AddPage(values, def_levels, max_def_level);
  InitScanner(Repetition::OPTIONAL, max_def_level);

  DoubleScanner* scanner = reinterpret_cast<DoubleScanner*>(scanner_.get());
  double val;
  bool is_null;
  size_t j = 0;
  for (size_t i = 0; i < def_levels.size(); i++) {
    ASSERT_TRUE(scanner_->HasNext());
    ASSERT_TRUE(scanner->NextValue(&val, &is_null));
    ASSERT_EQ(def_levels[i] == 0, is_null);
    if (!is_null)
      ASSERT_EQ(values[j++], val);
  }
  ASSERT_FALSE(scanner_->HasNext());
}

} // namespace test

} // namespace parquet_cpp
