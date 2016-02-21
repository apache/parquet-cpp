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
#include <list>
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

bool operator==(const Int96 a, const Int96 b) {
  return a.value[0] == b.value[0] &&
         a.value[1] == b.value[1] &&
         a.value[2] == b.value[2];
}

bool operator==(const ByteArray a, const ByteArray b) {
  return a.len == b.len && 0 == memcmp(a.ptr, a.ptr, a.len);
}

static int FLBA_LENGTH = 8;
bool operator==(const FixedLenByteArray a, const FixedLenByteArray b) {
  return 0 == memcmp(a.ptr, a.ptr, FLBA_LENGTH);
}

namespace test {

template <int TYPE>
class TestFlatScanner : public ::testing::Test {
 public:
  void SetUp() {
    values_.resize(0);
    std::random_device rd;
    SEED = rd();
  }

  void TearDown() {}

  typedef typename type_traits<TYPE>::value_type T;

  void InitScanner(Repetition::type rep,
      int16_t max_def_level = 0, int16_t max_rep_level = 0) {
    NodePtr type = schema::PrimitiveNode::Make("c1", rep, static_cast<Type::type>(TYPE));
    descr_.reset(new ColumnDescriptor(type, max_def_level, max_rep_level));

    std::unique_ptr<PageReader> pager(new test::MockPageReader(pages_));
    scanner_ = Scanner::Make(ColumnReader::Make(descr_.get(), std::move(pager)));
  }

 protected:
  void AddPages(size_t pages, size_t num_values,
      std::vector<int16_t> def_levels, int16_t max_def_level,
      std::vector<int16_t> rep_levels, int16_t max_rep_level) {
    size_t page_size = std::max(def_levels.size(), num_values);
    std::vector<T> page(num_values);
    for (size_t i = 0; i < pages; i++) {
      random_numbers(num_values, SEED, &page[0]);
      buffers.push_back(std::vector<uint8_t>());
      pages_.push_back(
          MakeDataPage<TYPE>(page, def_levels, max_def_level,
              rep_levels, max_rep_level, &buffers.back()));
      values_.insert(values_.end(), page.begin(), page.end());
    }
  }

  void VerifyOutput(size_t total_values) {
    TypedScanner<TYPE>* scanner = reinterpret_cast<TypedScanner<TYPE>* >(scanner_.get());
    T val;
    bool is_null;
    size_t j = 0;
    for (size_t i = 0; i < total_values; i++) {
      ASSERT_TRUE(scanner->HasNext());
      ASSERT_TRUE(scanner->NextValue(&val, &is_null));
      if (!is_null)
        ASSERT_EQ(values_[j++], val);
    }
    ASSERT_FALSE(scanner->HasNext());
  }

  std::list< std::vector<uint8_t> > buffers;
  std::unique_ptr<ColumnDescriptor> descr_;
  std::vector< std::shared_ptr<Page> > pages_;
  std::shared_ptr<Scanner> scanner_;
  std::vector<T> values_;
  std::vector<uint8_t> data_buffer_; // For BA and FLBA

  uint32_t SEED;
  uint32_t PAGES = 4;
};

template <>
void TestFlatScanner<Type::BOOLEAN>::AddPages(size_t pages, size_t num_values,
    std::vector<int16_t> def_levels, int16_t max_def_level,
    std::vector<int16_t> rep_levels, int16_t max_rep_level) {
  size_t page_size = std::max(def_levels.size(), num_values);
  std::vector<bool> page;
  for (size_t i = 0; i < pages; i++) {
    page = flip_coins(num_values, 0.5);
    buffers.push_back(std::vector<uint8_t>());
    pages_.push_back(
        MakeDataPage<Type::BOOLEAN>(page, def_levels, max_def_level,
            rep_levels, max_rep_level, &buffers.back()));
    values_.insert(values_.end(), page.begin(), page.end());
  }
}

template <>
void TestFlatScanner<Type::BYTE_ARRAY>::AddPages(size_t pages, size_t num_values,
    std::vector<int16_t> def_levels, int16_t max_def_level,
    std::vector<int16_t> rep_levels, int16_t max_rep_level) {
  int max_byte_array_len = 12 + sizeof(uint32_t);
  size_t nbytes = num_values * max_byte_array_len;
  data_buffer_.resize(nbytes);

  size_t page_size = std::max(def_levels.size(), num_values);
  std::vector<ByteArray> page(num_values);
  for (size_t i = 0; i < pages; i++) {
    random_byte_array(num_values, 0.5, data_buffer_.data(), page, max_byte_array_len);
    buffers.push_back(std::vector<uint8_t>());
    pages_.push_back(
        MakeDataPage<Type::BYTE_ARRAY>(page, def_levels, max_def_level,
            rep_levels, max_rep_level, &buffers.back()));
    values_.insert(values_.end(), page.begin(), page.end());
  }
}

template <>
void TestFlatScanner<Type::FIXED_LEN_BYTE_ARRAY>::AddPages(
    size_t pages, size_t num_values,
    std::vector<int16_t> def_levels, int16_t max_def_level,
    std::vector<int16_t> rep_levels, int16_t max_rep_level) {
  size_t nbytes = num_values * FLBA_LENGTH;
  data_buffer_.resize(nbytes);

  size_t page_size = std::max(def_levels.size(), num_values);
  std::vector<FixedLenByteArray> page(num_values);
  for (size_t i = 0; i < pages; i++) {
    random_fixed_byte_array(num_values, 0.5, data_buffer_.data(), FLBA_LENGTH, page);
    buffers.push_back(std::vector<uint8_t>());
    pages_.push_back(
        MakeDataPage<Type::FIXED_LEN_BYTE_ARRAY>(page, def_levels, max_def_level,
            rep_levels, max_rep_level, &buffers.back(), FLBA_LENGTH));
    values_.insert(values_.end(), page.begin(), page.end());
  }
}

template <>
void TestFlatScanner<Type::FIXED_LEN_BYTE_ARRAY>::InitScanner(Repetition::type rep,
    int16_t max_def_level, int16_t max_rep_level) {
  NodePtr type = schema::PrimitiveNode::MakeFLBA(
      "c1", rep, FLBA_LENGTH, LogicalType::UTF8);
  descr_.reset(new ColumnDescriptor(type, max_def_level, max_rep_level));

  std::unique_ptr<PageReader> pager(new test::MockPageReader(pages_));
  scanner_ = Scanner::Make(ColumnReader::Make(descr_.get(), std::move(pager)));
}

typedef TestFlatScanner<Type::INT32> TestFlatInt32Scanner;
typedef TestFlatScanner<Type::INT64> TestFlatInt64Scanner;
typedef TestFlatScanner<Type::INT96> TestFlatInt96Scanner;
typedef TestFlatScanner<Type::BOOLEAN> TestFlatBoolScanner;
typedef TestFlatScanner<Type::FLOAT> TestFlatFloatScanner;
typedef TestFlatScanner<Type::DOUBLE> TestFlatDoubleScanner;
typedef TestFlatScanner<Type::BYTE_ARRAY> TestFlatByteArrayScanner;
typedef TestFlatScanner<Type::FIXED_LEN_BYTE_ARRAY> TestFlatFLBAScanner;


TEST_F(TestFlatInt32Scanner, TestBatchSize) {
  AddPages(PAGES, 1000, {}, 0, {}, 0);
  InitScanner(Repetition::REQUIRED);

  ASSERT_EQ(DEFAULT_SCANNER_BATCH_SIZE, scanner_->batch_size());

  scanner_->SetBatchSize(1);
  ASSERT_EQ(1, scanner_->batch_size());

  scanner_->SetBatchSize(1000000);
  ASSERT_EQ(1000000, scanner_->batch_size());
}

// PARQUET-502
TEST_F(TestFlatFLBAScanner, TestSmallBatch) {
  AddPages(PAGES, 1000, {}, 0, {}, 0);
  InitScanner(Repetition::REQUIRED);
  scanner_->SetBatchSize(1);
  VerifyOutput(PAGES * 1000);
}

TEST_F(TestFlatInt32Scanner, TestRequiredColumn) {
  AddPages(PAGES, 1000, {}, 0, {}, 0);
  InitScanner(Repetition::REQUIRED);
  VerifyOutput(PAGES * 1000);
}

TEST_F(TestFlatInt32Scanner, TestOptionalColumn) {
  std::vector<int16_t> def_levels = {1, 1, 0, 1, 0, 1, 0, 1, 1, 1};
  AddPages(PAGES, 7, def_levels, 1, {}, 0);
  InitScanner(Repetition::OPTIONAL, 1);
  VerifyOutput(PAGES * def_levels.size());
}

TEST_F(TestFlatInt32Scanner, TestRepeatedColumn) {
  std::vector<int16_t> def_levels = {1, 1, 1, 0, 1, 1, 0, 1, 1};
  std::vector<int16_t> rep_levels = {0, 1, 1, 0, 0, 1, 0, 1, 0};
  AddPages(PAGES, 7, def_levels, 1, rep_levels, 1);
  InitScanner(Repetition::REPEATED, 1, 1);
  VerifyOutput(PAGES * def_levels.size());
}

TEST_F(TestFlatInt64Scanner, TestRequiredColumn) {
  AddPages(PAGES, 1000, {}, 0, {}, 0);
  InitScanner(Repetition::REQUIRED);
  VerifyOutput(PAGES * 1000);
}

TEST_F(TestFlatInt64Scanner, TestOptionalColumn) {
  std::vector<int16_t> def_levels = {1, 1, 0, 1, 0, 1, 0, 1, 1, 1};
  AddPages(PAGES, 7, def_levels, 1, {}, 0);
  InitScanner(Repetition::OPTIONAL, 1);
  VerifyOutput(PAGES * def_levels.size());
}

TEST_F(TestFlatInt64Scanner, TestRepeatedColumn) {
  std::vector<int16_t> def_levels = {1, 1, 1, 0, 1, 1, 0, 1, 1};
  std::vector<int16_t> rep_levels = {0, 1, 1, 0, 0, 1, 0, 1, 0};
  AddPages(PAGES, 7, def_levels, 1, rep_levels, 1);
  InitScanner(Repetition::REPEATED, 1, 1);
  VerifyOutput(PAGES * def_levels.size());
}

TEST_F(TestFlatInt96Scanner, TestRequiredColumn) {
  AddPages(PAGES, 1000, {}, 0, {}, 0);
  InitScanner(Repetition::REQUIRED);
  VerifyOutput(PAGES * 1000);
}

TEST_F(TestFlatInt96Scanner, TestOptionalColumn) {
  std::vector<int16_t> def_levels = {1, 1, 0, 1, 0, 1, 0, 1, 1, 1};
  AddPages(PAGES, 7, def_levels, 1, {}, 0);
  InitScanner(Repetition::OPTIONAL, 1);
  VerifyOutput(PAGES * def_levels.size());
}

TEST_F(TestFlatInt96Scanner, TestRepeatedColumn) {
  std::vector<int16_t> def_levels = {1, 1, 1, 0, 1, 1, 0, 1, 1};
  std::vector<int16_t> rep_levels = {0, 1, 1, 0, 0, 1, 0, 1, 0};
  AddPages(PAGES, 7, def_levels, 1, rep_levels, 1);
  InitScanner(Repetition::REPEATED, 1, 1);
  VerifyOutput(PAGES * def_levels.size());
}

TEST_F(TestFlatBoolScanner, TestRequiredColumn) {
  AddPages(PAGES, 1000, {}, 0, {}, 0);
  InitScanner(Repetition::REQUIRED);
  VerifyOutput(PAGES * 1000);
}

TEST_F(TestFlatBoolScanner, TestOptionalColumn) {
  std::vector<int16_t> def_levels = {1, 1, 0, 1, 0, 1, 0, 1, 1, 1};
  AddPages(PAGES, 7, def_levels, 1, {}, 0);
  InitScanner(Repetition::OPTIONAL, 1);
  VerifyOutput(PAGES * def_levels.size());
}

TEST_F(TestFlatBoolScanner, TestRepeatedColumn) {
  std::vector<int16_t> def_levels = {1, 1, 1, 0, 1, 1, 0, 1, 1};
  std::vector<int16_t> rep_levels = {0, 1, 1, 0, 0, 1, 0, 1, 0};
  AddPages(PAGES, 7, def_levels, 1, rep_levels, 1);
  InitScanner(Repetition::REPEATED, 1, 1);
  VerifyOutput(PAGES * def_levels.size());
}

TEST_F(TestFlatFloatScanner, TestRequiredColumn) {
  AddPages(PAGES, 1000, {}, 0, {}, 0);
  InitScanner(Repetition::REQUIRED);
  VerifyOutput(PAGES * 1000);
}

TEST_F(TestFlatFloatScanner, TestOptionalColumn) {
  std::vector<int16_t> def_levels = {1, 1, 0, 1, 0, 1, 0, 1, 1, 1};
  AddPages(PAGES, 7, def_levels, 1, {}, 0);
  InitScanner(Repetition::OPTIONAL, 1);
  VerifyOutput(PAGES * def_levels.size());
}

TEST_F(TestFlatFloatScanner, TestRepeatedColumn) {
  std::vector<int16_t> def_levels = {1, 1, 1, 0, 1, 1, 0, 1, 1};
  std::vector<int16_t> rep_levels = {0, 1, 1, 0, 0, 1, 0, 1, 0};
  AddPages(PAGES, 7, def_levels, 1, rep_levels, 1);
  InitScanner(Repetition::REPEATED, 1, 1);
  VerifyOutput(PAGES * def_levels.size());
}

TEST_F(TestFlatDoubleScanner, TestRequiredColumn) {
  AddPages(4, 1000, {}, 0, {}, 0);
  InitScanner(Repetition::REQUIRED);
  VerifyOutput(4*1000);
}

TEST_F(TestFlatDoubleScanner, TestOptionalColumn) {
  std::vector<int16_t> def_levels = {1, 1, 0, 1, 0, 1, 0, 1, 1, 1};
  AddPages(PAGES, 7, def_levels, 1, {}, 0);
  InitScanner(Repetition::OPTIONAL, 1);
  VerifyOutput(PAGES * def_levels.size());
}

TEST_F(TestFlatDoubleScanner, TestRepeatedColumn) {
  std::vector<int16_t> def_levels = {1, 1, 1, 0, 1, 1, 0, 1, 1};
  std::vector<int16_t> rep_levels = {0, 1, 1, 0, 0, 1, 0, 1, 0};
  AddPages(PAGES, 7, def_levels, 1, rep_levels, 1);
  InitScanner(Repetition::REPEATED, 1, 1);
  VerifyOutput(PAGES * def_levels.size());
}

TEST_F(TestFlatByteArrayScanner, TestRequiredColumn) {
  AddPages(PAGES, 1000, {}, 0, {}, 0);
  InitScanner(Repetition::REQUIRED);
  VerifyOutput(PAGES * 1000);
}

TEST_F(TestFlatByteArrayScanner, TestOptionalColumn) {
  std::vector<int16_t> def_levels = {1, 1, 0, 1, 0, 1, 0, 1, 1, 1};
  AddPages(PAGES, 7, def_levels, 1, {}, 0);
  InitScanner(Repetition::OPTIONAL, 1);
  VerifyOutput(PAGES * def_levels.size());
}

TEST_F(TestFlatByteArrayScanner, TestRepeatedColumn) {
  std::vector<int16_t> def_levels = {1, 1, 1, 0, 1, 1, 0, 1, 1};
  std::vector<int16_t> rep_levels = {0, 1, 1, 0, 0, 1, 0, 1, 0};
  AddPages(PAGES, 7, def_levels, 1, rep_levels, 1);
  InitScanner(Repetition::REPEATED, 1, 1);
  VerifyOutput(PAGES * def_levels.size());
}

TEST_F(TestFlatFLBAScanner, TestRequiredColumn) {
  AddPages(PAGES, 1000, {}, 0, {}, 0);
  InitScanner(Repetition::REQUIRED);
  VerifyOutput(PAGES * 1000);
}

TEST_F(TestFlatFLBAScanner, TestOptionalColumn) {
  std::vector<int16_t> def_levels = {1, 1, 0, 1, 0, 1, 0, 1, 1, 1};
  AddPages(PAGES, 7, def_levels, 1, {}, 0);
  InitScanner(Repetition::OPTIONAL, 1);
  VerifyOutput(PAGES * def_levels.size());
}

TEST_F(TestFlatFLBAScanner, TestRepeatedColumn) {
  std::vector<int16_t> def_levels = {1, 1, 1, 0, 1, 1, 0, 1, 1};
  std::vector<int16_t> rep_levels = {0, 1, 1, 0, 0, 1, 0, 1, 0};
  AddPages(PAGES, 7, def_levels, 1, rep_levels, 1);
  InitScanner(Repetition::REPEATED, 1, 1);
  VerifyOutput(PAGES * def_levels.size());
}

} // namespace test

} // namespace parquet_cpp
