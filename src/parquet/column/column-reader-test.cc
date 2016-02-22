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

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include "parquet/types.h"
#include "parquet/column/page.h"
#include "parquet/column/reader.h"
#include "parquet/column/test-util.h"
#include "parquet/schema/descriptor.h"
#include "parquet/schema/types.h"
#include "parquet/util/test-common.h"

using std::string;
using std::vector;
using std::shared_ptr;

namespace parquet_cpp {

using schema::NodePtr;

namespace test {

class TestPrimitiveReader : public ::testing::Test {
 public:
  void init_def_levels() {
    num_values_ = 0;
    def_levels_.resize(num_levels_);
    random_levels(num_levels_, 0, max_def_level_, def_levels_.data());
    for (int i = 0; i < num_levels_; i++) {
      if (def_levels_[i] == max_def_level_) num_values_++;
    }
  }

  void init_rep_levels() {
    rep_levels_.resize(num_levels_);
    random_levels(num_levels_, 0, max_rep_level_, rep_levels_.data());
  }

  void init_values() {
    values_.resize(num_values_);
    random_numbers(num_values_, 0, values_.data());
  }

  void init_reader() {
    std::shared_ptr<DataPage> page = MakeDataPage<Type::INT32>(values_, def_levels_,
        max_def_level_, rep_levels_, max_rep_level_);
    pages_.push_back(page);
    pager_.reset(new test::MockPageReader(pages_));
    reader_ = ColumnReader::Make(descr_, std::move(pager_));
  }

  void check_results() {
    vector<int32_t> vresult(num_values_, -1);
    vector<int16_t> dresult(num_levels_, -1);
    vector<int16_t> rresult(num_levels_, -1);
    size_t values_read = 0;
    size_t total_values_read = 0;

    Int32Reader* reader = static_cast<Int32Reader*>(reader_.get());
    int offset = num_levels_ / 2;
    size_t batch_actual = reader->ReadBatch(num_levels_, &dresult[0], &rresult[0],
        &vresult[0], &values_read);
    total_values_read += values_read;
    batch_actual += reader->ReadBatch(offset, &dresult[0] + offset, &rresult[0] + offset,
        &vresult[0] + values_read, &values_read);
    total_values_read += values_read;

    ASSERT_EQ(num_levels_, batch_actual);
    ASSERT_EQ(num_values_, total_values_read);

    ASSERT_TRUE(vector_equal(values_, vresult));
    if (max_def_level_ > 0) {
      ASSERT_TRUE(vector_equal(def_levels_, dresult));
    }
    if (max_rep_level_ > 0) {
      ASSERT_TRUE(vector_equal(rep_levels_, rresult));
    }
  }

  void execute() {
    init_values();
    init_reader();
    check_results();
  }

 protected:
  int num_levels_;
  int num_values_;
  int16_t max_def_level_;
  int16_t max_rep_level_;
  const ColumnDescriptor *descr_;
  vector<shared_ptr<Page> > pages_;
  std::unique_ptr<PageReader> pager_;
  std::shared_ptr<ColumnReader> reader_;
  vector<int32_t> values_;
  vector<int16_t> def_levels_;
  vector<int16_t> rep_levels_;
};

TEST_F(TestPrimitiveReader, TestInt32FlatRequired) {
  num_levels_ = num_values_ = 1000;
  max_def_level_ = 0;
  max_rep_level_ = 0;
  NodePtr type = schema::Int32("a", Repetition::REQUIRED);
  const ColumnDescriptor descr(type, max_def_level_, max_rep_level_);
  descr_ = &descr;
  execute();
}

TEST_F(TestPrimitiveReader, TestInt32FlatOptional) {
  num_levels_ = 1000;
  max_def_level_ = 4;
  max_rep_level_ = 0;
  NodePtr type = schema::Int32("b", Repetition::OPTIONAL);
  const ColumnDescriptor descr(type, max_def_level_, max_rep_level_);
  descr_ = &descr;
  init_def_levels();
  execute();
}

TEST_F(TestPrimitiveReader, TestInt32FlatRepeated) {
  num_levels_ = 1000;
  max_def_level_ = 4;
  max_rep_level_ = 2;
  NodePtr type = schema::Int32("c", Repetition::REPEATED);
  const ColumnDescriptor descr(type, max_def_level_, max_rep_level_);
  descr_ = &descr;
  init_def_levels();
  init_rep_levels();
  execute();
}

} // namespace test
} // namespace parquet_cpp
