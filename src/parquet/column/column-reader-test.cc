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
  void make_pages() {
    num_values_ = 0;
    int levels_per_page = num_levels_ / num_pages_;
    vector<int16_t> defs;
    vector<int16_t> reps;
    vector<int32_t> vals;
    int values_per_page = 0;
    for (int p = 0; p < num_pages_; p++) {
      values_per_page = 0;
      // Create definition levels
      if (max_def_level_ > 0) {
        defs.resize(levels_per_page);
        random_levels(levels_per_page, 0, max_def_level_, defs.data());
        for (int i = 0; i < levels_per_page; i++) {
          if (defs[i] == max_def_level_) values_per_page++;
        }
        def_levels_.insert(def_levels_.end(), defs.begin(), defs.end());
      } else {
        values_per_page = levels_per_page;
      }
      // Create repitition levels
      if (max_rep_level_ > 0) {
        reps.resize(levels_per_page);
        random_levels(levels_per_page, 0, max_rep_level_, reps.data());
        rep_levels_.insert(rep_levels_.end(), reps.begin(), reps.end());
      }
      // Create values
      vals.resize(values_per_page);
      random_numbers(values_per_page, 0, vals.data());
      values_.insert(values_.end(), vals.begin(), vals.end());
      std::shared_ptr<DataPage> page = MakeDataPage<Type::INT32>(vals, defs,
          max_def_level_, reps, max_rep_level_);
      pages_.push_back(page);

      num_values_ += values_per_page;
    }
  }

  void init_reader() {
    pager_.reset(new test::MockPageReader(pages_));
    reader_ = ColumnReader::Make(descr_, std::move(pager_));
  }

  void check_results() {
    vector<int32_t> vresult(num_values_, -1);
    vector<int16_t> dresult(num_levels_, -1);
    vector<int16_t> rresult(num_levels_, -1);
    size_t values_read = 0;
    size_t total_values_read = 0;
    size_t batch_actual = 0;

    Int32Reader* reader = static_cast<Int32Reader*>(reader_.get());
    int32_t batch_size = 8;
    size_t batch = 0;
    // This will cover both the cases
    // 1) batch_size < page_size (multiple ReadBatch from a single page)
    // 2) batch_size > page_size (BatchRead from multiple pages)
    do {
      batch = reader->ReadBatch(batch_size, &dresult[0] + batch_actual,
          &rresult[0] + batch_actual, &vresult[0] + total_values_read, &values_read);
      total_values_read += values_read;
      batch_actual += batch;
      batch_size = batch_size * 2;
    } while (batch > 0);

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
    make_pages();
    init_reader();
    check_results();
  }

 protected:
  int num_levels_;
  int num_values_;
  int num_pages_;
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
  num_levels_ = 5000;
  num_pages_ = 50;
  max_def_level_ = 0;
  max_rep_level_ = 0;
  NodePtr type = schema::Int32("a", Repetition::REQUIRED);
  const ColumnDescriptor descr(type, max_def_level_, max_rep_level_);
  descr_ = &descr;
  execute();
}

TEST_F(TestPrimitiveReader, TestInt32FlatOptional) {
  num_levels_ = 5000;
  num_pages_ = 50;
  max_def_level_ = 4;
  max_rep_level_ = 0;
  NodePtr type = schema::Int32("b", Repetition::OPTIONAL);
  const ColumnDescriptor descr(type, max_def_level_, max_rep_level_);
  descr_ = &descr;
  execute();
}

TEST_F(TestPrimitiveReader, TestInt32FlatRepeated) {
  num_levels_ = 5000;
  num_pages_ = 50;
  max_def_level_ = 4;
  max_rep_level_ = 2;
  NodePtr type = schema::Int32("c", Repetition::REPEATED);
  const ColumnDescriptor descr(type, max_def_level_, max_rep_level_);
  descr_ = &descr;
  execute();
}

} // namespace test
} // namespace parquet_cpp
