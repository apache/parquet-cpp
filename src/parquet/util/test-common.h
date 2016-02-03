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

#ifndef PARQUET_UTIL_TEST_COMMON_H
#define PARQUET_UTIL_TEST_COMMON_H

#include <iostream>
#include <random>
#include <string>
#include <vector>

#include "parquet/reader.h"

using std::vector;

namespace parquet_cpp {

namespace test {

template <typename T>
static inline bool vector_equal(const vector<T>& left, const vector<T>& right) {
  if (left.size() != right.size()) {
    return false;
  }

  for (size_t i = 0; i < left.size(); ++i) {
    if (left[i] != right[i]) {
      std::cerr << "index " << i
                << " left was " << left[i]
                << " right was " << right[i]
                << std::endl;
      return false;
    }
  }

  return true;
}

static inline vector<bool> flip_coins_seed(size_t n, double p, uint32_t seed) {
  std::mt19937 gen(seed);
  std::bernoulli_distribution d(p);

  vector<bool> draws;
  for (size_t i = 0; i < n; ++i) {
    draws.push_back(d(gen));
  }
  return draws;
}


static inline vector<bool> flip_coins(size_t n, double p) {
  std::random_device rd;
  std::mt19937 gen(rd());

  std::bernoulli_distribution d(p);

  vector<bool> draws;
  for (size_t i = 0; i < n; ++i) {
    draws.push_back(d(gen));
  }
  return draws;
}

const char* data_dir = std::getenv("PARQUET_TEST_DATA");

class TestFileInfo {
 public:
  std::string filename_;

  explicit TestFileInfo(const std::string& filename): filename_(filename) {}
};

class TestReader: public testing::TestWithParam<TestFileInfo> {
 public:
  virtual ~TestReader();

  std::string getFilename() {
    std::ostringstream filename;
    std::string dir_string(data_dir);
    filename << dir_string << "/" << GetParam().filename_;
    return filename.str();
  }

  TestReader() {
    file_.Open(getFilename());
    reader_.Open(&file_);
  }

  // Find a column of a specific type
  ssize_t findColumn(parquet::Type::type t) {
    for (size_t c = 1; c < reader_.metadata().schema.size(); c++) {
      if (t == reader_.metadata().schema[c].type) {
        return static_cast<ssize_t>(c-1);
      }
    }
    return -1;
  }

 protected:
  LocalFile file_;
  ParquetFileReader reader_;
};

TestReader::~TestReader() {
  file_.Close();
}

} // namespace test

} // namespace parquet_cpp

#endif // PARQUET_UTIL_TEST_COMMON_H
