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
#include <limits>
#include <random>
#include <vector>

#include "parquet/types.h"

using std::vector;

namespace parquet_cpp {

namespace test {

template <typename T>
static inline void assert_vector_equal(const vector<T>& left,
    const vector<T>& right) {
  ASSERT_EQ(left.size(), right.size());

  for (size_t i = 0; i < left.size(); ++i) {
    ASSERT_EQ(left[i], right[i]) << i;
  }
}

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

template <typename T>
static vector<T> slice(const vector<T>& values, size_t start, size_t end) {
  if (end < start) {
    return vector<T>(0);
  }

  vector<T> out(end - start);
  for (size_t i = start; i < end; ++i) {
    out[i - start] = values[i];
  }
  return out;
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

void random_bools(int n, double p, uint32_t seed, bool* out) {
  std::mt19937 gen(seed);
  std::bernoulli_distribution d(p);

  for (int i = 0; i < n; ++i) {
    out[i] = d(gen);
  }
}


void random_bytes(int n, uint32_t seed, std::vector<uint8_t>* out) {
  std::mt19937 gen(seed);
  std::uniform_int_distribution<int> d(0, 255);

  for (int i = 0; i < n; ++i) {
    out->push_back(d(gen) & 0xFF);
  }
}

template <typename T>
void random_int_numbers(int n, uint32_t seed, std::vector<T>* out) {
  std::mt19937 gen(seed);
  std::uniform_int_distribution<T> d(std::numeric_limits<T>::lowest(),
      std::numeric_limits<T>::max());

  for (int i = 0; i < n; ++i) {
    out->push_back(d(gen));
  }
}

void random_int_numbers(int n, uint32_t seed, std::vector<Int96>* out) {
  std::mt19937 gen(seed);
  std::uniform_int_distribution<uint32_t> d(std::numeric_limits<uint32_t>::lowest(),
      std::numeric_limits<uint32_t>::max());

  Int96 *temp = &(*out)[0];
  for (int i = 0; i < n; ++i) {
    temp[i].value[0] = d(gen);
    temp[i].value[1] = d(gen);
    temp[i].value[2] = d(gen);
  }
}

void random_fixed_byte_array(int n, uint32_t seed, uint8_t *buf, int len,
    std::vector<FLBA>* out) {
  std::mt19937 gen(seed);
  std::uniform_int_distribution<int> d(0, 255);
  FLBA *temp = &(*out)[0];
  for (int i = 0; i < n; ++i) {
    temp[i].ptr = buf;
    for (int j = 0; j < len; ++j) {
      buf[j] = d(gen) & 0xFF;
    }
    buf += len;
  }
}

void random_byte_array(int n, uint32_t seed, uint8_t *buf,
    std::vector<ByteArray>* out, int max_size) {
  std::mt19937 gen(seed);
  std::uniform_int_distribution<int> d1(0, max_size);
  std::uniform_int_distribution<int> d2(0, 255);
  ByteArray temp;
  for (int i = 0; i < n; ++i) {
    temp.len = d1(gen);
    temp.ptr = buf;
    for (int j = 0; j < temp.len; ++j) {
      buf[j] = d2(gen) & 0xFF;
    }
    buf += temp.len;
    out->push_back(temp);
  }
}

template <typename T>
void random_real_numbers(int n, uint32_t seed, std::vector<T>* out) {
  std::mt19937 gen(seed);
  std::uniform_real_distribution<T> d(std::numeric_limits<T>::lowest(),
      std::numeric_limits<T>::max());

  for (int i = 0; i < n; ++i) {
    out->push_back(d(gen));
  }
}
} // namespace test
} // namespace parquet_cpp

#endif // PARQUET_UTIL_TEST_COMMON_H
