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

// From Apache Impala as of 2016-01-29

#include <gtest/gtest.h>
#include <stdio.h>
#include <cstdlib>
#include <iostream>
#include <set>
#include <vector>

#include "parquet/util/dict-encoding.h"
#include "parquet/util/test-common.h"
#include "parquet/types.h"

namespace parquet_cpp {

namespace test {

template <typename T>
void AssertDecoderEquals(DictDecoder<T>& decoder, const std::vector<T>& values,
    int type_length) {
  for (const T& i : values) {
    T j;
    decoder.GetValue(&j);
    EXPECT_EQ(i, j);
  }
}

template <>
void AssertDecoderEquals(DictDecoder<FLBA>& decoder,
    const std::vector<FLBA>& values, int type_length) {
  for (const FixedLenByteArray& expected : values) {
    FixedLenByteArray actual;
    decoder.GetValue(&actual);
    EXPECT_EQ(0, memcmp(expected.ptr, actual.ptr, type_length));
  }
}

template <typename T>
void ValidateDict(const std::vector<T>& values, int expected_distinct_values,
    int type_length) {
  MemPool pool;

  DictEncoder<T> encoder(&pool, type_length);
  for (size_t i = 0; i < values.size(); ++i) {
    encoder.Put(values[i]);
  }

  int num_dict_entries = encoder.num_entries();
  EXPECT_EQ(expected_distinct_values, num_dict_entries);

  uint8_t dict_buffer[encoder.dict_encoded_size()];
  encoder.WriteDict(dict_buffer);

  int data_buffer_len = encoder.EstimatedDataEncodedSize();
  uint8_t data_buffer[data_buffer_len];
  int data_len = encoder.WriteIndices(data_buffer, data_buffer_len);
  EXPECT_GT(data_len, 0);
  encoder.ClearIndices();

  DictDecoder<T> decoder(dict_buffer, encoder.dict_encoded_size(),
      num_dict_entries, type_length);
  decoder.SetData(data_buffer, data_len);

  AssertDecoderEquals(decoder, values, type_length);

  pool.FreeAll();
}

// TEST(DictTest, TestByteArray) {
//   StringValue sv1("hello world");
//   StringValue sv2("foo");
//   StringValue sv3("bar");
//   StringValue sv4("baz");

//   vector<StringValue> values;
//   values.push_back(sv1);
//   values.push_back(sv1);
//   values.push_back(sv1);
//   values.push_back(sv2);
//   values.push_back(sv1);
//   values.push_back(sv2);
//   values.push_back(sv2);
//   values.push_back(sv3);
//   values.push_back(sv3);
//   values.push_back(sv3);
//   values.push_back(sv4);

//   ValidateDict(values, -1);
// }

template <typename T>
void IncrementValue(T* t) { ++(*t); }

template <typename T>
void TestNumbers(int max_value, int repeat, int value_byte_size) {
  std::vector<T> values;
  for (T val = 0; val < max_value; IncrementValue(&val)) {
    for (int i = 0; i < repeat; ++i) {
      values.push_back(val);
    }
  }
  ValidateDict(values, max_value, value_byte_size);
}

template <typename T>
void TestNumbers() {
  int value_byte_size = sizeof(T);
  TestNumbers<T>(100, 1, value_byte_size);
  TestNumbers<T>(1, 100, value_byte_size);
  TestNumbers<T>(1, 1, value_byte_size);
  TestNumbers<T>(1, 2, value_byte_size);
}

class TestDict : public ::testing::Test {
 protected:
  std::vector<uint8_t> buffer_;
};

TEST(DictTest, TestNumbers) {
  TestNumbers<int32_t>();
  TestNumbers<int64_t>();
  TestNumbers<float>();
  TestNumbers<double>();
}

TEST_F(TestDict, TestByteArray) {
  int num_values = 50;
  int max_value_size = 24;
  int seed = 0;

  buffer_.resize(max_value_size * num_values);
  std::vector<ByteArray> values(num_values);

  random_byte_array(num_values, seed, buffer_.data(), &values[0],
      2, max_value_size);

  // Add some repeats
  values.resize(num_values * 2);
  for (size_t i = num_values; i < values.size(); ++i) {
    values[i] = values[i - num_values];
  }
  ValidateDict<ByteArray>(values, num_values, -1);
}

TEST_F(TestDict, TestByteArrayResizeHT) {
  int num_values = 2000;
  int seed = 0;
  int max_value_size = 10;

  buffer_.resize(max_value_size * num_values);

  std::vector<ByteArray> values(num_values);
  random_byte_array(num_values, seed, buffer_.data(), &values[0], max_value_size,
      max_value_size);
  ValidateDict<ByteArray>(values, num_values, -1);
}


TEST_F(TestDict, TestFixedLenByteArray) {
  int num_values = 50;
  int fixed_len_size = 18;
  int seed = 0;
  buffer_.resize(fixed_len_size * num_values);

  std::vector<FixedLenByteArray> values(num_values);
  random_fixed_byte_array(num_values, seed, buffer_.data(), fixed_len_size,
      &values[0]);

  // Add some repeats
  values.resize(num_values * 2);
  for (size_t i = num_values; i < values.size(); ++i) {
    values[i] = values[i - num_values];
  }
  ValidateDict<FixedLenByteArray>(values, num_values, fixed_len_size);
}

} // namespace test

} // namespace parquet_cpp
