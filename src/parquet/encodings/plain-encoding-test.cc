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
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "parquet/encodings/plain-encoding.h"
#include "parquet/types.h"
#include "parquet/schema/types.h"
#include "parquet/util/bit-util.h"
#include "parquet/util/output.h"
#include "parquet/util/test-common.h"

using std::string;
using std::vector;

namespace parquet_cpp {

namespace test {

TEST(BooleanTest1, TestEncodeDecode) {
  // PARQUET-454
  size_t nvalues = 10000;
  size_t nbytes = BitUtil::Ceil(nvalues, 8);

  // seed the prng so failure is deterministic
  vector<bool> draws = flip_coins_seed(nvalues, 0.5, 0);

  PlainEncoder<Type::BOOLEAN> encoder(nullptr);
  PlainDecoder<Type::BOOLEAN> decoder(nullptr);

  InMemoryOutputStream dst;
  encoder.Encode(draws, nvalues, &dst);

  std::vector<uint8_t> encode_buffer;
  dst.Transfer(&encode_buffer);

  ASSERT_EQ(nbytes, encode_buffer.size());

  std::vector<uint8_t> decode_buffer(nbytes);
  const uint8_t* decode_data = &decode_buffer[0];

  decoder.SetData(nvalues, &encode_buffer[0], encode_buffer.size());
  size_t values_decoded = decoder.Decode(&decode_buffer[0], nvalues);
  ASSERT_EQ(nvalues, values_decoded);

  for (size_t i = 0; i < nvalues; ++i) {
    ASSERT_EQ(draws[i], BitUtil::GetArrayBit(decode_data, i)) << i;
  }
}

TEST(BooleanTest2, TestEncodeDecode) {
  size_t nvalues = 10000;
  size_t nbytes = BitUtil::Ceil(nvalues, 8);

  // seed the prng so failure is deterministic
  vector<uint8_t> draws;
  draws.resize(nvalues);
  bool* bool_data = reinterpret_cast<bool*>(draws.data());
  random_bools(nvalues, 0.5, bool_data);

  PlainEncoder<Type::BOOLEAN> encoder(nullptr);
  PlainDecoder<Type::BOOLEAN> decoder(nullptr);

  InMemoryOutputStream dst;
  encoder.Encode(bool_data, nvalues, &dst);

  std::vector<uint8_t> encode_buffer;
  dst.Transfer(&encode_buffer);

  ASSERT_EQ(nbytes, encode_buffer.size());

  std::vector<uint8_t> decode_buffer(nvalues);
  const uint8_t* decode_data = &decode_buffer[0];

  decoder.SetData(nvalues, &encode_buffer[0], encode_buffer.size());
  size_t values_decoded = decoder.Decode(
      reinterpret_cast<bool*>(decode_buffer.data()), nvalues);
  ASSERT_EQ(nvalues, values_decoded);

  for (size_t i = 0; i < nvalues; ++i) {
    ASSERT_EQ(draws[i], decode_data[i]) << i;
  }
}

TEST(Int32Test, TestEncodeDecode) {
  size_t nvalues = 10000;
  size_t nbytes = nvalues * type_traits<Type::INT32>::value_byte_size;

  // seed the prng so failure is deterministic
  vector<int32_t> draws;
  random_int_numbers(nvalues, 0.5, &draws);

  PlainEncoder<Type::INT32> encoder(nullptr);
  PlainDecoder<Type::INT32> decoder(nullptr);

  InMemoryOutputStream dst;
  encoder.Encode(draws.data(), nvalues, &dst);

  std::vector<uint8_t> encode_buffer;
  dst.Transfer(&encode_buffer);

  ASSERT_EQ(nbytes, encode_buffer.size());

  std::vector<uint8_t> decode_buffer(nbytes);
  const int32_t* decode_data = reinterpret_cast<int32_t*>(decode_buffer.data());

  decoder.SetData(nvalues, &encode_buffer[0], encode_buffer.size());
  size_t values_decoded = decoder.Decode(
      reinterpret_cast<int32_t*>(decode_buffer.data()), nvalues);
  ASSERT_EQ(nvalues, values_decoded);

  for (size_t i = 0; i < nvalues; ++i) {
    ASSERT_EQ(draws[i], decode_data[i]) << i;
  }
}

TEST(Int64Test, TestEncodeDecode) {
  size_t nvalues = 10000;
  size_t nbytes = nvalues * type_traits<Type::INT64>::value_byte_size;

  // seed the prng so failure is deterministic
  vector<int64_t> draws;
  random_int_numbers(nvalues, 0.5, &draws);

  PlainEncoder<Type::INT64> encoder(nullptr);
  PlainDecoder<Type::INT64> decoder(nullptr);

  InMemoryOutputStream dst;
  encoder.Encode(draws.data(), nvalues, &dst);

  std::vector<uint8_t> encode_buffer;
  dst.Transfer(&encode_buffer);

  ASSERT_EQ(nbytes, encode_buffer.size());

  std::vector<uint8_t> decode_buffer(nbytes);
  const int64_t* decode_data = reinterpret_cast<int64_t*>(decode_buffer.data());

  decoder.SetData(nvalues, &encode_buffer[0], encode_buffer.size());
  size_t values_decoded = decoder.Decode(
      reinterpret_cast<int64_t*>(decode_buffer.data()), nvalues);
  ASSERT_EQ(nvalues, values_decoded);

  for (size_t i = 0; i < nvalues; ++i) {
    ASSERT_EQ(draws[i], decode_data[i]) << i;
  }
}

TEST(FloatTest, TestEncodeDecode) {
  size_t nvalues = 10000;
  size_t nbytes = nvalues * type_traits<Type::FLOAT>::value_byte_size;

  // seed the prng so failure is deterministic
  vector<float> draws;
  random_real_numbers(nvalues, 0.5, &draws);

  PlainEncoder<Type::FLOAT> encoder(nullptr);
  PlainDecoder<Type::FLOAT> decoder(nullptr);

  InMemoryOutputStream dst;
  encoder.Encode(draws.data(), nvalues, &dst);

  std::vector<uint8_t> encode_buffer;
  dst.Transfer(&encode_buffer);

  ASSERT_EQ(nbytes, encode_buffer.size());

  std::vector<uint8_t> decode_buffer(nbytes);
  const float* decode_data = reinterpret_cast<float*>(decode_buffer.data());

  decoder.SetData(nvalues, &encode_buffer[0], encode_buffer.size());
  size_t values_decoded = decoder.Decode(
      reinterpret_cast<float*>(decode_buffer.data()), nvalues);
  ASSERT_EQ(nvalues, values_decoded);

  for (size_t i = 0; i < nvalues; ++i) {
    ASSERT_EQ(draws[i], decode_data[i]) << i;
  }
}

TEST(DoubleTest, TestEncodeDecode) {
  size_t nvalues = 10000;
  size_t nbytes = nvalues * type_traits<Type::DOUBLE>::value_byte_size;

  // seed the prng so failure is deterministic
  vector<double> draws;
  random_real_numbers(nvalues, 0.5, &draws);

  PlainEncoder<Type::DOUBLE> encoder(nullptr);
  PlainDecoder<Type::DOUBLE> decoder(nullptr);

  InMemoryOutputStream dst;
  encoder.Encode(draws.data(), nvalues, &dst);

  std::vector<uint8_t> encode_buffer;
  dst.Transfer(&encode_buffer);

  ASSERT_EQ(nbytes, encode_buffer.size());

  std::vector<uint8_t> decode_buffer(nbytes);
  const double* decode_data = reinterpret_cast<double*>(decode_buffer.data());

  decoder.SetData(nvalues, &encode_buffer[0], encode_buffer.size());
  size_t values_decoded = decoder.Decode(
      reinterpret_cast<double*>(decode_buffer.data()), nvalues);
  ASSERT_EQ(nvalues, values_decoded);

  for (size_t i = 0; i < nvalues; ++i) {
    ASSERT_EQ(draws[i], decode_data[i]) << i;
  }
}

TEST(Int96Test, TestEncodeDecode) {
  size_t nvalues = 10000;
  size_t nbytes = nvalues * type_traits<Type::INT96>::value_byte_size;

  // seed the prng so failure is deterministic
  vector<Int96> draws;
  random_int_numbers(nvalues, 0.5, &draws);

  PlainEncoder<Type::INT96> encoder(nullptr);
  PlainDecoder<Type::INT96> decoder(nullptr);

  InMemoryOutputStream dst;
  encoder.Encode(draws.data(), nvalues, &dst);

  std::vector<uint8_t> encode_buffer;
  dst.Transfer(&encode_buffer);

  ASSERT_EQ(nbytes, encode_buffer.size());

  std::vector<uint8_t> decode_buffer(nbytes);
  const Int96* decode_data = reinterpret_cast<Int96*>(decode_buffer.data());

  decoder.SetData(nvalues, &encode_buffer[0], encode_buffer.size());
  size_t values_decoded = decoder.Decode(
      reinterpret_cast<Int96*>(decode_buffer.data()), nvalues);
  ASSERT_EQ(nvalues, values_decoded);

  for (size_t i = 0; i < nvalues; ++i) {
    ASSERT_EQ(draws[i].value[0], decode_data[i].value[0]) << i;
    ASSERT_EQ(draws[i].value[1], decode_data[i].value[1]) << i;
    ASSERT_EQ(draws[i].value[2], decode_data[i].value[2]) << i;
  }
}

TEST(ByteArrayTest, TestEncodeDecode) {
  size_t nvalues = 10000;
  int max_byte_array_len = 12 + sizeof(uint32_t);
  size_t nbytes = nvalues * max_byte_array_len;

  // seed the prng so failure is deterministic
  vector<ByteArray> draws;
  std::vector<uint8_t> data_buffer;
  data_buffer.resize(nbytes);
  random_byte_array(nvalues, 0.5, data_buffer.data(), &draws);

  PlainEncoder<Type::BYTE_ARRAY> encoder(nullptr);
  PlainDecoder<Type::BYTE_ARRAY> decoder(nullptr);

  InMemoryOutputStream dst;
  encoder.Encode(draws.data(), nvalues, &dst);

  std::vector<uint8_t> encode_buffer;
  dst.Transfer(&encode_buffer);

  std::vector<uint8_t> decode_buffer(nbytes);
  const ByteArray* decode_data = reinterpret_cast<ByteArray*>(decode_buffer.data());

  decoder.SetData(nvalues, &encode_buffer[0], encode_buffer.size());
  size_t values_decoded = decoder.Decode(
      reinterpret_cast<ByteArray*>(decode_buffer.data()), nvalues);
  ASSERT_EQ(nvalues, values_decoded);

  for (size_t i = 0; i < nvalues; ++i) {
    ASSERT_EQ(draws[i].len, decode_data[i].len) << i;
    for (size_t j = 0; j < draws[i].len; ++j) {
      ASSERT_EQ(draws[i].ptr[j], decode_data[i].ptr[j]) << i;
    }
  }
}

TEST(FLBATest, TestEncodeDecode) {
  size_t nvalues = 10000;
  int flba_length = 8;
  size_t nbytes = nvalues * flba_length;

  // seed the prng so failure is deterministic
  vector<FLBA> draws;
  std::vector<uint8_t> data_buffer;
  data_buffer.resize(nbytes);
  random_fixed_byte_array(nvalues, 0.5, data_buffer.data(), flba_length, &draws);

  schema::NodePtr node;
  node = schema::PrimitiveNode::MakeFLBA("name", Repetition::OPTIONAL,
            Type::FIXED_LEN_BYTE_ARRAY, flba_length, LogicalType::UTF8);
  ColumnDescriptor d(node, 0, 0);

  PlainEncoder<Type::FIXED_LEN_BYTE_ARRAY> encoder(&d);
  PlainDecoder<Type::FIXED_LEN_BYTE_ARRAY> decoder(&d);

  InMemoryOutputStream dst;
  encoder.Encode(draws.data(), nvalues, &dst);

  std::vector<uint8_t> encode_buffer;
  dst.Transfer(&encode_buffer);

  std::vector<uint8_t> decode_buffer(nbytes);
  const FLBA* decode_data = reinterpret_cast<FLBA*>(decode_buffer.data());

  decoder.SetData(nvalues, &encode_buffer[0], encode_buffer.size());
  size_t values_decoded = decoder.Decode(
      reinterpret_cast<FLBA*>(decode_buffer.data()), nvalues);
  ASSERT_EQ(nvalues, values_decoded);

  for (size_t i = 0; i < nvalues; ++i) {
    for (size_t j = 0; j < flba_length; ++j) {
      ASSERT_EQ(draws[i].ptr[j], decode_data[i].ptr[j]) << i;
    }
  }
}

} // namespace test
} // namespace parquet_cpp
