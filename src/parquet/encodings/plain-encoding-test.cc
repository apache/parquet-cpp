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
#include <random>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include "parquet/util/test-common.h"

#include "parquet/encodings/encodings.h"

using std::string;
using std::vector;

namespace parquet_cpp {

namespace test {

static vector<bool> flip_coins(size_t n, double p) {
  std::random_device rd;
  std::mt19937 gen(rd());

  std::bernoulli_distribution d(p);

  vector<bool> draws;
  for (size_t i = 0; i < n; ++i) {
    draws.push_back(d(gen));
  }
  return draws;
}

TEST(BooleanTest, TestEncodeDecode) {
  // PARQUET-454

  size_t nvalues = 100;
  size_t nbytes = BitUtil::RoundUp(nvalues, 8) / 8;

  vector<bool> draws = flip_coins(nvalues, 0.5);

  PlainEncoder<Type::BOOLEAN> encoder(nullptr);
  PlainDecoder<Type::BOOLEAN> decoder(nullptr);

  std::vector<uint8_t> encode_buffer(nbytes);

  size_t encoded_bytes = encoder.Encode(draws, nvalues, &encode_buffer[0]);
  ASSERT_EQ(nbytes, encoded_bytes);

  std::vector<uint8_t> decode_buffer(nbytes);
  const uint8_t* decode_data = &decode_buffer[0];

  decoder.SetData(nvalues, &encode_buffer[0], encoded_bytes);
  size_t values_decoded = decoder.Decode(&decode_buffer[0], nvalues);
  ASSERT_EQ(nvalues, values_decoded);

  for (size_t i = 0; i < nvalues; ++i) {
    ASSERT_EQ(BitUtil::GetArrayBit(decode_data, i), draws[i]) << i;
  }
}

} // namespace test

} // namespace parquet_cpp
