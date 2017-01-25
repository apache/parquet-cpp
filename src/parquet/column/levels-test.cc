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
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>

#include "parquet/column/levels.h"
#include "parquet/types.h"

using std::string;

namespace parquet {

void GenerateLevels(int min_repeat_factor, int max_repeat_factor, int max_level,
    std::vector<int16_t>& input_levels) {
  // for each repetition count upto max_repeat_factor
  for (int repeat = min_repeat_factor; repeat <= max_repeat_factor; repeat++) {
    // repeat count increases by a factor of 2 for every iteration
    int repeat_count = (1 << repeat);
    // generate levels for repetition count upto the maximum level
    int value = 0;
    int bwidth = 0;
    while (value <= max_level) {
      for (int i = 0; i < repeat_count; i++) {
        input_levels.push_back(value);
      }
      value = (2 << bwidth) - 1;
      bwidth++;
    }
  }
}

void EncodeLevels(Encoding::type encoding, int max_level, int num_levels,
    const int16_t* input_levels, std::vector<uint8_t>& bytes) {
  LevelEncoder encoder;
  int levels_count = 0;
  bytes.resize(2 * num_levels);
  ASSERT_EQ(2 * num_levels, bytes.size());
  // encode levels
  if (encoding == Encoding::RLE) {
    // leave space to write the rle length value
    encoder.Init(
        encoding, max_level, num_levels, bytes.data() + sizeof(int32_t), bytes.size());

    levels_count = encoder.Encode(num_levels, input_levels);
    (reinterpret_cast<int32_t*>(bytes.data()))[0] = encoder.len();
  } else {
    encoder.Init(encoding, max_level, num_levels, bytes.data(), bytes.size());
    levels_count = encoder.Encode(num_levels, input_levels);
  }
  ASSERT_EQ(num_levels, levels_count);
}

void VerifyDecodingLevels(Encoding::type encoding, int max_level,
    std::vector<int16_t>& input_levels, std::vector<uint8_t>& bytes) {
  LevelDecoder decoder;
  int levels_count = 0;
  std::vector<int16_t> output_levels;
  int num_levels = input_levels.size();

  output_levels.resize(num_levels);
  ASSERT_EQ(num_levels, output_levels.size());

  // Decode levels and test with multiple decode calls
  decoder.SetData(encoding, max_level, num_levels, bytes.data());
  int decode_count = 4;
  int num_inner_levels = num_levels / decode_count;
  // Try multiple decoding on a single SetData call
  for (int ct = 0; ct < decode_count; ct++) {
    int offset = ct * num_inner_levels;
    levels_count = decoder.Decode(num_inner_levels, output_levels.data());
    ASSERT_EQ(num_inner_levels, levels_count);
    for (int i = 0; i < num_inner_levels; i++) {
      EXPECT_EQ(input_levels[i + offset], output_levels[i]);
    }
  }
  // check the remaining levels
  int num_levels_completed = decode_count * (num_levels / decode_count);
  int num_remaining_levels = num_levels - num_levels_completed;
  if (num_remaining_levels > 0) {
    levels_count = decoder.Decode(num_remaining_levels, output_levels.data());
    ASSERT_EQ(num_remaining_levels, levels_count);
    for (int i = 0; i < num_remaining_levels; i++) {
      EXPECT_EQ(input_levels[i + num_levels_completed], output_levels[i]);
    }
  }
  // Test zero Decode values
  ASSERT_EQ(0, decoder.Decode(1, output_levels.data()));
}

void VerifyDecodingMultipleSetData(Encoding::type encoding, int max_level,
    std::vector<int16_t>& input_levels, std::vector<std::vector<uint8_t>>& bytes) {
  LevelDecoder decoder;
  int levels_count = 0;
  std::vector<int16_t> output_levels;

  // Decode levels and test with multiple SetData calls
  int setdata_count = bytes.size();
  int num_levels = input_levels.size() / setdata_count;
  output_levels.resize(num_levels);
  // Try multiple SetData
  for (int ct = 0; ct < setdata_count; ct++) {
    int offset = ct * num_levels;
    ASSERT_EQ(num_levels, output_levels.size());
    decoder.SetData(encoding, max_level, num_levels, bytes[ct].data());
    levels_count = decoder.Decode(num_levels, output_levels.data());
    ASSERT_EQ(num_levels, levels_count);
    for (int i = 0; i < num_levels; i++) {
      EXPECT_EQ(input_levels[i + offset], output_levels[i]);
    }
  }
}

// Test levels with maximum bit-width from 1 to 8
// increase the repetition count for each iteration by a factor of 2
TEST(TestLevels, TestLevelsDecodeMultipleBitWidth) {
  int min_repeat_factor = 0;
  int max_repeat_factor = 7;  // 128
  int max_bit_width = 8;
  std::vector<int16_t> input_levels;
  std::vector<uint8_t> bytes;
  Encoding::type encodings[2] = {Encoding::RLE, Encoding::BIT_PACKED};

  // for each encoding
  for (int encode = 0; encode < 2; encode++) {
    Encoding::type encoding = encodings[encode];
    // BIT_PACKED requires a sequence of atleast 8
    if (encoding == Encoding::BIT_PACKED) min_repeat_factor = 3;
    // for each maximum bit-width
    for (int bit_width = 1; bit_width <= max_bit_width; bit_width++) {
      // find the maximum level for the current bit_width
      int max_level = (1 << bit_width) - 1;
      // Generate levels
      GenerateLevels(min_repeat_factor, max_repeat_factor, max_level, input_levels);
      EncodeLevels(encoding, max_level, input_levels.size(), input_levels.data(), bytes);
      VerifyDecodingLevels(encoding, max_level, input_levels, bytes);
      input_levels.clear();
    }
  }
}

// Test multiple decoder SetData calls
TEST(TestLevels, TestLevelsDecodeMultipleSetData) {
  int min_repeat_factor = 3;
  int max_repeat_factor = 7;  // 128
  int bit_width = 8;
  int max_level = (1 << bit_width) - 1;
  std::vector<int16_t> input_levels;
  std::vector<std::vector<uint8_t>> bytes;
  Encoding::type encodings[2] = {Encoding::RLE, Encoding::BIT_PACKED};
  GenerateLevels(min_repeat_factor, max_repeat_factor, max_level, input_levels);
  int num_levels = input_levels.size();
  int setdata_factor = 8;
  int split_level_size = num_levels / setdata_factor;
  bytes.resize(setdata_factor);

  // for each encoding
  for (int encode = 0; encode < 2; encode++) {
    Encoding::type encoding = encodings[encode];
    for (int rf = 0; rf < setdata_factor; rf++) {
      int offset = rf * split_level_size;
      EncodeLevels(encoding, max_level, split_level_size,
          reinterpret_cast<int16_t*>(input_levels.data()) + offset, bytes[rf]);
    }
    VerifyDecodingMultipleSetData(encoding, max_level, input_levels, bytes);
  }
}

TEST(TestLevelEncoder, MinimumBufferSize) {
  // PARQUET-676, PARQUET-698
  const int kNumToEncode = 1024;

  std::vector<int16_t> levels;
  for (int i = 0; i < kNumToEncode; ++i) {
    if (i % 9 == 0) {
      levels.push_back(0);
    } else {
      levels.push_back(1);
    }
  }

  std::vector<uint8_t> output(
      LevelEncoder::MaxBufferSize(Encoding::RLE, 1, kNumToEncode));

  LevelEncoder encoder;
  encoder.Init(Encoding::RLE, 1, kNumToEncode, output.data(), output.size());
  int encode_count = encoder.Encode(kNumToEncode, levels.data());

  ASSERT_EQ(kNumToEncode, encode_count);
}

TEST(TestLevelEncoder, MinimumBufferSize2) {
  // PARQUET-708
  // Test the worst case for bit_width=2 consisting of
  // LiteralRun(size=8)
  // RepeatedRun(size=8)
  // LiteralRun(size=8)
  // ...
  const int kNumToEncode = 1024;

  std::vector<int16_t> levels;
  for (int i = 0; i < kNumToEncode; ++i) {
    // This forces a literal run of 00000001
    // followed by eight 1s
    if ((i % 16) < 7) {
      levels.push_back(0);
    } else {
      levels.push_back(1);
    }
  }

  for (int bit_width = 1; bit_width <= 8; bit_width++) {
    std::vector<uint8_t> output(
        LevelEncoder::MaxBufferSize(Encoding::RLE, bit_width, kNumToEncode));

    LevelEncoder encoder;
    encoder.Init(Encoding::RLE, bit_width, kNumToEncode, output.data(), output.size());
    int encode_count = encoder.Encode(kNumToEncode, levels.data());

    ASSERT_EQ(kNumToEncode, encode_count);
  }
}

}  // namespace parquet
