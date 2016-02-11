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
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include "parquet/util/test-common.h"

#include "parquet/compression/codec.h"

using std::string;
using std::vector;

namespace parquet_cpp {

void CheckCodecRoundtrip(Codec* codec, const vector<uint8_t>& data) {
  int max_compressed_len = codec->MaxCompressedLen(data.size(), &data[0]);

  std::vector<uint8_t> compressed(max_compressed_len);
  int actual_size = codec->Compress(data.size(), &data[0], max_compressed_len,
      &compressed[0]);
  compressed.resize(actual_size);

  std::vector<uint8_t> decompressed(data.size());
  codec->Decompress(compressed.size(), &compressed[0],
      decompressed.size(), &decompressed[0]);

  ASSERT_TRUE(test::vector_equal(data, decompressed));
}

void CheckCodec(Codec* codec) {
  int sizes[] = {10000, 100000};
  for (int data_size : sizes) {
    vector<uint8_t> data;
    test::random_bytes(data_size, 1234, &data);
    CheckCodecRoundtrip(codec, data);
  }
}

TEST(TestCompressors, Snappy) {
  SnappyCodec codec;
  CheckCodec(&codec);
}

TEST(TestCompressors, Lz4) {
  Lz4Codec codec;
  CheckCodec(&codec);
}

TEST(TestCompressors, GZip) {
  GZipCodec codec(GZipCodec::GZIP);
  CheckCodec(&codec);
}

} // namespace parquet_cpp
