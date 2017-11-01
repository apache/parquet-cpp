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

#include "parquet/murmur3.h"
#include "parquet/bloom.h"

#include "parquet/util/memory.h"

namespace parquet {
namespace test {
TEST(Murmur3Test, TestBloomFilter) {
  const uint8_t bitset[8] = {0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7};
  int64_t result;
  MurmurHash3_x64_128(bitset, 8, Bloom::DEFAULT_SEED, &result);
  ASSERT_EQ(result, -3850979349427597861l);
}


TEST(FindTest, TestBloomFilter) {
  std::unique_ptr<Bloom> bloom(new Bloom(1024));
  for(int i = 0; i<10; i++) {
    uint64_t hash_value = bloom->hash(i);
    bloom->insert(hash_value);
  }
  std::shared_ptr<InMemoryOutputStream> sink;
  sink.reset(new InMemoryOutputStream());

  bloom->writeTo(sink);

  std::shared_ptr<InMemoryInputStream> source(new InMemoryInputStream(sink->GetBuffer()));

  int64_t bytes_avaliable;
  uint32_t length = *(reinterpret_cast<const uint32_t *>(
      source->Read(4, &bytes_avaliable)));
  ASSERT_EQ(length, 1024);

  uint32_t hash = *(reinterpret_cast<const uint32_t *>(
      source->Read(4, &bytes_avaliable)));
  ASSERT_EQ(hash, 0);

  uint32_t algo = *(reinterpret_cast<const uint32_t *>(
      source->Read(4, &bytes_avaliable)));
  ASSERT_EQ(algo, 0);

  const uint8_t* bitset = source->Read(length, &bytes_avaliable);

  std::unique_ptr<Bloom> de_bloom(new Bloom(bitset, length));

  for(int i = 0; i<10; i++) {
    ASSERT_TRUE(de_bloom->find(bloom->hash(i)));
  }
}

}//namespace test


}// namespace parquet
