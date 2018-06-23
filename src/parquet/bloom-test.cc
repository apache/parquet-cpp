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
#include <random>
#include <string>

#include "arrow/io/file.h"
#include "parquet/bloom.h"
#include "parquet/murmur3.h"
#include "parquet/util/memory.h"
#include "parquet/util/test-common.h"

namespace parquet {
namespace test {
TEST(Murmur3Test, TestBloomFilter) {
  int64_t result;
  const uint8_t bitset[8] = {0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7};
  MurmurHash3_x64_128(bitset, 8, Bloom::DEFAULT_SEED, &result);
  ASSERT_EQ(result, -3850979349427597861l);
}

TEST(FindTest, TestBloomFilter) {
  std::unique_ptr<Bloom> bloom(new Bloom(1024));

  for (int i = 0; i < 10; i++) {
    uint64_t hash_value = bloom->hash(i);
    bloom->insert(hash_value);
  }

  // Serialize bloom filter to memory output stream
  std::shared_ptr<InMemoryOutputStream> sink;
  sink.reset(new InMemoryOutputStream());
  bloom->writeTo(sink);

  // Deserialize bloom filter from memory
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
  for (int i = 0; i < 10; i++) {
    ASSERT_TRUE(de_bloom->find(bloom->hash(i)));
  }
}

TEST(FPPTest, TestBloomFilter) {
    int exist = 0;
    std::string str("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz");
    std::vector<std::string> random_strings;
    std::unique_ptr<Bloom> bloom(new Bloom(Bloom::optimalNumOfBits(100000, 0.01)));

    // Insert 100000 elements to bloom filter and serialize to memory
    std::random_device rd;
    std::mt19937 generator(rd());
    for (int i = 0; i < 100000; i++) {
      std::shuffle(str.begin(), str.end(), generator);
      std::string tmp = str.substr(0, 10);
      ByteArray byte_array(10, reinterpret_cast<const uint8_t*>(tmp.c_str()));
      random_strings.push_back(tmp);
      bloom->insert(bloom->hash(&byte_array));
    }

    std::shared_ptr<InMemoryOutputStream> sink;
    sink.reset(new InMemoryOutputStream());
    bloom->writeTo(sink);

    // Deserialize bloom filter from memory
    std::shared_ptr<InMemoryInputStream> source(
        new InMemoryInputStream(sink->GetBuffer()));
    int64_t bytes_avaliable;
    uint32_t length = *(reinterpret_cast<const uint32_t *>(
        source->Read(4, &bytes_avaliable)));

    uint32_t hash = *(reinterpret_cast<const uint32_t *>(
        source->Read(4, &bytes_avaliable)));
    ASSERT_EQ(hash, 0);

    uint32_t algo = *(reinterpret_cast<const uint32_t *>(
        source->Read(4, &bytes_avaliable)));
    ASSERT_EQ(algo, 0);

    const uint8_t* bitset = source->Read(length, &bytes_avaliable);

    std::unique_ptr<Bloom> de_bloom(new Bloom(bitset, length));
    for (int i = 0; i < 100000; i++) {
      ByteArray byte_array1(10,
          reinterpret_cast<const uint8_t*>(random_strings[i].c_str()));
      ASSERT_TRUE(de_bloom->find(de_bloom->hash(&byte_array1)));
      std::shuffle(str.begin(), str.end(), generator);
      std::string tmp = str.substr(0, 8);
      ByteArray byte_array2(8, reinterpret_cast<const uint8_t*>(tmp.c_str()));

      if (de_bloom->find(de_bloom->hash(&byte_array2))) {
        exist++;
      }
    }

    // The exist should be probably less than 1000 according default FPP 0.01.
    ASSERT_TRUE(exist < 1000);
}

TEST(CompatibilityTest, TestBloomFilter) {
    int64_t bytesRead;
    int length, hash, algorithm;
    std::unique_ptr<uint8_t[]> bitset;

    std::string testString[4] = {"hello", "parquet", "bloom", "filter"};
	std::string data_dir(test::get_data_dir());
    std::string bloomDataPath = data_dir + "/bloom_filter.bin";
    std::shared_ptr<::arrow::io::ReadableFile> handle;

    PARQUET_THROW_NOT_OK(::arrow::io::ReadableFile::Open(bloomDataPath, &handle));

    handle->Read(4, &bytesRead, reinterpret_cast<void*>(&length));
    ASSERT_TRUE(bytesRead == 4);
    handle->Read(4, &bytesRead, reinterpret_cast<void*>(&hash));
    ASSERT_TRUE(hash == 0);
    handle->Read(4, &bytesRead, reinterpret_cast<void*>(&algorithm));
    ASSERT_TRUE(algorithm == 0);

    bitset.reset(new uint8_t[length]);
    handle->Read(length, &bytesRead, reinterpret_cast<void*>(bitset.get()));
    ASSERT_TRUE(length == bytesRead);

    std::unique_ptr<Bloom> bloom_filter(new Bloom(bitset.get(), length));

    for (int i = 0; i < 4; i++) {
      ByteArray tmp(static_cast<uint32_t>(testString[i].length()),
          reinterpret_cast<const uint8_t*>(testString[i].c_str()));
      ASSERT_TRUE(bloom_filter->find(bloom_filter->hash(&tmp)));
    }
}

}//namespace test


}// namespace parquet
