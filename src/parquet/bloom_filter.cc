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

#include <algorithm>
#include <cmath>
#include <cstdint>

#include "arrow/status.h"
#include "arrow/util/bit-util.h"
#include "parquet/bloom_filter.h"
#include "parquet/exception.h"
#include "parquet/murmur3.h"
#include "parquet/types.h"
#include "parquet/util/logging.h"

namespace parquet {
constexpr uint32_t BlockSplitBloomFilter::SALT[BITS_SET_PER_BLOCK];

BlockSplitBloomFilter::BlockSplitBloomFilter(uint32_t num_bytes)
    : pool_(::arrow::default_memory_pool()),
      hash_strategy_(HashStrategy::MURMUR3_X64_128),
      algorithm_(Algorithm::BLOCK) {
  if (num_bytes < BloomFilter::MINIMUM_BLOOM_FILTER_BYTES) {
    num_bytes = MINIMUM_BLOOM_FILTER_BYTES;
  }

  // Get next power of 2 if it is not power of 2.
  if ((num_bytes & (num_bytes - 1)) != 0) {
    num_bytes = static_cast<uint32_t>(::arrow::BitUtil::NextPower2(num_bytes));
  }

  if (num_bytes > MAXIMUM_BLOOM_FILTER_BYTES) {
    num_bytes = MAXIMUM_BLOOM_FILTER_BYTES;
  }

  num_bytes_ = num_bytes;
  PARQUET_THROW_NOT_OK(::arrow::AllocateBuffer(pool_, num_bytes_, &data_));
  memset(data_->mutable_data(), 0, num_bytes_);

  this->hasher_.reset(new MurmurHash3());
}

BlockSplitBloomFilter::BlockSplitBloomFilter(const uint8_t* bitset, uint32_t num_bytes)
    : pool_(::arrow::default_memory_pool()),
      hash_strategy_(HashStrategy::MURMUR3_X64_128),
      algorithm_(Algorithm::BLOCK) {
  if (!bitset) {
    throw ParquetException("Given bitset is NULL");
  }

  if (num_bytes < MINIMUM_BLOOM_FILTER_BYTES || num_bytes > MAXIMUM_BLOOM_FILTER_BYTES ||
      (num_bytes & (num_bytes - 1)) != 0) {
    throw ParquetException("Given length of bitset is illegal");
  }

  num_bytes_ = num_bytes;
  PARQUET_THROW_NOT_OK(::arrow::AllocateBuffer(pool_, num_bytes_, &data_));
  memcpy(data_->mutable_data(), bitset, num_bytes_);

  this->hasher_.reset(new MurmurHash3());
}

BloomFilter* BlockSplitBloomFilter::Deserialize(InputStream* input) {
  int64_t bytes_available;
  const uint8_t* read_buffer = NULL;

  uint32_t len;
  read_buffer = input->Read(sizeof(uint32_t), &bytes_available);
  if (static_cast<uint32_t>(bytes_available) != sizeof(uint32_t) || !read_buffer) {
    throw ParquetException("Failed to deserialize from input stream");
  }
  memcpy(&len, read_buffer, sizeof(uint32_t));

  uint32_t hash;
  read_buffer = input->Read(sizeof(uint32_t), &bytes_available);
  if (static_cast<uint32_t>(bytes_available) != sizeof(uint32_t) || !read_buffer) {
    throw ParquetException("Failed to deserialize from input stream");
  }
  memcpy(&hash, read_buffer, sizeof(uint32_t));
  if (static_cast<uint32_t>(bytes_available) != sizeof(uint32_t)) {
    throw ParquetException("Failed to deserialize from input stream");
  }
  if (static_cast<HashStrategy>(hash) != HashStrategy::MURMUR3_X64_128) {
    throw ParquetException("Unsupported hash strategy");
  }

  uint32_t algorithm;
  read_buffer = input->Read(sizeof(uint32_t), &bytes_available);
  if (static_cast<uint32_t>(bytes_available) != sizeof(uint32_t) || !read_buffer) {
    throw ParquetException("Failed to deserialize from input stream");
  }
  memcpy(&algorithm, read_buffer, sizeof(uint32_t));
  if (static_cast<uint32_t>(bytes_available) != sizeof(uint32_t)) {
    throw ParquetException("Failed to deserialize from input stream");
  }
  if (static_cast<Algorithm>(algorithm) != BloomFilter::Algorithm::BLOCK) {
    throw ParquetException("Unsupported Bloom filter algorithm");
  }

  return new BlockSplitBloomFilter(input->Read(len, &bytes_available), len);
}

void BlockSplitBloomFilter::InsertHash(uint64_t hash) { SetBits(hash); }

bool BlockSplitBloomFilter::FindHash(uint64_t hash) const { return TestBits(hash); }

void BlockSplitBloomFilter::WriteTo(OutputStream* sink) const {
  if (!sink) {
    throw ParquetException("Given output stream is NULL.");
  }

  sink->Write(reinterpret_cast<const uint8_t*>(&num_bytes_), sizeof(num_bytes_));
  sink->Write(reinterpret_cast<const uint8_t*>(&hash_strategy_), sizeof(hash_strategy_));
  sink->Write(reinterpret_cast<const uint8_t*>(&algorithm_), sizeof(algorithm_));
  sink->Write(data_->mutable_data(), num_bytes_);
}

void BlockSplitBloomFilter::SetMask(uint32_t key, BlockMask& block_mask) const {
  for (int i = 0; i < BITS_SET_PER_BLOCK; ++i) {
    block_mask.item[i] = key * SALT[i];
  }

  for (int i = 0; i < BITS_SET_PER_BLOCK; ++i) {
    block_mask.item[i] = block_mask.item[i] >> 27;
  }

  for (int i = 0; i < BITS_SET_PER_BLOCK; ++i) {
    block_mask.item[i] = UINT32_C(0x1) << block_mask.item[i];
  }
}

bool BlockSplitBloomFilter::TestBits(uint64_t hash) const {
  const uint32_t bucket_index =
      static_cast<uint32_t>((hash >> 32) & (num_bytes_ / BYTES_PER_FILTER_BLOCK - 1));
  uint32_t key = static_cast<uint32_t>(hash);
  uint32_t* bitset32 = reinterpret_cast<uint32_t*>(data_->mutable_data());

  // Calculate mask for bucket.
  BlockMask block_mask;
  SetMask(key, block_mask);

  for (int i = 0; i < BITS_SET_PER_BLOCK; ++i) {
    if (0 == (bitset32[BITS_SET_PER_BLOCK * bucket_index + i] & block_mask.item[i])) {
      return false;
    }
  }
  return true;
}

void BlockSplitBloomFilter::SetBits(uint64_t hash) {
  const uint32_t bucket_index =
      static_cast<uint32_t>(hash >> 32) & (num_bytes_ / BYTES_PER_FILTER_BLOCK - 1);
  uint32_t key = static_cast<uint32_t>(hash);
  uint32_t* bitset32 = reinterpret_cast<uint32_t*>(data_->mutable_data());

  // Calculate mask for bucket.
  BlockMask block_mask;
  SetMask(key, block_mask);

  for (int i = 0; i < BITS_SET_PER_BLOCK; i++) {
    bitset32[bucket_index * BITS_SET_PER_BLOCK + i] |= block_mask.item[i];
  }
}

}  // namespace parquet
