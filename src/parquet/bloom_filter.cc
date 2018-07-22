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
#include "parquet/bloom_filter_algorithm.h"
#include "parquet/exception.h"
#include "parquet/murmur3.h"
#include "parquet/util/logging.h"

namespace parquet {

BloomFilter::BloomFilter(uint32_t num_bytes)
    : hash_strategy_(HashStrategy::MURMUR3_X64_128), algorithm_(Algorithm::BLOCK) {
  switch (hash_strategy_) {
    case HashStrategy::MURMUR3_X64_128:
      this->hasher_.reset(new MurmurHash3());
      break;
    default:
      throw parquet::ParquetException("Unsupported hash strategy.");
  }

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

  switch (algorithm_) {
    case Algorithm::BLOCK:
      this->bloom_filter_algorithm_.reset(new BlockBasedAlgorithm(num_bytes));
      break;
    default:
      throw parquet::ParquetException("Unsupported Bloom filter algorithm");
  }
}

BloomFilter::BloomFilter(const uint8_t* bitset, uint32_t num_bytes)
    : hash_strategy_(HashStrategy::MURMUR3_X64_128), algorithm_(Algorithm::BLOCK) {
  switch (hash_strategy_) {
    case HashStrategy::MURMUR3_X64_128:
      this->hasher_.reset(new MurmurHash3());
      break;
    default:
      throw parquet::ParquetException("Unsupported hash strategy");
  }

  if (!bitset) {
    throw parquet::ParquetException("bitset pointer is NULL");
  }

  if (num_bytes < MINIMUM_BLOOM_FILTER_BYTES || num_bytes > MAXIMUM_BLOOM_FILTER_BYTES ||
      (num_bytes & (num_bytes - 1)) != 0) {
    throw parquet::ParquetException("Given length of bitset is illegal");
  }

  switch (algorithm_) {
    case Algorithm::BLOCK:
      this->bloom_filter_algorithm_.reset(new BlockBasedAlgorithm(bitset, num_bytes));
      break;
    default:
      throw parquet::ParquetException("Unsupported Bloom filter algorithm");
  }
}

BloomFilter* BloomFilter::Deserialize(InputStream* input) {
  int64_t bytes_available;

  uint32_t len;
  memcpy(&len, input->Read(sizeof(uint32_t), &bytes_available), sizeof(uint32_t));
  if (static_cast<uint32_t>(bytes_available) != sizeof(uint32_t)) {
    throw ParquetException("Failed to deserialize from input stream");
  }

  uint32_t hash;
  memcpy(&hash, input->Read(sizeof(uint32_t), &bytes_available), sizeof(uint32_t));
  if (static_cast<uint32_t>(bytes_available) != sizeof(uint32_t)) {
    throw ParquetException("Failed to deserialize from input stream");
  }
  if (static_cast<HashStrategy>(hash) != HashStrategy::MURMUR3_X64_128) {
    throw ParquetException("Unsupported hash strategy");
  }

  uint32_t algorithm;
  memcpy(&algorithm, input->Read(sizeof(uint32_t), &bytes_available), sizeof(uint32_t));
  if (static_cast<uint32_t>(bytes_available) != sizeof(uint32_t)) {
    throw ParquetException("Failed to deserialize from input stream");
  }
  if (static_cast<Algorithm>(algorithm) != BloomFilter::Algorithm::BLOCK) {
    throw ParquetException("Unsupported Bloom filter algorithm");
  }

  return new BloomFilter(input->Read(len, &bytes_available), len);
}

uint32_t BloomFilter::OptimalNumOfBits(uint32_t ndv, double fpp) {
  DCHECK(fpp > 0.0 && fpp < 1.0);
  const double m = -8.0 * ndv / log(1 - pow(fpp, 1.0 / 8));
  uint32_t num_bits = static_cast<uint32_t>(m);

  // Handle overflow.
  if (m < 0 || m > MAXIMUM_BLOOM_FILTER_BYTES << 3) {
    num_bits = static_cast<uint32_t>(MAXIMUM_BLOOM_FILTER_BYTES << 3);
  }

  // Round up to lower bound
  if (num_bits < MINIMUM_BLOOM_FILTER_BYTES << 3) {
    num_bits = MINIMUM_BLOOM_FILTER_BYTES << 3;
  }

  // Get next power of 2 if bits is not power of 2.
  if ((num_bits & (num_bits - 1)) != 0) {
    num_bits = static_cast<uint32_t>(::arrow::BitUtil::NextPower2(num_bits));
  }

  // Round down to upper bound
  if (num_bits > MAXIMUM_BLOOM_FILTER_BYTES << 3) {
    num_bits = MAXIMUM_BLOOM_FILTER_BYTES << 3;
  }

  return num_bits;
}

void BloomFilter::InsertHash(uint64_t hash) { bloom_filter_algorithm_->SetBits(hash); }

bool BloomFilter::FindHash(uint64_t hash) const {
  return bloom_filter_algorithm_->TestBits(hash);
}

void BloomFilter::WriteTo(OutputStream* sink) const {
  if (!sink) {
    throw ParquetException("Given output stream is NULL.");
  }

  const uint32_t num_bytes = bloom_filter_algorithm_->GetBitsetSize();
  sink->Write(reinterpret_cast<const uint8_t*>(&num_bytes), sizeof(num_bytes));
  sink->Write(reinterpret_cast<const uint8_t*>(&hash_strategy_), sizeof(hash_strategy_));
  sink->Write(reinterpret_cast<const uint8_t*>(&algorithm_), sizeof(algorithm_));
  bloom_filter_algorithm_->WriteTo(sink);
}

}  // namespace parquet
