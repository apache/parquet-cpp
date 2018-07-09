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
    : pool_(::arrow::default_memory_pool()),
	  bitset_(NULL),
	  is_internal_bitset_(true),
      num_bytes_(num_bytes),
      hash_strategy_(HashStrategy::MURMUR3_X64_128),
      algorithm_(Algorithm::BLOCK) {
  switch (hash_strategy_) {
    case HashStrategy::MURMUR3_X64_128:
      this->hasher_.reset(new MurmurHash3());
      break;
    default:
      throw parquet::ParquetException("Unsupported hash strategy.");
  }

  switch (algorithm_) {
  case Algorithm::BLOCK:
	  this->bloom_algorithm_.reset(new BlockBasedAlgorithm());
	  break;
  default:
	  throw parquet::ParquetException("Unsupported Bloom filter algorithm");
  }

  InitBitset(num_bytes);
}

void BloomFilter::InitBitset(uint32_t num_bytes) {
  if (num_bytes < MINIMUM_BLOOM_FILTER_BYTES) {
    num_bytes = MINIMUM_BLOOM_FILTER_BYTES;
  }

  // Get next power of 2 if it is not power of 2.
  if ((num_bytes & (num_bytes - 1)) != 0) {
    num_bytes = static_cast<uint32_t>(::arrow::BitUtil::NextPower2(num_bytes));
  }

  if (num_bytes > MAXIMUM_BLOOM_FILTER_BYTES) {
	  num_bytes = MAXIMUM_BLOOM_FILTER_BYTES;
  }

  ::arrow::Status status = pool_->Allocate(num_bytes,
                                           reinterpret_cast<uint8_t**>(&bitset_));
  if (!status.ok()) {
    throw parquet::ParquetException("Failed to allocate buffer for bitset");
  }

  memset(bitset_, 0, num_bytes_);
}

BloomFilter::BloomFilter(const uint32_t* bitset, uint32_t num_bytes)
    : pool_(::arrow::default_memory_pool()),
	  bitset_(NULL),
	  is_internal_bitset_(false),
	  num_bytes_(num_bytes),
      hash_strategy_(HashStrategy::MURMUR3_X64_128),
      algorithm_(Algorithm::BLOCK) {
  switch (hash_strategy_) {
    case HashStrategy::MURMUR3_X64_128:
      this->hasher_.reset(new MurmurHash3());
      break;
    default:
      throw parquet::ParquetException("Unsupported hash strategy");
  }

  switch (algorithm_) {
  case Algorithm::BLOCK:
	  this->bloom_algorithm_.reset(new BlockBasedAlgorithm());
	  break;
  default:
	  throw parquet::ParquetException("Unsupported Bloom filter algorithm");
  }

  if (!bitset) {
	  throw parquet::ParquetException("bitset pointer is NULL");
  }

  if (num_bytes < MINIMUM_BLOOM_FILTER_BYTES ||
		  num_bytes > MAXIMUM_BLOOM_FILTER_BYTES ||
		  (num_bytes & (num_bytes -1)) != 0) {
    throw parquet::ParquetException("Given length of bitset is illegal");
  }

  if ((reinterpret_cast<std::uintptr_t>(bitset) & (X64_CACHE_ALIGNMENT - 1)) == 0) {
    this->bitset_ = const_cast<uint32_t*>(bitset);
  } else {
    InitBitset(num_bytes);
    memcpy(bitset_, bitset, num_bytes);
    is_internal_bitset_ = true;
  }
}

uint32_t BloomFilter::OptimalNumOfBits(uint32_t ndv, double fpp) {
  DCHECK(fpp > 0.0 && fpp < 1.0);
  const double m = -8.0 * ndv / log(1 - pow(fpp, 1.0 / 8));
  const double MAX = BloomFilter::MAXIMUM_BLOOM_FILTER_BYTES << 3;
  uint32_t num_bits = static_cast<uint32_t>(m);

  // Handle overflow.
  if (m > MAX || m < 0) {
    num_bits = static_cast<uint32_t>(MAX);
  }

  // Round up to lower bound
  if (num_bits < MINIMUM_BLOOM_FILTER_BYTES << 3) {
	  num_bits = MINIMUM_BLOOM_FILTER_BYTES << 3;
  }

  // Get next power of 2 if bits is not power of 2.
  if ((num_bits & (num_bits - 1)) != 0) {
    num_bits = static_cast<uint32_t>(::arrow::BitUtil::NextPower2(num_bits));
  }

  return num_bits;
}

void BloomFilter::InsertHash(uint64_t hash) {
  bloom_algorithm_->SetBits(bitset_, num_bytes_, hash);
}

bool BloomFilter::FindHash(uint64_t hash) const {
  return bloom_algorithm_->TestBits(bitset_, num_bytes_, hash);
}

void BloomFilter::WriteTo(OutputStream* sink) const {
  sink->Write(reinterpret_cast<const uint8_t*>(&num_bytes_), sizeof(num_bytes_));
  sink->Write(reinterpret_cast<const uint8_t*>(&hash_strategy_), sizeof(hash_strategy_));
  sink->Write(reinterpret_cast<const uint8_t*>(&algorithm_), sizeof(algorithm_));
  sink->Write(reinterpret_cast<const uint8_t*>(bitset_), num_bytes_);
}

}  // namespace parquet
