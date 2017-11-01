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
#include "parquet/bloom.h"

#include <algorithm>
#include <cstdint>

#include "arrow/util/bit-util.h"

#include "parquet/exception.h"
#include "parquet/murmur3.h"
#include "parquet/util/logging.h"

namespace parquet {
constexpr uint32_t Bloom::SALT[8];

Bloom::Bloom(uint32_t num_bytes)
    : num_bytes(num_bytes),
      hash_strategy(MURMUR3_X64_128),
      algorithm(BLOCK),
      hashFunc(NULL) {
  initBitset(num_bytes);

  switch (hash_strategy) {
  case MURMUR3_X64_128:
    this->hashFunc = &MurmurHash3_x64_128;
    break;
  default:
    throw parquet::ParquetException("Unknown hash strategy.");
  }
}

void Bloom::initBitset(uint32_t num_bytes) {
  if (num_bytes < BYTES_PER_FILTER_BLOCK) {
    num_bytes = BYTES_PER_FILTER_BLOCK;
  }

  if (num_bytes > DEFAULT_MAXIMUM_BLOOM_FILTER_BYTES) {
    num_bytes = DEFAULT_MAXIMUM_BLOOM_FILTER_BYTES;
  }

  // Get next power of 2 if it is not power of 2.
  if ((num_bytes & (num_bytes - 1)) != 0) {
    num_bytes = static_cast<uint32_t>(::arrow::BitUtil::NextPower2(num_bytes));
  }

  this->bitset = new uint8_t[num_bytes];
}

Bloom::Bloom(const uint8_t* bitset, uint32_t num_bytes)
    : num_bytes(num_bytes),
      hash_strategy(MURMUR3_X64_128),
      algorithm(BLOCK){
  this->bitset = new uint8_t[num_bytes];
  memcpy(this->bitset, bitset, num_bytes);
  switch (hash_strategy) {
  case MURMUR3_X64_128:
    this->hashFunc = &MurmurHash3_x64_128;
    break;
  default:
    throw new parquet::ParquetException("Not supported hash strategy");
  }
}

void Bloom::setMask(uint32_t key, uint32_t mask[8]) {
  for (int i = 0; i < 8; ++i) {
    mask[i] = key * SALT[i];
  }
  
  for (int i = 0; i < 8; ++i) {
    mask[i] = mask[i] >> 27;
  }
  
  for (int i = 0; i < 8; ++i) {
    mask[i] = 0x1U << mask[i];
  }
}

uint32_t optimalNumOfBits(uint32_t ndv, double fpp) {
  DCHECK(fpp > 0.0 && fpp < 1.0);
  const double M = -8 * ndv / log(1 - pow(fpp, 1.0 / 8));
  const double MAX = Bloom::DEFAULT_MAXIMUM_BLOOM_FILTER_BYTES << 3;

  int num_bits = static_cast<uint32_t>(M);

  // Handle overflow.
  if (M > MAX || M < 0) {
    num_bits = static_cast<uint32_t>(MAX);
  }

  // Get next power of 2 if bits is not power of 2.
  if ((num_bits & (num_bits - 1)) != 0) {
    num_bits = static_cast<uint32_t>(::arrow::BitUtil::NextPower2(num_bits));
  }

  // Minimum
  if (num_bits < (Bloom::BYTES_PER_FILTER_BLOCK << 3)) {
    num_bits = Bloom::BYTES_PER_FILTER_BLOCK << 3;
  }

  return num_bits;
}

void Bloom::addElement(uint64_t hash) {
  uint32_t* const bitset32 = reinterpret_cast<uint32_t*>(bitset);
  const uint32_t bucketIndex = static_cast<uint32_t>(hash >> 32)
      & (num_bytes / BYTES_PER_FILTER_BLOCK - 1);
  uint32_t key = static_cast<uint32_t>(hash);

  // Calculate mask for bucket.
  uint32_t mask[8];
  setMask(key, mask);

  for (int i = 0; i < 8; i++) {
    bitset32[bucketIndex * 8 + i] |= mask[i];
  }
}

bool Bloom::contains(uint64_t hash) {
  uint32_t * const bitset32 = reinterpret_cast<uint32_t * const>(bitset);
  const uint32_t bucketIndex = static_cast<uint32_t>((hash >> 32)
      & (num_bytes / BYTES_PER_FILTER_BLOCK - 1));
  uint32_t key = static_cast<uint32_t>(hash);

  // Calculate mask for bucket.
  uint32_t mask[8];
  setMask(key, mask);

  for (int i = 0; i < 8; ++i) {
    if (0 == (bitset32[8 * bucketIndex + i] & mask[i])) {
      return false;
    }
  }
  return true;
}

bool Bloom::find(uint64_t hash) {
  return contains(hash);
}

void Bloom::insert(unsigned long long hash) {
  addElement(hash);
}

uint64_t Bloom::hash(int value) {
  uint64_t out[2];
  (*hashFunc)((void*)&value, sizeof(int), DEFAULT_SEED, &out);
  return out[0];
}

uint64_t Bloom::hash(const long value) {
  uint64_t out[2];
  (*hashFunc)((void*)&value, sizeof(long), DEFAULT_SEED, &out);
  return out[0];
}

uint64_t Bloom::hash(const float value) {
  uint64_t out[2];
  (*hashFunc)((void*)&value, sizeof(float), DEFAULT_SEED, &out);
  return out[0];
}


uint64_t Bloom::hash(const double value) {
  uint64_t out[2];
  (*hashFunc)((void*)&value, sizeof(double), DEFAULT_SEED, &out);
  return out[0];
}


uint64_t Bloom::hash(const Int96 &value) {
  uint64_t out[2];
  (*hashFunc)((void*)value.value, sizeof(value.value), DEFAULT_SEED, &out);
  return out[0];
}

uint64_t Bloom::hash(const ByteArray &value) {
  uint64_t out[2];
  (*hashFunc)((void*)value.ptr, value.len, DEFAULT_SEED, &out);
  return out[0];
}

uint64_t Bloom::hash(const FLBA &value, uint32_t len) {
  uint64_t out[2];
  (*hashFunc)((void*)value.ptr, len, DEFAULT_SEED, &out);
  return out[0];
}

void Bloom::writeTo(const std::shared_ptr<OutputStream>& sink){
  sink->Write(reinterpret_cast<const uint8_t *>(&num_bytes), sizeof(uint32_t));
  sink->Write(reinterpret_cast<const uint8_t *>(&hash_strategy), sizeof(uint32_t));
  sink->Write(reinterpret_cast<const uint8_t *>(&algorithm), sizeof(uint32_t));
  sink->Write(bitset, num_bytes);
}

Bloom::~Bloom() {
  if (bitset) {
    free(bitset);
  }
}
} // namespace parquet

