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

#ifndef PARQUET_BLOOM_H
#define PARQUET_BLOOM_H

#include <set>
#include <cstdint>

#include "parquet/types.h"
#include "parquet/util/memory.h"

namespace parquet {
class OutputStream;

// Bloom Filter is a compact structure to indicate whether an item is not in set or
// probably in set. Bloom class is underlying class of Bloom Filter which stores a
// bit set represents elements set, hash strategy and bloom filter algorithm.

// Bloom Filter algorithm is implemented using block Bloom filters from Putze et al.'s
// "Cache-,Hash- and Space-Efficient Bloom Filters". The basic idea is to hash the
// item to a tiny Bloom Filter which size fit a single cache line or smaller. This
// implementation sets 8 bits in each tiny Bloom Filter. Tiny bloom filter are 32
// bytes to take advantage of 32-bytes SIMD instruction.

class Bloom final {
 public:
  // Hash strategy available for bloom filter.
  enum HashStrategy {
    MURMUR3_X64_128
  };

  // Bloom filter algorithm.
  enum Algorithm {
    BLOCK
  };

  // Bytes in a tiny bloom filter block.
  static constexpr int BYTES_PER_FILTER_BLOCK = 32;

  // Default seed for hash function, to keep cross compatibility the seed is same as
  // value defined in parquet-mr.
  static constexpr int DEFAULT_SEED = 104729;

  // Default maximum bloom filter size (need to discuss)
  static constexpr int DEFAULT_MAXIMUM_BLOOM_FILTER_BYTES = 16 * 1024 * 1024;

  // The block-based algorithm needs eight odd SALT values to calculate eight indexes
  // of bit to set, one bit in each 32-bit word.
  static constexpr uint32_t SALT[8] = { 0x47b6137bU, 0x44974d91U, 0x8824ad5bU,
      0xa2b7289dU, 0x705495c7U, 0x2df1424bU, 0x9efc4947U, 0x5c6bfb31U };

  typedef void (*HashFunc)(const void *key, int length, uint32_t seed, void* out);

 public:
  /// Constructor of bloom filter. The range of num_bytes should be within
  /// [BYTES_PER_FILTER_BLOCK, DEFAULT_MAXIMUM_BLOOM_FILTER_BYTES], it will be
  /// rounded up/down to lower/upper bound if num_bytes is out of range. It will use
  /// murmur3_x64_128 as its default hash function and the block based algorithm.
  /// @param num_bytes The number of bytes to store bloom filter bitset.
  explicit Bloom(uint32_t num_bytes);

  /// Construct the bloom filter with given bit set, it is used when reconstruct
  /// bloom filter from parquet file. It use murmur3_x64_128 as its default hash
  /// function and block-based algorithm as default algorithm.
  /// @param bitset The given bitset to construct bloom filter.
  /// @param len Length of bitset.
  Bloom(const uint8_t* bitset, uint32_t len);

  Bloom(const Bloom& orig) = delete;
  ~Bloom() = default;

  // Calculate optimal size according to the number of distinct values and false
  // positive probability.
  // @param ndv: The number of distinct values.
  // @param fpp: The false positive probability.
  // @return optimal number of bits of given n and p.
  static uint32_t optimalNumOfBits(uint32_t ndv, double fpp);

  // Determine whether an element exist in set or not.
  // @param hash the element to contain.
  // @return false if value is definitely not in set, and true means PROBABLY in set.
  bool find(uint64_t hash);

  // Insert element to set represented by bloom bitset.
  // @param hash the hash of value to insert into bloom filter..
  void insert(uint64_t hash);

  // Compute hash for int value by using its plain encoding result.
  // @param value the value to hash.
  // @return hash result.
  uint64_t hash(int value);

  // Compute hash for long value by using its plain encoding result.
  // @param value the value to hash.
  // @return hash result.
  uint64_t hash(int64_t value);

  // Compute hash for float value by using its plain encoding result.
  // @param value the value to hash.
  // @return hash result.
  uint64_t hash(float value);

  // Compute hash for double value by using its plain encoding result.
  // @param value the value to hash.
  // @return hash result.
  uint64_t hash(double value);

  // Compute hash for Int96 value by using its plain encoding result.
  // @param value the value to hash.
  // @return hash result.
  uint64_t hash(const Int96 *value);

  // Compute hash for ByteArray value by using its plain encoding result.
  // @param value the value to hash.
  // @return hash result.
  uint64_t hash(const ByteArray *value);

  // Compute hash for Fixed Length Byte Array value by using its plain encoding result.
  // @param value the value to hash.
  // @return hash result.
  uint64_t hash(const FLBA *value, uint32_t len);

  // Write bloom filter to output stream. A bloom filter structure should include
  // bitset length, hash strategy, algorithm, and bitset.
  // @param sink output stream to write
  void writeTo(const std::shared_ptr<OutputStream>& sink);

 private:
  // Create a new bitset for bloom filter, the size will be at least 32 bytes.
  // @param num_bytes number of bytes for bitset. The range of num_bytes should be within
  // [BYTES_PER_FILTER_BLOCK, DEFAULT_MAXIMUM_BLOOM_FILTER_BYTES], it will be
  // rounded up/down to lower/upper bound if num_bytes is out of range.
  void initBitset(uint32_t num_bytes);

  // Set bits in mask array according to input key.
  // @param key the value to calculate mask values.
  // @mask mask the mask array is used to set or clear bits inside a block
  void setMask(uint32_t key, uint32_t mask[8]);

  // Add an element into bloom filter bitset.
  // @param hash the hash value of element.
  void addElement(uint64_t hash);

  // Determine where an element is exist in bloom filter or not.
  // @param hash the hash value of element
  // @return return false if element is not in bloom filter, return true means the element
  // may be exist in bloom filter.
  bool contains(uint64_t hash);

  // The number of bytes of bloom filter bitset.
  uint32_t num_bytes;

  // Hash strategy used in this bloom filter.
  HashStrategy hash_strategy;

  // Algorithm applied of this bloom filter.
  Algorithm algorithm;

  // The underlying byte array for bloom filter bitset.
  std::unique_ptr<uint32_t[]> bitset;

  // Hash function applied.
  HashFunc hashFunc;
};
} // namespace parquet
#endif /* BLOOM_H */

