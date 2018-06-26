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

#include <cstdint>

#include "parquet/types.h"
#include "parquet/util/memory.h"

namespace parquet {
class OutputStream;

// A Bloom filter is a compact structure to indicate whether an item is not in set or
// probably in set. Bloom class is underlying class of Bloom filter which stores a
// bit set represents elements set, hash strategy and Bloom filter algorithm.

// Bloom filter algorithm is implemented using block Bloom filters from Putze et al.'s
// "Cache-,Hash- and Space-Efficient Bloom filters". The basic idea is to hash the
// item to a tiny Bloom filter which size fit a single cache line or smaller. This
// implementation sets 8 bits in each tiny Bloom filter. Tiny Bloom filter are 32
// bytes to take advantage of 32-bytes SIMD instruction.

class Bloom final {
 public:
  // Hash strategy available for Bloom filter.
  enum class HashStrategy { MURMUR3_X64_128 = 0 };

  // Bloom filter algorithm.
  enum class Algorithm { BLOCK = 0 };

  // Bytes in a tiny Bloom filter block.
  static constexpr int BYTES_PER_FILTER_BLOCK = 32;

  // Default seed for hash function which comes from Murmur3 implementation in Hive
  static constexpr int DEFAULT_SEED = 104729;

  // Default maximum Bloom filter size (need to discuss)
  static constexpr int DEFAULT_MAXIMUM_BLOOM_FILTER_BYTES = 16 * 1024 * 1024;

  // The block-based algorithm needs eight odd SALT values to calculate eight indexes
  // of bit to set, one bit in each 32-bit word.
  static constexpr uint32_t SALT[8] = {0x47b6137bU, 0x44974d91U, 0x8824ad5bU,
                                       0xa2b7289dU, 0x705495c7U, 0x2df1424bU,
                                       0x9efc4947U, 0x5c6bfb31U};

  typedef void (*HashFunc)(const void* key, int length, uint32_t seed, void* out);

 public:
  /// Constructor of Bloom filter. The range of num_bytes should be within
  /// [BYTES_PER_FILTER_BLOCK, DEFAULT_MAXIMUM_BLOOM_FILTER_BYTES], it will be
  /// rounded up/down to lower/upper bound if num_bytes is out of range and also
  /// will be rounded up to a power of 2. It will use murmur3_x64_128 as its default
  /// hash function and the block based algorithm.
  ///
  /// @param num_bytes The number of bytes to store Bloom filter bitset.
  explicit Bloom(uint32_t num_bytes);

  /// Construct the Bloom filter with given bit set, it is used when reconstruct
  /// Bloom filter from parquet file. It use murmur3_x64_128 as its default hash
  /// function and block-based algorithm as default algorithm.
  ///
  /// @param bitset The given bitset to construct Bloom filter.
  /// @param len Length of bitset.
  Bloom(const uint8_t* bitset, uint32_t len);

  Bloom(const Bloom& orig) = delete;
  ~Bloom() = default;

  /// Calculate optimal size according to the number of distinct values and false
  /// positive probability.
  ///
  /// @param ndv The number of distinct values.
  /// @param fpp The false positive probability.
  /// @return optimal number of bits of given n and p.
  static uint32_t OptimalNumOfBits(uint32_t ndv, double fpp);

  /// Determine whether an element exist in set or not.
  ///
  /// @param hash the element to contain.
  /// @return false if value is definitely not in set, and true means PROBABLY in set.
  bool FindHash(uint64_t hash);

  /// Insert element to set represented by bloom bitset.
  /// @param hash the hash of value to insert into Bloom filter.
  void InsertHash(uint64_t hash);

  /// Compute hash for int value by using its plain encoding result.
  ///
  /// @param value the value to hash.
  /// @return hash result.
  template <typename T>
  uint64_t Hash(T value) {
    uint64_t out[2];
    (*hash_function_)(reinterpret_cast<void*>(&value), sizeof(int), DEFAULT_SEED, &out);
    return out[0];
  }

  /// Compute hash for Fixed Length Byte Array value by using its plain encoding result.
  ///
  /// @param value the value to hash.
  /// @return hash result.
  uint64_t Hash(const FLBA* value, uint32_t len);

  /// Write Bloom filter to output stream. A Bloom filter structure should include
  /// bitset length, hash strategy, algorithm, and bitset.
  ///
  /// @param sink output stream to write
  void WriteTo(OutputStream* sink);

 private:
  /// Create a new bitset for Bloom filter, the size will be at least
  /// BYTES_PER_FILTER_BLOCK bytes.
  ///
  /// @param num_bytes number of bytes for bitset. The range of num_bytes should be within
  ///   [BYTES_PER_FILTER_BLOCK, DEFAULT_MAXIMUM_BLOOM_FILTER_BYTES], it will be
  ///   rounded up/down to lower/upper bound if num_bytes is out of range nd also
  ///   will be rounded up to a power of 2.
  void InitBitset(uint32_t num_bytes);

  /// Set bits in mask array according to input key.
  /// @param key the value to calculate mask values.
  /// @mask mask the mask array is used to set or clear bits inside a block
  void SetMask(uint32_t key, uint32_t mask[8]);

  // The number of bytes of Bloom filter bitset.
  uint32_t num_bytes_;

  // Hash strategy used in this Bloom filter.
  HashStrategy hash_strategy_;

  // Algorithm used in this Bloom filter.
  Algorithm algorithm_;

  // The underlying byte array for Bloom filter bitset.
  std::unique_ptr<uint32_t[]> bitset_;

  // Hash function applied.
  HashFunc hash_function_;
};

template <>
uint64_t Bloom::Hash<const Int96*>(const Int96* value);

template <>
uint64_t Bloom::Hash<const ByteArray*>(const ByteArray* value);

}  // namespace parquet
#endif  // PARQUET_BLOOM_H
