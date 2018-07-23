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

#ifndef PARQUET_BLOOM_FILTER_H
#define PARQUET_BLOOM_FILTER_H

#include <cstdint>

#include "parquet/bloom_filter_algorithm.h"
#include "parquet/hasher.h"
#include "parquet/types.h"
#include "parquet/util/memory.h"

namespace parquet {
class OutputStream;

// A Bloom filter is a compact structure to indicate whether an item is not in a set or
// probably in a set. Bloom filter class is underlying class of Bloom filter which stores
// a bit set that represents a set of elements, a hash strategy and a Bloom filter
// algorithm.

// This Bloom filter uses Murmur3 hash strategy and block-based Bloom filter algorithm.
class BloomFilter final {
 public:
  /// Constructor of Bloom filter. The range of num_bytes should be within
  /// [MINIMUM_BLOOM_FILTER_BYTES, MAXIMUM_BLOOM_FILTER_BYTES], it will be
  /// rounded up/down to lower/upper bound if num_bytes is out of range and also
  /// will be rounded up to a power of 2. It will use murmur3_x64_128 as its default
  /// hash function and the block-based algorithm.
  ///
  /// @param num_bytes The number of bytes to store Bloom filter bitset.
  explicit BloomFilter(uint32_t num_bytes);

  /// Construct the Bloom filter with given bitset, it is used when reconstructing
  /// a Bloom filter from a parquet file. It uses murmur3_x64_128 as its default hash
  /// function and block-based algorithm as default algorithm.
  ///
  /// @param bitset The given bitset to construct Bloom filter.
  /// @param num_bytes  The number of bytes of given bitset.
  BloomFilter(const uint8_t* bitset, uint32_t num_bytes);

  /// Deserialize the Bloom filter from an input stream, it is used when reconstructing
  /// a Bloom filter from a parquet filter.
  ///
  /// @param input_stream The input stream from which to construct the Bloom filter
  /// @return it returns a Bloom filter pointer point to a new allocated Bloom filter.
  static BloomFilter* Deserialize(InputStream* input_stream);

  /// Get the number of bytes of bitset
  uint32_t GetBitsetSize() const { return bloom_filter_algorithm_->GetBitsetSize(); }

  // Hash strategy available for Bloom filter.
  enum class HashStrategy : uint32_t { MURMUR3_X64_128 = 0 };

  // Bloom filter algorithm.
  enum class Algorithm : uint32_t { BLOCK = 0 };

  // Maximum Bloom filter size, it sets to HDFS default block size 128MB
  // This value will be reconsidered when implementing Bloom filter producer.
  static constexpr uint32_t MAXIMUM_BLOOM_FILTER_BYTES = 128 * 1024 * 1024;

  // Minimum Bloom filter size, it sets to 32 bytes to fit a tiny Bloom filter
  // in block-based algorithm.
  static constexpr uint32_t MINIMUM_BLOOM_FILTER_BYTES = 32;

  BloomFilter(const BloomFilter& orig) = delete;

  /// Calculate optimal size according to the number of distinct values and false
  /// positive probability.
  ///
  /// @param ndv The number of distinct values.
  /// @param fpp The false positive probability.
  /// @return it always return a value between MINIMUM_BLOOM_FILTER_BYTES and
  /// MAXIMUM_BLOOM_FILTER_BYTES, and the return value is always a power of 2
  static uint32_t OptimalNumOfBits(uint32_t ndv, double fpp);

  /// Determine whether an element exist in set or not.
  ///
  /// @param hash the element to contain.
  /// @return false if value is definitely not in set, and true means PROBABLY in set.
  bool FindHash(uint64_t hash) const;

  /// Insert element to set represented by Bloom filter bitset.
  /// @param hash the hash of value to insert into Bloom filter.
  void InsertHash(uint64_t hash);

  /// Compute hash for value by using its plain encoding result.
  ///
  /// @param value the value to hash.
  /// @return hash result.
  template <typename T>
  uint64_t Hash(T value) const {
    return hasher_->Hash(value);
  }

  /// Compute hash for Fixed Length Byte Array value by using its plain encoding result.
  ///
  /// @param value the value to hash.
  /// @return hash result.
  uint64_t Hash(const FLBA* value, uint32_t len) const {
    return hasher_->Hash(value, len);
  }

  /// Write this Bloom filter to an output stream. A Bloom filter structure should
  /// include bitset length, hash strategy, algorithm, and bitset.
  ///
  /// @param sink the output stream to write
  void WriteTo(OutputStream* sink) const;

 private:
  // Hash strategy used in this Bloom filter.
  HashStrategy hash_strategy_;

  // Algorithm used in this Bloom filter.
  Algorithm algorithm_;

  // The hash pointer points to actual hash class used.
  std::unique_ptr<Hasher> hasher_;

  // The algorithm pointer points to actual algirithm used.
  std::unique_ptr<BloomFilterAlgorithm> bloom_filter_algorithm_;
};

}  // namespace parquet

#endif  // PARQUET_BLOOM_FILTER_H
