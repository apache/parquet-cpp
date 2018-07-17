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

#ifndef PARQUET_BLOOM_FILTER_ALGORITHM_H
#define PARQUET_BLOOM_FILTER_ALGORITHM_H

#include <cstdint>
#include "parquet/types.h"

namespace parquet {

// Abstract class for Bloom filter algorithm
class BloomFilterAlgorithm {
 public:
  /// Test bits in the bitset according to the hash value.
  ///
  /// @param bitset the bitset to test
  /// @param num_bytes the number of bytes of bitset
  /// @param hash the hash used to calculate bit index to test
  /// @return true
  virtual bool TestBits(const uint32_t* bitset, uint32_t num_bytes,
                        uint64_t hash) const = 0;

  /// Set bits in the bitset according to the hash value
  ///
  /// @param bitset the bitset to test
  /// @param num_bytes the number of bytes of bitset
  /// @param hash the hash used to calculate bit index to set
  virtual void SetBits(uint32_t* bitset, uint32_t num_bytes, uint64_t hash) const = 0;

  virtual ~BloomFilterAlgorithm() = default;
};

// This Bloom filter algorithm is implemented using block Bloom filters from
// Putze et al.'s "Cache-,Hash- and Space-Efficient Bloom filters". The basic idea is to
// hash the item to a tiny Bloom filter which size fit a single cache line or smaller.
// This implementation sets 8 bits in each tiny Bloom filter. Each tiny Bloom filter is
// 32 bytes to take advantage of 32-byte SIMD instructions.
class BlockBasedAlgorithm : public BloomFilterAlgorithm {
 public:
  // Bytes in a tiny Bloom filter block.
  static constexpr int BYTES_PER_FILTER_BLOCK = 32;

  // The number of bits to be set in each tiny Bloom filter
  static constexpr int BITS_SET_PER_BLOCK = 8;

  // A mask structure used to set bits in each tiny Bloom filter.
  struct BlockMask {
    uint32_t item[BITS_SET_PER_BLOCK];
  };

  // The block-based algorithm needs eight odd SALT values to calculate eight indexes
  // of bit to set, one bit in each 32-bit word.
  static constexpr uint32_t SALT[BITS_SET_PER_BLOCK] = {
      0x47b6137bU, 0x44974d91U, 0x8824ad5bU, 0xa2b7289dU,
      0x705495c7U, 0x2df1424bU, 0x9efc4947U, 0x5c6bfb31U};

  bool TestBits(const uint32_t* bitset, uint32_t num_bytes, uint64_t hash) const override;

  void SetBits(uint32_t* bitset, uint32_t num_bytes, uint64_t hash) const override;

 private:
  /// Set bits in mask array according to input key.
  /// @param key the value to calculate mask values.
  /// @param mask the mask array is used to set inside a block
  void SetMask(uint32_t key, BlockMask& mask) const;
};

}  // namespace parquet

#endif  // PARQUET_BLOOM_FILTER_ALGORITHM_H
