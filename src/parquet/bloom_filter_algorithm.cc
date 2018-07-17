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

#include "parquet/bloom_filter_algorithm.h"
#include "parquet/types.h"

namespace parquet {
constexpr uint32_t BlockBasedAlgorithm::SALT[BITS_SET_PER_BLOCK];

void BlockBasedAlgorithm::SetMask(uint32_t key, BlockMask& block_mask) const {
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

bool BlockBasedAlgorithm::TestBits(const uint32_t* bitset, uint32_t num_bytes,
                                   uint64_t hash) const {
  const uint32_t bucket_index =
      static_cast<uint32_t>((hash >> 32) & (num_bytes / BYTES_PER_FILTER_BLOCK - 1));
  uint32_t key = static_cast<uint32_t>(hash);

  // Calculate mask for bucket.
  BlockMask block_mask;
  SetMask(key, block_mask);

  for (int i = 0; i < BITS_SET_PER_BLOCK; ++i) {
    if (0 == (bitset[BITS_SET_PER_BLOCK * bucket_index + i] & block_mask.item[i])) {
      return false;
    }
  }
  return true;
}

void BlockBasedAlgorithm::SetBits(uint32_t* bitset, uint32_t num_bytes,
                                  uint64_t hash) const {
  const uint32_t bucket_index =
      static_cast<uint32_t>(hash >> 32) & (num_bytes / BYTES_PER_FILTER_BLOCK - 1);
  uint32_t key = static_cast<uint32_t>(hash);

  // Calculate mask for bucket.
  BlockMask block_mask;
  SetMask(key, block_mask);

  for (int i = 0; i < BITS_SET_PER_BLOCK; i++) {
    bitset[bucket_index * BITS_SET_PER_BLOCK + i] |= block_mask.item[i];
  }
}

}  // namespace parquet
