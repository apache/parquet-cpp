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

//-----------------------------------------------------------------------------
// MurmurHash3 was written by Austin Appleby, and is placed in the public
// domain. The author hereby disclaims copyright to this source code.

#ifndef _MURMURHASH3_H_
#define _MURMURHASH3_H_

#include <cstdint>

#include "parquet/hasher.h"
#include "parquet/types.h"

namespace parquet {

/// Source:
/// https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp
/// (Modified to adapt to coding conventions and to inherit the Hasher abstract class)
class MurmurHash3 : public Hasher {
 public:
  MurmurHash3() : seed_(DEFAULT_SEED) {}

  /// Original murmur3 hash implementation
  void Hash_x86_32(const void* key, int len, uint32_t seed, void* out);
  void Hash_x86_128(const void* key, int len, uint32_t seed, void* out);
  void Hash_x64_128(const void* key, int len, uint32_t seed, void* out);

  uint64_t Hash(int32_t value);
  uint64_t Hash(int64_t value);
  uint64_t Hash(float value);
  uint64_t Hash(double value);
  uint64_t Hash(const Int96* value);
  uint64_t Hash(const ByteArray* value);
  uint64_t Hash(const FLBA* val, uint32_t len);

  // Default seed for hash which comes from Murmur3 implementation in Hive
  static constexpr int DEFAULT_SEED = 104729;

  MurmurHash3(const MurmurHash3&) = delete;
  ~MurmurHash3() = default;

 private:
  uint32_t seed_;
};

}  // namespace parquet

#endif  // _MURMURHASH3_H_
