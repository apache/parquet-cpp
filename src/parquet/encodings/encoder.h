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

#ifndef PARQUET_ENCODINGS_ENCODER_H
#define PARQUET_ENCODINGS_ENCODER_H

#include <cstdint>
#include <memory>

#include "parquet/exception.h"
#include "parquet/types.h"
#include "parquet/util/bit-util.h"
#include "parquet/util/memory.h"

namespace parquet {

class ColumnDescriptor;

// Base class for value encoders. Since encoders may or not have state (e.g.,
// dictionary encoding) we use a class instance to maintain any state.
//
// TODO(wesm): Encode interface API is temporary
template <typename DType>
class Encoder {
 public:
  typedef typename DType::c_type T;

  virtual ~Encoder() {}

  virtual int64_t EstimatedDataEncodedSize() = 0;
  virtual std::shared_ptr<Buffer> FlushValues() = 0;
  virtual void Put(const T* src, int num_values) = 0;
  virtual void PutSpaced(const T* src, int num_values, const uint8_t* valid_bits,
      int64_t valid_bits_offset) {
    PoolBuffer buffer(allocator_);
    buffer.Resize(num_values * sizeof(T));
    int32_t num_valid_values = 0;
    INIT_BITSET(valid_bits, valid_bits_offset);
    T* data = reinterpret_cast<T*>(buffer.mutable_data());
    for (int32_t i = 0; i < num_values; i++) {
      if (bitset_valid_bits & (1 << bit_offset_valid_bits)) {
        data[num_valid_values++] = src[i];
      }
      READ_NEXT_BITSET(valid_bits);
    }
    Put(data, num_valid_values);
  }

  const Encoding::type encoding() const { return encoding_; }

 protected:
  explicit Encoder(const ColumnDescriptor* descr, const Encoding::type& encoding,
      MemoryAllocator* allocator)
      : descr_(descr), encoding_(encoding), allocator_(allocator) {}

  // For accessing type-specific metadata, like FIXED_LEN_BYTE_ARRAY
  const ColumnDescriptor* descr_;
  const Encoding::type encoding_;
  MemoryAllocator* allocator_;
};

}  // namespace parquet

#endif  // PARQUET_ENCODINGS_ENCODER_H
