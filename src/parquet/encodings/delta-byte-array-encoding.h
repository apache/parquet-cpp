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

#ifndef PARQUET_DELTA_BYTE_ARRAY_ENCODING_H
#define PARQUET_DELTA_BYTE_ARRAY_ENCODING_H

#include <algorithm>

#include "parquet/encodings/decoder.h"
#include "parquet/encodings/delta-length-byte-array-encoding.h"
#include "parquet/encodings/delta-bit-pack-encoding.h"

namespace parquet {

class DeltaByteArrayDecoder : public Decoder<Type::BYTE_ARRAY> {
 public:
  explicit DeltaByteArrayDecoder(const ColumnDescriptor* descr,
      MemoryAllocator* allocator = default_allocator())
      : Decoder<Type::BYTE_ARRAY>(descr, Encoding::DELTA_BYTE_ARRAY),
      prefix_len_decoder_(nullptr, allocator),
      suffix_decoder_(nullptr, allocator) {
  }

  virtual void SetData(int num_values, const uint8_t* data, int len) {
    num_values_ = num_values;
    if (len == 0) return;
    int prefix_len_length = *reinterpret_cast<const int*>(data);
    data += 4;
    len -= 4;
    prefix_len_decoder_.SetData(num_values, data, prefix_len_length);
    data += prefix_len_length;
    len -= prefix_len_length;
    suffix_decoder_.SetData(num_values, data, len);
  }

  // TODO: this doesn't work and requires memory management. We need to allocate
  // new strings to store the results.
  virtual int Decode(ByteArray* buffer, int max_values) {
    max_values = std::min(max_values, num_values_);
    for (int  i = 0; i < max_values; ++i) {
      int prefix_len = 0;
      prefix_len_decoder_.Decode(&prefix_len, 1);
      ByteArray suffix;
      suffix_decoder_.Decode(&suffix, 1);
      buffer[i].len = prefix_len + suffix.len;

      uint8_t* result = reinterpret_cast<uint8_t*>(malloc(buffer[i].len));
      memcpy(result, last_value_.ptr, prefix_len);
      memcpy(result + prefix_len, suffix.ptr, suffix.len);

      buffer[i].ptr = result;
      last_value_ = buffer[i];
    }
    num_values_ -= max_values;
    return max_values;
  }

 private:
  using Decoder<Type::BYTE_ARRAY>::num_values_;

  DeltaBitPackDecoder<Type::INT32> prefix_len_decoder_;
  DeltaLengthByteArrayDecoder suffix_decoder_;
  ByteArray last_value_;
};

} // namespace parquet

#endif
