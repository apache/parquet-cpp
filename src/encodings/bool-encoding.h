// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef PARQUET_BOOL_ENCODING_H
#define PARQUET_BOOL_ENCODING_H

#include "encodings.h"

namespace parquet_cpp {

class BoolDecoder : public Decoder {
 public:
  BoolDecoder() : Decoder(parquet::Type::BOOLEAN, parquet::Encoding::PLAIN) { }

  virtual void SetData(int num_values, const uint8_t* data, int len) {
    if (len * 8 < num_values) {
      std::cerr << "len " << len << std::endl;
      std::cerr << "num_values " << num_values << std::endl;
      throw ParquetException("not enough data to decode bool values");
    }
    num_values_ = num_values;
    buffered_bits_ = 0;
    data_ = data;
  }

  virtual int GetBool(bool* buffer, int max_values) {
    max_values = std::min(max_values, num_values_);
    for (int i = 0; i < max_values; ++i) {
      if (buffered_bits_ == 0) {
        values_ = *data_++;
        buffered_bits_ = 8;
      }
      buffer[i] = values_ & 1;
      values_ >>= 1;
      --buffered_bits_;
    }
    num_values_ -= max_values;
    return max_values;
  }
 private:
  int buffered_bits_;
  uint8_t values_;
  const uint8_t* data_;
};

}

#endif

