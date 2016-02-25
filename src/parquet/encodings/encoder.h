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

#include "parquet/exception.h"
#include "parquet/types.h"

namespace parquet_cpp {

class ColumnDescriptor;
class OutputStream;

// Base class for value encoders. Since encoders may or not have state (e.g.,
// dictionary encoding) we use a class instance to maintain any state.
//
// TODO(wesm): Encode interface API is temporary
template <int TYPE>
class Encoder {
 public:
  typedef typename type_traits<TYPE>::value_type T;

  virtual ~Encoder() {}

  const Encoding::type encoding() const { return encoding_; }

 protected:
  explicit Encoder(const ColumnDescriptor* descr,
      const Encoding::type& encoding)
      : descr_(descr), encoding_(encoding) {}

  // For accessing type-specific metadata, like FIXED_LEN_BYTE_ARRAY
  const ColumnDescriptor* descr_;
  const Encoding::type encoding_;
};

} // namespace parquet_cpp

#endif // PARQUET_ENCODINGS_ENCODER_H
