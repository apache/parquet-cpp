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

#ifndef PARQUET_COMPRESSION_CODEC_H
#define PARQUET_COMPRESSION_CODEC_H

#include <cstdint>

#include <zlib.h>

#include "parquet/exception.h"

namespace parquet_cpp {

class Codec {
 public:
  virtual ~Codec() {}
  virtual void Decompress(int64_t input_len, const uint8_t* input,
      int64_t output_len, uint8_t* output_buffer) = 0;

  virtual int64_t Compress(int64_t input_len, const uint8_t* input,
      int64_t output_buffer_len, uint8_t* output_buffer) = 0;

  virtual int64_t MaxCompressedLen(int64_t input_len, const uint8_t* input) = 0;

  virtual const char* name() const = 0;
};


// Snappy codec.
class SnappyCodec : public Codec {
 public:
  virtual void Decompress(int64_t input_len, const uint8_t* input,
      int64_t output_len, uint8_t* output_buffer);

  virtual int64_t Compress(int64_t input_len, const uint8_t* input,
      int64_t output_buffer_len, uint8_t* output_buffer);

  virtual int64_t MaxCompressedLen(int64_t input_len, const uint8_t* input);

  virtual const char* name() const { return "snappy"; }
};

// Lz4 codec.
class Lz4Codec : public Codec {
 public:
  virtual void Decompress(int64_t input_len, const uint8_t* input,
      int64_t output_len, uint8_t* output_buffer);

  virtual int64_t Compress(int64_t input_len, const uint8_t* input,
      int64_t output_buffer_len, uint8_t* output_buffer);

  virtual int64_t MaxCompressedLen(int64_t input_len, const uint8_t* input);

  virtual const char* name() const { return "lz4"; }
};

// GZip codec.
class GZipCodec : public Codec {
 public:
  /// Compression formats supported by the zlib library
  enum Format {
    ZLIB,
    DEFLATE,
    GZIP,
  };

  explicit GZipCodec(Format format);

  virtual void Decompress(int64_t input_len, const uint8_t* input,
      int64_t output_len, uint8_t* output_buffer);

  virtual int64_t Compress(int64_t input_len, const uint8_t* input,
      int64_t output_buffer_len, uint8_t* output_buffer);

  virtual int64_t MaxCompressedLen(int64_t input_len, const uint8_t* input);

  virtual const char* name() const { return "gzip"; }

 private:
  z_stream stream_;
};

} // namespace parquet_cpp

#endif
