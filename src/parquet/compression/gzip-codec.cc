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

#include "parquet/compression/codec.h"

#include <zlib.h>

namespace parquet_cpp {

void GZipCodec::Decompress(int input_len, const uint8_t* input,
    int output_len, uint8_t* output_buffer) {
  z_stream stream;
  stream.zalloc = reinterpret_cast<alloc_func>(0);
  stream.zfree = reinterpret_cast<free_func>(0);
  stream.next_in = reinterpret_cast<Bytef*>(const_cast<uint8_t*>(input));
  stream.avail_in = input_len;
  stream.next_out = reinterpret_cast<Bytef*>(output_buffer);
  stream.avail_out = output_len;
  int rc = inflateInit2(&stream, 16+MAX_WBITS);
  if (rc != Z_OK) {
    throw ParquetException("zlib internal error.");
  }
  rc = inflate(&stream, Z_FINISH);
  if (rc == Z_STREAM_END) {
    rc = inflateEnd(&stream);
  }
  if (rc != Z_OK) {
    throw ParquetException("Corrupt gzip compressed data.");
  }
}

int GZipCodec::MaxCompressedLen(int input_len, const uint8_t* input) {
  return compressBound(input_len);
}

int GZipCodec::Compress(int input_len, const uint8_t* input,
    int output_buffer_len, uint8_t* output_buffer) {
  uLongf dstLen = output_buffer_len;
  int rc = compress2(reinterpret_cast<Bytef*>(output_buffer), &dstLen,
      reinterpret_cast<const Bytef*>(input), input_len, 1);
  return rc == Z_OK ? dstLen : input_len;
}

} // namespace parquet_cpp
