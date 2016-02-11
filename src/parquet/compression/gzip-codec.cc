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

#include <cstring>
#include <sstream>

namespace parquet_cpp {

// These are magic numbers from zlib.h.  Not clear why they are not defined
// there.
static constexpr int WINDOW_BITS = 15;    // Maximum window size
static constexpr int GZIP_CODEC = 16;     // Output Gzip.

GZipCodec::GZipCodec(Format format) {
  memset(&stream_, 0, sizeof(stream_));

  int ret;
  // Initialize to run specified format
  int window_bits = WINDOW_BITS;
  if (format == DEFLATE) {
    window_bits = -window_bits;
  } else if (format == GZIP) {
    window_bits += GZIP_CODEC;
  }
  if ((ret = deflateInit2(&stream_, Z_DEFAULT_COMPRESSION, Z_DEFLATED,
              window_bits, 9, Z_DEFAULT_STRATEGY)) != Z_OK) {
    throw ParquetException("zlib deflateInit failed: " +
        std::string(stream_.msg));
  }
}

void GZipCodec::Decompress(int64_t input_len, const uint8_t* input,
    int64_t output_len, uint8_t* output) {
  stream_.zalloc = reinterpret_cast<alloc_func>(0);
  stream_.zfree = reinterpret_cast<free_func>(0);
  stream_.next_in = reinterpret_cast<Bytef*>(const_cast<uint8_t*>(input));
  stream_.avail_in = input_len;
  stream_.next_out = reinterpret_cast<Bytef*>(output);
  stream_.avail_out = output_len;
  int rc = inflateInit2(&stream_, 16+MAX_WBITS);
  if (rc != Z_OK) {
    throw ParquetException("zlib internal error.");
  }
  rc = inflate(&stream_, Z_FINISH);
  if (rc == Z_STREAM_END) {
    rc = inflateEnd(&stream_);
  }
  if (rc != Z_OK) {
    throw ParquetException("Corrupt gzip compressed data.");
  }
}

int64_t GZipCodec::MaxCompressedLen(int64_t input_len, const uint8_t* input) {
  // TODO(wesm): deal with zlib < 1.2.3 (see Impala codebase)
  return deflateBound(&stream_, static_cast<uLong>(input_len));
}

int64_t GZipCodec::Compress(int64_t input_length, const uint8_t* input,
    int64_t output_length, uint8_t* output) {
  stream_.next_in = const_cast<Bytef*>(reinterpret_cast<const Bytef*>(input));
  stream_.avail_in = input_length;
  stream_.next_out = reinterpret_cast<Bytef*>(output);
  stream_.avail_out = output_length;

  int64_t ret = 0;
  if ((ret = deflate(&stream_, Z_FINISH)) != Z_STREAM_END) {
    if (ret == Z_OK) {
      // will return Z_OK (and stream.msg NOT set) if stream.avail_out is too
      // small
      throw ParquetException("zlib deflate failed, output buffer to small");
    }
    std::stringstream ss;
    ss << "zlib deflate failed: " << stream_.msg;
    throw ParquetException(ss.str());
  }

  if (deflateReset(&stream_) != Z_OK) {
    throw ParquetException("zlib deflateReset failed: " +
        std::string(stream_.msg));
  }

  return output_length - stream_.avail_out;
}

} // namespace parquet_cpp
