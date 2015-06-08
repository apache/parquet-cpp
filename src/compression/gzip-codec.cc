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

#include "codec.h"

#include <zlib.h>

using namespace parquet_cpp;

void GZipCodec::Decompress(int input_len, const uint8_t* input,
      int output_len, uint8_t* output_buffer) 
{
    z_stream stream;
    stream.zalloc = (alloc_func)0;
    stream.zfree = (free_func)0;
    stream.next_in = (z_const Bytef *)input;
    stream.avail_in = (uInt)input_len;
    stream.next_out = (Bytef*)output_buffer;
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
    int rc = compress2((Bytef*)output_buffer, &dstLen, (Bytef*)input, input_len, 1);
    return rc == Z_OK ? dstLen : input_len;
}
