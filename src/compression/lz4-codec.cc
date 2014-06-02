#include "codec.h"

#include <lz4.h>

using namespace parquet_cpp;

void Lz4Codec::Decompress(int input_len, const uint8_t* input,
      int output_len, uint8_t* output_buffer) {
  int n = LZ4_uncompress(reinterpret_cast<const char*>(input),
      reinterpret_cast<char*>(output_buffer), output_len);
  if (n != input_len) {
    throw ParquetException("Corrupt lz4 compressed data.");
  }
}

int Lz4Codec::MaxCompressedLen(int input_len, const uint8_t* input) {
  return LZ4_compressBound(input_len);
}

int Lz4Codec::Compress(int input_len, const uint8_t* input,
    int output_buffer_len, uint8_t* output_buffer) {
  return LZ4_compress(reinterpret_cast<const char*>(input),
      reinterpret_cast<char*>(output_buffer), input_len);
}
