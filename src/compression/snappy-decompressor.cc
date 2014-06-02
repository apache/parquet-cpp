#include "codec.h"

#include <snappy.h>

using namespace parquet_cpp;

void SnappyDecompressor::Decompress(int input_len, const uint8_t* input,
      int output_len, uint8_t* output_buffer) {
  if (!snappy::RawUncompress(reinterpret_cast<const char*>(input),
      static_cast<size_t>(input_len), reinterpret_cast<char*>(output_buffer))) {
    throw ParquetException("Corrupt snappy compressed data.");
  }
}

int SnappyCompressor::MaxCompressedLen(int input_len, const uint8_t* input) {
  return snappy::MaxCompressedLength(input_len);
}

int SnappyCompressor::Compress(int input_len, const uint8_t* input,
    int output_buffer_len, uint8_t* output_buffer) {
  size_t output_len;
  snappy::RawCompress(reinterpret_cast<const char*>(input),
      static_cast<size_t>(input_len), reinterpret_cast<char*>(output_buffer),
      &output_len);
  return output_len;
}
