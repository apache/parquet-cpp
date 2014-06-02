#ifndef PARQUET_COMPRESSION_CODEC_H
#define PARQUET_COMPRESSION_CODEC_H

#include "parquet/parquet.h"

#include <boost/cstdint.hpp>
#include "gen-cpp/parquet_constants.h"
#include "gen-cpp/parquet_types.h"

namespace parquet_cpp {

class Decompressor {
 public:
  virtual ~Decompressor() {}
  virtual void Decompress(int input_len, const uint8_t* input,
      int output_len, uint8_t* output_buffer) = 0;
};

class Compressor {
 public:
  virtual ~Compressor() {}
  virtual int Compress(int input_len, const uint8_t* input,
      int output_buffer_len, uint8_t* output_buffer) = 0;

  virtual int MaxCompressedLen(int input_len, const uint8_t* input) = 0;
};

// Snappy codec.
class SnappyDecompressor : public Decompressor {
 public:
  virtual void Decompress(int input_len, const uint8_t* input,
      int output_len, uint8_t* output_buffer);
};

class SnappyCompressor : public Compressor {
 public:
  virtual int Compress(int input_len, const uint8_t* input,
      int output_buffer_len, uint8_t* output_buffer);

  virtual int MaxCompressedLen(int input_len, const uint8_t* input);
};

}

#endif

