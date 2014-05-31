#ifndef PARQUET_BOOL_ENCODING_H
#define PARQUET_BOOL_ENCODING_H

#include "encodings.h"

namespace parquet_cpp {

class BoolDecoder : public Decoder {
 public:
  BoolDecoder() : Decoder(parquet::Type::BOOLEAN, parquet::Encoding::PLAIN) { }

  virtual void SetData(int num_values, const uint8_t* data, int len) {
    num_values_ = num_values;
    decoder_ = impala::RleDecoder(data, len, 1);
  }

  virtual int GetBool(bool* buffer, int max_values) {
    max_values = std::min(max_values, num_values_);
    for (int i = 0; i < max_values; ++i) {
      if (!decoder_.Get(&buffer[i])) ParquetException::EofException();
    }
    num_values_ -= max_values;
    return max_values;
  }

 private:
  impala::RleDecoder decoder_;
};

}

#endif

