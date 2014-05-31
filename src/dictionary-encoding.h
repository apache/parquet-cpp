#ifndef PARQUET_DICTIONARY_ENCODING_H
#define PARQUET_DICTIONARY_ENCODING_H

#include "encodings.h"

namespace parquet_cpp {

class DictionaryDecoder : public Decoder {
 public:
  DictionaryDecoder(const parquet::SchemaElement* schema, Decoder* dictionary)
    : Decoder(schema, parquet::Encoding::RLE_DICTIONARY) {
    int num_dictionary_values = dictionary->values_left();
    switch (schema->type) {
      case parquet::Type::BOOLEAN:
        throw ParquetException("Boolean cols should not be dictionary encoded.");

      case parquet::Type::INT32:
        int32_dictionary_.resize(num_dictionary_values);
        dictionary->GetInt32(&int32_dictionary_[0], num_dictionary_values);
        break;
      case parquet::Type::INT64:
        int64_dictionary_.resize(num_dictionary_values);
        dictionary->GetInt64(&int64_dictionary_[0], num_dictionary_values);
        break;
      case parquet::Type::FLOAT:
        float_dictionary_.resize(num_dictionary_values);
        dictionary->GetFloat(&float_dictionary_[0], num_dictionary_values);
        break;
      case parquet::Type::DOUBLE:
        double_dictionary_.resize(num_dictionary_values);
        dictionary->GetDouble(&double_dictionary_[0], num_dictionary_values);
        break;
      case parquet::Type::BYTE_ARRAY:
        byte_array_dictionary_.resize(num_dictionary_values);
        dictionary->GetByteArray(&byte_array_dictionary_[0], num_dictionary_values);
        break;
      default:
        ParquetException::NYI("Unsupported dictionary type");
    }
  }

  virtual void SetData(int num_values, const uint8_t* data, int len) {
    num_values_ = num_values;
    if (len == 0) return;
    uint8_t bit_width = *data;
    ++data;
    --len;
    idx_decoder_ = impala::RleDecoder(data, len, bit_width);
  }

  virtual int GetInt32(int32_t* buffer, int max_values) {
    max_values = std::min(max_values, num_values_);
    for (int i = 0; i < max_values; ++i) {
      buffer[i] = int32_dictionary_[index()];
    }
    return max_values;
  }

  virtual int GetInt64(int64_t* buffer, int max_values) {
    max_values = std::min(max_values, num_values_);
    for (int i = 0; i < max_values; ++i) {
      buffer[i] = int64_dictionary_[index()];
    }
    return max_values;
  }

  virtual int GetFloat(float* buffer, int max_values) {
    max_values = std::min(max_values, num_values_);
    for (int i = 0; i < max_values; ++i) {
      buffer[i] = float_dictionary_[index()];
    }
    return max_values;
  }

  virtual int GetDouble(double* buffer, int max_values) {
    max_values = std::min(max_values, num_values_);
    for (int i = 0; i < max_values; ++i) {
      buffer[i] = double_dictionary_[index()];
    }
    return max_values;
  }

  virtual int GetByteArray(ByteArray* buffer, int max_values) {
    max_values = std::min(max_values, num_values_);
    for (int i = 0; i < max_values; ++i) {
      buffer[i] = byte_array_dictionary_[index()];
    }
    return max_values;
  }

 private:
  int index() {
    int idx = 0;
    if (!idx_decoder_.Get(&idx)) ParquetException::EofException();
    --num_values_;
    return idx;
  }

  // Only one is set.
  std::vector<int32_t> int32_dictionary_;
  std::vector<int64_t> int64_dictionary_;
  std::vector<float> float_dictionary_;
  std::vector<double> double_dictionary_;
  std::vector<ByteArray> byte_array_dictionary_;

  impala::RleDecoder idx_decoder_;
};

}

#endif

