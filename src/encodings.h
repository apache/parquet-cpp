#ifndef PARQUET_ENCODINGS_H
#define PARQUET_ENCODINGS_H

#include <boost/cstdint.hpp>
#include "gen-cpp/parquet_constants.h"
#include "gen-cpp/parquet_types.h"

#include "impala/rle-encoding.h"

namespace parquet_cpp {

class Decoder {
 public:
  virtual ~Decoder() {}

  // Sets the data for a new page. This will be called multiple times on the same
  // decoder and should reset all internal state.
  virtual void SetData(int num_values, const uint8_t* data, int len) = 0;

  // Subclasses should override the ones they support
  virtual bool GetBool() {
    throw ParquetException("Decoder does not implement this type.");
  }
  virtual int32_t GetInt32() {
    throw ParquetException("Decoder does not implement this type.");
  }
  virtual int64_t GetInt64() {
    throw ParquetException("Decoder does not implement this type.");
  }
  virtual float GetFloat() {
    throw ParquetException("Decoder does not implement this type.");
  }
  virtual String GetString() {
    throw ParquetException("Decoder does not implement this type.");
  }

  // Returns the number of values left (for the last call to SetData()). This is
  // the number of values left in this page.
  int values_left() const { return num_values_; }

  const parquet::Encoding::type encoding() const { return encoding_; }

 protected:
  Decoder(const parquet::SchemaElement* schema, const parquet::Encoding::type& encoding)
    : schema_(schema), encoding_(encoding), num_values_(0) {}

  const parquet::SchemaElement* schema_;
  const parquet::Encoding::type encoding_;
  int num_values_;
};

class BoolDecoder : public Decoder {
 public:
  BoolDecoder(const parquet::SchemaElement* schema)
    : Decoder(schema, parquet::Encoding::PLAIN) { }

  virtual void SetData(int num_values, const uint8_t* data, int len) {
    num_values_ = num_values;
    decoder_ = impala::RleDecoder(data, len, 1);
  }

  virtual bool GetBool() {
    bool result;
    if (!decoder_.Get(&result)) ParquetException::EofException();
    --num_values_;
    return result;
  }

 private:
  impala::RleDecoder decoder_;
};

class PlainDecoder : public Decoder {
 public:
  PlainDecoder(const parquet::SchemaElement* schema)
    : Decoder(schema, parquet::Encoding::PLAIN), data_(NULL), len_(0) {
  }

  virtual void SetData(int num_values, const uint8_t* data, int len) {
    num_values_ = num_values;
    data_ = data;
    len_ = len;
  }

  virtual int32_t GetInt32() {
    if (len_ < sizeof(int32_t)) ParquetException::EofException();
    int32_t val = *reinterpret_cast<const int32_t*>(data_);
    data_ += sizeof(int32_t);
    len_ -= sizeof(int32_t);
    --num_values_;
    return val;
  }

  virtual int64_t GetInt64() {
    if (len_ < sizeof(int64_t)) ParquetException::EofException();
    int64_t val = *reinterpret_cast<const int64_t*>(data_);
    data_ += sizeof(int64_t);
    len_ -= sizeof(int64_t);
    --num_values_;
    return val;
  }

  virtual float GetFloat() {
    if (len_ < sizeof(float)) ParquetException::EofException();
    float val = *reinterpret_cast<const float*>(data_);
    data_ += sizeof(float);
    len_ -= sizeof(float);
    --num_values_;
    return val;
  }

  virtual String GetString() {
    String result;
    if (len_ < sizeof(uint32_t)) ParquetException::EofException();
    result.len = *reinterpret_cast<const uint32_t*>(data_);
    data_ += sizeof(uint32_t);
    len_ -= sizeof(uint32_t);
    if (len_ < result.len) ParquetException::EofException();
    result.ptr = data_;
    data_ += result.len;
    len_ -= result.len;
    --num_values_;
    return result;
  }

 private:
  const uint8_t* data_;
  int len_;
};

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
        for (int i = 0; i < num_dictionary_values; ++i) {
          int32_dictionary_[i] = dictionary->GetInt32();
        }
        break;
      case parquet::Type::INT64:
        int64_dictionary_.resize(num_dictionary_values);
        for (int i = 0; i < num_dictionary_values; ++i) {
          int64_dictionary_[i] = dictionary->GetInt64();
        }
        break;
      case parquet::Type::FLOAT:
        float_dictionary_.resize(num_dictionary_values);
        for (int i = 0; i < num_dictionary_values; ++i) {
          float_dictionary_[i] = dictionary->GetFloat();
        }
        break;
      case parquet::Type::BYTE_ARRAY:
        string_dictionary_.resize(num_dictionary_values);
        for (int i = 0; i < num_dictionary_values; ++i) {
          string_dictionary_[i] = dictionary->GetString();
        }
        break;
      default:
        ParquetException::NYI();
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

  virtual int32_t GetInt32() { return int32_dictionary_[index()]; }
  virtual int64_t GetInt64() { return int64_dictionary_[index()]; }
  virtual float GetFloat() { return float_dictionary_[index()]; }
  virtual String GetString() { return string_dictionary_[index()]; }

 private:
  int index() {
    int idx;
    if (!idx_decoder_.Get(&idx)) ParquetException::EofException();
    --num_values_;
    return idx;
  }

  // Only one is set.
  std::vector<int32_t> int32_dictionary_;
  std::vector<int64_t> int64_dictionary_;
  std::vector<float> float_dictionary_;
  std::vector<String> string_dictionary_;

  impala::RleDecoder idx_decoder_;
};

}

#endif
