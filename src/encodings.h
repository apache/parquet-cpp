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

  virtual void SetData(int num_values, const uint8_t* data, int len) = 0;

  // Subclasses should override the ones they support
  virtual bool GetBool() { return false; }
  virtual int32_t GetInt32() { return 0; }
  virtual int64_t GetInt64() { return 0; }
  virtual float GetFloat() { return 0; }
  virtual String GetString() { return String(); }

  int value_left() const { return num_values_; }

 protected:
  Decoder(const parquet::SchemaElement* schema) : schema_(schema), num_values_(0) {}
  const parquet::SchemaElement* schema_;
  int num_values_;
};

class BoolDecoder : public Decoder {
 public:
  BoolDecoder(const parquet::SchemaElement* schema) : Decoder(schema) { }

  virtual void SetData(int num_values, const uint8_t* data, int len) {
    num_values_ = num_values;
    decoder_ = impala::RleDecoder(data, len, 1);
  }

  virtual bool GetBool() {
    bool result;
    if (!decoder_.Get(&result)) throw "EOF";
    --num_values_;
    return result;
  }

 private:
  impala::RleDecoder decoder_;
};

class PlainDecoder : public Decoder {
 public:
  PlainDecoder(const parquet::SchemaElement* schema)
    : Decoder(schema), data_(NULL), len_(0) {
  }

  virtual void SetData(int num_values, const uint8_t* data, int len) {
    num_values_ = num_values;
    data_ = data;
    len_ = len;
  }

  virtual int32_t GetInt32() {
    if (len_ < sizeof(int32_t)) throw "EOF";
    int32_t val = *reinterpret_cast<const int32_t*>(data_);
    data_ += sizeof(int32_t);
    len_ -= sizeof(int32_t);
    --num_values_;
    return val;
  }

  virtual int64_t GetInt64() {
    if (len_ < sizeof(int64_t)) throw "EOF";
    int64_t val = *reinterpret_cast<const int64_t*>(data_);
    data_ += sizeof(int64_t);
    len_ -= sizeof(int64_t);
    --num_values_;
    return val;
  }

  virtual float GetFloat() {
    if (len_ < sizeof(float)) throw "EOF";
    float val = *reinterpret_cast<const float*>(data_);
    data_ += sizeof(float);
    len_ -= sizeof(float);
    --num_values_;
    return val;
  }

  virtual String GetString() {
    String result;
    if (len_ < sizeof(uint32_t)) throw "EOF";
    result.len = *reinterpret_cast<const uint32_t*>(data_);
    data_ += sizeof(uint32_t);
    len_ -= sizeof(uint32_t);
    if (len_ < result.len) throw "EOF";
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
    : Decoder(schema) {
    int num_dictionary_values = dictionary->value_left();
    switch (schema->type) {
      case parquet::Type::BOOLEAN: throw "Boolean cols should not be dictionary encoded.";
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
        throw "NYI";
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
    if (!idx_decoder_.Get(&idx)) throw "EOF";
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
