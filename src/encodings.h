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

  virtual void SetData(const uint8_t* data, int len) = 0;

  // Subclasses should override the ones they support
  virtual bool GetBool() { return false; }
  virtual int32_t GetInt32() { return 0; }
  virtual int64_t GetInt64() { return 0; }
  virtual float GetFloat() { return 0; }
  virtual String GetString() { return String(); }

 protected:
  Decoder(parquet::SchemaElement* schema) : schema_(schema) {}
  parquet::SchemaElement* schema_;
};

class BoolDecoder : public Decoder {
 public:
  BoolDecoder(parquet::SchemaElement* schema) : Decoder(schema) { }

  virtual void SetData(const uint8_t* data, int len) {
  }

 virtual bool GetBool() {
   return false;
 }
};

class PlainDecoder : public Decoder {
 public:
  PlainDecoder(parquet::SchemaElement* schema)
    : Decoder(schema), data_(NULL), len_(0) {
  }

  virtual void SetData(const uint8_t* data, int len) {
    data_ = data;
    len_ = len;
  }

  virtual int32_t GetInt32() {
    if (len_ < sizeof(int32_t)) throw "Bad";
    int32_t val = *reinterpret_cast<const int32_t*>(data_);
    data_ += sizeof(int32_t);
    len_ -= sizeof(int32_t);
    return val;
  }

  virtual int64_t GetInt64() {
    if (len_ < sizeof(int64_t)) throw "Bad";
    int64_t val = *reinterpret_cast<const int64_t*>(data_);
    data_ += sizeof(int64_t);
    len_ -= sizeof(int64_t);
    return val;
  }

  virtual float GetFloat() {
    if (len_ < sizeof(float)) throw "Bad";
    float val = *reinterpret_cast<const float*>(data_);
    data_ += sizeof(float);
    len_ -= sizeof(float);
    return val;
  }

  virtual String GetString() {
    String result;
    if (len_ < sizeof(uint32_t)) throw "Bad";
    result.len = *reinterpret_cast<const uint32_t*>(data_);
    data_ += sizeof(uint32_t);
    len_ -= sizeof(uint32_t);
    if (len_ < result.len) throw "Bad";
    result.ptr = data_;
    data_ += result.len;
    len_ -= result.len;
    return result;
  }

 private:
  const uint8_t* data_;
  int len_;
};

class DictionaryDecoder : public Decoder {
 public:
  DictionaryDecoder(parquet::SchemaElement* schema, Decoder* dictionary)
    : Decoder(schema) {
  }

  virtual void SetData(const uint8_t* data, int len) {
  }

  virtual int32_t GetInt32() {
    return 0;
  }
  virtual int64_t GetInt64() {
    return 0;
  }
  virtual float GetFloat() {
    return 0;
  }
  virtual String GetString() {
    return String();
  }
};

}

#endif
