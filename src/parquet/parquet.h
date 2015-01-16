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

#ifndef PARQUET_PARQUET_H
#define PARQUET_PARQUET_H

#include <exception>
#include <sstream>
#include <boost/cstdint.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/unordered_map.hpp>
#include "gen-cpp/parquet_constants.h"
#include "gen-cpp/parquet_types.h"

#include "impala/rle-encoding.h"

// TCompactProtocol requires some #defines to work right.
#define SIGNED_RIGHT_SHIFT_IS 1
#define ARITHMETIC_RIGHT_SHIFT 1
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/TApplicationException.h>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include "parquet/schema.h"

namespace parquet_cpp {

class Codec;
class Decoder;

struct ByteArray {
  uint32_t len;
  const uint8_t* ptr;
};

std::string PrintRepetitionType(const parquet::SchemaElement& e);
std::string PrintType(const parquet::SchemaElement& e);

#define PARQUET_NOT_YET_IMPLEMENTED(msg) \
  ParquetException::NYI(msg, __FILE__, __LINE__)

class ParquetException : public std::exception {
 public:
  static void EofException() { throw ParquetException("Unexpected end of stream."); }

  static void NYI(const std::string& msg, const std::string& file, int line) {
    std::stringstream ss;
    ss << "Not yet implemented: " << msg << " @" << file << ":" << line;
    throw ParquetException(ss.str());
  }

  explicit ParquetException(const char* msg) : msg_(msg) {}
  explicit ParquetException(const std::string& msg) : msg_(msg) {}
  explicit ParquetException(const char* msg, exception& e) : msg_(msg) {}

  virtual ~ParquetException() throw() {}
  virtual const char* what() const throw() { return msg_.c_str(); }

 private:
  std::string msg_;
};

// Interface for the column reader to get the bytes. The interface is a stream
// interface, meaning the bytes in order and once a byte is read, it does not
// need to be read again.
class InputStream {
 public:
  // Returns the next 'num_to_peek' without advancing the current position.
  // *num_bytes will contain the number of bytes returned which can only be
  // less than num_to_peek at end of stream cases.
  // Since the position is not advanced, calls to this function are idempotent.
  // The buffer returned to the caller is still owned by the input stream and must
  // stay valid until the next call to Peek() or Read().
  virtual const uint8_t* Peek(int num_to_peek, int* num_bytes) = 0;

  // Identical to Peek(), except the current position in the stream is advanced by
  // *num_bytes.
  virtual const uint8_t* Read(int num_to_read, int* num_bytes) = 0;

  virtual ~InputStream() {}

 protected:
  InputStream() {}
};

// Implementation of an InputStream when all the bytes are in memory.
class InMemoryInputStream : public InputStream {
 public:
  InMemoryInputStream(const uint8_t* buffer, int64_t len);
  virtual const uint8_t* Peek(int num_to_peek, int* num_bytes);
  virtual const uint8_t* Read(int num_to_read, int* num_bytes);

 private:
  const uint8_t* buffer_;
  int64_t len_;
  int64_t offset_;
};

// API to read values from a single column. This is the main client facing API.
class ColumnReader {
 public:
  struct Config {
    int batch_size;

    static Config DefaultConfig() {
      Config config;
      config.batch_size = 128;
      return config;
    }
  };

  ColumnReader(const parquet::ColumnMetaData*,
      const Schema::Element*, InputStream* stream);

  ~ColumnReader();

  const std::string& name() const { return schema_->parquet_schema().name; }
  const std::string& full_name() const { return schema_->full_name(); }
  parquet::Type::type type() const { return schema_->parquet_schema().type; }

  // Returns true if there are still values in this column.
  bool HasNext();

  // Returns the next value of this type.
  // TODO: batchify this interface.
  bool GetBool(bool* is_null, int* def_level, int* rep_level);
  int32_t GetInt32(bool* is_null, int* def_level, int* rep_level);
  int64_t GetInt64(bool* is_null, int* def_level, int* rep_level);
  float GetFloat(bool* is_null, int* def_level, int* rep_level);
  double GetDouble(bool* is_null, int* def_level, int* rep_level);
  ByteArray GetByteArray(bool* is_null, int* def_level, int* rep_level);

 private:
  bool ReadNewPage();

  // Reads the next definition and repetition level. Returns true if the value is NULL.
  bool ReadDefRepLevels(int* def_level, int* rep_level);

  void BatchDecode();

  Config config_;

  const parquet::ColumnMetaData* metadata_;
  const Schema::Element* schema_;

  // Values with this definition level are non-null
  const int max_def_level_;
  InputStream* stream_;

  // Compression codec to use.
  boost::scoped_ptr<Codec> decompressor_;
  std::vector<uint8_t> decompression_buffer_;

  // Map of compression type to decompressor object.
  boost::unordered_map<parquet::Encoding::type, boost::shared_ptr<Decoder> > decoders_;

  parquet::PageHeader current_page_header_;

  // Not set if field is required.
  boost::scoped_ptr<impala::RleDecoder> def_level_decoder_;
  // Not set for flat schemas.
  boost::scoped_ptr<impala::RleDecoder> rep_level_decoder_;
  Decoder* current_decoder_;
  int num_buffered_values_;

  std::vector<uint8_t> values_buffer_;
  int num_decoded_values_;
  int buffered_values_offset_;
};


inline bool ColumnReader::HasNext() {
  if (num_buffered_values_ == 0) {
    ReadNewPage();
    if (num_buffered_values_ == 0) return false;
  }
  return true;
}

inline bool ColumnReader::GetBool(bool* is_null, int* def_level, int* rep_level) {
  *is_null = ReadDefRepLevels(def_level, rep_level);
  if (*is_null) return bool();
  if (buffered_values_offset_ == num_decoded_values_) BatchDecode();
  return reinterpret_cast<bool*>(&values_buffer_[0])[buffered_values_offset_++];
}

inline int32_t ColumnReader::GetInt32(bool* is_null, int* def_level, int* rep_level) {
  *is_null = ReadDefRepLevels(def_level, rep_level);
  if (*is_null) return int32_t();
  if (buffered_values_offset_ == num_decoded_values_) BatchDecode();
  return reinterpret_cast<int32_t*>(&values_buffer_[0])[buffered_values_offset_++];
}

inline int64_t ColumnReader::GetInt64(bool* is_null, int* def_level, int* rep_level) {
  *is_null = ReadDefRepLevels(def_level, rep_level);
  if (*is_null) return int64_t();
  if (buffered_values_offset_ == num_decoded_values_) BatchDecode();
  return reinterpret_cast<int64_t*>(&values_buffer_[0])[buffered_values_offset_++];
}

inline float ColumnReader::GetFloat(bool* is_null, int* def_level, int* rep_level) {
  *is_null = ReadDefRepLevels(def_level, rep_level);
  if (*is_null) return float();
  if (buffered_values_offset_ == num_decoded_values_) BatchDecode();
  return reinterpret_cast<float*>(&values_buffer_[0])[buffered_values_offset_++];
}

inline double ColumnReader::GetDouble(bool* is_null, int* def_level, int* rep_level) {
  *is_null = ReadDefRepLevels(def_level, rep_level);
  if (*is_null) return double();
  if (buffered_values_offset_ == num_decoded_values_) BatchDecode();
  return reinterpret_cast<double*>(&values_buffer_[0])[buffered_values_offset_++];
}

inline ByteArray ColumnReader::GetByteArray(bool* is_null,
    int* def_level, int* rep_level) {
  *is_null = ReadDefRepLevels(def_level, rep_level);
  if (*is_null) return ByteArray();
  if (buffered_values_offset_ == num_decoded_values_) BatchDecode();
  return reinterpret_cast<ByteArray*>(&values_buffer_[0])[buffered_values_offset_++];
}

inline bool ColumnReader::ReadDefRepLevels(int* def_level, int* rep_level) {
  if (!HasNext()) ParquetException::EofException();
  *def_level = 0;
  *rep_level = 0;
  if (def_level_decoder_.get() != NULL && !def_level_decoder_->Get(def_level)) {
    ParquetException::EofException();
  }
  if (rep_level_decoder_.get() != NULL && !rep_level_decoder_->Get(rep_level)) {
    ParquetException::EofException();
  }
  --num_buffered_values_;
  return *def_level != max_def_level_;
}

// Deserialize a thrift message from buf/len.  buf/len must at least contain
// all the bytes needed to store the thrift message.  On return, len will be
// set to the actual length of the header.
template <class T>
inline void DeserializeThriftMsg(const uint8_t* buf, uint32_t* len, T* deserialized_msg) {
  // Deserialize msg bytes into c++ thrift msg using memory transport.
  boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> tmem_transport(
      new apache::thrift::transport::TMemoryBuffer(const_cast<uint8_t*>(buf), *len));
  apache::thrift::protocol::TCompactProtocolFactoryT<
      apache::thrift::transport::TMemoryBuffer> tproto_factory;
  boost::shared_ptr<apache::thrift::protocol::TProtocol> tproto =
      tproto_factory.getProtocol(tmem_transport);
  try {
    deserialized_msg->read(tproto.get());
  } catch (apache::thrift::protocol::TProtocolException& e) {
    throw ParquetException("Couldn't deserialize thrift.", e);
  }
  uint32_t bytes_left = tmem_transport->available_read();
  *len = *len - bytes_left;
}

}

#endif

