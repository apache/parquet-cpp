#ifndef PARQUET_PARQUET_H_
#define PARQUET_PARQUET_H_

#include <exception>
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

namespace parquet_cpp {

class Decoder;

struct ByteArray {
  uint32_t len;
  const uint8_t* ptr;
};

class ParquetException : public std::exception {
 public:
  static void EofException() { throw ParquetException("Expected end of stream."); }
  static void NYI() { throw ParquetException("Not yet implemented."); }

  explicit ParquetException(const char* msg) : msg_(msg) {}
  explicit ParquetException(const std::string& msg) : msg_(msg) {}

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

// API to read values from a single column.
class ColumnReader {
 public:
  ColumnReader(const parquet::ColumnMetaData*,
      const parquet::SchemaElement*, InputStream* stream);

  // Returns true if there are still values in this column.
  bool HasNext();

  // Returns the next value of this type.
  // TODO: This might be too inefficient. We should add a buffered API
  // i.e. GetBoolValues(bool* result_buffer)
  bool GetBool(int* definition_level, int* repetition_level);
  int32_t GetInt32(int* definition_level, int* repetition_level);
  int64_t GetInt64(int* definition_level, int* repetition_level);
  float GetFloat(int* definition_level, int* repetition_level);
  double GetDouble(int* definition_level, int* repetition_level);
  ByteArray GetByteArray(int* definition_level, int* repetition_level);

 private:
  bool ReadNewPage();
  // Reads the next definition and repetition level. Returns true if the value is NULL.
  bool ReadDefinitionRepetitionLevels(int* def_level, int* rep_level);

  const parquet::ColumnMetaData* metadata_;
  const parquet::SchemaElement* schema_;
  InputStream* stream_;

  // Map of encoding type to decoder object.
  boost::unordered_map<parquet::Encoding::type, boost::shared_ptr<Decoder> > decoders_;

  int num_buffered_values_;
  parquet::PageHeader current_page_header_;

  // Not set if field is required.
  boost::scoped_ptr<impala::RleDecoder> definition_level_decoder_;
  // Not set for flat schemas.
  boost::scoped_ptr<impala::RleDecoder> repetition_level_decoder_;
  Decoder* current_decoder_;
};


inline bool ColumnReader::HasNext() {
  if (num_buffered_values_ == 0) {
    ReadNewPage();
    if (num_buffered_values_ == 0) return false;
  }
  return true;
}

// Deserialize a thrift message from buf/len.  buf/len must at least contain
// all the bytes needed to store the thrift message.  On return, len will be
// set to the actual length of the header.
template <class T>
inline bool DeserializeThriftMsg(const uint8_t* buf, uint32_t* len, T* deserialized_msg) {
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
    std::cerr << "couldn't deserialize thrift msg:\n" << e.what() << std::endl;
    return false;
  }
  uint32_t bytes_left = tmem_transport->available_read();
  *len = *len - bytes_left;
  return true;
}

}

#endif

