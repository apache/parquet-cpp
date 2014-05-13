#ifndef PARQUET_PARQUET_H_
#define PARQUET_PARQUET_H_

#include <boost/cstdint.hpp>
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

struct String {
  uint32_t len;
  const uint8_t* ptr;
};

class InputStream {
 public:
  virtual ~InputStream() {}
  virtual int Read(uint8_t* buffer, int buffer_len) = 0;

 protected:
  InputStream() {}
};

class InMemoryInputStream : public InputStream {
 public:
  InMemoryInputStream(const uint8_t* buffer, int64_t len);
  int Read(uint8_t* buffer, int buffer_len);

 private:
  const uint8_t* buffer_;
  int64_t len_;
  int64_t offset_;
};

class ColumnReader {
 public:
  ColumnReader(const parquet::SchemaElement* schema, InputStream* stream);
  bool HasNext();

  bool GetInt32(int32_t* result);

 private:
  bool ReadNewPage();
  void InitDictionary();

  const parquet::SchemaElement* schema_;
  InputStream* stream_;
  int num_buffered_values_;
  parquet::PageHeader current_page_header_;
  std::vector<uint8_t> buffered_bytes_;
  size_t num_buffered_bytes_;
  size_t buffered_bytes_offset_;

  impala::RleDecoder definition_level_decoder_;
  boost::shared_ptr<Decoder> decoder_;
};

// Deserialize a thrift message from buf/len.  buf/len must at least contain
// all the bytes needed to store the thrift message.  On return, len will be
// set to the actual length of the header.
template <class T>
inline bool DeserializeThriftMsg(uint8_t* buf, uint32_t* len, T* deserialized_msg) {
  // Deserialize msg bytes into c++ thrift msg using memory transport.
  boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> tmem_transport(
      new apache::thrift::transport::TMemoryBuffer(buf, *len));
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

