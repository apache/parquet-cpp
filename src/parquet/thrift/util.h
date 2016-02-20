#ifndef PARQUET_THRIFT_UTIL_H
#define PARQUET_THRIFT_UTIL_H

#include <cstdint>

// Needed for thrift
#include <boost/shared_ptr.hpp>

// TCompactProtocol requires some #defines to work right.
#define SIGNED_RIGHT_SHIFT_IS 1
#define ARITHMETIC_RIGHT_SHIFT 1
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/TApplicationException.h>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <sstream>

#include "parquet/exception.h"
#include "parquet/thrift/parquet_types.h"
#include "parquet/util/logging.h"
#include "parquet/util/output.h"

namespace parquet_cpp {

// ----------------------------------------------------------------------
// Convert Thrift enums to / from parquet_cpp enums

static inline Type::type FromThrift(parquet::Type::type type) {
  return static_cast<Type::type>(type);
}

static inline LogicalType::type FromThrift(parquet::ConvertedType::type type) {
  // item 0 is NONE
  return static_cast<LogicalType::type>(static_cast<int>(type) + 1);
}

static inline Repetition::type FromThrift(parquet::FieldRepetitionType::type type) {
  return static_cast<Repetition::type>(type);
}

static inline Encoding::type FromThrift(parquet::Encoding::type type) {
  return static_cast<Encoding::type>(type);
}

static inline Compression::type FromThrift(parquet::CompressionCodec::type type) {
  return static_cast<Compression::type>(type);
}

// ----------------------------------------------------------------------
// Thrift struct serialization / deserialization utilities

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
  } catch (std::exception& e) {
    std::stringstream ss;
    ss << "Couldn't deserialize thrift: " << e.what() << "\n";
    throw ParquetException(ss.str());
  }
  uint32_t bytes_left = tmem_transport->available_read();
  *len = *len - bytes_left;
}

// Serialize obj into a buffer. The result is returned as a string.
// The arguments are the object to be serialized and
// the expected size of the serialized object
template <class T>
inline void SerializeThriftMsg(T* obj, uint32_t len, OutputStream* out) {
  boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> mem_buffer(
      new apache::thrift::transport::TMemoryBuffer(len));
  apache::thrift::protocol::TCompactProtocolFactoryT<
      apache::thrift::transport::TMemoryBuffer> tproto_factory;
  boost::shared_ptr<apache::thrift::protocol::TProtocol> tproto =
      tproto_factory.getProtocol(mem_buffer);
  try {
    mem_buffer->resetBuffer();
    obj->write(tproto.get());
  } catch (std::exception& e) {
    std::stringstream ss;
    ss << "Couldn't serialize thrift: " << e.what() << "\n";
    throw ParquetException(ss.str());
  }

  uint8_t* out_buffer;
  uint32_t out_length;
  mem_buffer->getBuffer(&out_buffer, &out_length);
  out->Write(out_buffer, out_length);
}

} // namespace parquet_cpp

#endif // PARQUET_THRIFT_UTIL_H
