#ifndef PARQUET_EXAMPLE_UTIL_H
#define PARQUET_EXAMPLE_UTIL_H

#include <string>
#include <parquet/parquet.h>

// TCompactProtocol requires some #defines to work right.
// TODO: is there a better include to use?
#define SIGNED_RIGHT_SHIFT_IS 1
#define ARITHMETIC_RIGHT_SHIFT 1
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/TApplicationException.h>
#include <thrift/transport/TBufferTransports.h>

bool GetFileMetadata(const std::string& path, parquet::FileMetaData* metadata);

inline boost::shared_ptr<apache::thrift::protocol::TProtocol> CreateDeserializeProtocol(
    boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> mem, bool compact) {
  if (compact) {
    apache::thrift::protocol::TCompactProtocolFactoryT<
        apache::thrift::transport::TMemoryBuffer> tproto_factory;
    return tproto_factory.getProtocol(mem);
  } else {
    apache::thrift::protocol::TBinaryProtocolFactoryT<
        apache::thrift::transport::TMemoryBuffer> tproto_factory;
    return tproto_factory.getProtocol(mem);
  }
}

// Deserialize a thrift message from buf/len.  buf/len must at least contain
// all the bytes needed to store the thrift message.  On return, len will be
// set to the actual length of the header.
template <class T>
inline bool DeserializeThriftMsg(uint8_t* buf, uint32_t* len, bool compact,
    T* deserialized_msg) {
  // Deserialize msg bytes into c++ thrift msg using memory transport.
  boost::shared_ptr<apache::thrift::transport::TMemoryBuffer> tmem_transport(
      new apache::thrift::transport::TMemoryBuffer(buf, *len));
  boost::shared_ptr<apache::thrift::protocol::TProtocol> tproto =
      CreateDeserializeProtocol(tmem_transport, compact);
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

#endif
