#ifndef PARQUET_PARQUET_H_
#define PARQUET_PARQUET_H_

#include <boost/cstdint.hpp>
#include "gen-cpp/parquet_constants.h"
#include "gen-cpp/parquet_types.h"

namespace parquet_cpp {

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

 private:
  const parquet::SchemaElement* schema_;
  InputStream* stream_;
};

}
#endif

