#include <parquet/parquet.h>
#include <string>
#include <string.h>

using namespace parquet;
using namespace std;

namespace parquet_cpp {

InMemoryInputStream::InMemoryInputStream(const uint8_t* buffer, int64_t len) :
  buffer_(buffer), len_(len), offset_(0) {
}

int InMemoryInputStream::Read(uint8_t* buffer, int buffer_len) {
  if (len_ == offset_) return 0;
  int bytes_read = ::min(static_cast<int64_t>(buffer_len), len_ - offset_);
  memcpy(buffer, buffer_ + offset_, bytes_read);
  offset_ += bytes_read;
  return bytes_read;
}

ColumnReader::ColumnReader(const SchemaElement* schema, InputStream* stream)
  : schema_(schema),
    stream_(stream) {
}

bool ColumnReader::HasNext() {
  return false;
}

}

