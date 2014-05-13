#include <parquet/parquet.h>
#include <string>
#include <string.h>

#include <thrift/protocol/TDebugProtocol.h>

const int DATA_PAGE_SIZE = 64 * 1024;

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
    stream_(stream),
    num_buffered_values_(0) {
  buffered_bytes_.resize(DATA_PAGE_SIZE);
  num_buffered_bytes_ = 0;
  buffered_bytes_offset_ = 0;
}

bool ColumnReader::ReadNewPage() {
  uint32_t bytes_read = stream_->Read(&buffered_bytes_[0], buffered_bytes_.size());
  uint32_t header_size = bytes_read;
  if (!DeserializeThriftMsg(&buffered_bytes_[0], &header_size, &current_page_header_)) {
    return false;
  }
  buffered_bytes_offset_ += header_size;
  // TODO: handle decompression.

  if (current_page_header_.type == PageType::DICTIONARY_PAGE) {
    InitDictionary();
//    return ReadNewPage();
    return true;
  }

  return true;
}

void ColumnReader::InitDictionary() {
  for (int i = 0; i < current_page_header_.dictionary_page_header.num_values; ++i) {
    uint8_t* data = &buffered_bytes_[buffered_bytes_offset_];
    cout << *reinterpret_cast<int32_t*>(data) << endl;
    buffered_bytes_offset_ += sizeof(int32_t);
  }
}

bool ColumnReader::HasNext() {
  if (num_buffered_values_ == 0) {
    ReadNewPage();
    if (num_buffered_values_ == 0) return false;
  }
  return false;
}

}

