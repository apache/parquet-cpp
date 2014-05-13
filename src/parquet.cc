#include "parquet/parquet.h"
#include "encodings.h"

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

bool ColumnReader::GetInt32(int32_t* result) {
  --num_buffered_values_;
  int def_level;
  if (!definition_level_decoder_.Get(&def_level)) throw "EOF";
  if (def_level == 0) return true;
  *result = decoder_->GetInt32();
  return false;
}

bool ColumnReader::ReadNewPage() {
  uint32_t bytes_read = stream_->Read(&buffered_bytes_[0], buffered_bytes_.size());
  if (bytes_read == 0) return false;
  uint32_t header_size = bytes_read;
  if (!DeserializeThriftMsg(&buffered_bytes_[0], &header_size, &current_page_header_)) {
    return false;
  }
  buffered_bytes_offset_ += header_size;
  // TODO: handle decompression.
  cout << apache::thrift::ThriftDebugString(current_page_header_) << endl;

  if (current_page_header_.type == PageType::DICTIONARY_PAGE) {
    InitDictionary();
    header_size = bytes_read - buffered_bytes_offset_;
    if (!DeserializeThriftMsg(&buffered_bytes_[buffered_bytes_offset_],
          &header_size, &current_page_header_)) {
      return false;
    }
    cout << apache::thrift::ThriftDebugString(current_page_header_) << endl;
    buffered_bytes_offset_ += header_size;
  }
  int num_definition_bytes = *reinterpret_cast<uint32_t*>(&buffered_bytes_[buffered_bytes_offset_]);
  buffered_bytes_offset_ += 4;
  definition_level_decoder_ =
      impala::RleDecoder(&buffered_bytes_[buffered_bytes_offset_], num_definition_bytes, 1);
  buffered_bytes_offset_ += num_definition_bytes;
  num_buffered_values_ = current_page_header_.data_page_header.num_values;
  decoder_->SetData(num_buffered_values_,
      &buffered_bytes_[buffered_bytes_offset_], bytes_read - buffered_bytes_offset_);
  return true;
}

void ColumnReader::InitDictionary() {
  uint8_t* data = &buffered_bytes_[buffered_bytes_offset_];
  int len = current_page_header_.uncompressed_page_size;
  buffered_bytes_offset_ += len;
  PlainDecoder dictionary(schema_);
  dictionary.SetData(current_page_header_.dictionary_page_header.num_values, data, len);
  decoder_.reset(new DictionaryDecoder(schema_, &dictionary));
}

bool ColumnReader::HasNext() {
  if (num_buffered_values_ == 0) {
    ReadNewPage();
    if (num_buffered_values_ == 0) return false;
  }
  return true;
}

}

