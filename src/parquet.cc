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

const uint8_t* InMemoryInputStream::Peek(int num_to_peek, int* num_bytes) {
  *num_bytes = ::min(static_cast<int64_t>(num_to_peek), len_ - offset_);
  return buffer_ + offset_;
}

const uint8_t* InMemoryInputStream::Read(int num_to_read, int* num_bytes) {
  const uint8_t* result = Peek(num_to_read, num_bytes);
  offset_ += *num_bytes;
  return result;
}

ColumnReader::ColumnReader(const SchemaElement* schema, InputStream* stream)
  : schema_(schema),
    stream_(stream),
    num_buffered_values_(0) {
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
  // Loop until we find the next data page.
  while (true) {
    int bytes_read = 0;
    const uint8_t* buffer = stream_->Peek(DATA_PAGE_SIZE, &bytes_read);
    if (bytes_read == 0) return false;
    uint32_t header_size = bytes_read;
    if (!DeserializeThriftMsg(buffer, &header_size, &current_page_header_)) {
      return false;
    }
    stream_->Read(header_size, &bytes_read);
    cout << apache::thrift::ThriftDebugString(current_page_header_) << endl;

    // TODO: handle decompression.
    int uncompressed_len = current_page_header_.uncompressed_page_size;
    buffer = stream_->Read(uncompressed_len, &bytes_read);
    if (bytes_read != uncompressed_len) throw "EOF";

    if (current_page_header_.type == PageType::DICTIONARY_PAGE) {
      PlainDecoder dictionary(schema_);
      dictionary.SetData(current_page_header_.dictionary_page_header.num_values,
          buffer, uncompressed_len);
      decoder_.reset(new DictionaryDecoder(schema_, &dictionary));
      continue;
    } else if (current_page_header_.type == PageType::DATA_PAGE) {
      // Read a data page.
      num_buffered_values_ = current_page_header_.data_page_header.num_values;

      // Read definition levels.
      int num_definition_bytes = *reinterpret_cast<const uint32_t*>(buffer);
      buffer += sizeof(uint32_t);
      definition_level_decoder_ = impala::RleDecoder(buffer, num_definition_bytes, 1);
      buffer += num_definition_bytes;

      // TODO: repetition levels

      // Now buffer is at the start of the data.
      decoder_->SetData(num_buffered_values_, buffer,
          uncompressed_len - sizeof(uint32_t) - num_definition_bytes);
    } else {
      // We don't know what this page type is. just skip it.
      continue;
    }
  }
  return true;
}

bool ColumnReader::HasNext() {
  if (num_buffered_values_ == 0) {
    ReadNewPage();
    if (num_buffered_values_ == 0) return false;
  }
  return true;
}

}

