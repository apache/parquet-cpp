// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "parquet/file/writer-internal.h"

#include "parquet/column/writer.h"
#include "parquet/schema/converter.h"
#include "parquet/thrift/util.h"
#include "parquet/util/output.h"

using parquet::schema::GroupNode;
using parquet::schema::SchemaFlattener;

namespace parquet {

// FIXME: copied from reader-internal.cc
static constexpr uint8_t PARQUET_MAGIC[4] = {'P', 'A', 'R', '1'};

// ----------------------------------------------------------------------
// SerializedPageWriter

SerializedPageWriter::SerializedPageWriter(OutputStream* sink, Compression::type codec,
    ColumnChunkMetaDataBuilder* metadata, MemoryAllocator* allocator)
    : sink_(sink),
      metadata_(metadata),
      num_values_(0),
      dictionary_page_offset_(0),
      data_page_offset_(0),
      total_uncompressed_size_(0),
      total_compressed_size_(0),
      compression_buffer_(std::make_shared<OwnedMutableBuffer>(0, allocator)) {
  compressor_ = Codec::Create(codec);
}

void SerializedPageWriter::Close() {
  // index_page_offset = 0 since they are not supported
  // TODO: Remove default fallback = 'false' when implemented
  metadata_->Finish(num_values_, dictionary_page_offset_, 0, data_page_offset_,
      total_compressed_size_, total_uncompressed_size_, false);
}

std::shared_ptr<Buffer> SerializedPageWriter::Compress(
    const std::shared_ptr<Buffer>& buffer) {
  // Fast path, no compressor available.
  if (!compressor_) return buffer;

  // Compress the data
  int64_t max_compressed_size =
      compressor_->MaxCompressedLen(buffer->size(), buffer->data());
  compression_buffer_->Resize(max_compressed_size);
  int64_t compressed_size = compressor_->Compress(buffer->size(), buffer->data(),
      max_compressed_size, compression_buffer_->mutable_data());
  compression_buffer_->Resize(compressed_size);
  return compression_buffer_;
}

int64_t SerializedPageWriter::WriteDataPage(const DataPage& page) {
  int64_t uncompressed_size = page.size();
  std::shared_ptr<Buffer> compressed_data = Compress(page.buffer());

  format::DataPageHeader data_page_header;
  data_page_header.__set_num_values(page.num_values());
  data_page_header.__set_encoding(ToThrift(page.encoding()));
  data_page_header.__set_definition_level_encoding(
      ToThrift(page.definition_level_encoding()));
  data_page_header.__set_repetition_level_encoding(
      ToThrift(page.repetition_level_encoding()));
  // TODO(PARQUET-593) statistics

  format::PageHeader page_header;
  page_header.__set_type(format::PageType::DATA_PAGE);
  page_header.__set_uncompressed_page_size(uncompressed_size);
  page_header.__set_compressed_page_size(compressed_data->size());
  page_header.__set_data_page_header(data_page_header);
  // TODO(PARQUET-594) crc checksum

  int64_t start_pos = sink_->Tell();
  if (data_page_offset_ == 0) { data_page_offset_ = start_pos; }
  SerializeThriftMsg(&page_header, sizeof(format::PageHeader), sink_);
  int64_t header_size = sink_->Tell() - start_pos;
  sink_->Write(compressed_data->data(), compressed_data->size());

  total_uncompressed_size_ += uncompressed_size + header_size;
  total_compressed_size_ += compressed_data->size() + header_size;
  num_values_ += page.num_values();

  return sink_->Tell() - start_pos;
}

int64_t SerializedPageWriter::WriteDictionaryPage(const DictionaryPage& page) {
  int64_t uncompressed_size = page.size();
  std::shared_ptr<Buffer> compressed_data = Compress(page.buffer());

  format::DictionaryPageHeader dict_page_header;
  dict_page_header.__set_num_values(page.num_values());
  dict_page_header.__set_encoding(ToThrift(page.encoding()));
  dict_page_header.__set_is_sorted(page.is_sorted());

  format::PageHeader page_header;
  page_header.__set_type(format::PageType::DICTIONARY_PAGE);
  page_header.__set_uncompressed_page_size(uncompressed_size);
  page_header.__set_compressed_page_size(compressed_data->size());
  page_header.__set_dictionary_page_header(dict_page_header);
  // TODO(PARQUET-594) crc checksum

  int64_t start_pos = sink_->Tell();
  if (dictionary_page_offset_ == 0) { dictionary_page_offset_ = start_pos; }
  SerializeThriftMsg(&page_header, sizeof(format::PageHeader), sink_);
  int64_t header_size = sink_->Tell() - start_pos;
  sink_->Write(compressed_data->data(), compressed_data->size());

  total_uncompressed_size_ += uncompressed_size + header_size;
  total_compressed_size_ += compressed_data->size() + header_size;

  return sink_->Tell() - start_pos;
}

// ----------------------------------------------------------------------
// RowGroupSerializer

int RowGroupSerializer::num_columns() const {
  return metadata_->num_columns();
}

int64_t RowGroupSerializer::num_rows() const {
  return num_rows_;
}

ColumnWriter* RowGroupSerializer::NextColumn() {
  // Throws an error if more columns are being written
  auto col_meta = metadata_->NextColumnChunk();

  if (current_column_writer_) { total_bytes_written_ = current_column_writer_->Close(); }

  const ColumnDescriptor* column_descr = col_meta->descr();
  std::unique_ptr<PageWriter> pager(
      new SerializedPageWriter(sink_, properties_->compression(column_descr->path()),
          col_meta, properties_->allocator()));
  current_column_writer_ =
      ColumnWriter::Make(col_meta->descr(), std::move(pager), num_rows_, properties_);
  return current_column_writer_.get();
}

void RowGroupSerializer::Close() {
  if (!closed_) {
    closed_ = true;

    if (current_column_writer_) {
      total_bytes_written_ = current_column_writer_->Close();
      current_column_writer_.reset();
    }
    // Ensures all columns have been written
    metadata_->Finish(total_bytes_written_);
  }
}

// ----------------------------------------------------------------------
// FileSerializer

std::unique_ptr<ParquetFileWriter::Contents> FileSerializer::Open(
    std::shared_ptr<OutputStream> sink, const std::shared_ptr<GroupNode>& schema,
    const std::shared_ptr<WriterProperties>& properties) {
  std::unique_ptr<ParquetFileWriter::Contents> result(
      new FileSerializer(sink, schema, properties));

  return result;
}

void FileSerializer::Close() {
  if (is_open_) {
    if (row_group_writer_) { row_group_writer_->Close(); }
    row_group_writer_.reset();

    // Write magic bytes and metadata
    WriteMetaData();

    sink_->Close();
    is_open_ = false;
  }
}

int FileSerializer::num_columns() const {
  return schema_.num_columns();
}

int FileSerializer::num_row_groups() const {
  return num_row_groups_;
}

int64_t FileSerializer::num_rows() const {
  return num_rows_;
}

const std::shared_ptr<WriterProperties>& FileSerializer::properties() const {
  return properties_;
}

RowGroupWriter* FileSerializer::AppendRowGroup(int64_t num_rows) {
  if (row_group_writer_) { row_group_writer_->Close(); }
  num_rows_ += num_rows;
  num_row_groups_++;
  auto rg_metadata = metadata_->AppendRowGroup(num_rows);
  std::unique_ptr<RowGroupWriter::Contents> contents(
      new RowGroupSerializer(num_rows, sink_.get(), rg_metadata, properties_.get()));
  row_group_writer_.reset(new RowGroupWriter(std::move(contents)));
  return row_group_writer_.get();
}

FileSerializer::~FileSerializer() {
  Close();
}

void FileSerializer::WriteMetaData() {
  // Write MetaData
  uint32_t metadata_len = sink_->Tell();

  // Get a FileMetaData
  auto metadata = metadata_->Finish();
  metadata->WriteTo(sink_.get());
  metadata_len = sink_->Tell() - metadata_len;

  // Write Footer
  sink_->Write(reinterpret_cast<uint8_t*>(&metadata_len), 4);
  sink_->Write(PARQUET_MAGIC, 4);
}

FileSerializer::FileSerializer(std::shared_ptr<OutputStream> sink,
    const std::shared_ptr<GroupNode>& schema,
    const std::shared_ptr<WriterProperties>& properties)
    : sink_(sink),
      is_open_(true),
      properties_(properties),
      num_row_groups_(0),
      num_rows_(0) {
  schema_.Init(schema);
  metadata_ = FileMetaDataBuilder::Make(&schema_, properties);
  StartFile();
}

void FileSerializer::StartFile() {
  // Parquet files always start with PAR1
  sink_->Write(PARQUET_MAGIC, 4);
}

}  // namespace parquet
