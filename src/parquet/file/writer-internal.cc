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

#include "parquet/schema/converter.h"
#include "parquet/thrift/util.h"
#include "parquet/util/output.h"

using parquet::schema::GroupNode;
using parquet::schema::SchemaFlattener;

namespace parquet {

// FIXME: copied from reader-internal.cc
static constexpr uint8_t PARQUET_MAGIC[4] = {'P', 'A', 'R', '1'};

SerializedPageWriter::SerializedPageWriter(OutputStream* sink,
        Compression::type codec,
        MemoryAllocator* allocator) : sink_(sink), allocator_(allocator) {
  compressor_ = Codec::Create(codec);
}

void SerializedPageWriter::Close() {}
  
void SerializedPageWriter::WriteDataPage(int32_t num_rows, int32_t num_values, int32_t num_nulls,
        const std::shared_ptr<Buffer>& definition_levels, Encoding::type definition_level_encoding,
        const std::shared_ptr<Buffer>& repetition_levels, Encoding::type repetition_level_encoding,
        const std::shared_ptr<Buffer>& values, Encoding::type encoding) {
  int64_t uncompressed_size = definition_levels->size() + repetition_levels->size() + values->size();
 
  // Concatenate data into a single buffer
  std::shared_ptr<OwnedMutableBuffer> uncompressed_data = std::make_shared<OwnedMutableBuffer>(uncompressed_size, allocator_);
  uint8_t* uncompressed_ptr = uncompressed_data->mutable_data();
  memcpy(uncompressed_ptr, definition_levels->data(), definition_levels->size());
  uncompressed_ptr += definition_levels->size();
  memcpy(uncompressed_ptr, repetition_levels->data(), repetition_levels->size());
  uncompressed_ptr += repetition_levels->size();
  memcpy(uncompressed_ptr, values->data(), values->size());
  
  // Compress the data
  int64_t compressed_size = uncompressed_size;
  std::shared_ptr<OwnedMutableBuffer> compressed_data = uncompressed_data;
  if (compressor_) {
    // TODO
    // int64_t max_compressed_size = compressor_->MaxCompressedLen(uncompressed_data.size(), uncompressed_data.data());
    // OwnedMutableBuffer compressed_data(compressor_->MaxCompressedLen(uncompressed_data.size(), uncompressed_data.data()));
  }
  // Compressed data is not needed anymore, so immediately get rid of it. 
  uncompressed_data.reset();

  format::DataPageHeader data_page_header;
  data_page_header.__set_num_values(num_rows);
  data_page_header.__set_encoding(ToThrift(encoding));
  data_page_header.__set_definition_level_encoding(ToThrift(definition_level_encoding));
  data_page_header.__set_repetition_level_encoding(ToThrift(repetition_level_encoding));
  // TODO: statistics

  format::PageHeader page_header;
  page_header.__set_type(format::PageType::DATA_PAGE);
  page_header.__set_uncompressed_page_size(uncompressed_size);
  page_header.__set_compressed_page_size(compressed_size);
  page_header.__set_data_page_header(data_page_header);
  // TODO: crc checksum
  
  SerializeThriftMsg(&page_header, sizeof(format::PageHeader), sink_);
  sink_->Write(repetition_levels->data(), repetition_levels->size());
  sink_->Write(definition_levels->data(), definition_levels->size());
  sink_->Write(values->data(), values->size());
}

int RowGroupSerializer::num_columns() const {
  return schema_->num_columns();
}

int64_t RowGroupSerializer::num_rows() const {
  return num_rows_;
}

const SchemaDescriptor* RowGroupSerializer::schema() const  {
  return schema_;
}

PageWriter* RowGroupSerializer::NextColumn(Compression::type codec) {
  if (current_column_index_ == schema_->num_columns()) {
    throw ParquetException("All columns have already been written.");
  }
  current_column_index_++;

  if (current_column_writer_) {
    current_column_writer_->Close();
  }

  current_column_writer_.reset(new SerializedPageWriter(sink_, codec, allocator_));
  return current_column_writer_.get();
}

void RowGroupSerializer::Close() {
  if (current_column_index_ != schema_->num_columns()) {
    throw ParquetException("Not all column were written in the current rowgroup.");
  }

  if (current_column_writer_) {
    current_column_writer_->Close();
    current_column_writer_.reset();
  }
}

std::unique_ptr<ParquetFileWriter::Contents> FileSerializer::Open(
    std::unique_ptr<OutputStream> sink, std::shared_ptr<GroupNode>& schema,
    MemoryAllocator* allocator) {
  std::unique_ptr<ParquetFileWriter::Contents> result(
      new FileSerializer(std::move(sink), schema, allocator));

  return result;
}

void FileSerializer::Close() {
  if (row_group_writer_) {
    row_group_writer_->Close();
  }
  row_group_writer_.reset();

  // Write magic bytes and metadata
  WriteMetaData();

  sink_->Close();
}

int FileSerializer::num_columns() const {
  return schema_.num_columns();
}

RowGroupWriter* FileSerializer::AppendRowGroup(int64_t num_rows) {
  if (row_group_writer_) {
    row_group_writer_->Close();
  }
  num_rows_ += num_rows;
  // TODO: Create Contents
  std::unique_ptr<RowGroupWriter::Contents> contents(new RowGroupSerializer(num_rows, &schema_, sink_.get(), allocator_));
  row_group_writer_.reset(new RowGroupWriter(std::move(contents), allocator_));
  return row_group_writer_.get();
}

FileSerializer::~FileSerializer() {
  Close();
}

void FileSerializer::WriteMetaData() {
  // Write MetaData
  uint32_t metadata_len = sink_->Tell();
  format::FileMetaData metadata;
  // TODO: version
  SchemaFlattener flattener(static_cast<GroupNode*>(schema_.schema().get()),
      &metadata.schema);
  flattener.Flatten();
  // TODO: num_rows
  // TODO: row_groups
  // TODO: key_value_metadata
  // TODO: created_by
  // TODO: fill metadata
  SerializeThriftMsg(&metadata, 1024, sink_.get());
  metadata_len = sink_->Tell() - metadata_len;

  // Write Footer
  sink_->Write(PARQUET_MAGIC, 4);
  sink_->Write(reinterpret_cast<uint8_t*>(&metadata_len), 4);
}

FileSerializer::FileSerializer(
    std::unique_ptr<OutputStream> sink,
    std::shared_ptr<GroupNode>& schema,
    MemoryAllocator* allocator = default_allocator()) :
        sink_(std::move(sink)), allocator_(allocator),
        state_(SerializerState::STARTED),
        num_row_groups_(0), num_rows_(0) {
  schema_.Init(schema);
  StartFile();
}

void FileSerializer::StartFile() {
  // Parquet files always start with PAR1
  sink_->Write(PARQUET_MAGIC, 4);
}

} // namespace parquet
