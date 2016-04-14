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
