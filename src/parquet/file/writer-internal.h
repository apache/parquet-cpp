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

#ifndef PARQUET_FILE_WRITER_INTERNAL_H
#define PARQUET_FILE_WRITER_INTERNAL_H

#include <memory>
#include <vector>

#include "parquet/column_page.h"
#include "parquet/file/metadata.h"
#include "parquet/file/writer.h"
#include "parquet/parquet_types.h"
#include "parquet/util/memory.h"

namespace arrow {

class Codec;
};

namespace parquet {

// This subclass delimits pages appearing in a serialized stream, each preceded
// by a serialized Thrift format::PageHeader indicating the type of each page
// and the page metadata.
class SerializedPageWriter : public PageWriter {
 public:
  SerializedPageWriter(OutputStream* sink, Compression::type codec,
      ColumnChunkMetaDataBuilder* metadata,
      ::arrow::MemoryPool* pool = ::arrow::default_memory_pool());

  virtual ~SerializedPageWriter() {}

  int64_t WriteDataPage(const CompressedDataPage& page) override;

  int64_t WriteDictionaryPage(const DictionaryPage& page) override;

  /**
   * Compress a buffer.
   */
  void Compress(const Buffer& src_buffer, ResizableBuffer* dest_buffer) override;

  bool has_compressor() override { return (compressor_ != nullptr); }

  void Close(bool has_dictionary, bool fallback) override;

 private:
  OutputStream* sink_;
  ColumnChunkMetaDataBuilder* metadata_;
  ::arrow::MemoryPool* pool_;
  int64_t num_values_;
  int64_t dictionary_page_offset_;
  int64_t data_page_offset_;
  int64_t total_uncompressed_size_;
  int64_t total_compressed_size_;

  // Compression codec to use.
  std::unique_ptr<::arrow::Codec> compressor_;
};

// RowGroupWriter::Contents implementation for the Parquet file specification
class RowGroupSerializer : public RowGroupWriter::Contents {
 public:
  RowGroupSerializer(int64_t num_rows, OutputStream* sink,
      RowGroupMetaDataBuilder* metadata, const WriterProperties* properties)
      : num_rows_(num_rows),
        sink_(sink),
        metadata_(metadata),
        properties_(properties),
        total_bytes_written_(0),
        closed_(false) {}

  int num_columns() const override;
  int64_t num_rows() const override;

  ColumnWriter* NextColumn() override;
  int current_column() const override;
  void Close() override;

 private:
  int64_t num_rows_;
  OutputStream* sink_;
  RowGroupMetaDataBuilder* metadata_;
  const WriterProperties* properties_;
  int64_t total_bytes_written_;
  bool closed_;

  std::shared_ptr<ColumnWriter> current_column_writer_;
};

// An implementation of ParquetFileWriter::Contents that deals with the Parquet
// file structure, Thrift serialization, and other internal matters

class FileSerializer : public ParquetFileWriter::Contents {
 public:
  static std::unique_ptr<ParquetFileWriter::Contents> Open(
      const std::shared_ptr<OutputStream>& sink,
      const std::shared_ptr<schema::GroupNode>& schema,
      const std::shared_ptr<WriterProperties>& properties = default_writer_properties(),
      const std::shared_ptr<const KeyValueMetadata>& key_value_metadata = nullptr);

  void Close() override;

  RowGroupWriter* AppendRowGroup(int64_t num_rows) override;

  const std::shared_ptr<WriterProperties>& properties() const override;

  int num_columns() const override;
  int num_row_groups() const override;
  int64_t num_rows() const override;

  virtual ~FileSerializer();

 private:
  explicit FileSerializer(const std::shared_ptr<OutputStream>& sink,
      const std::shared_ptr<schema::GroupNode>& schema,
      const std::shared_ptr<WriterProperties>& properties,
      const std::shared_ptr<const KeyValueMetadata>& key_value_metadata);

  std::shared_ptr<OutputStream> sink_;
  bool is_open_;
  const std::shared_ptr<WriterProperties> properties_;
  int num_row_groups_;
  int64_t num_rows_;
  std::unique_ptr<FileMetaDataBuilder> metadata_;
  std::unique_ptr<RowGroupWriter> row_group_writer_;

  void StartFile();
  void WriteMetaData();
};

}  // namespace parquet

#endif  // PARQUET_FILE_WRITER_INTERNAL_H
