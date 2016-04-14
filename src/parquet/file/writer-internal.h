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

#include "parquet/column/page.h"
#include "parquet/compression/codec.h"
#include "parquet/file/writer.h"
#include "parquet/thrift/parquet_types.h"

namespace parquet {

struct SerializerState {
  enum state {
    STARTED,
    ROWGROUP_OPEN,
    ROWGROUP_END
  };
};

// This subclass delimits pages appearing in a serialized stream, each preceded
// by a serialized Thrift format::PageHeader indicating the type of each page
// and the page metadata.
class SerializedPageWriter : public PageWriter {
 public:
  SerializedPageWriter(OutputStream* sink,
      Compression::type codec, MemoryAllocator* allocator = default_allocator());

  virtual ~SerializedPageWriter() {}

  void WritePage(const std::shared_ptr<Buffer>& definition_levels,
      const std::shared_ptr<Buffer>& repetition_levels,
      const std::shared_ptr<Buffer>& values);

  void set_max_page_header_size(uint32_t size) {
    max_page_header_size_ = size;
  }

  void Close() override;

 private:
  OutputStream* sink_;

  // TODO: Continue here.
  format::PageHeader current_page_header_;
  std::shared_ptr<Page> current_page_;

  // Compression codec to use.
  std::unique_ptr<Codec> decompressor_;
  OwnedMutableBuffer decompression_buffer_;
  // Maximum allowed page size
  uint32_t max_page_header_size_;
};

// RowGroupWriter::Contents implementation for the Parquet file specification
class RowGroupSerializer : public RowGroupWriter::Contents {
 public:
  RowGroupSerializer(int64_t num_rows,
      const SchemaDescriptor* schema,
      OutputStream* sink,
      MemoryAllocator* allocator) :
      num_rows_(num_rows), schema_(schema), sink_(sink),
      allocator_(allocator), current_column_index_(-1) {}

  int num_columns() const override;
  int64_t num_rows() const override;
  const SchemaDescriptor* schema() const override;
  
  // TODO: PARQUET-579
  // void WriteRowGroupStatitics() override;
  PageWriter* NextColumn(Compression::type codec) override;
  void Close() override;

 private:
  int64_t num_rows_;
  const SchemaDescriptor* schema_;
  OutputStream* sink_;
  MemoryAllocator* allocator_;
  
  int64_t current_column_index_;
  std::unique_ptr<PageWriter> current_column_writer_;
};

// An implementation of ParquetFileWriter::Contents that deals with the Parquet
// file structure, Thrift serialization, and other internal matters

class FileSerializer : public ParquetFileWriter::Contents {
 public:
  // TODO?: This class does _not_ take ownership of the data source. You must manage its
  // lifetime separately
  static std::unique_ptr<ParquetFileWriter::Contents> Open(
      std::unique_ptr<OutputStream> sink,
      std::shared_ptr<schema::GroupNode>& schema,
      MemoryAllocator* allocator = default_allocator());
  void Close() override;
  RowGroupWriter* AppendRowGroup(int64_t num_rows) override;
  // virtual std::shared_ptr<RowGroupReader> GetRowGroup(int i);
  // virtual int64_t num_rows() const;
  int num_columns() const override;
  // virtual int num_row_groups() const;
  virtual ~FileSerializer();

 private:
  // TODO?: This class takes ownership of the provided data sink
  explicit FileSerializer(std::unique_ptr<OutputStream> sink,
      std::shared_ptr<schema::GroupNode>& schema,
      MemoryAllocator* allocator);

  std::unique_ptr<OutputStream> sink_;
  format::FileMetaData metadata_;
  MemoryAllocator* allocator_;
  SerializerState::state state_;
  int num_row_groups_;
  int num_rows_;
  std::unique_ptr<RowGroupWriter> row_group_writer_;

  void StartFile();
  void StartRowGroup();
  void EndRowGroup();
  void StartColumn();
  void EndColumn();
  void WriteMetaData();
};

} // namespace parquet

#endif // PARQUET_FILE_WRITER_INTERNAL_H
