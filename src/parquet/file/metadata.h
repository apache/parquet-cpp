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

#ifndef PARQUET_FILE_METADATA_H
#define PARQUET_FILE_METADATA_H

#include <string>
#include <vector>
#include <set>

#include "parquet/column/properties.h"
#include "parquet/compression/codec.h"
#include "parquet/schema/descriptor.h"
#include "parquet/types.h"
#include "parquet/util/output.h"
#include "parquet/util/visibility.h"

namespace parquet {

struct ColumnStatistics {
  int64_t null_count;
  int64_t distinct_count;
  const std::string* min;
  const std::string* max;
};

class PARQUET_EXPORT ColumnMetaData {
 public:
  // API convenience to get a MetaData accessor
  static std::unique_ptr<ColumnMetaData> GetColumnMetaData(const uint8_t* metadata);

  explicit ColumnMetaData(const uint8_t* metadata);
  ~ColumnMetaData();

  // column chunk
  int64_t file_offset() const;
  const std::string& file_path() const;
  // column metadata
  Type::type type() const;
  int64_t num_values() const;
  const std::shared_ptr<schema::ColumnPath> path_in_schema() const;
  bool IsStatsSet() const;
  ColumnStatistics GetStats() const;
  Compression::type GetCompression() const;
  std::vector<Encoding::type> GetEncodings() const;
  int64_t GetCompressedSize() const;
  int64_t GetUnCompressedSize() const;

 private:
  // PIMPL Idiom
  class ColumnMetaDataImpl;
  std::unique_ptr<ColumnMetaDataImpl> impl_;
};

class PARQUET_EXPORT RowGroupMetaData {
 public:
  // API convenience to get a MetaData accessor
  static std::unique_ptr<RowGroupMetaData> GetRowGroupMetaData(const uint8_t* metadata);

  explicit RowGroupMetaData(const uint8_t* metadata);
  ~RowGroupMetaData();

  // row-group metadata
  int num_columns() const;
  int64_t num_rows() const;
  int64_t total_byte_size() const;
  std::unique_ptr<ColumnMetaData> GetColumnMetaData(int i) const;

 private:
  // PIMPL Idiom
  class RowGroupMetaDataImpl;
  std::unique_ptr<RowGroupMetaDataImpl> impl_;
};

class PARQUET_EXPORT FileMetaData {
 public:
  // API convenience to get a MetaData accessor
  static std::unique_ptr<FileMetaData> GetFileMetaData(const uint8_t* metadata);

  explicit FileMetaData(const uint8_t* metadata);
  ~FileMetaData();

  // file metadata
  int64_t num_rows() const;
  int num_row_groups() const;
  int32_t version() const;
  const std::string& created_by() const;
  int num_schema_elements() const;
  std::unique_ptr<RowGroupMetaData> GetRowGroupMetaData(int i) const;

  void WriteTo(OutputStream* dst);

  // Return const-pointer to make it clear that this object is not to be copied
  const SchemaDescriptor* schema() const;

 private:
  // PIMPL Idiom
  class FileMetaDataImpl;
  std::unique_ptr<FileMetaDataImpl> impl_;
};

// Builder API
class PARQUET_EXPORT ColumnMetaDataBuilder {
 public:
  // API convenience to get a MetaData reader
  static std::unique_ptr<ColumnMetaDataBuilder> GetColumnMetaDataBuilder(
      std::shared_ptr<WriterProperties> props, const ColumnDescriptor* column,
      uint8_t* contents);

  explicit ColumnMetaDataBuilder(std::shared_ptr<WriterProperties> props,
      const ColumnDescriptor* column, uint8_t* contents);
  ~ColumnMetaDataBuilder();

  // column chunk
  void set_file_path(const std::string& path);
  // column metadata
  void set_encodings(const std::set<Encoding::type>& val);
  void SetStatistics(const ColumnStatistics& stats);

  // commit the metadata
  void Finish(int64_t num_values, int64_t dictonary_page_offset,
      int64_t index_page_offset, int64_t data_page_offset, int64_t compressed_size,
      int64_t uncompressed_size);

 private:
  // PIMPL Idiom
  class ColumnMetaDataBuilderImpl;
  std::unique_ptr<ColumnMetaDataBuilderImpl> impl_;
};

class PARQUET_EXPORT RowGroupMetaDataBuilder {
 public:
  // API convenience to get a MetaData reader
  static std::unique_ptr<RowGroupMetaDataBuilder> GetRowGroupMetaDataBuilder(
      std::shared_ptr<WriterProperties> props, const SchemaDescriptor* schema_,
      uint8_t* contents);

  explicit RowGroupMetaDataBuilder(std::shared_ptr<WriterProperties> props,
      const SchemaDescriptor* schema_, uint8_t* contents);
  ~RowGroupMetaDataBuilder();

  ColumnMetaDataBuilder* NextColumnMetaData();

  // commit the metadata
  void Finish(int64_t num_rows);

 private:
  // PIMPL Idiom
  class RowGroupMetaDataBuilderImpl;
  std::unique_ptr<RowGroupMetaDataBuilderImpl> impl_;
};

class PARQUET_EXPORT FileMetaDataBuilder {
 public:
  // API convenience to get a MetaData reader
  static std::unique_ptr<FileMetaDataBuilder> GetFileMetaDataBuilder(
      const SchemaDescriptor* schema, std::shared_ptr<WriterProperties> props);

  explicit FileMetaDataBuilder(
      const SchemaDescriptor* schema, std::shared_ptr<WriterProperties> props);
  ~FileMetaDataBuilder();

  RowGroupMetaDataBuilder* AppendRowGroupMetaData();

  // commit the metadata
  std::unique_ptr<FileMetaData> Finish();

 private:
  // PIMPL Idiom
  class FileMetaDataBuilderImpl;
  std::unique_ptr<FileMetaDataBuilderImpl> impl_;
};

}  // namespace parquet

#endif  // PARQUET_FILE_METADATA_H
