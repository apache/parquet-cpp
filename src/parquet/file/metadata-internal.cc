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

#include <vector>

#include "parquet/file/metadata.h"
#include "parquet/schema/converter.h"
#include "parquet/thrift/util.h"

namespace parquet {

// MetaData Accessor
// Column metadata
class ColumnMetaData::ColumnMetaDataImpl {
 public:
  explicit ColumnMetaDataImpl(const format::ColumnChunk* column) : column_(column) {}
  ~ColumnMetaDataImpl() {}

  // column chunk
  int64_t file_offset() const { return column_->file_offset; }
  const std::string& file_path() const { return column_->file_path; }

  // column metadata
  Type::type type() { return FromThrift(column_->meta_data.type); }

  int64_t num_values() const { return column_->meta_data.num_values; }
  std::shared_ptr<schema::ColumnPath> path_in_schema() {
    return std::make_shared<schema::ColumnPath>(column_->meta_data.path_in_schema);
  }

  bool is_stats_set() const { return column_->meta_data.__isset.statistics; }

  ColumnStatistics Stats() const {
    const format::ColumnMetaData& meta_data = column_->meta_data;

    ColumnStatistics result;
    result.null_count = meta_data.statistics.null_count;
    result.distinct_count = meta_data.statistics.distinct_count;
    result.max = &meta_data.statistics.max;
    result.min = &meta_data.statistics.min;
    return result;
  }

  Compression::type compression() const { return FromThrift(column_->meta_data.codec); }

  std::vector<Encoding::type> Encodings() const {
    const format::ColumnMetaData& meta_data = column_->meta_data;

    std::vector<Encoding::type> encodings;
    for (auto encoding : meta_data.encodings) {
      encodings.push_back(FromThrift(encoding));
    }
    return encodings;
  }

  int64_t total_compressed_size() const {
    return column_->meta_data.total_compressed_size;
  }

  int64_t total_uncompressed_size() const {
    return column_->meta_data.total_uncompressed_size;
  }

 private:
  const format::ColumnChunk* column_;
};

std::unique_ptr<ColumnMetaData> ColumnMetaData::GetColumnMetaData(
    const uint8_t* metadata) {
  return std::unique_ptr<ColumnMetaData>(new ColumnMetaData(metadata));
}

ColumnMetaData::ColumnMetaData(const uint8_t* metadata)
    : impl_{std::unique_ptr<ColumnMetaDataImpl>(new ColumnMetaDataImpl(
          reinterpret_cast<const format::ColumnChunk*>(metadata)))} {}
ColumnMetaData::~ColumnMetaData() {}

// column chunk
int64_t ColumnMetaData::file_offset() const {
  return impl_->file_offset();
}

const std::string& ColumnMetaData::file_path() const {
  return impl_->file_path();
}

// column metadata
Type::type ColumnMetaData::type() const {
  return impl_->type();
}

int64_t ColumnMetaData::num_values() const {
  return impl_->num_values();
}

std::shared_ptr<schema::ColumnPath> ColumnMetaData::path_in_schema() const {
  return impl_->path_in_schema();
}

ColumnStatistics ColumnMetaData::Stats() const {
  return impl_->Stats();
}

bool ColumnMetaData::is_stats_set() const {
  return impl_->is_stats_set();
}

Compression::type ColumnMetaData::compression() const {
  return impl_->compression();
}

std::vector<Encoding::type> ColumnMetaData::Encodings() const {
  return impl_->Encodings();
}

int64_t ColumnMetaData::total_uncompressed_size() const {
  return impl_->total_uncompressed_size();
}

int64_t ColumnMetaData::total_compressed_size() const {
  return impl_->total_compressed_size();
}

// row-group metadata
class RowGroupMetaData::RowGroupMetaDataImpl {
 public:
  explicit RowGroupMetaDataImpl(const format::RowGroup* row_group)
      : row_group_(row_group) {}
  ~RowGroupMetaDataImpl() {}

  int num_columns() const { return row_group_->columns.size(); }

  int64_t num_rows() const { return row_group_->num_rows; }

  int64_t total_byte_size() const { return row_group_->total_byte_size; }

  std::unique_ptr<ColumnMetaData> GetColumnMetaData(int i) {
    DCHECK(i < num_columns()) << "The file only has " << num_columns()
                              << " columns, requested metadata for column: " << i;
    return ColumnMetaData::GetColumnMetaData(
        reinterpret_cast<const uint8_t*>(&row_group_->columns[i]));
  }

 private:
  const format::RowGroup* row_group_;
};

std::unique_ptr<RowGroupMetaData> RowGroupMetaData::GetRowGroupMetaData(
    const uint8_t* metadata) {
  return std::unique_ptr<RowGroupMetaData>(new RowGroupMetaData(metadata));
}

RowGroupMetaData::RowGroupMetaData(const uint8_t* metadata)
    : impl_{std::unique_ptr<RowGroupMetaDataImpl>(new RowGroupMetaDataImpl(
          reinterpret_cast<const format::RowGroup*>(metadata)))} {}
RowGroupMetaData::~RowGroupMetaData() {}

int RowGroupMetaData::num_columns() const {
  return impl_->num_columns();
}

int64_t RowGroupMetaData::num_rows() const {
  return impl_->num_rows();
}

int64_t RowGroupMetaData::total_byte_size() const {
  return impl_->total_byte_size();
}

std::unique_ptr<ColumnMetaData> RowGroupMetaData::GetColumnMetaData(int i) const {
  return impl_->GetColumnMetaData(i);
}

// file metadata
class FileMetaData::FileMetaDataImpl {
 public:
  explicit FileMetaDataImpl(const uint8_t* metadata)
      : metadata_{std::unique_ptr<const format::FileMetaData>(
            reinterpret_cast<const format::FileMetaData*>(metadata))} {
    schema::FlatSchemaConverter converter(
        &metadata_->schema[0], metadata_->schema.size());
    schema_.Init(converter.Convert());
  }
  ~FileMetaDataImpl() {}
  int64_t num_rows() const { return metadata_->num_rows; }
  int num_row_groups() const { return metadata_->row_groups.size(); }
  int32_t version() const { return metadata_->version; }
  const std::string& created_by() const { return metadata_->created_by; }
  int num_schema_elements() const { return metadata_->schema.size(); }

  void WriteTo(OutputStream* dst) { SerializeThriftMsg(metadata_.get(), 1024, dst); }

  std::unique_ptr<RowGroupMetaData> GetRowGroupMetaData(int i) {
    DCHECK(i < num_row_groups())
        << "The file only has " << num_row_groups()
        << " row groups, requested metadata for row group: " << i;
    return RowGroupMetaData::GetRowGroupMetaData(
        reinterpret_cast<const uint8_t*>(&metadata_->row_groups[i]));
  }

  const SchemaDescriptor* schema() const { return &schema_; }

 private:
  std::unique_ptr<const format::FileMetaData> metadata_;
  SchemaDescriptor schema_;
};

std::unique_ptr<FileMetaData> FileMetaData::GetFileMetaData(const uint8_t* metadata) {
  return std::unique_ptr<FileMetaData>(new FileMetaData(metadata));
}

FileMetaData::FileMetaData(const uint8_t* metadata)
    : impl_{std::unique_ptr<FileMetaDataImpl>(new FileMetaDataImpl(metadata))} {}
FileMetaData::~FileMetaData() {}

std::unique_ptr<RowGroupMetaData> FileMetaData::GetRowGroupMetaData(int i) const {
  return impl_->GetRowGroupMetaData(i);
}

int64_t FileMetaData::num_rows() const {
  return impl_->num_rows();
}

int FileMetaData::num_row_groups() const {
  return impl_->num_row_groups();
}

int32_t FileMetaData::version() const {
  return impl_->version();
}

const std::string& FileMetaData::created_by() const {
  return impl_->created_by();
}

int FileMetaData::num_schema_elements() const {
  return impl_->num_schema_elements();
}

const SchemaDescriptor* FileMetaData::schema() const {
  return impl_->schema();
}

void FileMetaData::WriteTo(OutputStream* dst) {
  return impl_->WriteTo(dst);
}

// MetaData Builders
// row-group metadata
class ColumnMetaDataBuilder::ColumnMetaDataBuilderImpl {
 public:
  explicit ColumnMetaDataBuilderImpl(const std::shared_ptr<WriterProperties>& props,
      const ColumnDescriptor* column, uint8_t* contents)
      : properties_(props), column_(column) {
    column_chunk_ = reinterpret_cast<format::ColumnChunk*>(contents);
    column_chunk_->meta_data.__set_type(ToThrift(column->physical_type()));
    column_chunk_->meta_data.__set_path_in_schema(column->path()->ToDotVector());
    column_chunk_->meta_data.__set_codec(
        ToThrift(properties_->compression(column->path())));
  }
  ~ColumnMetaDataBuilderImpl() {}

  // column chunk
  void set_file_path(const std::string& val) { column_chunk_->__set_file_path(val); }

  // column metadata
  void SetStatistics(const ColumnStatistics& val) {
    format::Statistics stats;
    stats.null_count = val.null_count;
    stats.distinct_count = val.distinct_count;
    stats.max = *val.max;
    stats.min = *val.min;

    column_chunk_->meta_data.statistics = stats;
    column_chunk_->meta_data.__isset.statistics = true;
  }

  void Finish(int64_t num_values, int64_t dictionary_page_offset,
      int64_t index_page_offset, int64_t data_page_offset, int64_t compressed_size,
      int64_t uncompressed_size, bool dictionary_fallback = false) {
    if (dictionary_page_offset > 0) {
      column_chunk_->__set_file_offset(dictionary_page_offset + compressed_size);
    } else {
      column_chunk_->__set_file_offset(data_page_offset + compressed_size);
    }
    column_chunk_->__isset.meta_data = true;
    column_chunk_->meta_data.__set_num_values(num_values);
    column_chunk_->meta_data.__set_dictionary_page_offset(dictionary_page_offset);
    column_chunk_->meta_data.__set_index_page_offset(index_page_offset);
    column_chunk_->meta_data.__set_data_page_offset(data_page_offset);
    column_chunk_->meta_data.__set_total_uncompressed_size(uncompressed_size);
    column_chunk_->meta_data.__set_total_compressed_size(compressed_size);
    std::vector<format::Encoding::type> thrift_encodings;
    thrift_encodings.push_back(ToThrift(properties_->level_encoding()));
    if (properties_->dictionary_enabled()) {
      thrift_encodings.push_back(ToThrift(properties_->dictionary_encoding()));
      // add the encoding only if it is unique
      if (properties_->version() == ParquetVersion::PARQUET_2_0) {
        thrift_encodings.push_back(ToThrift(properties_->dictionary_index_encoding()));
      }
    }
    if (!properties_->dictionary_enabled() || dictionary_fallback) {
      thrift_encodings.push_back(ToThrift(properties_->encoding(column_->path())));
    }
    column_chunk_->meta_data.__set_encodings(thrift_encodings);
  }

 private:
  format::ColumnChunk* column_chunk_;
  const std::shared_ptr<WriterProperties> properties_;
  const ColumnDescriptor* column_;
};

std::unique_ptr<ColumnMetaDataBuilder> ColumnMetaDataBuilder::GetColumnMetaDataBuilder(
    const std::shared_ptr<WriterProperties>& props, const ColumnDescriptor* column,
    uint8_t* contents) {
  return std::unique_ptr<ColumnMetaDataBuilder>(
      new ColumnMetaDataBuilder(props, column, contents));
}

ColumnMetaDataBuilder::ColumnMetaDataBuilder(
    const std::shared_ptr<WriterProperties>& props, const ColumnDescriptor* column,
    uint8_t* contents)
    : impl_{std::unique_ptr<ColumnMetaDataBuilderImpl>(
          new ColumnMetaDataBuilderImpl(props, column, contents))} {}

ColumnMetaDataBuilder::~ColumnMetaDataBuilder() {}

void ColumnMetaDataBuilder::set_file_path(const std::string& path) {
  impl_->set_file_path(path);
}

void ColumnMetaDataBuilder::Finish(int64_t num_values, int64_t dictionary_page_offset,
    int64_t index_page_offset, int64_t data_page_offset, int64_t compressed_size,
    int64_t uncompressed_size, bool dictionary_fallback) {
  impl_->Finish(num_values, dictionary_page_offset, index_page_offset, data_page_offset,
      compressed_size, uncompressed_size, dictionary_fallback);
}

void ColumnMetaDataBuilder::SetStatistics(const ColumnStatistics& result) {
  impl_->SetStatistics(result);
}

class RowGroupMetaDataBuilder::RowGroupMetaDataBuilderImpl {
 public:
  explicit RowGroupMetaDataBuilderImpl(const std::shared_ptr<WriterProperties>& props,
      const SchemaDescriptor* schema, uint8_t* contents)
      : properties_(props), schema_(schema), current_column_(0) {
    row_group_ = reinterpret_cast<format::RowGroup*>(contents);
    InitializeColumns(schema->num_columns());
  }
  ~RowGroupMetaDataBuilderImpl() {}

  ColumnMetaDataBuilder* NextColumnMetaData() {
    DCHECK(current_column_ < num_columns())
        << "The schema only has " << num_columns()
        << " columns, requested metadata for column: " << current_column_;
    auto column = schema_->Column(current_column_);
    auto column_builder = ColumnMetaDataBuilder::GetColumnMetaDataBuilder(properties_,
        column, reinterpret_cast<uint8_t*>(&row_group_->columns[current_column_++]));
    auto column_builder_ptr = column_builder.get();
    column_builders_.push_back(std::move(column_builder));
    return column_builder_ptr;
  }

  void Finish(int64_t num_rows) {
    DCHECK(current_column_ == schema_->num_columns())
        << "Only " << current_column_ - 1 << " out of " << schema_->num_columns()
        << " columns are initialized";
    size_t total_byte_size = 0;

    for (int i = 0; i < schema_->num_columns(); i++) {
      DCHECK(row_group_->columns[i].file_offset > 0) << "Column " << i
                                                     << " is not complete.";
      total_byte_size += row_group_->columns[i].meta_data.total_compressed_size;
    }

    row_group_->__set_total_byte_size(total_byte_size);
    row_group_->__set_num_rows(num_rows);
  }

 private:
  int num_columns() { return row_group_->columns.size(); }

  void InitializeColumns(int ncols) { row_group_->columns.resize(ncols); }

  format::RowGroup* row_group_;
  const std::shared_ptr<WriterProperties> properties_;
  const SchemaDescriptor* schema_;
  std::vector<std::unique_ptr<ColumnMetaDataBuilder>> column_builders_;
  int current_column_;
};

std::unique_ptr<RowGroupMetaDataBuilder>
RowGroupMetaDataBuilder::GetRowGroupMetaDataBuilder(
    const std::shared_ptr<WriterProperties>& props, const SchemaDescriptor* schema_,
    uint8_t* contents) {
  return std::unique_ptr<RowGroupMetaDataBuilder>(
      new RowGroupMetaDataBuilder(props, schema_, contents));
}

RowGroupMetaDataBuilder::RowGroupMetaDataBuilder(
    const std::shared_ptr<WriterProperties>& props, const SchemaDescriptor* schema_,
    uint8_t* contents)
    : impl_{std::unique_ptr<RowGroupMetaDataBuilderImpl>(
          new RowGroupMetaDataBuilderImpl(props, schema_, contents))} {}

RowGroupMetaDataBuilder::~RowGroupMetaDataBuilder() {}

ColumnMetaDataBuilder* RowGroupMetaDataBuilder::NextColumnMetaData() {
  return impl_->NextColumnMetaData();
}

void RowGroupMetaDataBuilder::Finish(int64_t num_rows) {
  impl_->Finish(num_rows);
}

// file metadata
class FileMetaDataBuilder::FileMetaDataBuilderImpl {
 public:
  explicit FileMetaDataBuilderImpl(
      const SchemaDescriptor* schema, const std::shared_ptr<WriterProperties>& props)
      : properties_(props), schema_(schema) {
    metadata_.reset(new format::FileMetaData());
  }
  ~FileMetaDataBuilderImpl() {}

  RowGroupMetaDataBuilder* AppendRowGroupMetaData() {
    auto row_group = std::unique_ptr<format::RowGroup>(new format::RowGroup());
    auto row_group_builder = RowGroupMetaDataBuilder::GetRowGroupMetaDataBuilder(
        properties_, schema_, reinterpret_cast<uint8_t*>(row_group.get()));
    RowGroupMetaDataBuilder* row_group_ptr = row_group_builder.get();
    row_group_builders_.push_back(std::move(row_group_builder));
    row_groups_.push_back(std::move(row_group));
    return row_group_ptr;
  }

  std::unique_ptr<FileMetaData> Finish() {
    int64_t total_rows = 0;
    std::vector<format::RowGroup> row_groups;
    for (auto row_group = row_groups_.begin(); row_group != row_groups_.end();
         row_group++) {
      auto rowgroup = *((*row_group).get());
      row_groups.push_back(rowgroup);
      total_rows += rowgroup.num_rows;
    }
    metadata_->__set_num_rows(total_rows);
    metadata_->__set_row_groups(row_groups);
    metadata_->__set_version(properties_->version());
    metadata_->__set_created_by(properties_->created_by());
    parquet::schema::SchemaFlattener flattener(
        static_cast<parquet::schema::GroupNode*>(schema_->schema().get()),
        &metadata_->schema);
    flattener.Flatten();
    return FileMetaData::GetFileMetaData(reinterpret_cast<uint8_t*>(metadata_.release()));
  }

 private:
  std::unique_ptr<format::FileMetaData> metadata_;
  const std::shared_ptr<WriterProperties> properties_;
  std::vector<std::unique_ptr<format::RowGroup>> row_groups_;
  std::vector<std::unique_ptr<RowGroupMetaDataBuilder>> row_group_builders_;
  const SchemaDescriptor* schema_;
};

std::unique_ptr<FileMetaDataBuilder> FileMetaDataBuilder::GetFileMetaDataBuilder(
    const SchemaDescriptor* schema, const std::shared_ptr<WriterProperties>& props) {
  return std::unique_ptr<FileMetaDataBuilder>(new FileMetaDataBuilder(schema, props));
}

FileMetaDataBuilder::FileMetaDataBuilder(
    const SchemaDescriptor* schema, const std::shared_ptr<WriterProperties>& props)
    : impl_{std::unique_ptr<FileMetaDataBuilderImpl>(
          new FileMetaDataBuilderImpl(schema, props))} {}

FileMetaDataBuilder::~FileMetaDataBuilder() {}

RowGroupMetaDataBuilder* FileMetaDataBuilder::AppendRowGroupMetaData() {
  return impl_->AppendRowGroupMetaData();
}

std::unique_ptr<FileMetaData> FileMetaDataBuilder::Finish() {
  return impl_->Finish();
}

}  // namespace parquet
