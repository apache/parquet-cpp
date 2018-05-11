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

#include "parquet/arrow/reader.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <map>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include "arrow/api.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/decimal.h"
#include "arrow/util/logging.h"
#include "arrow/util/parallel.h"

#include "parquet/arrow/deserializer.h"
#include "parquet/arrow/record_reader.h"
#include "parquet/arrow/schema.h"
#include "parquet/column_reader.h"
#include "parquet/schema.h"
#include "parquet/util/schema-util.h"

using arrow::Array;
using arrow::BooleanArray;
using arrow::Column;
using arrow::Field;
using arrow::Int32Array;
using arrow::ListArray;
using arrow::MemoryPool;
using arrow::PoolBuffer;
using arrow::Status;
using arrow::StructArray;
using arrow::Table;
using arrow::TimestampArray;

using parquet::schema::Node;

// Help reduce verbosity
using ParquetReader = parquet::ParquetFileReader;
using arrow::ParallelFor;
using arrow::RecordBatchReader;

using parquet::internal::RecordReader;

namespace parquet {
namespace arrow {

using ::arrow::BitUtil::BytesForBits;

template <typename ArrowType>
using ArrayType = typename ::arrow::TypeTraits<ArrowType>::ArrayType;

// ----------------------------------------------------------------------
// Iteration utilities

// Abstraction to decouple row group iteration details from the ColumnReader,
// so we can read only a single row group if we want
class FileColumnIterator {
 public:
  explicit FileColumnIterator(int column_index, ParquetFileReader* reader)
      : column_index_(column_index),
        reader_(reader),
        schema_(reader->metadata()->schema()) {}

  virtual ~FileColumnIterator() {}

  virtual std::unique_ptr<::parquet::PageReader> NextChunk() = 0;

  const SchemaDescriptor* schema() const { return schema_; }

  const ColumnDescriptor* descr() const { return schema_->Column(column_index_); }

  std::shared_ptr<FileMetaData> metadata() const { return reader_->metadata(); }

  int column_index() const { return column_index_; }

 protected:
  int column_index_;
  ParquetFileReader* reader_;
  const SchemaDescriptor* schema_;
};

class AllRowGroupsIterator : public FileColumnIterator {
 public:
  explicit AllRowGroupsIterator(int column_index, ParquetFileReader* reader)
      : FileColumnIterator(column_index, reader), next_row_group_(0) {}

  std::unique_ptr<::parquet::PageReader> NextChunk() override {
    std::unique_ptr<::parquet::PageReader> result;
    if (next_row_group_ < reader_->metadata()->num_row_groups()) {
      result = reader_->RowGroup(next_row_group_)->GetColumnPageReader(column_index_);
      next_row_group_++;
    } else {
      result = nullptr;
    }
    return result;
  }

 private:
  int next_row_group_;
};

class SingleRowGroupIterator : public FileColumnIterator {
 public:
  explicit SingleRowGroupIterator(int column_index, int row_group_number,
                                  ParquetFileReader* reader)
      : FileColumnIterator(column_index, reader),
        row_group_number_(row_group_number),
        done_(false) {}

  std::unique_ptr<::parquet::PageReader> NextChunk() override {
    if (done_) {
      return nullptr;
    }

    auto result =
        reader_->RowGroup(row_group_number_)->GetColumnPageReader(column_index_);
    done_ = true;
    return result;
  }

 private:
  int row_group_number_;
  bool done_;
};

class RowGroupRecordBatchReader : public ::arrow::RecordBatchReader {
 public:
  explicit RowGroupRecordBatchReader(const std::vector<int>& row_group_indices,
                                     const std::vector<int>& column_indices,
                                     std::shared_ptr<::arrow::Schema> schema,
                                     FileReader* reader)
      : row_group_indices_(row_group_indices),
        column_indices_(column_indices),
        schema_(schema),
        file_reader_(reader),
        next_row_group_(0) {}

  ~RowGroupRecordBatchReader() {}

  std::shared_ptr<::arrow::Schema> schema() const override { return schema_; }

  Status ReadNext(std::shared_ptr<::arrow::RecordBatch>* out) override {
    if (table_ != nullptr) {  // one row group has been loaded
      std::shared_ptr<::arrow::RecordBatch> tmp;
      RETURN_NOT_OK(table_batch_reader_->ReadNext(&tmp));
      if (tmp != nullptr) {  // some column chunks are left in table
        *out = tmp;
        return Status::OK();
      } else {  // the entire table is consumed
        table_batch_reader_.reset();
        table_.reset();
      }
    }

    // all row groups has been consumed
    if (next_row_group_ == row_group_indices_.size()) {
      *out = nullptr;
      return Status::OK();
    }

    RETURN_NOT_OK(file_reader_->ReadRowGroup(row_group_indices_[next_row_group_],
                                             column_indices_, &table_));

    next_row_group_++;
    table_batch_reader_.reset(new ::arrow::TableBatchReader(*table_.get()));
    return table_batch_reader_->ReadNext(out);
  }

 private:
  std::vector<int> row_group_indices_;
  std::vector<int> column_indices_;
  std::shared_ptr<::arrow::Schema> schema_;
  FileReader* file_reader_;
  size_t next_row_group_;
  std::shared_ptr<::arrow::Table> table_;
  std::unique_ptr<::arrow::TableBatchReader> table_batch_reader_;
};

// ----------------------------------------------------------------------
// File reader implementation

class FileReader::Impl {
 public:
  Impl(MemoryPool* pool, std::unique_ptr<ParquetFileReader> reader)
      : pool_(pool), reader_(std::move(reader)), num_threads_(1) {}

  virtual ~Impl() {}

  Status GetColumn(int i, std::unique_ptr<ColumnReader>* out);
  Status ReadSchemaField(int i, std::shared_ptr<Array>* out);
  Status ReadColumn(int i, std::shared_ptr<Array>* out);
  Status ReadColumnChunk(int column_index, int row_group_index,
                         std::shared_ptr<Array>* out);
  Status GetSchema(std::shared_ptr<::arrow::Schema>* out);
  Status GetSchema(const std::vector<int>& indices,
                   std::shared_ptr<::arrow::Schema>* out);
  Status ReadRowGroup(int row_group_index, const std::vector<int>& indices,
                      std::shared_ptr<::arrow::Table>* out);
  Status ReadTable(const std::vector<int>& indices, std::shared_ptr<Table>* table);
  Status ReadTable(std::shared_ptr<Table>* table);
  Status ReadRowGroup(int i, std::shared_ptr<Table>* table);

  bool CheckForFlatColumn(const ColumnDescriptor* descr);
  bool CheckForFlatListColumn(const ColumnDescriptor* descr);

  const ParquetFileReader* parquet_reader() const { return reader_.get(); }

  int num_row_groups() const { return reader_->metadata()->num_row_groups(); }

  int num_columns() const { return reader_->metadata()->num_columns(); }

  void set_num_threads(int num_threads) { num_threads_ = num_threads; }

  ParquetFileReader* reader() { return reader_.get(); }

 private:
  MemoryPool* pool_;
  std::shared_ptr<ParquetFileReader> reader_;

  int num_threads_;
};

FileReader::FileReader(MemoryPool* pool, std::unique_ptr<ParquetFileReader> reader)
    : impl_(new FileReader::Impl(pool, std::move(reader))) {}

FileReader::~FileReader() {}

Status FileReader::Impl::GetColumn(int i, std::unique_ptr<ColumnReader>* out) {
  DeserializerBuilder builder(reader_, pool_);
  std::unique_ptr<ArrayBatchedDeserializer> deserializer;
  RETURN_NOT_OK(builder.BuildArrayBatchedDeserializer(i, deserializer));
  (*out).reset(new ColumnReader(deserializer));
  return Status::OK();
}

Status FileReader::Impl::ReadSchemaField(int i, std::shared_ptr<Array>* out) {
  DeserializerBuilder builder(reader_, pool_);
  std::unique_ptr<ArrayDeserializer> deserializer;
  RETURN_NOT_OK(builder.BuildSchemaNodeDeserializer(i, deserializer));
  deserializer->DeserializeArray(*out);
  return Status::OK();
}

Status FileReader::Impl::ReadColumn(int i, std::shared_ptr<Array>* out) {
  DeserializerBuilder builder(reader_, pool_);
  std::unique_ptr<ArrayDeserializer> deserializer;
  RETURN_NOT_OK(builder.BuildColumnDeserializer(i, deserializer));
  deserializer->DeserializeArray(*out);
  return Status::OK();
}

Status FileReader::Impl::GetSchema(const std::vector<int>& indices,
                                   std::shared_ptr<::arrow::Schema>* out) {
  auto descr = reader_->metadata()->schema();
  auto parquet_key_value_metadata = reader_->metadata()->key_value_metadata();
  return FromParquetSchema(descr, indices, parquet_key_value_metadata, out);
}

Status FileReader::Impl::ReadColumnChunk(int column_index, int row_group_index,
                                         std::shared_ptr<Array>* out) {
  DeserializerBuilder builder(reader_, pool_);
  std::unique_ptr<ArrayDeserializer> deserializer;
  RETURN_NOT_OK(
      builder.BuildColumnChunkDeserializer(column_index, row_group_index, deserializer));
  deserializer->DeserializeArray(*out);
  return Status::OK();
}

Status FileReader::Impl::ReadRowGroup(int row_group_index,
                                      const std::vector<int>& indices,
                                      std::shared_ptr<::arrow::Table>* out) {
  DeserializerBuilder builder(reader_, pool_);
  std::unique_ptr<TableDeserializer> deserializer;
  RETURN_NOT_OK(
      builder.BuildRowGroupDeserializer(row_group_index, indices, deserializer));
  deserializer->DeserializeTable(*out);
  return Status::OK();
}

Status FileReader::Impl::ReadTable(const std::vector<int>& indices,
                                   std::shared_ptr<Table>* out) {
  DeserializerBuilder builder(reader_, pool_);
  std::unique_ptr<TableDeserializer> deserializer;
  RETURN_NOT_OK(builder.BuildFileDeserializer(indices, deserializer));
  deserializer->DeserializeTable(*out);
  return Status::OK();
}

Status FileReader::Impl::ReadTable(std::shared_ptr<Table>* table) {
  std::vector<int> indices(reader_->metadata()->num_columns());

  for (size_t i = 0; i < indices.size(); ++i) {
    indices[i] = static_cast<int>(i);
  }
  return ReadTable(indices, table);
}

Status FileReader::Impl::ReadRowGroup(int i, std::shared_ptr<Table>* table) {
  std::vector<int> indices(reader_->metadata()->num_columns());

  for (size_t i = 0; i < indices.size(); ++i) {
    indices[i] = static_cast<int>(i);
  }
  return ReadRowGroup(i, indices, table);
}

// Static ctor
Status OpenFile(const std::shared_ptr<::arrow::io::ReadableFileInterface>& file,
                MemoryPool* allocator, const ReaderProperties& props,
                const std::shared_ptr<FileMetaData>& metadata,
                std::unique_ptr<FileReader>* reader) {
  std::unique_ptr<RandomAccessSource> io_wrapper(new ArrowInputFile(file));
  std::unique_ptr<ParquetReader> pq_reader;
  PARQUET_CATCH_NOT_OK(pq_reader =
                           ParquetReader::Open(std::move(io_wrapper), props, metadata));
  reader->reset(new FileReader(allocator, std::move(pq_reader)));
  return Status::OK();
}

Status OpenFile(const std::shared_ptr<::arrow::io::ReadableFileInterface>& file,
                MemoryPool* allocator, std::unique_ptr<FileReader>* reader) {
  return OpenFile(file, allocator, ::parquet::default_reader_properties(), nullptr,
                  reader);
}

Status FileReader::GetColumn(int i, std::unique_ptr<ColumnReader>* out) {
  return impl_->GetColumn(i, out);
}

Status FileReader::GetSchema(const std::vector<int>& indices,
                             std::shared_ptr<::arrow::Schema>* out) {
  return impl_->GetSchema(indices, out);
}

Status FileReader::ReadColumn(int i, std::shared_ptr<Array>* out) {
  try {
    return impl_->ReadColumn(i, out);
  } catch (const ::parquet::ParquetException& e) {
    return ::arrow::Status::IOError(e.what());
  }
}

Status FileReader::ReadSchemaField(int i, std::shared_ptr<Array>* out) {
  try {
    return impl_->ReadSchemaField(i, out);
  } catch (const ::parquet::ParquetException& e) {
    return ::arrow::Status::IOError(e.what());
  }
}

Status FileReader::GetRecordBatchReader(const std::vector<int>& row_group_indices,
                                        std::shared_ptr<RecordBatchReader>* out) {
  std::vector<int> indices(impl_->num_columns());

  for (size_t j = 0; j < indices.size(); ++j) {
    indices[j] = static_cast<int>(j);
  }

  return GetRecordBatchReader(row_group_indices, indices, out);
}

Status FileReader::GetRecordBatchReader(const std::vector<int>& row_group_indices,
                                        const std::vector<int>& column_indices,
                                        std::shared_ptr<RecordBatchReader>* out) {
  // column indicies check
  std::shared_ptr<::arrow::Schema> schema;
  RETURN_NOT_OK(GetSchema(column_indices, &schema));

  // row group indices check
  int max_num = num_row_groups();
  for (auto row_group_index : row_group_indices) {
    if (row_group_index < 0 || row_group_index >= max_num) {
      std::ostringstream ss;
      ss << "Some index in row_group_indices is " << row_group_index
         << ", which is either < 0 or >= num_row_groups(" << max_num << ")";
      return Status::Invalid(ss.str());
    }
  }

  *out = std::make_shared<RowGroupRecordBatchReader>(row_group_indices, column_indices,
                                                     schema, this);
  return Status::OK();
}

Status FileReader::ReadTable(std::shared_ptr<Table>* out) {
  try {
    return impl_->ReadTable(out);
  } catch (const ::parquet::ParquetException& e) {
    return ::arrow::Status::IOError(e.what());
  }
}

Status FileReader::ReadTable(const std::vector<int>& indices,
                             std::shared_ptr<Table>* out) {
  try {
    return impl_->ReadTable(indices, out);
  } catch (const ::parquet::ParquetException& e) {
    return ::arrow::Status::IOError(e.what());
  }
}

Status FileReader::ReadRowGroup(int i, std::shared_ptr<Table>* out) {
  try {
    return impl_->ReadRowGroup(i, out);
  } catch (const ::parquet::ParquetException& e) {
    return ::arrow::Status::IOError(e.what());
  }
}

Status FileReader::ReadRowGroup(int i, const std::vector<int>& indices,
                                std::shared_ptr<Table>* out) {
  try {
    return impl_->ReadRowGroup(i, indices, out);
  } catch (const ::parquet::ParquetException& e) {
    return ::arrow::Status::IOError(e.what());
  }
}

std::shared_ptr<RowGroupReader> FileReader::RowGroup(int row_group_index) {
  return std::shared_ptr<RowGroupReader>(
      new RowGroupReader(impl_.get(), row_group_index));
}

int FileReader::num_row_groups() const { return impl_->num_row_groups(); }

void FileReader::set_num_threads(int num_threads) { impl_->set_num_threads(num_threads); }

Status FileReader::ScanContents(std::vector<int> columns, const int32_t column_batch_size,
                                int64_t* num_rows) {
  try {
    *num_rows = ScanFileContents(columns, column_batch_size, impl_->reader());
    return Status::OK();
  } catch (const ::parquet::ParquetException& e) {
    return Status::IOError(e.what());
  }
}

const ParquetFileReader* FileReader::parquet_reader() const {
  return impl_->parquet_reader();
}

ColumnReader::ColumnReader(std::unique_ptr<ArrayBatchedDeserializer>& deserializer)
    : deserializer_(std::move(deserializer)) {}

ColumnReader::~ColumnReader() {}

Status ColumnReader::NextBatch(int64_t records_to_read, std::shared_ptr<Array>* out) {
  return deserializer_->DeserializeBatch(records_to_read, *out);
}

std::shared_ptr<ColumnChunkReader> RowGroupReader::Column(int column_index) {
  return std::shared_ptr<ColumnChunkReader>(
      new ColumnChunkReader(impl_, row_group_index_, column_index));
}

Status RowGroupReader::ReadTable(const std::vector<int>& column_indices,
                                 std::shared_ptr<::arrow::Table>* out) {
  return impl_->ReadRowGroup(row_group_index_, column_indices, out);
}

Status RowGroupReader::ReadTable(std::shared_ptr<::arrow::Table>* out) {
  return impl_->ReadRowGroup(row_group_index_, out);
}

RowGroupReader::~RowGroupReader() {}

RowGroupReader::RowGroupReader(FileReader::Impl* impl, int row_group_index)
    : impl_(impl), row_group_index_(row_group_index) {}

Status ColumnChunkReader::Read(std::shared_ptr<::arrow::Array>* out) {
  return impl_->ReadColumnChunk(column_index_, row_group_index_, out);
}

ColumnChunkReader::~ColumnChunkReader() {}

ColumnChunkReader::ColumnChunkReader(FileReader::Impl* impl, int row_group_index,
                                     int column_index)
    : impl_(impl), column_index_(column_index), row_group_index_(row_group_index) {}

}  // namespace arrow
}  // namespace parquet
