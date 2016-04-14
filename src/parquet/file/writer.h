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

#ifndef PARQUET_FILE_WRITER_H
#define PARQUET_FILE_WRITER_H

#include <cstdint>
#include <memory>

#include "parquet/schema/descriptor.h"
#include "parquet/schema/types.h"
#include "parquet/util/mem-allocator.h"

namespace parquet {

class ColumnWriter;
class PageWriter;
class OutputStream;

/*
 * Basic Structure:
 * -> Write RowGroup
 *  -> Write Columns
 *    -> Write Batch
 *    -> Check if Batch has the correct size
 *  -> Check if all columns were writtern
 * -> Check number of RowGroups?
 * -> Write MetaData
 * -> Close File
 */
class RowGroupWriter {
 public:
  // Forward declare the PIMPL
  struct Contents {
    virtual int num_columns() const = 0;
    virtual int64_t num_rows() const = 0;

    // TODO: PARQUET-579
    // virtual void WriteRowGroupStatitics();
    virtual PageWriter* NextColumn(Compression::type codec) = 0;
    virtual void Close() = 0;
    
    // Return const-poitner to make it clear that this object is not to be copied
    virtual const SchemaDescriptor* schema() const = 0;
  };

  RowGroupWriter(std::unique_ptr<Contents> contents, MemoryAllocator* allocator);

  // Construct a ColumnWriter for the indicated row group-relative
  // column. Ownership is solely within the RowGroupWriter.
  // The ColumnWriter is only valid until the next call to NextColumn or Close.
  // TODO: Pass a std::weak_ref?
  ColumnWriter* NextColumn(Compression::type codec);
  void Close();
  int num_columns() const;
  int64_t num_rows() const;

  // TODO: PARQUET-579
  // virtual void WriteRowGroupStatitics();

 private:
  // Owned by the parent ParquetFileReader
  const SchemaDescriptor* schema_;

  // PIMPL idiom
  // This is declared in the .cc file so that we can hide compiled Thrift
  // headers from the public API and also more easily create test fixtures.
  std::unique_ptr<Contents> contents_;

  MemoryAllocator* allocator_;
};

class ParquetFileWriter {
 public:
  // Forward declare the PIMPL
  struct Contents {
    virtual ~Contents() {}
    // Perform any cleanup associated with the file contents
    virtual void Close() = 0;

    virtual RowGroupWriter* AppendRowGroup(int64_t num_rows) = 0;

    // virtual int64_t num_rows() const = 0;
    virtual int num_columns() const = 0;
    // virtual int num_row_groups() const = 0;

    // Return const-poitner to make it clear that this object is not to be copied
    const SchemaDescriptor* schema() const {
       return &schema_;
    }
    SchemaDescriptor schema_;
  };
  
  ParquetFileWriter();
  ~ParquetFileWriter();

  // API Convenience to open a serialized Parquet file on disk
  // static std::unique_ptr<ParquetFileWriter> OpenFile(const std::string& path,
  //     bool memory_map = true, MemoryAllocator* allocator = default_allocator());

  static std::unique_ptr<ParquetFileWriter> Open(
      std::unique_ptr<OutputStream> sink,
      std::shared_ptr<schema::GroupNode>& schema,
      MemoryAllocator* allocator = default_allocator());

  void Open(std::unique_ptr<Contents> contents);
  void Close();

  // Construct a RowGroupWriter for the indicated number of rows. Ownership
  // is solely within the ParquetFileWriter. The RowGroupWriter is only valid
  // until the next call to AppendRowGroup or Close.
  // TODO: Pass a std::weak_ref?
  RowGroupWriter* AppendRowGroup(int64_t num_rows);

  int num_columns() const;
  // int64_t num_rows() const;
  // int num_row_groups() const;

  // // Returns the file schema descriptor
  const SchemaDescriptor* descr() {
    return schema_;
  }

  const ColumnDescriptor* column_schema(int i) const {
    return schema_->Column(i);
  }
 
 private:
  // PIMPL idiom
  // This is declared in the .cc file so that we can hide compiled Thrift
  // headers from the public API and also more easily create test fixtures.
  std::unique_ptr<Contents> contents_;

  // The SchemaDescriptor is provided by the Contents impl
  const SchemaDescriptor* schema_;
};

} // namespace parquet

#endif // PARQUET_FILE_WRITER_H

