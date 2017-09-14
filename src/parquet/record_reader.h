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

#ifndef PARQUET_RECORD_READER_H
#define PARQUET_RECORD_READER_H

#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <unordered_map>
#include <vector>

#include <arrow/buffer.h>
#include <arrow/builder.h>
#include <arrow/memory_pool.h>
#include <arrow/util/bit-util.h>

#include "parquet/column_page.h"
#include "parquet/schema.h"
#include "parquet/util/visibility.h"

namespace parquet {

/// \brief Stateful column reader that delimits semantic records for both flat
/// and nested columns
///
/// \note API EXPERIMENTAL
/// \since 1.3.0
class PARQUET_EXPORT RecordReader {
 public:
  // So that we can create subclasses
  class RecordReaderImpl;

  static std::shared_ptr<RecordReader> Make(
      const ColumnDescriptor* descr,
      ::arrow::MemoryPool* pool = ::arrow::default_memory_pool());

  virtual ~RecordReader();

  const int16_t* def_levels() const;
  const int16_t* rep_levels() const;
  const uint8_t* values() const;

  int64_t ReadRecords(int64_t num_records);

  // For better flat read performance, this allows us to preallocate space to
  // avoid realloc/memcpy
  void Reserve(int64_t num_values);

  void Reset();

  std::shared_ptr<PoolBuffer> ReleaseValues();
  std::shared_ptr<PoolBuffer> ReleaseIsValid();
  ::arrow::ArrayBuilder* builder();

  /// \brief Number of values written including nulls (if any)
  int64_t values_written() const;

  int64_t levels_position() const;
  int64_t levels_written() const;
  int64_t null_count() const;
  bool nullable_values() const;

  bool HasMoreData() const;

  void SetPageReader(std::unique_ptr<PageReader> reader);

 private:
  std::unique_ptr<RecordReaderImpl> impl_;
  explicit RecordReader(RecordReaderImpl*);
};

}  // namespace parquet

#endif  // PARQUET_RECORD_READER_H
