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

#ifndef PARQUET_COLUMN_WRITER_H
#define PARQUET_COLUMN_WRITER_H

#include "parquet/column/page.h"
#include "parquet/column/levels.h"
#include "parquet/schema/descriptor.h"
#include "parquet/types.h"
#include "parquet/util/mem-allocator.h"
#include "parquet/encodings/encoder.h"
#include "parquet/util/output.h"

namespace parquet {

// Constants used for the default size checks for paging
// TODO: Make configurable
const int32_t DEFAULT_PAGE_SIZE = 1024 * 1024;
const int32_t DEFAULT_DICTIONARY_PAGE_SIZE = DEFAULT_PAGE_SIZE;
const bool DEFAULT_ESTIMATE_ROW_COUNT_FOR_PAGE_SIZE_CHECK = true;
const int32_t DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK = 100;
const int32_t DEFAULT_MAXIMUM_RECORD_COUNT_FOR_CHECK = 10000;

class ColumnWriter {
 public:
  ColumnWriter(const ColumnDescriptor*, std::unique_ptr<PageWriter>,
      MemoryAllocator* allocator = default_allocator());

  static std::shared_ptr<ColumnWriter> Make(const ColumnDescriptor*,
      std::unique_ptr<PageWriter>, MemoryAllocator* allocator = default_allocator());

  Type::type type() const {
    return descr_->physical_type();
  }

  const ColumnDescriptor* descr() const {
    return descr_;
  }

  void Close();

 protected:
  void WriteNewPage();
  // virtual void CommitPages() = 0;
  // virtual void ClosePages() = 0;

  // Write multiple definition levels
  void WriteDefinitionLevels(int64_t num_levels, int16_t* levels);

  // Write multiple repetition levels
  void WriteRepetitionLevels(int64_t num_levels, int16_t* levels);

  const ColumnDescriptor* descr_;

  std::unique_ptr<PageWriter> pager_;
  std::shared_ptr<Page> current_page_;

  // Not set if full schema for this field has no optional or repeated elements
  LevelEncoder definition_level_encoder_;

  // Not set for flat schemas.
  LevelEncoder repetition_level_encoder_;

  MemoryAllocator* allocator_;
  
  // The total number of values stored in the data page. This is the maximum of
  // the number of encoded definition levels or encoded values. For
  // non-repeated, required columns, this is equal to the number of encoded
  // values. For repeated or optional values, there may be fewer data values
  // than levels, and this tells you how many encoded levels there are in that
  // case.
  int num_buffered_values_;

  // The total number of stored values. For repeated or optional values, this
  // number may be lower than num_buffered_values_.
  int num_buffered_encoded_values_;

  // The next count when we should do a size estimate.
  int num_buffered_values_next_size_check_;

  // Total number of rows written with this ColumnWriter
  int num_rows_;

  std::unique_ptr<InMemoryOutputStream> definition_levels_sink_;
  std::unique_ptr<InMemoryOutputStream> repetition_levels_sink_;
  std::unique_ptr<InMemoryOutputStream> values_sink_;
 
 private:
  void InitSinks();
};

// API to read values from a single column. This is the main client facing API.
template <int TYPE>
class TypedColumnWriter : public ColumnWriter {
 public:
  typedef typename type_traits<TYPE>::value_type T;

  TypedColumnWriter(const ColumnDescriptor* schema,
      std::unique_ptr<PageWriter> pager, MemoryAllocator* allocator = default_allocator());
  
  // Write a batch of repetition levels, definition levels, and values to the
  // column.
  void WriteBatch(int64_t num_values, int16_t* def_levels, int16_t* rep_levels,
      T* values);

 private:
  typedef Encoder<TYPE> EncoderType;

  // Write values to a temporary buffer before they are encoded into pages
  void WriteValues(int64_t num_values, T* values);

  // Advance to the next data page
  // bool WriteNewPage() override;
  // void CommitPages() override;
  // void ClosePages() override;


  // Map of encoding type to the respective encoder object. For example, a
  // column chunk's data pages may include both dictionary-encoded and
  // plain-encoded data.
  std::unordered_map<int, std::shared_ptr<EncoderType> > encoders_;

  void ConfigureDictionary(const DictionaryPage* page);

  std::unique_ptr<EncoderType> current_encoder_;
};

// TODO: This is just chosen at random, we should make better estimates.
// See also: parquet-column/src/main/java/org/apache/parquet/column/impl/ColumnWriteStoreV2.java:sizeCheck
const int64_t PAGE_ROW_COUNT = 1000;

template <int TYPE>
inline void TypedColumnWriter<TYPE>::WriteBatch(int64_t num_values, int16_t* def_levels,
    int16_t* rep_levels, T* values) {
  // Calculate how much rows we can write before we have to do the next size check.
  // int64_t values_to_next_size_check = num_buffered_values_next_size_check_ - num_buffered_values_;
  // TODO: Chunking, size check

  int64_t values_to_write = 0;

  // If the field is required and non-repeated, there are no definition levels
  if (descr_->max_definition_level() > 0) {
    for (int64_t i = 0; i < num_values; ++i) {
      if (def_levels[i] == descr_->max_definition_level()) {
        ++values_to_write;
      }
    }

    WriteDefinitionLevels(num_values, def_levels);
  } else {
    // Required field, write all values
    values_to_write = num_values;
  }

  // Not present for non-repeated fields
  if (descr_->max_repetition_level() > 0) {
    // TODO: Write repetition levels
    // WriteRepetitionLevels(num_values, rep_levels);
  }

  WriteValues(values_to_write, values);

  num_buffered_values_ += num_values;
  num_buffered_encoded_values_ += values_to_write;
  num_rows_ += num_values;
}

template <int TYPE>
void TypedColumnWriter<TYPE>::WriteValues(int64_t num_values, T* values) {
  current_encoder_->Encode(values, num_values, values_sink_.get());
}


typedef TypedColumnWriter<Type::BOOLEAN> BoolWriter;
typedef TypedColumnWriter<Type::INT32> Int32Writer;
typedef TypedColumnWriter<Type::INT64> Int64Writer;
typedef TypedColumnWriter<Type::INT96> Int96Writer;
typedef TypedColumnWriter<Type::FLOAT> FloatWriter;
typedef TypedColumnWriter<Type::DOUBLE> DoubleWriter;
typedef TypedColumnWriter<Type::BYTE_ARRAY> ByteArrayWriter;
typedef TypedColumnWriter<Type::FIXED_LEN_BYTE_ARRAY> FixedLenByteArrayWriter;

} // namespace parquet

#endif // PARQUET_COLUMN_READER_H

