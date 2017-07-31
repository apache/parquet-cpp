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

// This module defines an abstract interface for iterating through pages in a
// Parquet column chunk within a row group. It could be extended in the future
// to iterate through all data pages in all chunks in a file.

#ifndef PARQUET_COLUMN_PAGE_H
#define PARQUET_COLUMN_PAGE_H

#include <cstdint>
#include <memory>
#include <string>

#include "parquet/statistics.h"
#include "parquet/types.h"
#include "parquet/util/memory.h"

namespace parquet {

// TODO: Parallel processing is not yet safe because of memory-ownership
// semantics (the PageReader may or may not own the memory referenced by a
// page)
//
// TODO(wesm): In the future Parquet implementations may store the crc code
// in format::PageHeader. parquet-mr currently does not, so we also skip it
// here, both on the read and write path
class Page {
 public:
  Page(const std::shared_ptr<Buffer>& buffer, PageType::type type)
      : buffer_(buffer), type_(type) {}

  PageType::type type() const { return type_; }

  std::shared_ptr<Buffer> buffer() const { return buffer_; }

  // @returns: a pointer to the page's data
  const uint8_t* data() const { return buffer_->data(); }

  // @returns: the total size in bytes of the page's data buffer
  int32_t size() const { return static_cast<int32_t>(buffer_->size()); }

 private:
  std::shared_ptr<Buffer> buffer_;
  PageType::type type_;
};

class DataPage : public Page {
 public:
  DataPage(const std::shared_ptr<Buffer>& buffer, int32_t num_values,
           Encoding::type encoding, Encoding::type definition_level_encoding,
           Encoding::type repetition_level_encoding,
           const EncodedStatistics& statistics = EncodedStatistics())
      : Page(buffer, PageType::DATA_PAGE),
        num_values_(num_values),
        encoding_(encoding),
        definition_level_encoding_(definition_level_encoding),
        repetition_level_encoding_(repetition_level_encoding),
        statistics_(statistics) {}

  int32_t num_values() const { return num_values_; }

  Encoding::type encoding() const { return encoding_; }

  Encoding::type repetition_level_encoding() const { return repetition_level_encoding_; }

  Encoding::type definition_level_encoding() const { return definition_level_encoding_; }

  const EncodedStatistics& statistics() const { return statistics_; }

 private:
  int32_t num_values_;
  Encoding::type encoding_;
  Encoding::type definition_level_encoding_;
  Encoding::type repetition_level_encoding_;
  EncodedStatistics statistics_;
};

class CompressedDataPage : public DataPage {
 public:
  CompressedDataPage(const std::shared_ptr<Buffer>& buffer, int32_t num_values,
                     Encoding::type encoding, Encoding::type definition_level_encoding,
                     Encoding::type repetition_level_encoding, int64_t uncompressed_size,
                     const EncodedStatistics& statistics = EncodedStatistics())
      : DataPage(buffer, num_values, encoding, definition_level_encoding,
                 repetition_level_encoding, statistics),
        uncompressed_size_(uncompressed_size) {}

  int64_t uncompressed_size() const { return uncompressed_size_; }

 private:
  int64_t uncompressed_size_;
};

class DataPageV2 : public Page {
 public:
  DataPageV2(const std::shared_ptr<Buffer>& buffer, int32_t num_values, int32_t num_nulls,
             int32_t num_rows, Encoding::type encoding,
             int32_t definition_levels_byte_length, int32_t repetition_levels_byte_length,
             bool is_compressed = false)
      : Page(buffer, PageType::DATA_PAGE_V2),
        num_values_(num_values),
        num_nulls_(num_nulls),
        num_rows_(num_rows),
        encoding_(encoding),
        definition_levels_byte_length_(definition_levels_byte_length),
        repetition_levels_byte_length_(repetition_levels_byte_length),
        is_compressed_(is_compressed) {}

  int32_t num_values() const { return num_values_; }

  int32_t num_nulls() const { return num_nulls_; }

  int32_t num_rows() const { return num_rows_; }

  Encoding::type encoding() const { return encoding_; }

  int32_t definition_levels_byte_length() const { return definition_levels_byte_length_; }

  int32_t repetition_levels_byte_length() const { return repetition_levels_byte_length_; }

  bool is_compressed() const { return is_compressed_; }

 private:
  int32_t num_values_;
  int32_t num_nulls_;
  int32_t num_rows_;
  Encoding::type encoding_;
  int32_t definition_levels_byte_length_;
  int32_t repetition_levels_byte_length_;
  bool is_compressed_;

  // TODO(wesm): format::DataPageHeaderV2.statistics
};

class DictionaryPage : public Page {
 public:
  DictionaryPage(const std::shared_ptr<Buffer>& buffer, int32_t num_values,
                 Encoding::type encoding, bool is_sorted = false)
      : Page(buffer, PageType::DICTIONARY_PAGE),
        num_values_(num_values),
        encoding_(encoding),
        is_sorted_(is_sorted) {}

  int32_t num_values() const { return num_values_; }

  Encoding::type encoding() const { return encoding_; }

  bool is_sorted() const { return is_sorted_; }

 private:
  int32_t num_values_;
  Encoding::type encoding_;
  bool is_sorted_;
};

// Abstract page iterator interface. This way, we can feed column pages to the
// ColumnReader through whatever mechanism we choose
class PageReader {
 public:
  virtual ~PageReader() {}

  // @returns: shared_ptr<Page>(nullptr) on EOS, std::shared_ptr<Page>
  // containing new Page otherwise
  virtual std::shared_ptr<Page> NextPage() = 0;
};

class PageWriter {
 public:
  virtual ~PageWriter() {}

  // The Column Writer decides if dictionary encoding is used if set and
  // if the dictionary encoding has fallen back to default encoding on reaching dictionary
  // page limit
  virtual void Close(bool has_dictionary, bool fallback) = 0;

  virtual int64_t WriteDataPage(const CompressedDataPage& page) = 0;

  virtual int64_t WriteDictionaryPage(const DictionaryPage& page) = 0;

  virtual bool has_compressor() = 0;

  virtual void Compress(const Buffer& src_buffer, ResizableBuffer* dest_buffer) = 0;
};

}  // namespace parquet

#endif  // PARQUET_COLUMN_PAGE_H
