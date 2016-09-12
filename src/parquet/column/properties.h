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

#ifndef PARQUET_COLUMN_PROPERTIES_H
#define PARQUET_COLUMN_PROPERTIES_H

#include <memory>
#include <string>
#include <unordered_map>

#include "parquet/exception.h"
#include "parquet/schema/types.h"
#include "parquet/types.h"
#include "parquet/util/input.h"
#include "parquet/util/mem-allocator.h"
#include "parquet/util/visibility.h"

namespace parquet {

struct ParquetVersion {
  enum type { PARQUET_1_0, PARQUET_2_0 };
};

static int64_t DEFAULT_BUFFER_SIZE = 0;
static bool DEFAULT_USE_BUFFERED_STREAM = false;

class PARQUET_EXPORT ReaderProperties {
 public:
  explicit ReaderProperties(MemoryAllocator* allocator = default_allocator())
      : allocator_(allocator) {
    buffered_stream_enabled_ = DEFAULT_USE_BUFFERED_STREAM;
    buffer_size_ = DEFAULT_BUFFER_SIZE;
  }

  MemoryAllocator* allocator() { return allocator_; }

  std::unique_ptr<InputStream> GetStream(
      RandomAccessSource* source, int64_t start, int64_t num_bytes) {
    std::unique_ptr<InputStream> stream;
    if (buffered_stream_enabled_) {
      stream.reset(
          new BufferedInputStream(allocator_, buffer_size_, source, start, num_bytes));
    } else {
      stream.reset(new InMemoryInputStream(source, start, num_bytes));
    }
    return stream;
  }

  bool is_buffered_stream_enabled() const { return buffered_stream_enabled_; }

  void enable_buffered_stream() { buffered_stream_enabled_ = true; }

  void disable_buffered_stream() { buffered_stream_enabled_ = false; }

  void set_buffer_size(int64_t buf_size) { buffer_size_ = buf_size; }

  int64_t buffer_size() const { return buffer_size_; }

 private:
  MemoryAllocator* allocator_;
  int64_t buffer_size_;
  bool buffered_stream_enabled_;
};

ReaderProperties PARQUET_EXPORT default_reader_properties();

static constexpr int64_t DEFAULT_PAGE_SIZE = 1024 * 1024;
static constexpr bool DEFAULT_IS_DICTIONARY_ENABLED = true;
static constexpr int64_t DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT = DEFAULT_PAGE_SIZE;
static constexpr int64_t DEFAULT_WRITE_BATCH_SIZE = 1024;
static constexpr bool DEFAULT_COLLECT_STATISTICS = false;
static constexpr Encoding::type DEFAULT_ENCODING = Encoding::PLAIN;
static constexpr ParquetVersion::type DEFAULT_WRITER_VERSION =
    ParquetVersion::PARQUET_1_0;
static std::string DEFAULT_CREATED_BY = "Apache parquet-cpp";
static constexpr Compression::type DEFAULT_COMPRESSION_TYPE = Compression::UNCOMPRESSED;

class PARQUET_EXPORT ColumnSettings {
 public:
  ColumnSettings(Encoding::type encoding = DEFAULT_ENCODING,
      Compression::type codec = DEFAULT_COMPRESSION_TYPE,
      bool dictionary_enabled = DEFAULT_IS_DICTIONARY_ENABLED,
      bool collect_statistics = DEFAULT_COLLECT_STATISTICS)
      : encoding(encoding),
        codec(codec),
        dictionary_enabled(dictionary_enabled),
        collect_statistics(collect_statistics) {}

  Encoding::type encoding;
  Compression::type codec;
  bool dictionary_enabled;
  bool collect_statistics;
};

class PARQUET_EXPORT WriterProperties {
 public:
  class Builder {
   public:
    Builder()
        : allocator_(default_allocator()),
          dictionary_pagesize_limit_(DEFAULT_DICTIONARY_PAGE_SIZE_LIMIT),
          write_batch_size_(DEFAULT_WRITE_BATCH_SIZE),
          pagesize_(DEFAULT_PAGE_SIZE),
          version_(DEFAULT_WRITER_VERSION),
          created_by_(DEFAULT_CREATED_BY) {}
    virtual ~Builder() {}

    Builder* allocator(MemoryAllocator* allocator) {
      allocator_ = allocator;
      return this;
    }

    Builder* enable_dictionary() {
      default_column_settings_.dictionary_enabled = true;
      return this;
    }

    Builder* disable_dictionary() {
      default_column_settings_.dictionary_enabled = false;
      return this;
    }

    Builder* enable_dictionary(const std::string& path) {
      column_settings_[path].dictionary_enabled = true;
      return this;
    }

    Builder* enable_dictionary(const std::shared_ptr<schema::ColumnPath>& path) {
      return this->enable_dictionary(path->ToDotString());
    }

    Builder* disable_dictionary(const std::string& path) {
      column_settings_[path].dictionary_enabled = false;
      return this;
    }

    Builder* disable_dictionary(const std::shared_ptr<schema::ColumnPath>& path) {
      return this->disable_dictionary(path->ToDotString());
    }

    Builder* dictionary_pagesize_limit(int64_t dictionary_psize_limit) {
      dictionary_pagesize_limit_ = dictionary_psize_limit;
      return this;
    }

    Builder* write_batch_size(int64_t write_batch_size) {
      write_batch_size_ = write_batch_size;
      return this;
    }

    Builder* data_pagesize(int64_t pg_size) {
      pagesize_ = pg_size;
      return this;
    }

    Builder* version(ParquetVersion::type version) {
      version_ = version;
      return this;
    }

    Builder* created_by(const std::string& created_by) {
      created_by_ = created_by;
      return this;
    }

    /**
     * Define the encoding that is used when we don't utilise dictionary encoding.
     *
     * This either apply if dictionary encoding is disabled or if we fallback
     * as the dictionary grew too large.
     */
    Builder* encoding(Encoding::type encoding_type) {
      if (encoding_type == Encoding::PLAIN_DICTIONARY ||
          encoding_type == Encoding::RLE_DICTIONARY) {
        throw ParquetException("Can't use dictionary encoding as fallback encoding");
      }

      default_column_settings_.encoding = encoding_type;
      return this;
    }

    /**
     * Define the encoding that is used when we don't utilise dictionary encoding.
     *
     * This either apply if dictionary encoding is disabled or if we fallback
     * as the dictionary grew too large.
     */
    Builder* encoding(const std::string& path, Encoding::type encoding_type) {
      if (encoding_type == Encoding::PLAIN_DICTIONARY ||
          encoding_type == Encoding::RLE_DICTIONARY) {
        throw ParquetException("Can't use dictionary encoding as fallback encoding");
      }

      column_settings_[path].encoding = encoding_type;
      return this;
    }

    /**
     * Define the encoding that is used when we don't utilise dictionary encoding.
     *
     * This either apply if dictionary encoding is disabled or if we fallback
     * as the dictionary grew too large.
     */
    Builder* encoding(
        const std::shared_ptr<schema::ColumnPath>& path, Encoding::type encoding_type) {
      return this->encoding(path->ToDotString(), encoding_type);
    }

    Builder* compression(Compression::type codec) {
      default_column_settings_.codec = codec;
      return this;
    }

    Builder* compression(const std::string& path, Compression::type codec) {
      column_settings_[path].codec = codec;
      return this;
    }

    Builder* compression(
        const std::shared_ptr<schema::ColumnPath>& path, Compression::type codec) {
      return this->compression(path->ToDotString(), codec);
    }

    Builder* collect_statistics(bool enable = true) {
      default_column_settings_.collect_statistics = enable;
      return this;
    }

    Builder* collect_statistics(const std::string& path, bool enable = true) {
      column_settings_[path].collect_statistics = enable;
      return this;
    }

    Builder* collect_statistics(
        const std::shared_ptr<schema::ColumnPath>& path, bool enable = true) {
      return this->collect_statistics(path->ToDotString(), enable);
    }

    std::shared_ptr<WriterProperties> build() {
      return std::shared_ptr<WriterProperties>(new WriterProperties(allocator_,
          dictionary_pagesize_limit_, write_batch_size_, pagesize_, version_, created_by_,
          default_column_settings_, column_settings_));
    }

   private:
    MemoryAllocator* allocator_;
    int64_t dictionary_pagesize_limit_;
    int64_t write_batch_size_;
    int64_t pagesize_;
    ParquetVersion::type version_;
    std::string created_by_;

    // Settings used for each column unless overridden in column_settings_
    ColumnSettings default_column_settings_;
    std::unordered_map<std::string, ColumnSettings> column_settings_;
  };

  inline MemoryAllocator* allocator() const { return allocator_; }

  inline int64_t dictionary_pagesize_limit() const { return dictionary_pagesize_limit_; }

  inline int64_t write_batch_size() const { return write_batch_size_; }

  inline int64_t data_pagesize() const { return pagesize_; }

  inline ParquetVersion::type version() const { return parquet_version_; }

  inline std::string created_by() const { return parquet_created_by_; }

  inline Encoding::type dictionary_index_encoding() const {
    if (parquet_version_ == ParquetVersion::PARQUET_1_0) {
      return Encoding::PLAIN_DICTIONARY;
    } else {
      return Encoding::RLE_DICTIONARY;
    }
  }

  inline Encoding::type dictionary_page_encoding() const {
    if (parquet_version_ == ParquetVersion::PARQUET_1_0) {
      return Encoding::PLAIN_DICTIONARY;
    } else {
      return Encoding::PLAIN;
    }
  }

  const ColumnSettings& column_settings(
      const std::shared_ptr<schema::ColumnPath>& path) const {
    auto it = column_settings_.find(path->ToDotString());
    if (it != column_settings_.end()) return it->second;
    return default_column_settings_;
  }

  Encoding::type encoding(const std::shared_ptr<schema::ColumnPath>& path) const {
    return column_settings(path).encoding;
  }

  Compression::type compression(const std::shared_ptr<schema::ColumnPath>& path) const {
    return column_settings(path).codec;
  }

  bool dictionary_enabled(const std::shared_ptr<schema::ColumnPath>& path) const {
    return column_settings(path).dictionary_enabled;
  }

  bool collect_statistics(const std::shared_ptr<schema::ColumnPath>& path) const {
    return column_settings(path).collect_statistics;
  }

 private:
  explicit WriterProperties(MemoryAllocator* allocator, int64_t dictionary_pagesize_limit,
      int64_t write_batch_size, int64_t pagesize, ParquetVersion::type version,
      const std::string& created_by, const ColumnSettings& default_column_settings,
      const std::unordered_map<std::string, ColumnSettings>& column_settings)
      : allocator_(allocator),
        dictionary_pagesize_limit_(dictionary_pagesize_limit),
        write_batch_size_(write_batch_size),
        pagesize_(pagesize),
        parquet_version_(version),
        parquet_created_by_(created_by),
        default_column_settings_(default_column_settings),
        column_settings_(column_settings) {}

  MemoryAllocator* allocator_;
  int64_t dictionary_pagesize_limit_;
  int64_t write_batch_size_;
  int64_t pagesize_;
  ParquetVersion::type parquet_version_;
  std::string parquet_created_by_;
  ColumnSettings default_column_settings_;
  std::unordered_map<std::string, ColumnSettings> column_settings_;
};

std::shared_ptr<WriterProperties> PARQUET_EXPORT default_writer_properties();

}  // namespace parquet

#endif  // PARQUET_COLUMN_PROPERTIES_H
