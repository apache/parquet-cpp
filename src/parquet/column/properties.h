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

#include "parquet/types.h"
#include "parquet/schema/types.h"
#include "parquet/util/input.h"
#include "parquet/util/mem-allocator.h"
#include "parquet/util/visibility.h"

namespace parquet {

struct ParquetVersion {
  enum type { PARQUET_1_0, PARQUET_2_0 };
};

struct ColumnEncodings {
  Encoding::type def_level_encoding;
  Encoding::type rep_level_encoding;
  Encoding::type dictionary_encoding;
  Encoding::type value_encoding;
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

static int64_t DEFAULT_PAGE_SIZE = 1024 * 1024;
static int64_t DEFAULT_DICTIONARY_PAGE_SIZE = DEFAULT_PAGE_SIZE;
static int64_t DEFAULT_DICTIONARY_SIZE_THRESHOLD = DEFAULT_PAGE_SIZE;
static Encoding::type DEFAULT_VALUE_ENCODING = Encoding::PLAIN_DICTIONARY;
static Encoding::type DEFAULT_DICTIONARY_ENCODING = Encoding::PLAIN_DICTIONARY;
static Encoding::type DEFAULT_LEVEL_ENCODING = Encoding::RLE;
static constexpr ParquetVersion::type DEFAULT_WRITER_VERSION =
    ParquetVersion::PARQUET_1_0;
static std::string DEFAULT_CREATED_BY = "Apache parquet-cpp";
static constexpr Compression::type DEFAULT_COMPRESSION_TYPE = Compression::UNCOMPRESSED;

using ColumnCodecs = std::unordered_map<std::string, Compression::type>;

class PARQUET_EXPORT WriterProperties {
 public:
  class Builder {
   public:
    Builder()
        : allocator_(default_allocator()),
          dictionary_pagesize_(DEFAULT_DICTIONARY_PAGE_SIZE),
          dictionary_size_threshold_(DEFAULT_DICTIONARY_SIZE_THRESHOLD),
          default_rep_level_encoding_(DEFAULT_LEVEL_ENCODING),
          default_def_level_encoding_(DEFAULT_LEVEL_ENCODING),
          default_dictionary_encoding_(DEFAULT_DICTIONARY_ENCODING),
          default_value_encoding_(DEFAULT_VALUE_ENCODING),
          pagesize_(DEFAULT_PAGE_SIZE),
          version_(DEFAULT_WRITER_VERSION),
          created_by_(DEFAULT_CREATED_BY),
          default_codec_(DEFAULT_COMPRESSION_TYPE) {}
    virtual ~Builder() {}

    Builder* allocator(MemoryAllocator* allocator) {
      allocator_ = allocator;
      return this;
    }

    Builder* dictionary_pagesize(int64_t dictionary_psize) {
      dictionary_pagesize_ = dictionary_psize;
      return this;
    }

    Builder* dictionary_size_threshold(int64_t dictionary_sizet) {
      dictionary_size_threshold_ = dictionary_sizet;
      return this;
    }

    Builder* data_pagesize(int64_t pg_size) {
      pagesize_ = pg_size;
      return this;
    }

    Builder* encodings(const std::shared_ptr<schema::ColumnPath>& path,
        Encoding::type rep_level_encoding, Encoding::type def_level_encoding) {
      return encodings(path->ToDotString(), rep_level_encoding, def_level_encoding);
    }

    Builder* encodings(const std::string& column_path, Encoding::type rep_level_encoding,
        Encoding::type def_level_encoding) {
      ColumnEncodings encodings;
      encodings.rep_level_encoding = rep_level_encoding;
      encodings.def_level_encoding = def_level_encoding;
      encodings.dictionary_encoding = default_dictionary_encoding_;
      encodings.value_encoding = default_value_encoding_;
      encodings_[column_path] = encodings;
      return this;
    }

    Builder* encodings(const std::shared_ptr<schema::ColumnPath>& path,
        Encoding::type rep_level_encoding, Encoding::type def_level_encoding,
        Encoding::type value_encoding) {
      return encodings(
          path->ToDotString(), rep_level_encoding, def_level_encoding, value_encoding);
    }

    Builder* encodings(const std::string& column_path, Encoding::type rep_level_encoding,
        Encoding::type def_level_encoding, Encoding::type value_encoding) {
      ColumnEncodings encodings;
      encodings.rep_level_encoding = rep_level_encoding;
      encodings.def_level_encoding = def_level_encoding;
      encodings.dictionary_encoding = default_dictionary_encoding_;
      encodings.value_encoding = value_encoding;
      encodings_[column_path] = encodings;
      return this;
    }

    Builder* value_encoding(Encoding::type encoding_type) {
      default_value_encoding_ = encoding_type;
      return this;
    }

    Builder* def_level_encoding(Encoding::type encoding_type) {
      default_def_level_encoding_ = encoding_type;
      return this;
    }

    Builder* rep_level_encoding(Encoding::type encoding_type) {
      default_rep_level_encoding_ = encoding_type;
      return this;
    }

    Builder* version(ParquetVersion::type version) {
      version_ = version;
      if (version == ParquetVersion::PARQUET_2_0) {
        default_value_encoding_ = Encoding::RLE_DICTIONARY;
        default_dictionary_encoding_ = Encoding::PLAIN;
      }
      return this;
    }

    Builder* created_by(const std::string& created_by) {
      created_by_ = created_by;
      return this;
    }

    Builder* compression(Compression::type codec) {
      default_codec_ = codec;
      return this;
    }

    Builder* compression(const std::string& path, Compression::type codec) {
      codecs_[path] = codec;
      return this;
    }

    Builder* compression(
        const std::shared_ptr<schema::ColumnPath>& path, Compression::type codec) {
      return this->compression(path->ToDotString(), codec);
    }

    std::shared_ptr<WriterProperties> build() {
      return std::shared_ptr<WriterProperties>(new WriterProperties(allocator_,
          dictionary_pagesize_, dictionary_size_threshold_, default_rep_level_encoding_,
          default_def_level_encoding_, default_dictionary_encoding_,
          default_value_encoding_, encodings_, pagesize_, version_, created_by_,
          default_codec_, codecs_));
    }

   private:
    MemoryAllocator* allocator_;
    int64_t dictionary_pagesize_;
    int64_t dictionary_size_threshold_;
    // Encoding used for each column if not a specialized one is defined as
    // part of encodings_
    Encoding::type default_rep_level_encoding_;
    Encoding::type default_def_level_encoding_;
    Encoding::type default_dictionary_encoding_;
    Encoding::type default_value_encoding_;
    std::unordered_map<std::string, ColumnEncodings> encodings_;
    int64_t pagesize_;
    ParquetVersion::type version_;
    std::string created_by_;
    // Default compression codec. This will be used for all columns that do
    // not have a specific codec set as part of codecs_
    Compression::type default_codec_;
    ColumnCodecs codecs_;
  };

  inline MemoryAllocator* allocator() const { return allocator_; }

  inline bool dictionary_size_threshold() const { return dictionary_size_threshold_; }

  inline int64_t dictionary_pagesize() const { return dictionary_pagesize_; }

  inline int64_t data_pagesize() const { return pagesize_; }

  inline ParquetVersion::type version() const { return parquet_version_; }

  inline std::string created_by() const { return parquet_created_by_; }

  inline const ColumnEncodings& encodings(
      const std::shared_ptr<schema::ColumnPath>& path) const {
    auto it = encodings_.find(path->ToDotString());
    if (it != encodings_.end()) { return it->second; }
    return default_encodings_;
  }

  inline Encoding::type rep_level_encoding(
      const std::shared_ptr<schema::ColumnPath>& path) const {
    auto it = encodings_.find(path->ToDotString());
    if (it != encodings_.end()) { return it->second.rep_level_encoding; }
    return default_rep_level_encoding_;
  }

  inline Encoding::type def_level_encoding(
      const std::shared_ptr<schema::ColumnPath>& path) const {
    auto it = encodings_.find(path->ToDotString());
    if (it != encodings_.end()) { return it->second.def_level_encoding; }
    return default_def_level_encoding_;
  }

  inline Encoding::type dictionary_encoding(
      const std::shared_ptr<schema::ColumnPath>& path) const {
    auto it = encodings_.find(path->ToDotString());
    if (it != encodings_.end()) { return it->second.dictionary_encoding; }
    return default_dictionary_encoding_;
  }

  inline Encoding::type value_encoding(
      const std::shared_ptr<schema::ColumnPath>& path) const {
    auto it = encodings_.find(path->ToDotString());
    if (it != encodings_.end()) { return it->second.value_encoding; }
    return default_value_encoding_;
  }

  inline Compression::type compression(
      const std::shared_ptr<schema::ColumnPath>& path) const {
    auto it = codecs_.find(path->ToDotString());
    if (it != codecs_.end()) return it->second;
    return default_codec_;
  }

 private:
  explicit WriterProperties(MemoryAllocator* allocator, int64_t dictionary_pagesize,
      int64_t dictionary_size_threshold, Encoding::type rep_level_encoding,
      Encoding::type def_level_encoding, Encoding::type dictionary_encoding,
      Encoding::type value_encoding,
      const std::unordered_map<std::string, ColumnEncodings>& encodings, int64_t pagesize,
      ParquetVersion::type version, const std::string& created_by,
      Compression::type default_codec, const ColumnCodecs& codecs)
      : allocator_(allocator),
        dictionary_pagesize_(dictionary_pagesize),
        default_rep_level_encoding_(rep_level_encoding),
        default_def_level_encoding_(def_level_encoding),
        default_dictionary_encoding_(dictionary_encoding),
        default_value_encoding_(value_encoding),
        encodings_(encodings),
        pagesize_(pagesize),
        parquet_version_(version),
        parquet_created_by_(created_by),
        default_codec_(default_codec),
        codecs_(codecs) {
    if (version == ParquetVersion::PARQUET_2_0) {
      default_value_encoding_ = Encoding::RLE_DICTIONARY;
      default_dictionary_encoding_ = Encoding::PLAIN;
    }
    pagesize_ = DEFAULT_PAGE_SIZE;
    default_encodings_.def_level_encoding = default_def_level_encoding_;
    default_encodings_.rep_level_encoding = default_rep_level_encoding_;
    default_encodings_.dictionary_encoding = default_dictionary_encoding_;
    default_encodings_.value_encoding = default_value_encoding_;
  }
  MemoryAllocator* allocator_;
  int64_t dictionary_pagesize_;
  int64_t dictionary_size_threshold_;
  Encoding::type default_rep_level_encoding_;
  Encoding::type default_def_level_encoding_;
  Encoding::type default_dictionary_encoding_;
  Encoding::type default_value_encoding_;
  std::unordered_map<std::string, ColumnEncodings> encodings_;
  int64_t pagesize_;
  ParquetVersion::type parquet_version_;
  std::string parquet_created_by_;
  Compression::type default_codec_;
  ColumnEncodings default_encodings_;
  ColumnCodecs codecs_;
};

std::shared_ptr<WriterProperties> PARQUET_EXPORT default_writer_properties();

}  // namespace parquet

#endif  // PARQUET_COLUMN_PROPERTIES_H
