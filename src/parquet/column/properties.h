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

#include "parquet/util/input.h"
#include "parquet/util/mem-allocator.h"
#include "parquet/types.h"

namespace parquet {

static int DEFAULT_PAGE_SIZE = 1024 * 1024;
static int DEFAULT_DICTIONARY_PAGE_SIZE = DEFAULT_PAGE_SIZE;
static bool DEFAULT_IS_DICTIONARY_ENABLED = true;

class ReaderProperties {
 public:
  ReaderProperties(bool use_buffered_stream = false, int64_t buffer_size = 0,
      MemoryAllocator* allocator = default_allocator()) {
    use_buffered_stream_ = use_buffered_stream;
    buffer_size_ = buffer_size;
    allocator_ = allocator;
  }

  static inline std::string type_printer(Type::type parquet_type, const char* val,
      int length) {
    std::stringstream result;
    switch (parquet_type) {
      case Type::BOOLEAN:
        result << reinterpret_cast<const bool*>(val)[0];
        break;
      case Type::INT32:
        result << reinterpret_cast<const int32_t*>(val)[0];
        break;
      case Type::INT64:
        result << reinterpret_cast<const int64_t*>(val)[0];
        break;
      case Type::DOUBLE:
        result << reinterpret_cast<const double*>(val)[0];
        break;
      case Type::FLOAT:
        result << reinterpret_cast<const float*>(val)[0];
        break;
      case Type::INT96: {
        for (int i = 0; i < 3; i++) {
          result << reinterpret_cast<const int32_t*>(val)[i] << " ";
        }
        break;
      }
      case Type::BYTE_ARRAY: {
        const ByteArray* a = reinterpret_cast<const ByteArray*>(val);
        for (int i = 0; i < static_cast<int>(a->len); i++) {
          result << a[0].ptr[i]  << " ";
        }
        break;
      }
      case Type::FIXED_LEN_BYTE_ARRAY: {
        const FLBA* a = reinterpret_cast<const FLBA*>(val);
        for (int i = 0; i < length; i++) {
          result << a[0].ptr[i]  << " ";
        }
        break;
      }
      default:
        break;
    }
    return result.str();
  }

  MemoryAllocator* get_allocator() {
    return allocator_;
  }

  std::unique_ptr<InputStream> GetInputStream() {
    std::unique_ptr<InputStream> stream;
    if (use_buffered_stream_) {
      stream.reset(new BufferedInputStream(allocator_, buffer_size_));
    } else {
      stream.reset(new InMemoryInputStream());
    }
    return stream;
  }

  bool use_buffered_stream() {
    return use_buffered_stream_;
  }

  int64_t get_buffer_size() {
    return buffer_size_;
  }

 private:
  MemoryAllocator* allocator_;
  bool use_buffered_stream_;
  int64_t buffer_size_;
};

class WriterProperties {
 public:
  WriterProperties(int pagesize = DEFAULT_PAGE_SIZE,
      int dictionary_pagesize = DEFAULT_DICTIONARY_PAGE_SIZE,
      bool enable_dictionary = DEFAULT_IS_DICTIONARY_ENABLED,
      MemoryAllocator* allocator = default_allocator()) {
    pagesize_ = pagesize;
    dictionary_pagesize_ = dictionary_pagesize;
    enable_dictionary_ = enable_dictionary;
    allocator_ = allocator;
  }

  int get_dictionary_pagesize_threshold() {
    return dictionary_pagesize_;
  }

  int get_pagesize_threshold() {
    return pagesize_;
  }

  bool is_enable_dictionary() {
    return enable_dictionary_;
  }

  MemoryAllocator* get_allocator() {
    return allocator_;
  }

 private:
  int pagesize_;
  int dictionary_pagesize_;
  bool enable_dictionary_;
  MemoryAllocator* allocator_;
};

} // namespace parquet

#endif // PARQUET_COLUMN_PROPERTIES_H
