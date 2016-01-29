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

#ifndef PARQUET_COLUMN_SCANNER_H
#define PARQUET_COLUMN_SCANNER_H

#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "parquet/column_reader.h"
#include "parquet/thrift/parquet_types.h"

namespace parquet_cpp {

class ColumnReader;

template <int TYPE>
class TypedColumnReader;

class Scanner {
 public:
  explicit Scanner(std::shared_ptr<ColumnReader> reader) :
      reader_(reader),
      level_offset_(0),
      levels_buffered_(0),
      value_offset_(0),
      values_buffered_(0) {
    // TODO: don't allocate for required fields
    def_levels_.resize(BATCHSIZE);
    rep_levels_.resize(BATCHSIZE);
  }

  virtual void PrintNext(std::ostream& out, int width) = 0;

  bool HasNext() {
    return value_offset_ < values_buffered_ || reader_->HasNext();
  }

  bool NextLevels(int16_t* def_level, int16_t* rep_level) {
    if (level_offset_ == levels_buffered_) {
      levels_buffered_ = reader_->ReadDefinitionLevels(BATCHSIZE, &def_levels_[0]);

      // TODO: repetition levels

      level_offset_ = 0;
      if (!levels_buffered_) {
        return false;
      }
    }
    *def_level = def_levels_[level_offset_++];
    *rep_level = 1;
    return true;
  }

 protected:
  std::shared_ptr<ColumnReader> reader_;

  static constexpr size_t BATCHSIZE = 128;

  std::vector<int16_t> def_levels_;
  std::vector<int16_t> rep_levels_;
  size_t level_offset_;
  size_t levels_buffered_;

  std::vector<uint8_t> value_buffer_;
  size_t value_offset_;
  size_t values_buffered_;
};


template <int TYPE>
class TypedScanner : public Scanner {
 public:
  typedef typename type_traits<TYPE>::value_type T;

  explicit TypedScanner(std::shared_ptr<ColumnReader> reader) :
      Scanner(reader) {
    typed_reader_ = static_cast<TypedColumnReader<TYPE>*>(reader.get());
    size_t value_byte_size = type_traits<TYPE>::value_byte_size;
    value_buffer_.resize(BATCHSIZE * value_byte_size);
    values_ = reinterpret_cast<T*>(&value_buffer_[0]);
  }

  // Returns true if there is a next value
  bool NextValue(T* val, bool* is_null) {
    if (value_offset_ == values_buffered_) {
      if (!reader_->HasNext()) {
        // Out of data pages
        return false;
      }
    }

    // Out of values
    int16_t def_level;
    int16_t rep_level;
    NextLevels(&def_level, &rep_level);
    *is_null = def_level < rep_level;

    if (*is_null) {
      return true;
    }

    if (value_offset_ == values_buffered_) {
      // At this point, the ColumnReader has indicated there is more data
      // available, so we batch decode it here
      values_buffered_ = typed_reader_->ReadValues(BATCHSIZE, values_);
      value_offset_ = 0;
    }
    *val = values_[value_offset_++];
    return true;
  }

  virtual void PrintNext(std::ostream& out, int width) {
    T val;
    bool is_null;

    char buffer[25];
    NextValue(&val, &is_null);

    if (is_null) {
      std::string null_fmt = format_fwf<parquet::Type::BYTE_ARRAY>(width);
      snprintf(buffer, 25, null_fmt.c_str(), "NULL");
    } else {
      format_value<TYPE>(buffer, 25, width, &val);
    }
    out << buffer;
  }

 private:
  // The ownership of this object is expressed through the reader_ variable in the base
  TypedColumnReader<TYPE>* typed_reader_;

  T* values_;
};

typedef TypedScanner<parquet::Type::BOOLEAN> BoolScanner;
typedef TypedScanner<parquet::Type::INT32> Int32Scanner;
typedef TypedScanner<parquet::Type::INT64> Int64Scanner;
typedef TypedScanner<parquet::Type::INT96> Int96Scanner;
typedef TypedScanner<parquet::Type::FLOAT> FloatScanner;
typedef TypedScanner<parquet::Type::DOUBLE> DoubleScanner;
typedef TypedScanner<parquet::Type::BYTE_ARRAY> ByteArrayScanner;
// typedef TypedScanner<parquet::Type::FIXED_LEN_BYTE_ARRAY> FixedLenByteArrayScanner;

} // namespace parquet_cpp

#endif // PARQUET_COLUMN_SCANNER_H
