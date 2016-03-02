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

#ifndef PARQUET_COLUMN_TEST_UTIL_H
#define PARQUET_COLUMN_TEST_UTIL_H

#include <algorithm>
#include <limits>
#include <memory>
#include <vector>
#include <string>

#include "parquet/column/levels.h"
#include "parquet/column/page.h"

// Depended on by SerializedPageReader test utilities for now
#include "parquet/encodings/plain-encoding.h"
#include "parquet/encodings/dictionary-encoding.h"
#include "parquet/util/input.h"
#include "parquet/util/test-common.h"

using std::vector;
using std::shared_ptr;

namespace parquet_cpp {

namespace test {

class MockPageReader : public PageReader {
 public:
  explicit MockPageReader(const vector<shared_ptr<Page> >& pages) :
      pages_(pages),
      page_index_(0) {}

  // Implement the PageReader interface
  virtual shared_ptr<Page> NextPage() {
    if (page_index_ == static_cast<int>(pages_.size())) {
      // EOS to consumer
      return shared_ptr<Page>(nullptr);
    }
    return pages_[page_index_++];
  }

 private:
  vector<shared_ptr<Page> > pages_;
  int page_index_;
};

// TODO(wesm): this is only used for testing for now. Refactor to form part of
// primary file write path
template <typename Type>
class DataPageBuilder {
 public:
  typedef typename Type::c_type T;

  // This class writes data and metadata to the passed inputs
  explicit DataPageBuilder(InMemoryOutputStream* sink) :
      sink_(sink),
      num_values_(0),
      encoding_(Encoding::PLAIN),
      definition_level_encoding_(Encoding::RLE),
      repetition_level_encoding_(Encoding::RLE),
      have_def_levels_(false),
      have_rep_levels_(false),
      have_values_(false) {
  }

  void AppendDefLevels(const vector<int16_t>& levels, int16_t max_level,
      Encoding::type encoding = Encoding::RLE) {
    AppendLevels(levels, max_level, encoding);

    num_values_ = std::max(static_cast<int32_t>(levels.size()), num_values_);
    definition_level_encoding_ = encoding;
    have_def_levels_ = true;
  }

  void AppendRepLevels(const vector<int16_t>& levels, int16_t max_level,
      Encoding::type encoding = Encoding::RLE) {
    AppendLevels(levels, max_level, encoding);

    num_values_ = std::max(static_cast<int32_t>(levels.size()), num_values_);
    repetition_level_encoding_ = encoding;
    have_rep_levels_ = true;
  }

  void AppendValues(const ColumnDescriptor *d, const vector<T>& values,
      Encoding::type encoding = Encoding::PLAIN) {
    PlainEncoder<Type::type_num> encoder(d);
    encoder.Encode(&values[0], values.size(), sink_);

    num_values_ = std::max(static_cast<int32_t>(values.size()), num_values_);
    encoding_ = encoding;
    have_values_ = true;
  }

  int32_t num_values() const {
    return num_values_;
  }

  Encoding::type encoding() const {
    return encoding_;
  }

  Encoding::type rep_level_encoding() const {
    return repetition_level_encoding_;
  }

  Encoding::type def_level_encoding() const {
    return definition_level_encoding_;
  }

 private:
  InMemoryOutputStream* sink_;

  int32_t num_values_;
  Encoding::type encoding_;
  Encoding::type definition_level_encoding_;
  Encoding::type repetition_level_encoding_;

  bool have_def_levels_;
  bool have_rep_levels_;
  bool have_values_;

  // Used internally for both repetition and definition levels
  void AppendLevels(const vector<int16_t>& levels, int16_t max_level,
      Encoding::type encoding) {
    if (encoding != Encoding::RLE) {
      ParquetException::NYI("only rle encoding currently implemented");
    }

    // TODO: compute a more precise maximum size for the encoded levels
    vector<uint8_t> encode_buffer(levels.size() * 2);

    // We encode into separate memory from the output stream because the
    // RLE-encoded bytes have to be preceded in the stream by their absolute
    // size.
    LevelEncoder encoder;
    encoder.Init(encoding, max_level, levels.size(),
        encode_buffer.data(), encode_buffer.size());

    encoder.Encode(levels.size(), levels.data());

    uint32_t rle_bytes = encoder.len();
    sink_->Write(reinterpret_cast<const uint8_t*>(&rle_bytes), sizeof(uint32_t));
    sink_->Write(encode_buffer.data(), rle_bytes);
  }
};

template <typename TYPE>
class DictionaryPageBuilder {
 public:
  typedef typename TYPE::c_type TC;
  static constexpr int TN = TYPE::type_num;

  // This class writes data and metadata to the passed inputs
  explicit DictionaryPageBuilder(InMemoryOutputStream* sink) :
      sink_(sink),
      num_dict_values_(0),
      encoding_(Encoding::RLE_DICTIONARY),
      have_values_(false) {
  }

  void AppendValues(const ColumnDescriptor *d, const vector<TC>& values,
      Encoding::type encoding = Encoding::RLE_DICTIONARY) {
    int type_length = 0;
    dict_buffer_ = std::make_shared<OwnedMutableBuffer>();
    rle_indices_ = std::make_shared<OwnedMutableBuffer>();
    if (TN == Type::FIXED_LEN_BYTE_ARRAY) {
      type_length = d->type_length();
    }
    int num_values = values.size();
    DictEncoder<TC> encoder(&pool_, type_length);
    for (int i = 0; i < num_values; ++i) {
      encoder.Put(values[i]);
    }
    // Dictionary encoding
    dict_buffer_->Resize(encoder.dict_encoded_size());
    encoder.WriteDict(dict_buffer_->mutable_data());
    rle_indices_->Resize(4*encoder.EstimatedDataEncodedSize());
    int actual_bytes = encoder.WriteIndices(rle_indices_->mutable_data(),
      rle_indices_->size());
    rle_indices_->Resize(actual_bytes);
    num_dict_values_ = dict_buffer_->size() / sizeof(TC);

    sink_->Write(reinterpret_cast<const uint8_t*>(dict_buffer_->data()),
        dict_buffer_->size());

    encoding_ = encoding;
    have_values_ = true;
  }

  int32_t num_values() const {
    return num_dict_values_;
  }

  Encoding::type encoding() const {
    return encoding_;
  }

  const uint8_t* rle_indices() {
    return rle_indices_->data();
  }

  int32_t rle_indices_size() const {
    return rle_indices_->size();
  }

  void free_pool() {
    pool_.FreeAll();
  }

 private:
  InMemoryOutputStream* sink_;
  shared_ptr<OwnedMutableBuffer> rle_indices_;
  shared_ptr<OwnedMutableBuffer> dict_buffer_;
  MemPool pool_;

  int32_t num_dict_values_;
  Encoding::type encoding_;

  bool have_values_;
};

template <typename Type>
static shared_ptr<DictionaryPage> MakeDictPage(const ColumnDescriptor *d,
    const vector<typename Type::c_type>& values, Encoding::type encoding,
    vector<uint8_t>& rle_indices) {
  InMemoryOutputStream page_stream;
  test::DictionaryPageBuilder<Type> page_builder(&page_stream);

  page_builder.AppendValues(d, values, encoding);

  rle_indices.assign(page_builder.rle_indices(),
      page_builder.rle_indices() + page_builder.rle_indices_size());

  auto buffer = page_stream.GetBuffer();

  page_builder.free_pool();
  return std::make_shared<DictionaryPage>(buffer, page_builder.num_values(),
      page_builder.encoding());
}

template<>
void DictionaryPageBuilder<BooleanType>::AppendValues(const ColumnDescriptor *d,
    const vector<bool>& values, Encoding::type encoding) {
  ParquetException::NYI("only plain encoding currently implemented");
}

template<>
void DataPageBuilder<BooleanType>::AppendValues(const ColumnDescriptor *d,
    const vector<bool>& values, Encoding::type encoding) {
  if (encoding != Encoding::PLAIN) {
    ParquetException::NYI("only plain encoding currently implemented");
  }
  PlainEncoder<Type::BOOLEAN> encoder(d);
  encoder.Encode(values, values.size(), sink_);

  num_values_ = std::max(static_cast<int32_t>(values.size()), num_values_);
  encoding_ = encoding;
  have_values_ = true;
}

template <typename Type>
static shared_ptr<DataPage> MakeDataPage(const ColumnDescriptor *d,
    const vector<typename Type::c_type>& values, int num_vals,
    Encoding::type encoding, const uint8_t* indices, int indices_size,
    const vector<int16_t>& def_levels, int16_t max_def_level,
    const vector<int16_t>& rep_levels, int16_t max_rep_level) {
  int num_values = 0;

  InMemoryOutputStream page_stream;
  test::DataPageBuilder<Type> page_builder(&page_stream);

  if (!rep_levels.empty()) {
    page_builder.AppendRepLevels(rep_levels, max_rep_level);
  }
  if (!def_levels.empty()) {
    page_builder.AppendDefLevels(def_levels, max_def_level);
  }

  if (encoding == Encoding::PLAIN) {
    page_builder.AppendValues(d, values, encoding);
    num_values = page_builder.num_values();
  } else {//DICTIONARY PAGES
    page_stream.Write(indices, indices_size);
    num_values = std::max(page_builder.num_values(), num_vals);
  }

  auto buffer = page_stream.GetBuffer();

  return std::make_shared<DataPage>(buffer, num_values,
      encoding,
      page_builder.def_level_encoding(),
      page_builder.rep_level_encoding());
}



// Given def/rep levels and values create multiple pages
template <typename Type>
static void Paginate(const ColumnDescriptor *d,
    const vector<typename Type::c_type>& values,
    const vector<int16_t>& def_levels, int16_t max_def_level,
    const vector<int16_t>& rep_levels, int16_t max_rep_level,
    int num_levels_per_page, const vector<int>& values_per_page,
    vector<shared_ptr<Page> >& pages,
    Encoding::type encoding = Encoding::PLAIN) {
  int num_pages = values_per_page.size();
  int def_level_start = 0;
  int def_level_end = 0;
  int rep_level_start = 0;
  int rep_level_end = 0;
  int value_start = 0;
  for (int i = 0; i < num_pages; i++) {
    if (max_def_level > 0) {
      def_level_start = i * num_levels_per_page;
      def_level_end = (i + 1) * num_levels_per_page;
    }
    if (max_rep_level > 0) {
      rep_level_start = i * num_levels_per_page;
      rep_level_end = (i + 1) * num_levels_per_page;
    }
    shared_ptr<DataPage> page = MakeDataPage<Type>(d,
        slice(values, value_start, value_start + values_per_page[i]), values_per_page[i],
        encoding, NULL, 0,
        slice(def_levels, def_level_start, def_level_end), max_def_level,
        slice(rep_levels, rep_level_start, rep_level_end), max_rep_level);
    pages.push_back(page);
    value_start += values_per_page[i];
  }
}

template <typename T>
static void InitValues(int num_values, vector<T>& values,
    vector<uint8_t>& buffer) {
  random_numbers(num_values, 0, std::numeric_limits<T>::min(),
      std::numeric_limits<T>::max(), values.data());
}

template<>
void InitValues(int num_values,  vector<bool>& values,
     vector<uint8_t>& buffer) {
  values = flip_coins(num_values, 0);
}

template<>
void InitValues(int num_values, vector<Int96>& values,
    vector<uint8_t>& buffer) {
  random_Int96_numbers(num_values, 0, std::numeric_limits<int32_t>::min(),
      std::numeric_limits<int32_t>::max(), values.data());
}

template<>
void InitValues(int num_values, vector<ByteArray>& values,
    vector<uint8_t>& buffer) {
  int max_byte_array_len = 12;
  int num_bytes = max_byte_array_len + sizeof(uint32_t);
  size_t nbytes = num_values * num_bytes;
  buffer.resize(nbytes);
  random_byte_array(num_values, 0, buffer.data(), values.data(),
      max_byte_array_len);
}

template<>
void InitValues(int num_values, vector<FLBA>& values,
    vector<uint8_t>& buffer) {
  int flba_length = 12;
  size_t nbytes = num_values * flba_length;
  buffer.resize(nbytes);
  random_fixed_byte_array(num_values, 0, buffer.data(), flba_length,
      values.data());
}

template <typename T>
static void InitDictValues(int num_values, int dict_per_page,
    vector<T>& values, vector<uint8_t>& buffer) {
  int repeat_factor = num_values / dict_per_page;
  InitValues<T>(dict_per_page, values, buffer);
  // add some repeated values
  for (int j = 1; j < repeat_factor; ++j) {
    for (int i = 0; i < dict_per_page; ++i) {
      values[dict_per_page * j + i] = values[i];
    }
  }
  // computed only dict_per_page * repeat_factor - 1 values < num_values
  // compute remaining
  for (int i = dict_per_page * repeat_factor; i < num_values; ++i) {
    values[i] = values[i - dict_per_page * repeat_factor];
  }
}

// Generates plain pages from randomly generated data
template <typename Type>
static int MakePages(const ColumnDescriptor *d, int num_pages, int levels_per_page,
    vector<int16_t>& def_levels, vector<int16_t>& rep_levels,
    vector<typename Type::c_type>& values, vector<uint8_t>& buffer,
    vector<shared_ptr<Page> >& pages,
    Encoding::type encoding = Encoding::PLAIN) {
  int num_levels = levels_per_page * num_pages;
  int num_values = 0;
  uint32_t seed = 0;
  int16_t zero = 0;
  int16_t max_def_level = d->max_definition_level();
  int16_t max_rep_level = d->max_repetition_level();
  vector<int> values_per_page(num_pages, levels_per_page);
  // Create definition levels
  if (max_def_level > 0) {
    def_levels.resize(num_levels);
    random_numbers(num_levels, seed, zero, max_def_level, def_levels.data());
    for (int p = 0; p < num_pages; p++) {
      int num_values_per_page = 0;
      for (int i = 0; i < levels_per_page; i++) {
        if (def_levels[i + p * levels_per_page] == max_def_level) {
          num_values_per_page++;
          num_values++;
        }
      }
      values_per_page[p] = num_values_per_page;
    }
  } else {
    num_values = num_levels;
  }
  // Create repitition levels
  if (max_rep_level > 0) {
    rep_levels.resize(num_levels);
    random_numbers(num_levels, seed, zero, max_rep_level, rep_levels.data());
  }
  // Create values
  values.resize(num_values);
  if (encoding == Encoding::PLAIN) {
    InitValues<typename Type::c_type>(num_values, values, buffer);
    Paginate<Type>(d, values, def_levels, max_def_level,
        rep_levels, max_rep_level, levels_per_page, values_per_page, pages);
  } else if (encoding == Encoding::RLE_DICTIONARY) {
    // Calls InitValues and repeats the data
    InitDictValues<typename Type::c_type>(num_values, levels_per_page, values, buffer);
    vector<uint8_t> rle_indices;

    shared_ptr<DictionaryPage> dict_page = MakeDictPage<Type>(d, values, encoding,
        rle_indices);
    pages.push_back(dict_page);

    shared_ptr<DataPage> data_page = MakeDataPage<Int32Type>(d, {}, num_values,
        encoding, rle_indices.data(), rle_indices.size(),
        def_levels, d->max_definition_level(),
        rep_levels, d->max_repetition_level());
    pages.push_back(data_page);
  }

  return num_values;
}

} // namespace test

} // namespace parquet_cpp

#endif // PARQUET_COLUMN_TEST_UTIL_H
