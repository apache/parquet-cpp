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

#include "parquet/column_reader.h"

#include <algorithm>
#include <cstdint>
#include <memory>

#include "arrow/util/rle-encoding.h"

#include "parquet/column_page.h"
#include "parquet/encoding-internal.h"
#include "parquet/properties.h"

using arrow::MemoryPool;

namespace parquet {

LevelDecoder::LevelDecoder() : num_values_remaining_(0) {}

LevelDecoder::~LevelDecoder() {}

int LevelDecoder::SetData(Encoding::type encoding, int16_t max_level,
    int num_buffered_values, const uint8_t* data) {
  int32_t num_bytes = 0;
  encoding_ = encoding;
  num_values_remaining_ = num_buffered_values;
  bit_width_ = BitUtil::Log2(max_level + 1);
  switch (encoding) {
    case Encoding::RLE: {
      num_bytes = *reinterpret_cast<const int32_t*>(data);
      const uint8_t* decoder_data = data + sizeof(int32_t);
      if (!rle_decoder_) {
        rle_decoder_.reset(new ::arrow::RleDecoder(decoder_data, num_bytes, bit_width_));
      } else {
        rle_decoder_->Reset(decoder_data, num_bytes, bit_width_);
      }
      return sizeof(int32_t) + num_bytes;
    }
    case Encoding::BIT_PACKED: {
      num_bytes =
          static_cast<int32_t>(BitUtil::Ceil(num_buffered_values * bit_width_, 8));
      if (!bit_packed_decoder_) {
        bit_packed_decoder_.reset(new ::arrow::BitReader(data, num_bytes));
      } else {
        bit_packed_decoder_->Reset(data, num_bytes);
      }
      return num_bytes;
    }
    default:
      throw ParquetException("Unknown encoding type for levels.");
  }
  return -1;
}

int LevelDecoder::Decode(int batch_size, int16_t* levels) {
  int num_decoded = 0;

  int num_values = std::min(num_values_remaining_, batch_size);
  if (encoding_ == Encoding::RLE) {
    num_decoded = rle_decoder_->GetBatch(levels, num_values);
  } else {
    num_decoded = bit_packed_decoder_->GetBatch(bit_width_, levels, num_values);
  }
  num_values_remaining_ -= num_decoded;
  return num_decoded;
}

ReaderProperties default_reader_properties() {
  static ReaderProperties default_reader_properties;
  return default_reader_properties;
}

ColumnReader::ColumnReader(
    const ColumnDescriptor* descr, std::unique_ptr<PageReader> pager, MemoryPool* pool)
    : descr_(descr),
      pager_(std::move(pager)),
      num_buffered_values_(0),
      num_decoded_values_(0),
      pool_(pool) {}

ColumnReader::~ColumnReader() {}

template <typename DType>
void TypedColumnReader<DType>::ConfigureDictionary(const DictionaryPage* page) {
  int encoding = static_cast<int>(page->encoding());
  if (page->encoding() == Encoding::PLAIN_DICTIONARY ||
      page->encoding() == Encoding::PLAIN) {
    encoding = static_cast<int>(Encoding::RLE_DICTIONARY);
  }

  auto it = decoders_.find(encoding);
  if (it != decoders_.end()) {
    throw ParquetException("Column cannot have more than one dictionary.");
  }

  if (page->encoding() == Encoding::PLAIN_DICTIONARY ||
      page->encoding() == Encoding::PLAIN) {
    PlainDecoder<DType> dictionary(descr_);
    dictionary.SetData(page->num_values(), page->data(), page->size());

    // The dictionary is fully decoded during DictionaryDecoder::Init, so the
    // DictionaryPage buffer is no longer required after this step
    //
    // TODO(wesm): investigate whether this all-or-nothing decoding of the
    // dictionary makes sense and whether performance can be improved

    auto decoder = std::make_shared<DictionaryDecoder<DType>>(descr_, pool_);
    decoder->SetDict(&dictionary);
    decoders_[encoding] = decoder;
  } else {
    ParquetException::NYI("only plain dictionary encoding has been implemented");
  }

  current_decoder_ = decoders_[encoding].get();
}

// PLAIN_DICTIONARY is deprecated but used to be used as a dictionary index
// encoding.
static bool IsDictionaryIndexEncoding(const Encoding::type& e) {
  return e == Encoding::RLE_DICTIONARY || e == Encoding::PLAIN_DICTIONARY;
}

template <typename DType>
bool TypedColumnReader<DType>::ReadNewPage() {
  // Loop until we find the next data page.
  const uint8_t* buffer;

  while (true) {
    current_page_ = pager_->NextPage();
    if (!current_page_) {
      // EOS
      return false;
    }

    if (current_page_->type() == PageType::DICTIONARY_PAGE) {
      ConfigureDictionary(static_cast<const DictionaryPage*>(current_page_.get()));
      continue;
    } else if (current_page_->type() == PageType::DATA_PAGE) {
      const DataPage* page = static_cast<const DataPage*>(current_page_.get());

      // Read a data page.
      num_buffered_values_ = page->num_values();

      // Have not decoded any values from the data page yet
      num_decoded_values_ = 0;

      buffer = page->data();

      // If the data page includes repetition and definition levels, we
      // initialize the level decoder and subtract the encoded level bytes from
      // the page size to determine the number of bytes in the encoded data.
      int64_t data_size = page->size();

      // Data page Layout: Repetition Levels - Definition Levels - encoded values.
      // Levels are encoded as rle or bit-packed.
      // Init repetition levels
      if (descr_->max_repetition_level() > 0) {
        int64_t rep_levels_bytes = repetition_level_decoder_.SetData(
            page->repetition_level_encoding(), descr_->max_repetition_level(),
            static_cast<int>(num_buffered_values_), buffer);
        buffer += rep_levels_bytes;
        data_size -= rep_levels_bytes;
      }
      // TODO figure a way to set max_definition_level_ to 0
      // if the initial value is invalid

      // Init definition levels
      if (descr_->max_definition_level() > 0) {
        int64_t def_levels_bytes = definition_level_decoder_.SetData(
            page->definition_level_encoding(), descr_->max_definition_level(),
            static_cast<int>(num_buffered_values_), buffer);
        buffer += def_levels_bytes;
        data_size -= def_levels_bytes;
      }

      // Get a decoder object for this page or create a new decoder if this is the
      // first page with this encoding.
      Encoding::type encoding = page->encoding();

      if (IsDictionaryIndexEncoding(encoding)) { encoding = Encoding::RLE_DICTIONARY; }

      auto it = decoders_.find(static_cast<int>(encoding));
      if (it != decoders_.end()) {
        if (encoding == Encoding::RLE_DICTIONARY) {
          DCHECK(current_decoder_->encoding() == Encoding::RLE_DICTIONARY);
        }
        current_decoder_ = it->second.get();
      } else {
        switch (encoding) {
          case Encoding::PLAIN: {
            std::shared_ptr<DecoderType> decoder(new PlainDecoder<DType>(descr_));
            decoders_[static_cast<int>(encoding)] = decoder;
            current_decoder_ = decoder.get();
            break;
          }
          case Encoding::RLE_DICTIONARY:
            throw ParquetException("Dictionary page must be before data page.");

          case Encoding::DELTA_BINARY_PACKED:
          case Encoding::DELTA_LENGTH_BYTE_ARRAY:
          case Encoding::DELTA_BYTE_ARRAY:
            ParquetException::NYI("Unsupported encoding");

          default:
            throw ParquetException("Unknown encoding type.");
        }
      }
      current_decoder_->SetData(
          static_cast<int>(num_buffered_values_), buffer, static_cast<int>(data_size));
      return true;
    } else {
      // We don't know what this page type is. We're allowed to skip non-data
      // pages.
      continue;
    }
  }
  return true;
}

// ----------------------------------------------------------------------
// Batch read APIs

int64_t ColumnReader::ReadDefinitionLevels(int64_t batch_size, int16_t* levels) {
  if (descr_->max_definition_level() == 0) { return 0; }
  return definition_level_decoder_.Decode(static_cast<int>(batch_size), levels);
}

int64_t ColumnReader::ReadRepetitionLevels(int64_t batch_size, int16_t* levels) {
  if (descr_->max_repetition_level() == 0) { return 0; }
  return repetition_level_decoder_.Decode(static_cast<int>(batch_size), levels);
}

// ----------------------------------------------------------------------
// Dynamic column reader constructor

std::shared_ptr<ColumnReader> ColumnReader::Make(
    const ColumnDescriptor* descr, std::unique_ptr<PageReader> pager, MemoryPool* pool) {
  switch (descr->physical_type()) {
    case Type::BOOLEAN:
      return std::make_shared<BoolReader>(descr, std::move(pager), pool);
    case Type::INT32:
      return std::make_shared<Int32Reader>(descr, std::move(pager), pool);
    case Type::INT64:
      return std::make_shared<Int64Reader>(descr, std::move(pager), pool);
    case Type::INT96:
      return std::make_shared<Int96Reader>(descr, std::move(pager), pool);
    case Type::FLOAT:
      return std::make_shared<FloatReader>(descr, std::move(pager), pool);
    case Type::DOUBLE:
      return std::make_shared<DoubleReader>(descr, std::move(pager), pool);
    case Type::BYTE_ARRAY:
      return std::make_shared<ByteArrayReader>(descr, std::move(pager), pool);
    case Type::FIXED_LEN_BYTE_ARRAY:
      return std::make_shared<FixedLenByteArrayReader>(descr, std::move(pager), pool);
    default:
      ParquetException::NYI("type reader not implemented");
  }
  // Unreachable code, but supress compiler warning
  return std::shared_ptr<ColumnReader>(nullptr);
}

// ----------------------------------------------------------------------
// Instantiate templated classes

template class PARQUET_TEMPLATE_EXPORT TypedColumnReader<BooleanType>;
template class PARQUET_TEMPLATE_EXPORT TypedColumnReader<Int32Type>;
template class PARQUET_TEMPLATE_EXPORT TypedColumnReader<Int64Type>;
template class PARQUET_TEMPLATE_EXPORT TypedColumnReader<Int96Type>;
template class PARQUET_TEMPLATE_EXPORT TypedColumnReader<FloatType>;
template class PARQUET_TEMPLATE_EXPORT TypedColumnReader<DoubleType>;
template class PARQUET_TEMPLATE_EXPORT TypedColumnReader<ByteArrayType>;
template class PARQUET_TEMPLATE_EXPORT TypedColumnReader<FLBAType>;

}  // namespace parquet
