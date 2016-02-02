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

#include "parquet/column/reader.h"

#include <algorithm>
#include <memory>
#include <string>
#include <string.h>

#include "parquet/column/page.h"

#include "parquet/encodings/encodings.h"

namespace parquet_cpp {

using parquet::Encoding;
using parquet::FieldRepetitionType;
using parquet::PageType;
using parquet::Type;

ColumnReader::ColumnReader(const parquet::SchemaElement* schema,
    std::unique_ptr<PageReader> pager)
  : schema_(schema),
    pager_(std::move(pager)),
    num_buffered_values_(0),
    num_decoded_values_(0) {}

template <int TYPE>
void TypedColumnReader<TYPE>::ConfigureDictionary(const DictionaryPage* page) {
  auto it = decoders_.find(Encoding::RLE_DICTIONARY);
  if (it != decoders_.end()) {
    throw ParquetException("Column cannot have more than one dictionary.");
  }

  PlainDecoder<TYPE> dictionary(schema_);
  dictionary.SetData(page->num_values(), page->data(), page->size());

  // The dictionary is fully decoded during DictionaryDecoder::Init, so the
  // DictionaryPage buffer is no longer required after this step
  //
  // TODO(wesm): investigate whether this all-or-nothing decoding of the
  // dictionary makes sense and whether performance can be improved
  std::shared_ptr<DecoderType> decoder(
      new DictionaryDecoder<TYPE>(schema_, &dictionary));

  decoders_[Encoding::RLE_DICTIONARY] = decoder;
  current_decoder_ = decoders_[Encoding::RLE_DICTIONARY].get();
}


static size_t InitializeLevelDecoder(const uint8_t* buffer,
    int16_t max_level, std::unique_ptr<RleDecoder>& decoder) {
  int num_definition_bytes = *reinterpret_cast<const uint32_t*>(buffer);

  decoder.reset(new RleDecoder(buffer + sizeof(uint32_t),
          num_definition_bytes,
          BitUtil::NumRequiredBits(max_level)));

  return sizeof(uint32_t) + num_definition_bytes;
}

// PLAIN_DICTIONARY is deprecated but used to be used as a dictionary index
// encoding.
static bool IsDictionaryIndexEncoding(const Encoding::type& e) {
  return e == Encoding::RLE_DICTIONARY || e == Encoding::PLAIN_DICTIONARY;
}

template <int TYPE>
bool TypedColumnReader<TYPE>::ReadNewPage() {
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
      size_t data_size = page->size();

      // Read definition levels.
      if (schema_->repetition_type != FieldRepetitionType::REQUIRED) {
        // Temporary hack until schema resolution implemented
        max_definition_level_ = 1;

        size_t def_levels_bytes = InitializeLevelDecoder(buffer,
            max_definition_level_, definition_level_decoder_);

        buffer += def_levels_bytes;
        data_size -= def_levels_bytes;
      } else {
        // REQUIRED field
        max_definition_level_ = 0;
      }

      // TODO: repetition levels

      // Get a decoder object for this page or create a new decoder if this is the
      // first page with this encoding.
      Encoding::type encoding = page->encoding();

      if (IsDictionaryIndexEncoding(encoding)) encoding = Encoding::RLE_DICTIONARY;

      auto it = decoders_.find(encoding);
      if (it != decoders_.end()) {
        current_decoder_ = it->second.get();
      } else {
        switch (encoding) {
          case Encoding::PLAIN: {
            std::shared_ptr<DecoderType> decoder(new PlainDecoder<TYPE>(schema_));
            decoders_[encoding] = decoder;
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
      current_decoder_->SetData(num_buffered_values_, buffer, data_size);
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

static size_t DecodeMany(RleDecoder* decoder, int16_t* levels, size_t batch_size) {
  size_t num_decoded = 0;

  // TODO(wesm): Push this decoding down into RleDecoder itself
  for (size_t i = 0; i < batch_size; ++i) {
    if (!decoder->Get(levels + i)) {
      break;
    }
    ++num_decoded;
  }
  return num_decoded;
}

size_t ColumnReader::ReadDefinitionLevels(size_t batch_size, int16_t* levels) {
  if (!definition_level_decoder_) {
    return 0;
  }
  return DecodeMany(definition_level_decoder_.get(), levels, batch_size);
}

size_t ColumnReader::ReadRepetitionLevels(size_t batch_size, int16_t* levels) {
  if (!repetition_level_decoder_) {
    return 0;
  }
  return DecodeMany(repetition_level_decoder_.get(), levels, batch_size);
}

// ----------------------------------------------------------------------
// Dynamic column reader constructor

std::shared_ptr<ColumnReader> ColumnReader::Make(
    const parquet::SchemaElement* element,
    std::unique_ptr<PageReader> pager) {
  switch (element->type) {
    case Type::BOOLEAN:
      return std::make_shared<BoolReader>(element, std::move(pager));
    case Type::INT32:
      return std::make_shared<Int32Reader>(element, std::move(pager));
    case Type::INT64:
      return std::make_shared<Int64Reader>(element, std::move(pager));
    case Type::INT96:
      return std::make_shared<Int96Reader>(element, std::move(pager));
    case Type::FLOAT:
      return std::make_shared<FloatReader>(element, std::move(pager));
    case Type::DOUBLE:
      return std::make_shared<DoubleReader>(element, std::move(pager));
    case Type::BYTE_ARRAY:
      return std::make_shared<ByteArrayReader>(element, std::move(pager));
    case Type::FIXED_LEN_BYTE_ARRAY:
      return std::make_shared<FixedLenByteArrayReader>(element, std::move(pager));
    default:
      ParquetException::NYI("type reader not implemented");
  }
  // Unreachable code, but supress compiler warning
  return std::shared_ptr<ColumnReader>(nullptr);
}

} // namespace parquet_cpp
