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

#ifndef PARQUET_COLUMN_READER_H
#define PARQUET_COLUMN_READER_H

#include <exception>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "parquet/exception.h"
#include "parquet/types.h"
#include "parquet/thrift/parquet_constants.h"
#include "parquet/thrift/parquet_types.h"
#include "parquet/util/input_stream.h"
#include "parquet/encodings/encodings.h"
#include "parquet/util/rle-encoding.h"

namespace std {

template <>
struct hash<parquet::Encoding::type> {
  std::size_t operator()(const parquet::Encoding::type& k) const {
    return hash<int>()(static_cast<int>(k));
  }
};

} // namespace std

namespace parquet_cpp {

class Codec;

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

class Scanner;

class ColumnReader {
 public:

  struct Config {
    int batch_size;

    static Config DefaultConfig() {
      Config config;
      config.batch_size = 128;
      return config;
    }
  };

  ColumnReader(const parquet::ColumnMetaData*, const parquet::SchemaElement*,
      std::unique_ptr<InputStream> stream);

  static std::shared_ptr<ColumnReader> Make(const parquet::ColumnMetaData*,
      const parquet::SchemaElement*, std::unique_ptr<InputStream> stream);

  virtual bool ReadNewPage() = 0;

  virtual std::shared_ptr<Scanner> GetScanner() = 0;

  // Returns true if there are still values in this column.
  bool HasNext() {
    // Either there is no data page available yet, or the data page has been
    // exhausted
    if (!num_buffered_values_ || num_decoded_values_ == num_buffered_values_) {
      if (!ReadNewPage() || num_buffered_values_ == 0) {
        return false;
      }
    }
    return true;
  }

  // Read multiple definition levels into preallocated memory
  //
  // Returns the number of decoded definition levels
  size_t ReadDefinitionLevels(size_t batch_size, int16_t* levels) {
    if (!definition_level_decoder_) {
      return 0;
    }
    return DecodeMany(definition_level_decoder_.get(), levels, batch_size);
  }

  // Read multiple repetition levels into preallocated memory
  //
  // Returns the number of decoded repetition levels
  size_t ReadRepetitionLevels(size_t batch_size, int16_t* levels) {
    if (!repetition_level_decoder_) {
      return 0;
    }
    return DecodeMany(repetition_level_decoder_.get(), levels, batch_size);
  }

  parquet::Type::type type() const {
    return metadata_->type;
  }

  const parquet::ColumnMetaData* metadata() const {
    return metadata_;
  }

 protected:
  Config config_;

  const parquet::ColumnMetaData* metadata_;
  const parquet::SchemaElement* schema_;
  std::unique_ptr<InputStream> stream_;

  // Compression codec to use.
  std::unique_ptr<Codec> decompressor_;
  std::vector<uint8_t> decompression_buffer_;

  parquet::PageHeader current_page_header_;

  // Not set if field is required (flat schemas), or if the whole schema tree
  // contains no optional elements
  std::unique_ptr<RleDecoder> definition_level_decoder_;

  // Not set for flat schemas.
  std::unique_ptr<RleDecoder> repetition_level_decoder_;

  // The total number of data values stored in the data page.
  int num_buffered_values_;

  // The number of values from the current data page that have been decoded
  // into memory
  int num_decoded_values_;
};

// API to read values from a single column. This is the main client facing API.
template <int TYPE>
class TypedColumnReader
    : public ColumnReader, std::enable_shared_from_this<TypedColumnReader<TYPE>> {
 public:
  typedef typename type_traits<TYPE>::value_type T;

  TypedColumnReader(const parquet::ColumnMetaData* metadata,
      const parquet::SchemaElement* schema, std::unique_ptr<InputStream> stream) :
      ColumnReader(metadata, schema, std::move(stream)),
      current_decoder_(NULL) {
    size_t value_byte_size = type_traits<TYPE>::value_byte_size;
    values_buffer_.resize(config_.batch_size * value_byte_size);
  }

  // Advance to the next data page
  virtual bool ReadNewPage();

  virtual std::shared_ptr<Scanner> GetScanner();

 private:
  typedef Decoder<TYPE> DecoderType;

  // Map of compression type to decompressor object.
  std::unordered_map<parquet::Encoding::type, std::shared_ptr<DecoderType> > decoders_;

  DecoderType* current_decoder_;
  std::vector<uint8_t> values_buffer_;
};


typedef TypedColumnReader<parquet::Type::BOOLEAN> BoolReader;
typedef TypedColumnReader<parquet::Type::INT32> Int32Reader;
typedef TypedColumnReader<parquet::Type::INT64> Int64Reader;
typedef TypedColumnReader<parquet::Type::INT96> Int96Reader;
typedef TypedColumnReader<parquet::Type::FLOAT> FloatReader;
typedef TypedColumnReader<parquet::Type::DOUBLE> DoubleReader;
typedef TypedColumnReader<parquet::Type::BYTE_ARRAY> ByteArrayReader;
typedef TypedColumnReader<parquet::Type::FIXED_LEN_BYTE_ARRAY> FixedLenByteArrayReader;

} // namespace parquet_cpp

#endif // PARQUET_COLUMN_READER_H
