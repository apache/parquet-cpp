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

#include "parquet/column/writer.h"

#include "parquet/encodings/plain-encoding.h"

namespace parquet {

// ----------------------------------------------------------------------
// ColumnWriter

ColumnWriter::ColumnWriter(const ColumnDescriptor* descr, std::unique_ptr<PageWriter> pager,
        MemoryAllocator* allocator) :  descr_(descr), pager_(std::move(pager)), allocator_(allocator),
        num_buffered_values_(0), num_buffered_encoded_values_(0), 
        // TODO: Get from WriterProperties
        num_buffered_values_next_size_check_(DEFAULT_MINIMUM_RECORD_COUNT_FOR_CHECK),
        num_rows_(0)
  {
  // TODO: Initialize level encoders already here
  InitSinks();
}

void ColumnWriter::InitSinks() {
  definition_levels_sink_ = std::unique_ptr<InMemoryOutputStream>(new InMemoryOutputStream()); 
  repetition_levels_sink_ = std::unique_ptr<InMemoryOutputStream>(new InMemoryOutputStream()); 
  values_sink_ = std::unique_ptr<InMemoryOutputStream>(new InMemoryOutputStream()); 
}

void ColumnWriter::WriteNewPage() {
  // TODO: Currently we only support writing DataPages
  std::shared_ptr<Buffer> definition_levels = definition_levels_sink_->GetBuffer();
  std::shared_ptr<Buffer> repetition_levels = repetition_levels_sink_->GetBuffer();
  std::shared_ptr<Buffer> values = values_sink_->GetBuffer();

  // TODO: Encodings are hard-coded
  pager_->WriteDataPage(num_buffered_values_, num_buffered_encoded_values_,
      // num_nulls = Difference of enocoded and actual values
      num_buffered_values_ - num_buffered_encoded_values_,
      definition_levels, Encoding::RLE,
      repetition_levels, Encoding::BIT_PACKED,
      values, Encoding::PLAIN);

  // Re-initialize the sinks as GetBuffer made them invalid.
  InitSinks();
}

void ColumnWriter::Close() {
  // TODO: no-op if already closed
  // TODO: Check if enough rows were written
 
  // Write all outstanding data to a new page 
  if (num_buffered_values_ > 0) {
    WriteNewPage();
  }

  pager_->Close();
}

// ----------------------------------------------------------------------
// TypedColumnWriter

template <int TYPE>
TypedColumnWriter<TYPE>::TypedColumnWriter(const ColumnDescriptor* schema,
      std::unique_ptr<PageWriter> pager, MemoryAllocator* allocator) :
      ColumnWriter(schema, std::move(pager), allocator) {
  // Get decoder type from WriterProperties
  current_encoder_ = std::unique_ptr<EncoderType>(new PlainEncoder<TYPE>(schema, allocator));
}

// ----------------------------------------------------------------------
// Dynamic column writer constructor

std::shared_ptr<ColumnWriter> ColumnWriter::Make(
    const ColumnDescriptor* descr,
    std::unique_ptr<PageWriter> pager,
    MemoryAllocator* allocator) {
  switch (descr->physical_type()) {
    case Type::BOOLEAN:
      return std::make_shared<BoolWriter>(descr, std::move(pager), allocator);
    case Type::INT32:
      return std::make_shared<Int32Writer>(descr, std::move(pager), allocator);
    case Type::INT64:
      return std::make_shared<Int64Writer>(descr, std::move(pager), allocator);
    case Type::INT96:
      return std::make_shared<Int96Writer>(descr, std::move(pager), allocator);
    case Type::FLOAT:
      return std::make_shared<FloatWriter>(descr, std::move(pager), allocator);
    case Type::DOUBLE:
      return std::make_shared<DoubleWriter>(descr, std::move(pager), allocator);
    case Type::BYTE_ARRAY:
      return std::make_shared<ByteArrayWriter>(descr, std::move(pager), allocator);
    case Type::FIXED_LEN_BYTE_ARRAY:
      return std::make_shared<FixedLenByteArrayWriter>(descr,
          std::move(pager), allocator);
    default:
      ParquetException::NYI("type reader not implemented");
  }
  // Unreachable code, but supress compiler warning
  return std::shared_ptr<ColumnWriter>(nullptr);
}



} // namespace parquet
