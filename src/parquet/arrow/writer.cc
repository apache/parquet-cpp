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

#include "parquet/arrow/writer.h"

#include <algorithm>

#include "parquet/util/bit-util.h"
#include "parquet/util/logging.h"

#include "parquet/arrow/schema.h"

#include "arrow/api.h"
#include "arrow/type_traits.h"

using arrow::Array;
using arrow::BinaryArray;
using arrow::BooleanArray;
using arrow::Int16Array;
using arrow::Int16Builder;
using arrow::MemoryPool;
using arrow::PoolBuffer;
using arrow::PrimitiveArray;
using arrow::ListArray;
using arrow::Status;
using arrow::Table;

using parquet::ParquetFileWriter;
using parquet::ParquetVersion;
using parquet::schema::GroupNode;

namespace parquet {
namespace arrow {

namespace BitUtil = ::arrow::BitUtil;

class FileWriter::Impl {
 public:
  Impl(MemoryPool* pool, std::unique_ptr<ParquetFileWriter> writer);

  Status NewRowGroup(int64_t chunk_size);
  template <typename ParquetType, typename ArrowType>
  Status TypedWriteBatch(ColumnWriter* writer, const Array* data, int64_t offset,
      int64_t num_values, int64_t num_levels, const int16_t* def_levels,
      const int16_t* rep_levels);

  template <typename ParquetType, typename ArrowType>
  Status WriteNullableBatch(TypedColumnWriter<ParquetType>* writer, int64_t num_values,
      int64_t num_levels, const int16_t* def_levels, const int16_t* rep_levels,
      const uint8_t* valid_bits, int64_t valid_bits_offset,
      const typename ArrowType::c_type* data_ptr);

  Status GenerateLevels(ColumnWriter* column_writer, const Array* data, int64_t offset,
      int64_t length, int16_t** def_levels, int16_t** rep_levels);

  // TODO(uwe): Same code as in reader.cc the only difference is the name of the temporary
  // buffer
  template <typename InType, typename OutType>
  struct can_copy_ptr {
    static constexpr bool value =
        std::is_same<InType, OutType>::value ||
        (std::is_integral<InType>{} && std::is_integral<OutType>{} &&
            (sizeof(InType) == sizeof(OutType)));
  };

  template <typename InType, typename OutType,
      typename std::enable_if<can_copy_ptr<InType, OutType>::value>::type* = nullptr>
  Status ConvertPhysicalType(const InType* in_ptr, int64_t, const OutType** out_ptr) {
    *out_ptr = reinterpret_cast<const OutType*>(in_ptr);
    return Status::OK();
  }

  template <typename InType, typename OutType,
      typename std::enable_if<not can_copy_ptr<InType, OutType>::value>::type* = nullptr>
  Status ConvertPhysicalType(
      const InType* in_ptr, int64_t length, const OutType** out_ptr) {
    RETURN_NOT_OK(data_buffer_.Resize(length * sizeof(OutType)));
    OutType* mutable_out_ptr = reinterpret_cast<OutType*>(data_buffer_.mutable_data());
    std::copy(in_ptr, in_ptr + length, mutable_out_ptr);
    *out_ptr = mutable_out_ptr;
    return Status::OK();
  }

  Status WriteColumnChunk(const BinaryArray* data, int64_t offset, int64_t length);
  Status WriteColumnChunk(const ListArray* data, int64_t offset, int64_t length);
  Status WriteColumnChunk(const PrimitiveArray* data, int64_t offset, int64_t length);
  Status Close();

  virtual ~Impl() {}

 private:
  friend class FileWriter;

  MemoryPool* pool_;
  // Buffer used for storing the data of an array converted to the physical type
  // as expected by parquet-cpp.
  PoolBuffer data_buffer_;
  PoolBuffer def_levels_buffer_;
  std::unique_ptr<ParquetFileWriter> writer_;
  RowGroupWriter* row_group_writer_;
};

FileWriter::Impl::Impl(MemoryPool* pool, std::unique_ptr<ParquetFileWriter> writer)
    : pool_(pool),
      data_buffer_(pool),
      writer_(std::move(writer)),
      row_group_writer_(nullptr) {}

Status FileWriter::Impl::NewRowGroup(int64_t chunk_size) {
  if (row_group_writer_ != nullptr) { PARQUET_CATCH_NOT_OK(row_group_writer_->Close()); }
  PARQUET_CATCH_NOT_OK(row_group_writer_ = writer_->AppendRowGroup(chunk_size));
  return Status::OK();
}

Status FileWriter::Impl::GenerateLevels(ColumnWriter* writer, const Array* data,
    int64_t offset, int64_t length, int16_t** def_levels, int16_t** rep_levels) {
  int16_t* def_levels_ptr = nullptr;
  int16_t* rep_levels_ptr = nullptr;
  if (writer->descr()->max_definition_level() > 0) {
    RETURN_NOT_OK(def_levels_buffer_.Resize(length * sizeof(int16_t)));
    def_levels_ptr = reinterpret_cast<int16_t*>(def_levels_buffer_.mutable_data());
    if (data->null_count() == 0) {
      std::fill(def_levels_ptr, def_levels_ptr + length, 1);
    } else {
      const uint8_t* valid_bits = data->null_bitmap_data();
      INIT_BITSET(valid_bits, offset);
      for (int i = 0; i < length; i++) {
        if (bitset_valid_bits & (1 << bit_offset_valid_bits)) {
          def_levels_ptr[i] = 1;
        } else {
          def_levels_ptr[i] = 0;
        }
        READ_NEXT_BITSET(valid_bits);
      }
    }
  }

  *def_levels = def_levels_ptr;
  *rep_levels = rep_levels_ptr;
  return Status::OK();
}

template <typename ParquetType, typename ArrowType>
Status FileWriter::Impl::TypedWriteBatch(ColumnWriter* column_writer, const Array* array,
    int64_t offset, int64_t num_values, int64_t num_levels, const int16_t* def_levels,
    const int16_t* rep_levels) {
  using ArrowCType = typename ArrowType::c_type;
  using ParquetCType = typename ParquetType::c_type;

  DCHECK((offset + num_values) <= array->length());
  auto data = static_cast<const PrimitiveArray*>(array);
  auto data_ptr = reinterpret_cast<const ArrowCType*>(data->data()->data()) + offset;
  auto writer = reinterpret_cast<TypedColumnWriter<ParquetType>*>(column_writer);

  if (writer->descr()->schema_node()->is_required() || (data->null_count() == 0)) {
    // no nulls, just dump the data
    const ParquetCType* data_writer_ptr = nullptr;
    RETURN_NOT_OK((ConvertPhysicalType<ArrowCType, ParquetCType>(
        data_ptr, num_values, &data_writer_ptr)));
    PARQUET_CATCH_NOT_OK(
        writer->WriteBatch(num_levels, def_levels, rep_levels, data_writer_ptr));
  } else {
    const uint8_t* valid_bits = data->null_bitmap_data();
    RETURN_NOT_OK((WriteNullableBatch<ParquetType, ArrowType>(writer, num_values,
        num_levels, def_levels, rep_levels, valid_bits, offset, data_ptr)));
  }
  PARQUET_CATCH_NOT_OK(writer->Close());
  return Status::OK();
}

template <typename ParquetType, typename ArrowType>
Status FileWriter::Impl::WriteNullableBatch(TypedColumnWriter<ParquetType>* writer,
    int64_t num_values, int64_t num_levels, const int16_t* def_levels,
    const int16_t* rep_levels, const uint8_t* valid_bits, int64_t valid_bits_offset,
    const typename ArrowType::c_type* data_ptr) {
  using ParquetCType = typename ParquetType::c_type;

  RETURN_NOT_OK(data_buffer_.Resize(num_values * sizeof(ParquetCType)));
  auto buffer_ptr = reinterpret_cast<ParquetCType*>(data_buffer_.mutable_data());
  INIT_BITSET(valid_bits, valid_bits_offset);
  for (int i = 0; i < num_values; i++) {
    if (bitset_valid_bits & (1 << bit_offset_valid_bits)) {
      buffer_ptr[i] = static_cast<ParquetCType>(data_ptr[i]);
    }
    READ_NEXT_BITSET(valid_bits);
  }
  PARQUET_CATCH_NOT_OK(writer->WriteBatchSpaced(
      num_levels, def_levels, rep_levels, valid_bits, valid_bits_offset, buffer_ptr));

  return Status::OK();
}

#define NULLABLE_BATCH_FAST_PATH(ParquetType, ArrowType, CType)                        \
  template <>                                                                          \
  Status FileWriter::Impl::WriteNullableBatch<ParquetType, ArrowType>(                 \
      TypedColumnWriter<ParquetType> * writer, int64_t num_values, int64_t num_levels, \
      const int16_t* def_levels, const int16_t* rep_levels, const uint8_t* valid_bits, \
      int64_t valid_bits_offset, const CType* data_ptr) {                              \
    PARQUET_CATCH_NOT_OK(writer->WriteBatchSpaced(                                     \
        num_levels, def_levels, rep_levels, valid_bits, valid_bits_offset, data_ptr)); \
    return Status::OK();                                                               \
  }

NULLABLE_BATCH_FAST_PATH(Int32Type, ::arrow::Int32Type, int32_t)
NULLABLE_BATCH_FAST_PATH(Int64Type, ::arrow::Int64Type, int64_t)
NULLABLE_BATCH_FAST_PATH(Int64Type, ::arrow::TimestampType, int64_t)
NULLABLE_BATCH_FAST_PATH(FloatType, ::arrow::FloatType, float)
NULLABLE_BATCH_FAST_PATH(DoubleType, ::arrow::DoubleType, double)

// This specialization seems quite similar but it significantly differs in two points:
// * offset is added at the most latest time to the pointer as we have sub-byte access
// * Arrow data is stored bitwise thus we cannot use std::copy to transform from
//   ArrowType::c_type to ParquetType::c_type
template <>
Status FileWriter::Impl::TypedWriteBatch<BooleanType, ::arrow::BooleanType>(
    ColumnWriter* column_writer, const Array* array, int64_t offset, int64_t num_values,
    int64_t num_levels, const int16_t* def_levels, const int16_t* rep_levels) {
  DCHECK((offset + num_values) <= array->length());
  RETURN_NOT_OK(data_buffer_.Resize(num_values));
  auto data = static_cast<const BooleanArray*>(array);
  auto data_ptr = reinterpret_cast<const uint8_t*>(data->data()->data());
  auto buffer_ptr = reinterpret_cast<bool*>(data_buffer_.mutable_data());
  auto writer = reinterpret_cast<TypedColumnWriter<BooleanType>*>(column_writer);

  int buffer_idx = 0;
  for (int i = 0; i < num_values; i++) {
    if (!data->IsNull(offset + i)) {
      buffer_ptr[buffer_idx++] = BitUtil::GetBit(data_ptr, offset + i);
    }
  }
  PARQUET_CATCH_NOT_OK(
      writer->WriteBatch(num_levels, def_levels, rep_levels, buffer_ptr));
  PARQUET_CATCH_NOT_OK(writer->Close());
  return Status::OK();
}

template <>
Status FileWriter::Impl::TypedWriteBatch<ByteArrayType, ::arrow::BinaryType>(
    ColumnWriter* column_writer, const Array* array, int64_t offset, int64_t num_values,
    int64_t num_levels, const int16_t* def_levels, const int16_t* rep_levels) {
  DCHECK((offset + num_values) <= array->length());
  RETURN_NOT_OK(data_buffer_.Resize(num_values * sizeof(ByteArray)));
  auto data = static_cast<const BinaryArray*>(array);
  auto buffer_ptr = reinterpret_cast<ByteArray*>(data_buffer_.mutable_data());
  // In the case of an array consisting of only empty strings or all null,
  // data->data() points already to a nullptr, thus data->data()->data() will
  // segfault.
  const uint8_t* data_ptr = nullptr;
  if (data->data()) {
    data_ptr = reinterpret_cast<const uint8_t*>(data->data()->data());
    DCHECK(data_ptr != nullptr);
  }
  auto writer = reinterpret_cast<TypedColumnWriter<ByteArrayType>*>(column_writer);

  if (writer->descr()->schema_node()->is_required() || (data->null_count() == 0)) {
    // no nulls, just dump the data
    for (int64_t i = 0; i < num_values; i++) {
      buffer_ptr[i] =
          ByteArray(data->value_length(i + offset), data_ptr + data->value_offset(i));
    }
    PARQUET_CATCH_NOT_OK(
        writer->WriteBatch(num_levels, def_levels, rep_levels, buffer_ptr));
  } else {
    int buffer_idx = 0;
    for (int64_t i = 0; i < num_values; i++) {
      if (!data->IsNull(offset + i)) {
        buffer_ptr[buffer_idx++] = ByteArray(
            data->value_length(i + offset), data_ptr + data->value_offset(i + offset));
      }
    }
    PARQUET_CATCH_NOT_OK(
        writer->WriteBatch(num_levels, def_levels, rep_levels, buffer_ptr));
  }
  PARQUET_CATCH_NOT_OK(writer->Close());
  return Status::OK();
}

Status FileWriter::Impl::Close() {
  if (row_group_writer_ != nullptr) { PARQUET_CATCH_NOT_OK(row_group_writer_->Close()); }
  PARQUET_CATCH_NOT_OK(writer_->Close());
  return Status::OK();
}

#define TYPED_BATCH_CASE(ENUM, ArrowType, ParquetType)                 \
  case ::arrow::Type::ENUM:                                            \
    return TypedWriteBatch<ParquetType, ArrowType>(                    \
        writer, data, offset, length, length, def_levels, rep_levels); \
    break;

Status FileWriter::Impl::WriteColumnChunk(
    const PrimitiveArray* data, int64_t offset, int64_t length) {
  ColumnWriter* writer;
  PARQUET_CATCH_NOT_OK(writer = row_group_writer_->NextColumn());

  // Note: Primitive array, so the number of levels is the same as the number
  // of spaced values.
  int16_t* def_levels;
  int16_t* rep_levels;
  RETURN_NOT_OK(GenerateLevels(writer, data, offset, length, &def_levels, &rep_levels));
  switch (data->type_enum()) {
    TYPED_BATCH_CASE(BOOL, ::arrow::BooleanType, BooleanType)
    TYPED_BATCH_CASE(UINT8, ::arrow::UInt8Type, Int32Type)
    TYPED_BATCH_CASE(INT8, ::arrow::Int8Type, Int32Type)
    TYPED_BATCH_CASE(UINT16, ::arrow::UInt16Type, Int32Type)
    TYPED_BATCH_CASE(INT16, ::arrow::Int16Type, Int32Type)
    case ::arrow::Type::UINT32:
      if (writer_->properties()->version() == ParquetVersion::PARQUET_1_0) {
        // Parquet 1.0 reader cannot read the UINT_32 logical type. Thus we need
        // to use the larger Int64Type to store them lossless.
        return TypedWriteBatch<Int64Type, ::arrow::UInt32Type>(
            writer, data, offset, length, length, def_levels, rep_levels);
      } else {
        return TypedWriteBatch<Int32Type, ::arrow::UInt32Type>(
            writer, data, offset, length, length, def_levels, rep_levels);
      }
      TYPED_BATCH_CASE(INT32, ::arrow::Int32Type, Int32Type)
      TYPED_BATCH_CASE(UINT64, ::arrow::UInt64Type, Int64Type)
      TYPED_BATCH_CASE(INT64, ::arrow::Int64Type, Int64Type)
      TYPED_BATCH_CASE(TIMESTAMP, ::arrow::TimestampType, Int64Type)
      TYPED_BATCH_CASE(FLOAT, ::arrow::FloatType, FloatType)
      TYPED_BATCH_CASE(DOUBLE, ::arrow::DoubleType, DoubleType)
    default:
      return Status::NotImplemented(data->type()->ToString());
  }
}

Status FileWriter::Impl::WriteColumnChunk(
    const BinaryArray* data, int64_t offset, int64_t length) {
  ColumnWriter* column_writer;
  PARQUET_CATCH_NOT_OK(column_writer = row_group_writer_->NextColumn());
  int16_t* def_levels;
  int16_t* rep_levels;
  RETURN_NOT_OK(
      GenerateLevels(column_writer, data, offset, length, &def_levels, &rep_levels));
  return TypedWriteBatch<ByteArrayType, ::arrow::BinaryType>(
      column_writer, data, offset, length, length, def_levels, rep_levels);
}

FileWriter::FileWriter(MemoryPool* pool, std::unique_ptr<ParquetFileWriter> writer)
    : impl_(new FileWriter::Impl(pool, std::move(writer))) {}

Status FileWriter::NewRowGroup(int64_t chunk_size) {
  return impl_->NewRowGroup(chunk_size);
}

Status FileWriter::Impl::WriteColumnChunk(
    const ListArray* data, int64_t offset, int64_t length) {
  ColumnWriter* column_writer;
  PARQUET_CATCH_NOT_OK(column_writer = row_group_writer_->NextColumn());
  DCHECK((offset + length) <= data->length());

  auto list_type = std::static_pointer_cast<::arrow::ListType>(data->type());
  bool nullable_elements = list_type->value_field()->nullable;
  bool nullable_lists =
      (column_writer->descr()->max_definition_level() == 3) ||
      (!nullable_elements && (column_writer->descr()->max_definition_level() == 2));

  if (column_writer->descr()->max_repetition_level() != 1) {
    return Status::NotImplemented(
        "Only primitive arrays with repetition level == 1 are supported yet");
  }
  // With maximum definition level 3, we have the following definition levels:
  // 0 -> list is null
  // 1 -> list is non-null but empty
  // 2 -> list is non-null, element is null
  // 3 -> list is non-null, element is non-null
  int16_t valid_element = 3;
  int16_t null_element = 2;
  int16_t empty_list = 1;
  int16_t null_list = 0;
  if (column_writer->descr()->max_definition_level() != 3) {
    if (column_writer->descr()->max_definition_level() > 3) {
      return Status::NotImplemented(
          "Only primitive arrays with max definition level <= 3 are supported yet");
    } else if ((column_writer->descr()->max_definition_level() == 2) &&
               nullable_elements) {
      // Lists are required
      valid_element = 2;
      null_element = 1;
      empty_list = 0;
    } else if ((column_writer->descr()->max_definition_level() == 2) && nullable_lists) {
      // Elements are required
      valid_element--;
    } else if ((column_writer->descr()->max_definition_level() == 1) && !nullable_lists &&
               !nullable_elements) {
      valid_element = 1;
      null_element = 1;
      empty_list = 0;
      null_list = 0;
    } else {
      return Status::NotImplemented(
          "Primitive arrays with max definition level == 0 aren't supported yet");
    }
  }

  // Generate repetition levels
  Int16Builder rep_levels(pool_, ::arrow::int16());
  Int16Builder def_levels(pool_, ::arrow::int16());
  const uint8_t* valid_lists = data->null_bitmap_data();
  const int32_t* raw_offsets = data->raw_offsets();
  int64_t values_offset = raw_offsets[offset];
  int64_t num_values = raw_offsets[offset + length] - values_offset;
  INIT_BITSET(valid_lists, offset);
  const uint8_t* valid_values = data->values()->null_bitmap_data();
  INIT_BITSET(valid_values, values_offset);
  for (int64_t i = 0; i < length; i++) {
    rep_levels.Append(0);
    if (bitset_valid_lists & (1 << bit_offset_valid_lists)) {
      // not null, so we have offsets
      int32_t len = raw_offsets[i + 1] - raw_offsets[i];
      if (len == 0) {
        def_levels.Append(empty_list);
      } else {
        if (bitset_valid_values & (1 << bit_offset_valid_values)) {
          def_levels.Append(valid_element);
        } else {
          def_levels.Append(null_element);
        }
        READ_NEXT_BITSET(valid_values);
        for (int32_t j = 1; j < len; j++) {
          rep_levels.Append(1);
          if (bitset_valid_values & (1 << bit_offset_valid_values)) {
            def_levels.Append(valid_element);
          } else {
            def_levels.Append(null_element);
          }
          READ_NEXT_BITSET(valid_values);
        }
      }
    } else {
      def_levels.Append(null_list);
    }
    READ_NEXT_BITSET(valid_lists);
  }
  std::shared_ptr<Array> def_levels_array;
  def_levels.Finish(&def_levels_array);
  const int16_t* def_levels_ptr = reinterpret_cast<const int16_t*>(
      static_cast<Int16Array*>(def_levels_array.get())->data()->data());

  std::shared_ptr<Array> rep_levels_array;
  rep_levels.Finish(&rep_levels_array);
  const int16_t* rep_levels_ptr = reinterpret_cast<const int16_t*>(
      static_cast<Int16Array*>(rep_levels_array.get())->data()->data());

#define WRITE_LIST_BATCH_CASE(ArrowEnum, ArrowType, ParquetType)                     \
  case ::arrow::Type::ArrowEnum:                                                     \
    return TypedWriteBatch<ParquetType, ::arrow::ArrowType>(column_writer,           \
        data->values().get(), values_offset, num_values, rep_levels_array->length(), \
        def_levels_ptr, rep_levels_ptr);                                             \
    break;

  switch (data->value_type()->type) {
    case ::arrow::Type::UINT32: {
      if (writer_->properties()->version() == ParquetVersion::PARQUET_1_0) {
        // Parquet 1.0 reader cannot read the UINT_32 logical type. Thus we need
        // to use the larger Int64Type to store them lossless.
        return TypedWriteBatch<Int64Type, ::arrow::UInt32Type>(column_writer,
            data->values().get(), values_offset, num_values, rep_levels_array->length(),
            def_levels_ptr, rep_levels_ptr);
      } else {
        return TypedWriteBatch<Int32Type, ::arrow::UInt32Type>(column_writer,
            data->values().get(), values_offset, num_values, rep_levels_array->length(),
            def_levels_ptr, rep_levels_ptr);
      }
    }
      WRITE_LIST_BATCH_CASE(BOOL, BooleanType, BooleanType)
      WRITE_LIST_BATCH_CASE(INT8, Int8Type, Int32Type)
      WRITE_LIST_BATCH_CASE(UINT8, UInt8Type, Int32Type)
      WRITE_LIST_BATCH_CASE(INT16, Int16Type, Int32Type)
      WRITE_LIST_BATCH_CASE(UINT16, UInt16Type, Int32Type)
      WRITE_LIST_BATCH_CASE(INT32, Int32Type, Int32Type)
      WRITE_LIST_BATCH_CASE(INT64, Int64Type, Int64Type)
      WRITE_LIST_BATCH_CASE(TIMESTAMP, TimestampType, Int64Type)
      WRITE_LIST_BATCH_CASE(UINT64, UInt64Type, Int64Type)
      WRITE_LIST_BATCH_CASE(FLOAT, FloatType, FloatType)
      WRITE_LIST_BATCH_CASE(DOUBLE, DoubleType, DoubleType)
      WRITE_LIST_BATCH_CASE(BINARY, BinaryType, ByteArrayType)
      WRITE_LIST_BATCH_CASE(STRING, BinaryType, ByteArrayType)
    default:
      std::stringstream ss;
      ss << "Data type not supported as list value: " << data->value_type()->ToString();
      return Status::NotImplemented(ss.str());
  }

  PARQUET_CATCH_NOT_OK(column_writer->Close());

  return Status::OK();
}

Status FileWriter::WriteColumnChunk(
    const ::arrow::Array* array, int64_t offset, int64_t length) {
  int64_t real_length = length;
  if (length == -1) { real_length = array->length(); }
  if (is_primitive(array->type_enum())) {
    auto primitive_array = dynamic_cast<const PrimitiveArray*>(array);
    DCHECK(primitive_array);
    return impl_->WriteColumnChunk(primitive_array, offset, real_length);
  } else if (is_binary_like(array->type_enum())) {
    auto binary_array = static_cast<const ::arrow::BinaryArray*>(array);
    DCHECK(binary_array);
    return impl_->WriteColumnChunk(binary_array, offset, real_length);
  } else if (array->type_enum() == ::arrow::Type::LIST) {
    auto list_array = static_cast<const ListArray*>(array);
    return impl_->WriteColumnChunk(list_array, offset, real_length);
  }

  std::stringstream ss;
  ss << "No support for the given array type: " << array->type()->ToString();
  return Status::NotImplemented(ss.str());
}

Status FileWriter::Close() {
  return impl_->Close();
}

MemoryPool* FileWriter::memory_pool() const {
  return impl_->pool_;
}

FileWriter::~FileWriter() {}

Status WriteTable(const Table* table, MemoryPool* pool,
    const std::shared_ptr<OutputStream>& sink, int64_t chunk_size,
    const std::shared_ptr<WriterProperties>& properties) {
  std::shared_ptr<SchemaDescriptor> parquet_schema;
  RETURN_NOT_OK(
      ToParquetSchema(table->schema().get(), *properties.get(), &parquet_schema));
  auto schema_node = std::static_pointer_cast<GroupNode>(parquet_schema->schema_root());
  std::unique_ptr<ParquetFileWriter> parquet_writer =
      ParquetFileWriter::Open(sink, schema_node, properties);
  FileWriter writer(pool, std::move(parquet_writer));

  // TODO(ARROW-232) Support writing chunked arrays.
  for (int i = 0; i < table->num_columns(); i++) {
    if (table->column(i)->data()->num_chunks() != 1) {
      return Status::NotImplemented("No support for writing chunked arrays yet.");
    }
  }

  for (int chunk = 0; chunk * chunk_size < table->num_rows(); chunk++) {
    int64_t offset = chunk * chunk_size;
    int64_t size = std::min(chunk_size, table->num_rows() - offset);
    RETURN_NOT_OK_ELSE(writer.NewRowGroup(size), PARQUET_IGNORE_NOT_OK(writer.Close()));
    for (int i = 0; i < table->num_columns(); i++) {
      std::shared_ptr<Array> array = table->column(i)->data()->chunk(0);
      RETURN_NOT_OK_ELSE(writer.WriteColumnChunk(array.get(), offset, size),
          PARQUET_IGNORE_NOT_OK(writer.Close()));
    }
  }

  return writer.Close();
}

Status WriteTable(const Table* table, MemoryPool* pool,
    const std::shared_ptr<::arrow::io::OutputStream>& sink, int64_t chunk_size,
    const std::shared_ptr<WriterProperties>& properties) {
  auto wrapper = std::make_shared<ArrowOutputStream>(sink);
  return WriteTable(table, pool, wrapper, chunk_size, properties);
}

}  // namespace arrow

}  // namespace parquet
