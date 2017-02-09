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

#include "gtest/gtest.h"

#include <sstream>

#include "parquet/api/reader.h"
#include "parquet/api/writer.h"

#include "parquet/arrow/reader.h"
#include "parquet/arrow/test-util.h"
#include "parquet/arrow/writer.h"

#include "arrow/api.h"
#include "arrow/io/memory.h"
#include "arrow/test-util.h"

using arrow::Array;
using arrow::Buffer;
using arrow::Column;
using arrow::ChunkedArray;
using arrow::default_memory_pool;
using arrow::io::BufferReader;
using arrow::ListArray;
using arrow::PoolBuffer;
using arrow::PrimitiveArray;
using arrow::Status;
using arrow::Table;

using ParquetType = parquet::Type;
using parquet::schema::GroupNode;
using parquet::schema::NodePtr;
using parquet::schema::PrimitiveNode;

namespace parquet {
namespace arrow {

const int SMALL_SIZE = 100;
const int LARGE_SIZE = 10000;

constexpr uint32_t kDefaultSeed = 0;

template <typename TestType>
struct test_traits {};

template <>
struct test_traits<::arrow::BooleanType> {
  static constexpr ParquetType::type parquet_enum = ParquetType::BOOLEAN;
  static constexpr LogicalType::type logical_enum = LogicalType::NONE;
  static uint8_t const value;
};

const uint8_t test_traits<::arrow::BooleanType>::value(1);

template <>
struct test_traits<::arrow::UInt8Type> {
  static constexpr ParquetType::type parquet_enum = ParquetType::INT32;
  static constexpr LogicalType::type logical_enum = LogicalType::UINT_8;
  static uint8_t const value;
};

const uint8_t test_traits<::arrow::UInt8Type>::value(64);

template <>
struct test_traits<::arrow::Int8Type> {
  static constexpr ParquetType::type parquet_enum = ParquetType::INT32;
  static constexpr LogicalType::type logical_enum = LogicalType::INT_8;
  static int8_t const value;
};

const int8_t test_traits<::arrow::Int8Type>::value(-64);

template <>
struct test_traits<::arrow::UInt16Type> {
  static constexpr ParquetType::type parquet_enum = ParquetType::INT32;
  static constexpr LogicalType::type logical_enum = LogicalType::UINT_16;
  static uint16_t const value;
};

const uint16_t test_traits<::arrow::UInt16Type>::value(1024);

template <>
struct test_traits<::arrow::Int16Type> {
  static constexpr ParquetType::type parquet_enum = ParquetType::INT32;
  static constexpr LogicalType::type logical_enum = LogicalType::INT_16;
  static int16_t const value;
};

const int16_t test_traits<::arrow::Int16Type>::value(-1024);

template <>
struct test_traits<::arrow::UInt32Type> {
  static constexpr ParquetType::type parquet_enum = ParquetType::INT32;
  static constexpr LogicalType::type logical_enum = LogicalType::UINT_32;
  static uint32_t const value;
};

const uint32_t test_traits<::arrow::UInt32Type>::value(1024);

template <>
struct test_traits<::arrow::Int32Type> {
  static constexpr ParquetType::type parquet_enum = ParquetType::INT32;
  static constexpr LogicalType::type logical_enum = LogicalType::NONE;
  static int32_t const value;
};

const int32_t test_traits<::arrow::Int32Type>::value(-1024);

template <>
struct test_traits<::arrow::UInt64Type> {
  static constexpr ParquetType::type parquet_enum = ParquetType::INT64;
  static constexpr LogicalType::type logical_enum = LogicalType::UINT_64;
  static uint64_t const value;
};

const uint64_t test_traits<::arrow::UInt64Type>::value(1024);

template <>
struct test_traits<::arrow::Int64Type> {
  static constexpr ParquetType::type parquet_enum = ParquetType::INT64;
  static constexpr LogicalType::type logical_enum = LogicalType::NONE;
  static int64_t const value;
};

const int64_t test_traits<::arrow::Int64Type>::value(-1024);

template <>
struct test_traits<::arrow::TimestampType> {
  static constexpr ParquetType::type parquet_enum = ParquetType::INT64;
  static constexpr LogicalType::type logical_enum = LogicalType::TIMESTAMP_MILLIS;
  static int64_t const value;
};

const int64_t test_traits<::arrow::TimestampType>::value(14695634030000);

template <>
struct test_traits<::arrow::FloatType> {
  static constexpr ParquetType::type parquet_enum = ParquetType::FLOAT;
  static constexpr LogicalType::type logical_enum = LogicalType::NONE;
  static float const value;
};

const float test_traits<::arrow::FloatType>::value(2.1f);

template <>
struct test_traits<::arrow::DoubleType> {
  static constexpr ParquetType::type parquet_enum = ParquetType::DOUBLE;
  static constexpr LogicalType::type logical_enum = LogicalType::NONE;
  static double const value;
};

const double test_traits<::arrow::DoubleType>::value(4.2);

template <>
struct test_traits<::arrow::StringType> {
  static constexpr ParquetType::type parquet_enum = ParquetType::BYTE_ARRAY;
  static constexpr LogicalType::type logical_enum = LogicalType::UTF8;
  static std::string const value;
};

template <>
struct test_traits<::arrow::BinaryType> {
  static constexpr ParquetType::type parquet_enum = ParquetType::BYTE_ARRAY;
  static constexpr LogicalType::type logical_enum = LogicalType::NONE;
  static std::string const value;
};

const std::string test_traits<::arrow::StringType>::value("Test");
const std::string test_traits<::arrow::BinaryType>::value("\x00\x01\x02\x03");

template <typename T>
using ParquetDataType = DataType<test_traits<T>::parquet_enum>;

template <typename T>
using ParquetWriter = TypedColumnWriter<ParquetDataType<T>>;

template <typename TestType>
class TestParquetIO : public ::testing::Test {
 public:
  virtual void SetUp() {}

  std::shared_ptr<GroupNode> MakeSchema(Repetition::type repetition) {
    auto pnode = PrimitiveNode::Make("column1", repetition,
        test_traits<TestType>::parquet_enum, test_traits<TestType>::logical_enum);
    NodePtr node_ =
        GroupNode::Make("schema", Repetition::REQUIRED, std::vector<NodePtr>({pnode}));
    return std::static_pointer_cast<GroupNode>(node_);
  }

  std::unique_ptr<ParquetFileWriter> MakeWriter(
      const std::shared_ptr<GroupNode>& schema) {
    sink_ = std::make_shared<InMemoryOutputStream>();
    return ParquetFileWriter::Open(sink_, schema);
  }

  void ReaderFromSink(std::unique_ptr<FileReader>* out) {
    std::shared_ptr<Buffer> buffer = sink_->GetBuffer();
    ASSERT_OK_NO_THROW(
        OpenFile(std::make_shared<BufferReader>(buffer), ::arrow::default_memory_pool(),
            ::parquet::default_reader_properties(), nullptr, out));
  }

  void ReadSingleColumnFile(
      std::unique_ptr<FileReader> file_reader, std::shared_ptr<Array>* out) {
    std::unique_ptr<ColumnReader> column_reader;
    ASSERT_OK_NO_THROW(file_reader->GetColumn(0, &column_reader));
    ASSERT_NE(nullptr, column_reader.get());

    ASSERT_OK(column_reader->NextBatch(SMALL_SIZE, out));
    ASSERT_NE(nullptr, out->get());
  }

  void ReadAndCheckSingleColumnFile(::arrow::Array* values) {
    std::shared_ptr<::arrow::Array> out;

    std::unique_ptr<FileReader> reader;
    ReaderFromSink(&reader);
    ReadSingleColumnFile(std::move(reader), &out);
    ASSERT_TRUE(values->Equals(out));
  }

  void ReadTableFromFile(
      std::unique_ptr<FileReader> reader, std::shared_ptr<Table>* out) {
    ASSERT_OK_NO_THROW(reader->ReadTable(out));
    ASSERT_NE(nullptr, out->get());
  }

  void ReadAndCheckSingleColumnTable(const std::shared_ptr<::arrow::Array>& values) {
    std::shared_ptr<::arrow::Table> out;
    std::unique_ptr<FileReader> reader;
    ReaderFromSink(&reader);
    ReadTableFromFile(std::move(reader), &out);
    ASSERT_EQ(1, out->num_columns());
    ASSERT_EQ(values->length(), out->num_rows());

    std::shared_ptr<ChunkedArray> chunked_array = out->column(0)->data();
    ASSERT_EQ(1, chunked_array->num_chunks());
    ASSERT_TRUE(values->Equals(chunked_array->chunk(0)));
  }

  void PrepareListTable(int64_t size, bool nullable_lists, bool nullable_elements,
      int64_t null_count, std::shared_ptr<Table>* out) {
    std::shared_ptr<Array> values;
    ASSERT_OK(NullableArray<TestType>(
        size * size, nullable_elements ? null_count : 0, kDefaultSeed, &values));
    // Also test that slice offsets are respected
    values = values->Slice(5, values->length() - 5);
    std::shared_ptr<ListArray> lists;
    ASSERT_OK(MakeListArary(
        values, size, nullable_lists ? null_count : 0, nullable_elements, &lists));
    *out = MakeSimpleTable(lists->Slice(3, size - 6), nullable_lists);
  }

  void PrepareListOfListTable(int64_t size, bool nullable_parent_lists,
      bool nullable_lists, bool nullable_elements, int64_t null_count,
      std::shared_ptr<Table>* out) {
    std::shared_ptr<Array> values;
    ASSERT_OK(NullableArray<TestType>(
        size * 6, nullable_elements ? null_count : 0, kDefaultSeed, &values));
    std::shared_ptr<ListArray> lists;
    ASSERT_OK(MakeListArary(
        values, size * 3, nullable_lists ? null_count : 0, nullable_elements, &lists));
    std::shared_ptr<ListArray> parent_lists;
    ASSERT_OK(MakeListArary(lists, size, nullable_parent_lists ? null_count : 0,
        nullable_lists, &parent_lists));
    *out = MakeSimpleTable(parent_lists, nullable_parent_lists);
  }

  void WriteReadAndCheckSingleColumnTable(const std::shared_ptr<Table>& table) {
    std::shared_ptr<Array> values = table->column(0)->data()->chunk(0);
    this->sink_ = std::make_shared<InMemoryOutputStream>();
    ASSERT_OK_NO_THROW(WriteTable(*table, ::arrow::default_memory_pool(), this->sink_,
        values->length(), default_writer_properties()));

    this->ReadAndCheckSingleColumnTable(values);
  }

  template <typename ArrayType>
  void WriteColumn(const std::shared_ptr<GroupNode>& schema,
      const std::shared_ptr<ArrayType>& values) {
    FileWriter writer(::arrow::default_memory_pool(), MakeWriter(schema));
    ASSERT_OK_NO_THROW(writer.NewRowGroup(values->length()));
    ASSERT_OK_NO_THROW(writer.WriteColumnChunk(*values));
    ASSERT_OK_NO_THROW(writer.Close());
  }

  std::shared_ptr<InMemoryOutputStream> sink_;
};

// We have separate tests for UInt32Type as this is currently the only type
// where a roundtrip does not yield the identical Array structure.
// There we write an UInt32 Array but receive an Int64 Array as result for
// Parquet version 1.0.

typedef ::testing::Types<::arrow::BooleanType, ::arrow::UInt8Type, ::arrow::Int8Type,
    ::arrow::UInt16Type, ::arrow::Int16Type, ::arrow::Int32Type, ::arrow::UInt64Type,
    ::arrow::Int64Type, ::arrow::TimestampType, ::arrow::FloatType, ::arrow::DoubleType,
    ::arrow::StringType, ::arrow::BinaryType>
    TestTypes;

TYPED_TEST_CASE(TestParquetIO, TestTypes);

TYPED_TEST(TestParquetIO, SingleColumnRequiredWrite) {
  std::shared_ptr<Array> values;
  ASSERT_OK(NonNullArray<TypeParam>(SMALL_SIZE, &values));

  std::shared_ptr<GroupNode> schema = this->MakeSchema(Repetition::REQUIRED);
  this->WriteColumn(schema, values);

  this->ReadAndCheckSingleColumnFile(values.get());
}

TYPED_TEST(TestParquetIO, SingleColumnTableRequiredWrite) {
  std::shared_ptr<Array> values;
  ASSERT_OK(NonNullArray<TypeParam>(SMALL_SIZE, &values));
  std::shared_ptr<Table> table = MakeSimpleTable(values, false);
  this->sink_ = std::make_shared<InMemoryOutputStream>();
  ASSERT_OK_NO_THROW(WriteTable(*table, ::arrow::default_memory_pool(), this->sink_,
      values->length(), default_writer_properties()));

  std::shared_ptr<Table> out;
  std::unique_ptr<FileReader> reader;
  this->ReaderFromSink(&reader);
  this->ReadTableFromFile(std::move(reader), &out);
  ASSERT_EQ(1, out->num_columns());
  ASSERT_EQ(100, out->num_rows());

  std::shared_ptr<ChunkedArray> chunked_array = out->column(0)->data();
  ASSERT_EQ(1, chunked_array->num_chunks());
  ASSERT_TRUE(values->Equals(chunked_array->chunk(0)));
}

TYPED_TEST(TestParquetIO, SingleColumnOptionalReadWrite) {
  // This also tests max_definition_level = 1
  std::shared_ptr<Array> values;

  ASSERT_OK(NullableArray<TypeParam>(SMALL_SIZE, 10, kDefaultSeed, &values));

  std::shared_ptr<GroupNode> schema = this->MakeSchema(Repetition::OPTIONAL);
  this->WriteColumn(schema, values);

  this->ReadAndCheckSingleColumnFile(values.get());
}

TYPED_TEST(TestParquetIO, SingleColumnRequiredSliceWrite) {
  std::shared_ptr<Array> values;
  ASSERT_OK(NonNullArray<TypeParam>(2 * SMALL_SIZE, &values));
  std::shared_ptr<GroupNode> schema = this->MakeSchema(Repetition::REQUIRED);

  std::shared_ptr<Array> sliced_values = values->Slice(SMALL_SIZE / 2, SMALL_SIZE);
  this->WriteColumn(schema, sliced_values);
  this->ReadAndCheckSingleColumnFile(sliced_values.get());

  // Slice offset 1 higher
  sliced_values = values->Slice(SMALL_SIZE / 2 + 1, SMALL_SIZE);
  this->WriteColumn(schema, sliced_values);
  this->ReadAndCheckSingleColumnFile(sliced_values.get());
}

TYPED_TEST(TestParquetIO, SingleColumnOptionalSliceWrite) {
  std::shared_ptr<Array> values;
  ASSERT_OK(NullableArray<TypeParam>(2 * SMALL_SIZE, SMALL_SIZE, kDefaultSeed, &values));
  std::shared_ptr<GroupNode> schema = this->MakeSchema(Repetition::OPTIONAL);

  std::shared_ptr<Array> sliced_values = values->Slice(SMALL_SIZE / 2, SMALL_SIZE);
  this->WriteColumn(schema, sliced_values);
  this->ReadAndCheckSingleColumnFile(sliced_values.get());

  // Slice offset 1 higher, thus different null bitmap.
  sliced_values = values->Slice(SMALL_SIZE / 2 + 1, SMALL_SIZE);
  this->WriteColumn(schema, sliced_values);
  this->ReadAndCheckSingleColumnFile(sliced_values.get());
}

TYPED_TEST(TestParquetIO, SingleColumnTableOptionalReadWrite) {
  // This also tests max_definition_level = 1
  std::shared_ptr<Array> values;

  ASSERT_OK(NullableArray<TypeParam>(SMALL_SIZE, 10, kDefaultSeed, &values));
  std::shared_ptr<Table> table = MakeSimpleTable(values, true);
  this->WriteReadAndCheckSingleColumnTable(table);
}

TYPED_TEST(TestParquetIO, SingleNullableListNullableColumnReadWrite) {
  std::shared_ptr<Table> table;
  this->PrepareListTable(SMALL_SIZE, true, true, 10, &table);
  this->WriteReadAndCheckSingleColumnTable(table);
}

TYPED_TEST(TestParquetIO, SingleRequiredListNullableColumnReadWrite) {
  std::shared_ptr<Table> table;
  this->PrepareListTable(SMALL_SIZE, false, true, 10, &table);
  this->WriteReadAndCheckSingleColumnTable(table);
}

TYPED_TEST(TestParquetIO, SingleNullableListRequiredColumnReadWrite) {
  std::shared_ptr<Table> table;
  this->PrepareListTable(SMALL_SIZE, true, false, 10, &table);
  this->WriteReadAndCheckSingleColumnTable(table);
}

TYPED_TEST(TestParquetIO, SingleRequiredListRequiredColumnReadWrite) {
  std::shared_ptr<Table> table;
  this->PrepareListTable(SMALL_SIZE, false, false, 0, &table);
  this->WriteReadAndCheckSingleColumnTable(table);
}

TYPED_TEST(TestParquetIO, SingleNullableListRequiredListRequiredColumnReadWrite) {
  std::shared_ptr<Table> table;
  this->PrepareListOfListTable(SMALL_SIZE, true, false, false, 0, &table);
  this->WriteReadAndCheckSingleColumnTable(table);
}

TYPED_TEST(TestParquetIO, SingleColumnRequiredChunkedWrite) {
  std::shared_ptr<Array> values;
  ASSERT_OK(NonNullArray<TypeParam>(SMALL_SIZE, &values));
  int64_t chunk_size = values->length() / 4;

  std::shared_ptr<GroupNode> schema = this->MakeSchema(Repetition::REQUIRED);
  FileWriter writer(default_memory_pool(), this->MakeWriter(schema));
  for (int i = 0; i < 4; i++) {
    ASSERT_OK_NO_THROW(writer.NewRowGroup(chunk_size));
    std::shared_ptr<Array> sliced_array = values->Slice(i * chunk_size, chunk_size);
    ASSERT_OK_NO_THROW(writer.WriteColumnChunk(*sliced_array));
  }
  ASSERT_OK_NO_THROW(writer.Close());

  this->ReadAndCheckSingleColumnFile(values.get());
}

TYPED_TEST(TestParquetIO, SingleColumnTableRequiredChunkedWrite) {
  std::shared_ptr<Array> values;
  ASSERT_OK(NonNullArray<TypeParam>(LARGE_SIZE, &values));
  std::shared_ptr<Table> table = MakeSimpleTable(values, false);
  this->sink_ = std::make_shared<InMemoryOutputStream>();
  ASSERT_OK_NO_THROW(WriteTable(
      *table, default_memory_pool(), this->sink_, 512, default_writer_properties()));

  this->ReadAndCheckSingleColumnTable(values);
}

TYPED_TEST(TestParquetIO, SingleColumnTableRequiredChunkedWriteArrowIO) {
  std::shared_ptr<Array> values;
  ASSERT_OK(NonNullArray<TypeParam>(LARGE_SIZE, &values));
  std::shared_ptr<Table> table = MakeSimpleTable(values, false);
  this->sink_ = std::make_shared<InMemoryOutputStream>();
  auto buffer = std::make_shared<::arrow::PoolBuffer>();

  {
    // BufferOutputStream closed on gc
    auto arrow_sink_ = std::make_shared<::arrow::io::BufferOutputStream>(buffer);
    ASSERT_OK_NO_THROW(WriteTable(
        *table, default_memory_pool(), arrow_sink_, 512, default_writer_properties()));

    // XXX: Remove this after ARROW-455 completed
    ASSERT_OK(arrow_sink_->Close());
  }

  auto pbuffer = std::make_shared<Buffer>(buffer->data(), buffer->size());

  auto source = std::make_shared<BufferReader>(pbuffer);
  std::shared_ptr<::arrow::Table> out;
  std::unique_ptr<FileReader> reader;
  ASSERT_OK_NO_THROW(OpenFile(source, ::arrow::default_memory_pool(), &reader));
  this->ReadTableFromFile(std::move(reader), &out);
  ASSERT_EQ(1, out->num_columns());
  ASSERT_EQ(values->length(), out->num_rows());

  std::shared_ptr<ChunkedArray> chunked_array = out->column(0)->data();
  ASSERT_EQ(1, chunked_array->num_chunks());
  ASSERT_TRUE(values->Equals(chunked_array->chunk(0)));
}

TYPED_TEST(TestParquetIO, SingleColumnOptionalChunkedWrite) {
  int64_t chunk_size = SMALL_SIZE / 4;
  std::shared_ptr<Array> values;

  ASSERT_OK(NullableArray<TypeParam>(SMALL_SIZE, 10, kDefaultSeed, &values));

  std::shared_ptr<GroupNode> schema = this->MakeSchema(Repetition::OPTIONAL);
  FileWriter writer(::arrow::default_memory_pool(), this->MakeWriter(schema));
  for (int i = 0; i < 4; i++) {
    ASSERT_OK_NO_THROW(writer.NewRowGroup(chunk_size));
    std::shared_ptr<Array> sliced_array = values->Slice(i * chunk_size, chunk_size);
    ASSERT_OK_NO_THROW(writer.WriteColumnChunk(*sliced_array));
  }
  ASSERT_OK_NO_THROW(writer.Close());

  this->ReadAndCheckSingleColumnFile(values.get());
}

TYPED_TEST(TestParquetIO, SingleColumnTableOptionalChunkedWrite) {
  // This also tests max_definition_level = 1
  std::shared_ptr<Array> values;

  ASSERT_OK(NullableArray<TypeParam>(LARGE_SIZE, 100, kDefaultSeed, &values));
  std::shared_ptr<Table> table = MakeSimpleTable(values, true);
  this->sink_ = std::make_shared<InMemoryOutputStream>();
  ASSERT_OK_NO_THROW(WriteTable(*table, ::arrow::default_memory_pool(), this->sink_, 512,
      default_writer_properties()));

  this->ReadAndCheckSingleColumnTable(values);
}

using TestInt96ParquetIO = TestParquetIO<::arrow::TimestampType>;

TEST_F(TestInt96ParquetIO, ReadIntoTimestamp) {
  // This test explicitly tests the conversion from an Impala-style timestamp
  // to a nanoseconds-since-epoch one.

  // 2nd January 1970, 11:35min 145738543ns
  Int96 day;
  day.value[2] = 2440589l;
  int64_t seconds = (11 * 60 + 35) * 60;
  *(reinterpret_cast<int64_t*>(&(day.value))) =
      seconds * 1000l * 1000l * 1000l + 145738543;
  // Compute the corresponding nanosecond timestamp
  struct tm datetime = {0};
  datetime.tm_year = 70;
  datetime.tm_mon = 0;
  datetime.tm_mday = 2;
  datetime.tm_hour = 11;
  datetime.tm_min = 35;
  struct tm epoch = {0};
  epoch.tm_year = 70;
  epoch.tm_mday = 1;
  // Nanoseconds since the epoch
  int64_t val = lrint(difftime(mktime(&datetime), mktime(&epoch))) * 1000000000;
  val += 145738543;

  std::vector<std::shared_ptr<schema::Node>> fields(
      {schema::PrimitiveNode::Make("int96", Repetition::REQUIRED, ParquetType::INT96)});
  std::shared_ptr<schema::GroupNode> schema = std::static_pointer_cast<GroupNode>(
      schema::GroupNode::Make("schema", Repetition::REQUIRED, fields));

  // We cannot write this column with Arrow, so we have to use the plain parquet-cpp API
  // to write an Int96 file.
  this->sink_ = std::make_shared<InMemoryOutputStream>();
  auto writer = ParquetFileWriter::Open(this->sink_, schema);
  RowGroupWriter* rg_writer = writer->AppendRowGroup(1);
  ColumnWriter* c_writer = rg_writer->NextColumn();
  auto typed_writer = dynamic_cast<TypedColumnWriter<Int96Type>*>(c_writer);
  ASSERT_NE(typed_writer, nullptr);
  typed_writer->WriteBatch(1, nullptr, nullptr, &day);
  c_writer->Close();
  rg_writer->Close();
  writer->Close();

  ::arrow::TimestampBuilder builder(
      default_memory_pool(), ::arrow::timestamp(::arrow::TimeUnit::NANO));
  builder.Append(val);
  std::shared_ptr<Array> values;
  ASSERT_OK(builder.Finish(&values));
  this->ReadAndCheckSingleColumnFile(values.get());
}

using TestUInt32ParquetIO = TestParquetIO<::arrow::UInt32Type>;

TEST_F(TestUInt32ParquetIO, Parquet_2_0_Compability) {
  // This also tests max_definition_level = 1
  std::shared_ptr<Array> values;

  ASSERT_OK(NullableArray<::arrow::UInt32Type>(LARGE_SIZE, 100, kDefaultSeed, &values));
  std::shared_ptr<Table> table = MakeSimpleTable(values, true);

  // Parquet 2.0 roundtrip should yield an uint32_t column again
  this->sink_ = std::make_shared<InMemoryOutputStream>();
  std::shared_ptr<::parquet::WriterProperties> properties =
      ::parquet::WriterProperties::Builder()
          .version(ParquetVersion::PARQUET_2_0)
          ->build();
  ASSERT_OK_NO_THROW(
      WriteTable(*table, default_memory_pool(), this->sink_, 512, properties));
  this->ReadAndCheckSingleColumnTable(values);
}

TEST_F(TestUInt32ParquetIO, Parquet_1_0_Compability) {
  // This also tests max_definition_level = 1
  std::shared_ptr<Array> arr;
  ASSERT_OK(NullableArray<::arrow::UInt32Type>(LARGE_SIZE, 100, kDefaultSeed, &arr));

  std::shared_ptr<::arrow::UInt32Array> values =
      std::dynamic_pointer_cast<::arrow::UInt32Array>(arr);

  std::shared_ptr<Table> table = MakeSimpleTable(values, true);

  // Parquet 1.0 returns an int64_t column as there is no way to tell a Parquet 1.0
  // reader that a column is unsigned.
  this->sink_ = std::make_shared<InMemoryOutputStream>();
  std::shared_ptr<::parquet::WriterProperties> properties =
      ::parquet::WriterProperties::Builder()
          .version(ParquetVersion::PARQUET_1_0)
          ->build();
  ASSERT_OK_NO_THROW(
      WriteTable(*table, ::arrow::default_memory_pool(), this->sink_, 512, properties));

  std::shared_ptr<Array> expected_values;
  std::shared_ptr<PoolBuffer> int64_data =
      std::make_shared<PoolBuffer>(::arrow::default_memory_pool());
  {
    ASSERT_OK(int64_data->Resize(sizeof(int64_t) * values->length()));
    int64_t* int64_data_ptr = reinterpret_cast<int64_t*>(int64_data->mutable_data());
    const uint32_t* uint32_data_ptr =
        reinterpret_cast<const uint32_t*>(values->data()->data());
    // std::copy might be faster but this is explicit on the casts)
    for (int64_t i = 0; i < values->length(); i++) {
      int64_data_ptr[i] = static_cast<int64_t>(uint32_data_ptr[i]);
    }
  }

  const int32_t kOffset = 0;
  ASSERT_OK(MakePrimitiveArray(std::make_shared<::arrow::Int64Type>(), values->length(),
      int64_data, values->null_bitmap(), values->null_count(), kOffset,
      &expected_values));
  this->ReadAndCheckSingleColumnTable(expected_values);
}

using TestStringParquetIO = TestParquetIO<::arrow::StringType>;

TEST_F(TestStringParquetIO, EmptyStringColumnRequiredWrite) {
  std::shared_ptr<Array> values;
  ::arrow::StringBuilder builder(::arrow::default_memory_pool());
  for (size_t i = 0; i < SMALL_SIZE; i++) {
    builder.Append("");
  }
  ASSERT_OK(builder.Finish(&values));
  std::shared_ptr<Table> table = MakeSimpleTable(values, false);
  this->sink_ = std::make_shared<InMemoryOutputStream>();
  ASSERT_OK_NO_THROW(WriteTable(*table, ::arrow::default_memory_pool(), this->sink_,
      values->length(), default_writer_properties()));

  std::shared_ptr<Table> out;
  std::unique_ptr<FileReader> reader;
  this->ReaderFromSink(&reader);
  this->ReadTableFromFile(std::move(reader), &out);
  ASSERT_EQ(1, out->num_columns());
  ASSERT_EQ(100, out->num_rows());

  std::shared_ptr<ChunkedArray> chunked_array = out->column(0)->data();
  ASSERT_EQ(1, chunked_array->num_chunks());
  ASSERT_TRUE(values->Equals(chunked_array->chunk(0)));
}

template <typename T>
using ParquetCDataType = typename ParquetDataType<T>::c_type;

template <typename T>
struct c_type_trait {
  using ArrowCType = typename T::c_type;
};

template <>
struct c_type_trait<::arrow::BooleanType> {
  using ArrowCType = uint8_t;
};

template <typename TestType>
class TestPrimitiveParquetIO : public TestParquetIO<TestType> {
 public:
  typedef typename c_type_trait<TestType>::ArrowCType T;

  void MakeTestFile(
      std::vector<T>& values, int num_chunks, std::unique_ptr<FileReader>* reader) {
    std::shared_ptr<GroupNode> schema = this->MakeSchema(Repetition::REQUIRED);
    std::unique_ptr<ParquetFileWriter> file_writer = this->MakeWriter(schema);
    size_t chunk_size = values.size() / num_chunks;
    // Convert to Parquet's expected physical type
    std::vector<uint8_t> values_buffer(
        sizeof(ParquetCDataType<TestType>) * values.size());
    auto values_parquet =
        reinterpret_cast<ParquetCDataType<TestType>*>(values_buffer.data());
    std::copy(values.cbegin(), values.cend(), values_parquet);
    for (int i = 0; i < num_chunks; i++) {
      auto row_group_writer = file_writer->AppendRowGroup(chunk_size);
      auto column_writer =
          static_cast<ParquetWriter<TestType>*>(row_group_writer->NextColumn());
      ParquetCDataType<TestType>* data = values_parquet + i * chunk_size;
      column_writer->WriteBatch(chunk_size, nullptr, nullptr, data);
      column_writer->Close();
      row_group_writer->Close();
    }
    file_writer->Close();
    this->ReaderFromSink(reader);
  }

  void CheckSingleColumnRequiredTableRead(int num_chunks) {
    std::vector<T> values(SMALL_SIZE, test_traits<TestType>::value);
    std::unique_ptr<FileReader> file_reader;
    ASSERT_NO_THROW(MakeTestFile(values, num_chunks, &file_reader));

    std::shared_ptr<Table> out;
    this->ReadTableFromFile(std::move(file_reader), &out);
    ASSERT_EQ(1, out->num_columns());
    ASSERT_EQ(SMALL_SIZE, out->num_rows());

    std::shared_ptr<ChunkedArray> chunked_array = out->column(0)->data();
    ASSERT_EQ(1, chunked_array->num_chunks());
    ExpectArrayT<TestType>(values.data(), chunked_array->chunk(0).get());
  }

  void CheckSingleColumnRequiredRead(int num_chunks) {
    std::vector<T> values(SMALL_SIZE, test_traits<TestType>::value);
    std::unique_ptr<FileReader> file_reader;
    ASSERT_NO_THROW(MakeTestFile(values, num_chunks, &file_reader));

    std::shared_ptr<Array> out;
    this->ReadSingleColumnFile(std::move(file_reader), &out);

    ExpectArrayT<TestType>(values.data(), out.get());
  }
};

typedef ::testing::Types<::arrow::BooleanType, ::arrow::UInt8Type, ::arrow::Int8Type,
    ::arrow::UInt16Type, ::arrow::Int16Type, ::arrow::UInt32Type, ::arrow::Int32Type,
    ::arrow::UInt64Type, ::arrow::Int64Type, ::arrow::FloatType, ::arrow::DoubleType>
    PrimitiveTestTypes;

TYPED_TEST_CASE(TestPrimitiveParquetIO, PrimitiveTestTypes);

TYPED_TEST(TestPrimitiveParquetIO, SingleColumnRequiredRead) {
  this->CheckSingleColumnRequiredRead(1);
}

TYPED_TEST(TestPrimitiveParquetIO, SingleColumnRequiredTableRead) {
  this->CheckSingleColumnRequiredTableRead(1);
}

TYPED_TEST(TestPrimitiveParquetIO, SingleColumnRequiredChunkedRead) {
  this->CheckSingleColumnRequiredRead(4);
}

TYPED_TEST(TestPrimitiveParquetIO, SingleColumnRequiredChunkedTableRead) {
  this->CheckSingleColumnRequiredTableRead(4);
}

void MakeDoubleTable(int num_columns, int num_rows, std::shared_ptr<Table>* out) {
  std::shared_ptr<::arrow::Column> column;
  std::vector<std::shared_ptr<::arrow::Column>> columns(num_columns);
  std::vector<std::shared_ptr<::arrow::Field>> fields(num_columns);

  std::shared_ptr<Array> values;
  for (int i = 0; i < num_columns; ++i) {
    ASSERT_OK(NullableArray<::arrow::DoubleType>(
        num_rows, num_rows / 10, static_cast<uint32_t>(i), &values));
    std::stringstream ss;
    ss << "col" << i;
    column = MakeColumn(ss.str(), values, true);

    columns[i] = column;
    fields[i] = column->field();
  }
  auto schema = std::make_shared<::arrow::Schema>(fields);
  *out = std::make_shared<Table>("schema", schema, columns);
}

void DoTableRoundtrip(const std::shared_ptr<Table>& table, int num_threads,
    const std::vector<int>& column_subset, std::shared_ptr<Table>* out) {
  auto sink = std::make_shared<InMemoryOutputStream>();

  ASSERT_OK_NO_THROW(WriteTable(
      *table, ::arrow::default_memory_pool(), sink, (table->num_rows() + 1) / 2));

  std::shared_ptr<Buffer> buffer = sink->GetBuffer();
  std::unique_ptr<FileReader> reader;
  ASSERT_OK_NO_THROW(
      OpenFile(std::make_shared<BufferReader>(buffer), ::arrow::default_memory_pool(),
          ::parquet::default_reader_properties(), nullptr, &reader));

  reader->set_num_threads(num_threads);

  if (column_subset.size() > 0) {
    ASSERT_OK_NO_THROW(reader->ReadTable(column_subset, out));
  } else {
    // Read everything
    ASSERT_OK_NO_THROW(reader->ReadTable(out));
  }
}

TEST(TestArrowReadWrite, MultithreadedRead) {
  const int num_columns = 20;
  const int num_rows = 1000;
  const int num_threads = 4;

  std::shared_ptr<Table> table;
  MakeDoubleTable(num_columns, num_rows, &table);

  std::shared_ptr<Table> result;
  DoTableRoundtrip(table, num_threads, {}, &result);

  ASSERT_TRUE(table->Equals(result));
}

TEST(TestArrowReadWrite, ReadColumnSubset) {
  const int num_columns = 20;
  const int num_rows = 1000;
  const int num_threads = 4;

  std::shared_ptr<Table> table;
  MakeDoubleTable(num_columns, num_rows, &table);

  std::shared_ptr<Table> result;
  std::vector<int> column_subset = {0, 4, 8, 10};
  DoTableRoundtrip(table, num_threads, column_subset, &result);

  std::vector<std::shared_ptr<::arrow::Column>> ex_columns;
  std::vector<std::shared_ptr<::arrow::Field>> ex_fields;
  for (int i : column_subset) {
    ex_columns.push_back(table->column(i));
    ex_fields.push_back(table->column(i)->field());
  }

  auto ex_schema = std::make_shared<::arrow::Schema>(ex_fields);
  auto expected = std::make_shared<Table>("schema", ex_schema, ex_columns);
  ASSERT_TRUE(result->Equals(expected));
}

}  // namespace arrow
}  // namespace parquet
