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

#include <gtest/gtest.h>

#include <arrow/io/file.h>

#include <parquet/api/reader.h>
#include <parquet/api/writer.h>

#include <memory>

namespace parquet {

using parquet::Repetition;
using parquet::Type;
using parquet::LogicalType;
using parquet::schema::PrimitiveNode;
using parquet::schema::GroupNode;

static int FLBA_LENGTH = 12;
static int NUM_VALUES = 10;

namespace test {

template <typename TestType>
class TestStatistics : public ::testing::Test {
 public:
  typedef typename TestType::c_type T;

  void AddNodes(std::string name);

  void SetUpSchema() {
    stats_.resize(fields_.size());
    values_.resize(NUM_VALUES);
    schema_ = std::static_pointer_cast<GroupNode>(
        GroupNode::Make("Schema", Repetition::REQUIRED, fields_));

    parquet_sink_ = std::make_shared<InMemoryOutputStream>();
  }

  void SetValues();

  void WriteParquet() {
    // Add writer properties
    parquet::WriterProperties::Builder builder;
    builder.compression(parquet::Compression::SNAPPY);
    builder.created_by("parquet-cpp version 1.3.0");
    std::shared_ptr<parquet::WriterProperties> props = builder.build();

    // Create a ParquetFileWriter instance
    auto file_writer = parquet::ParquetFileWriter::Open(parquet_sink_, schema_, props);

    // Append a RowGroup with a specific number of rows.
    auto rg_writer = file_writer->AppendRowGroup(NUM_VALUES);

    this->SetValues();

    // Insert Values
    for (size_t i = 0; i < fields_.size(); i++) {
      auto column_writer =
          static_cast<parquet::TypedColumnWriter<TestType>*>(rg_writer->NextColumn());
      column_writer->WriteBatch(NUM_VALUES, nullptr, nullptr, values_.data());
    }
  }

  void VerifyParquetStats() {
    auto pbuffer = parquet_sink_->GetBuffer();

    // Create a ParquetReader instance
    std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
        parquet::ParquetFileReader::Open(
            std::make_shared<arrow::io::BufferReader>(pbuffer));

    // Get the File MetaData
    std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();
    std::shared_ptr<parquet::RowGroupMetaData> rg_metadata = file_metadata->RowGroup(0);
    for (size_t i = 0; i < fields_.size(); i++) {
      std::shared_ptr<parquet::ColumnChunkMetaData> cc_metadata =
          rg_metadata->ColumnChunk(i);
      ASSERT_EQ(stats_[i].min(), cc_metadata->statistics()->EncodeMin());
      ASSERT_EQ(stats_[i].max(), cc_metadata->statistics()->EncodeMax());
    }
  }

 protected:
  std::vector<T> values_;
  std::vector<uint8_t> values_buf_;
  std::vector<schema::NodePtr> fields_;
  std::shared_ptr<schema::GroupNode> schema_;
  std::shared_ptr<InMemoryOutputStream> parquet_sink_;
  std::vector<EncodedStatistics> stats_;
};

using TestTypes = ::testing::Types<Int32Type, Int64Type, Int96Type, FloatType, DoubleType,
                                   ByteArrayType, FLBAType>;

// TYPE::INT32
template <>
void TestStatistics<Int32Type>::AddNodes(std::string name) {
  // UINT_32 logical type to set Unsigned Statistics
  fields_.push_back(schema::PrimitiveNode::Make(name, Repetition::REQUIRED, Type::INT32,
                                                LogicalType::UINT_32));
  // INT_32 logical type to set Signed Statistics
  fields_.push_back(schema::PrimitiveNode::Make(name, Repetition::REQUIRED, Type::INT32,
                                                LogicalType::INT_32));
}

template <>
void TestStatistics<Int32Type>::SetValues() {
  for (int i = 0; i < NUM_VALUES; i++) {
    values_[i] = i - 5;  // {-5, -4, -3, -2, -1, 0, 1, 2, 3, 4};
  }

  // Write UINT32 values and min,max
  stats_[0]
      .set_min(std::string(reinterpret_cast<const char*>(&values_[5]), sizeof(T)))
      .set_max(std::string(reinterpret_cast<const char*>(&values_[4]), sizeof(T)));

  // Write INT32 values and min,max
  stats_[1]
      .set_min(std::string(reinterpret_cast<const char*>(&values_[0]), sizeof(T)))
      .set_max(std::string(reinterpret_cast<const char*>(&values_[9]), sizeof(T)));
}

// TYPE::INT64
template <>
void TestStatistics<Int64Type>::AddNodes(std::string name) {
  // UINT_64 logical type to set Unsigned Statistics
  fields_.push_back(schema::PrimitiveNode::Make(name, Repetition::REQUIRED, Type::INT64,
                                                LogicalType::UINT_64));
  // INT_64 logical type to set Signed Statistics
  fields_.push_back(schema::PrimitiveNode::Make(name, Repetition::REQUIRED, Type::INT64,
                                                LogicalType::INT_64));
}

template <>
void TestStatistics<Int64Type>::SetValues() {
  for (int i = 0; i < NUM_VALUES; i++) {
    values_[i] = i - 5;  // {-5, -4, -3, -2, -1, 0, 1, 2, 3, 4};
  }

  // Write UINT64 values and min,max
  stats_[0]
      .set_min(std::string(reinterpret_cast<const char*>(&values_[5]), sizeof(T)))
      .set_max(std::string(reinterpret_cast<const char*>(&values_[4]), sizeof(T)));

  // Write INT64 values and min,max
  stats_[1]
      .set_min(std::string(reinterpret_cast<const char*>(&values_[0]), sizeof(T)))
      .set_max(std::string(reinterpret_cast<const char*>(&values_[9]), sizeof(T)));
}

// TYPE::INT96
template <>
void TestStatistics<Int96Type>::AddNodes(std::string name) {
  // INT96 physical type has only Unsigned Statistics
  fields_.push_back(schema::PrimitiveNode::Make(name, Repetition::REQUIRED, Type::INT96,
                                                LogicalType::NONE));
}

template <>
void TestStatistics<Int96Type>::SetValues() {
  for (int i = 0; i < NUM_VALUES; i++) {
    values_[i].value[0] = i - 5;  // {-5, -4, -3, -2, -1, 0, 1, 2, 3, 4};
    values_[i].value[1] = i - 5;  // {-5, -4, -3, -2, -1, 0, 1, 2, 3, 4};
    values_[i].value[2] = i - 5;  // {-5, -4, -3, -2, -1, 0, 1, 2, 3, 4};
  }

  // Write Int96 values and min,max
  stats_[0]
      .set_min(std::string(reinterpret_cast<const char*>(&values_[5]), sizeof(T)))
      .set_max(std::string(reinterpret_cast<const char*>(&values_[4]), sizeof(T)));
}

// TYPE::FLOAT
template <>
void TestStatistics<FloatType>::AddNodes(std::string name) {
  // Float physical type has only Signed Statistics
  fields_.push_back(schema::PrimitiveNode::Make(name, Repetition::REQUIRED, Type::FLOAT,
                                                LogicalType::NONE));
}

template <>
void TestStatistics<FloatType>::SetValues() {
  for (int i = 0; i < NUM_VALUES; i++) {
    values_[i] =
        (i * 1.0f) - 5;  // {-5.0, -4.0, -3.0, -2.0, -1.0, 0.0, 1.0, 2.0, 3.0, 4.0};
  }

  // Write Float values and min,max
  stats_[0]
      .set_min(std::string(reinterpret_cast<const char*>(&values_[0]), sizeof(T)))
      .set_max(std::string(reinterpret_cast<const char*>(&values_[9]), sizeof(T)));
}

// TYPE::DOUBLE
template <>
void TestStatistics<DoubleType>::AddNodes(std::string name) {
  // Double physical type has only Signed Statistics
  fields_.push_back(schema::PrimitiveNode::Make(name, Repetition::REQUIRED, Type::DOUBLE,
                                                LogicalType::NONE));
}

template <>
void TestStatistics<DoubleType>::SetValues() {
  for (int i = 0; i < NUM_VALUES; i++) {
    values_[i] =
        (i * 1.0f) - 5;  // {-5.0, -4.0, -3.0, -2.0, -1.0, 0.0, 1.0, 2.0, 3.0, 4.0};
  }

  // Write Double values and min,max
  stats_[0]
      .set_min(std::string(reinterpret_cast<const char*>(&values_[0]), sizeof(T)))
      .set_max(std::string(reinterpret_cast<const char*>(&values_[9]), sizeof(T)));
}

// TYPE::ByteArray
template <>
void TestStatistics<ByteArrayType>::AddNodes(std::string name) {
  // UTF8 logical type to set Unsigned Statistics
  fields_.push_back(schema::PrimitiveNode::Make(name, Repetition::REQUIRED,
                                                Type::BYTE_ARRAY, LogicalType::UTF8));
}

template <>
void TestStatistics<ByteArrayType>::SetValues() {
  int max_byte_array_len = 4;
  size_t nbytes = NUM_VALUES * max_byte_array_len;
  values_buf_.resize(nbytes);
  std::string vals[NUM_VALUES] = {u8"c123", u8"b123", u8"a123", u8"d123", u8"e123",
                                  u8"f123", u8"g123", u8"h123", u8"i123", u8"üa123"};

  for (int i = 0; i < NUM_VALUES; i++) {
    uint8_t* base = &values_buf_.data()[0] + (i * max_byte_array_len);
    memcpy(base, vals[i].c_str(), max_byte_array_len);
    values_[i].ptr = base;
    values_[i].len = max_byte_array_len;
  }

  // Write String values and min,max
  stats_[0]
      .set_min(
          std::string(reinterpret_cast<const char*>(vals[2].c_str()), max_byte_array_len))
      .set_max(std::string(reinterpret_cast<const char*>(vals[9].c_str()),
                           max_byte_array_len));
}

// TYPE::FLBAArray
template <>
void TestStatistics<FLBAType>::AddNodes(std::string name) {
  // FLBA has only Unsigned Statistics
  fields_.push_back(schema::PrimitiveNode::Make(name, Repetition::REQUIRED,
                                                Type::FIXED_LEN_BYTE_ARRAY,
                                                LogicalType::NONE, FLBA_LENGTH));
}

template <>
void TestStatistics<FLBAType>::SetValues() {
  size_t nbytes = NUM_VALUES * FLBA_LENGTH;
  values_buf_.resize(nbytes);
  std::string vals[NUM_VALUES] = {u8"a12345", u8"a123", u8"c123", u8"d123",  u8"e123",
                                  u8"f123",   u8"g123", u8"h123", u8"üa123", u8"üa12345"};

  for (int i = 0; i < NUM_VALUES; i++) {
    uint8_t* base = &values_buf_.data()[0] + (i * FLBA_LENGTH);
    memcpy(base, vals[i].c_str(), FLBA_LENGTH);
    values_[i].ptr = base;
  }

  // Write String values and min,max
  stats_[0]
      .set_min(std::string(reinterpret_cast<const char*>(vals[1].c_str()), FLBA_LENGTH))
      .set_max(std::string(reinterpret_cast<const char*>(vals[9].c_str()), FLBA_LENGTH));
}

TYPED_TEST_CASE(TestStatistics, TestTypes);

TYPED_TEST(TestStatistics, MinMax) {
  this->AddNodes("Column ");
  this->SetUpSchema();
  this->WriteParquet();
  this->VerifyParquetStats();
}

// Ensure UNKNOWN sort order is handled properly
using TestStatisticsFLBA = TestStatistics<FLBAType>;

TEST_F(TestStatisticsFLBA, UnknownSortOrder) {
  this->fields_.push_back(schema::PrimitiveNode::Make(
      "Column 0", Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY, LogicalType::INTERVAL,
      FLBA_LENGTH));
  this->SetUpSchema();
  this->WriteParquet();

  auto pbuffer = parquet_sink_->GetBuffer();
  // Create a ParquetReader instance
  std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
      parquet::ParquetFileReader::Open(
          std::make_shared<arrow::io::BufferReader>(pbuffer));
  // Get the File MetaData
  std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();
  std::shared_ptr<parquet::RowGroupMetaData> rg_metadata = file_metadata->RowGroup(0);
  std::shared_ptr<parquet::ColumnChunkMetaData> cc_metadata = rg_metadata->ColumnChunk(0);
  // stats should not be set
  ASSERT_FALSE(cc_metadata->is_stats_set());
}

}  // namespace test
}  // namespace parquet
