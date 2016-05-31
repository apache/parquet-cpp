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

#include "parquet/column/test-util.h"

#include "parquet/file/reader-internal.h"
#include "parquet/file/writer-internal.h"
#include "parquet/column/reader.h"
#include "parquet/column/writer.h"
#include "parquet/util/input.h"
#include "parquet/util/output.h"
#include "parquet/types.h"

namespace parquet {

using schema::NodePtr;
using schema::PrimitiveNode;

namespace test {

template <typename TestType>
class TestPrimitiveWriter : public ::testing::Test {
 public:
  typedef typename TestType::c_type T;

  void SetUpSchemaRequired() {
    node_ = PrimitiveNode::Make("column", Repetition::REQUIRED, TestType::type_num,
        LogicalType::NONE, FLBA_LENGTH);
    schema_ = std::make_shared<ColumnDescriptor>(node_, 0, 0);
  }

  void SetUpSchemaOptional() {
    node_ = PrimitiveNode::Make("column", Repetition::OPTIONAL, TestType::type_num,
        LogicalType::NONE, FLBA_LENGTH);
    schema_ = std::make_shared<ColumnDescriptor>(node_, 1, 0);
  }

  void SetUpSchemaRepeated() {
    node_ = PrimitiveNode::Make("column", Repetition::REPEATED, TestType::type_num,
        LogicalType::NONE, FLBA_LENGTH);
    schema_ = std::make_shared<ColumnDescriptor>(node_, 1, 1);
  }

  void GenerateData(int64_t num_values);

  void SetupValuesOut();

  void SetUp() {
    SetupValuesOut();
    definition_levels_out_.resize(100);
    repetition_levels_out_.resize(100);

    SetUpSchemaRequired();
  }

  void TearDown() {
    if (bool_buffer_) {
      delete[] bool_buffer_;
      bool_buffer_ = nullptr;
    }
    if (bool_buffer_out_) {
      delete[] bool_buffer_out_;
      bool_buffer_out_ = nullptr;
    }
  }

  void BuildReader() {
    auto buffer = sink_->GetBuffer();
    std::unique_ptr<InMemoryInputStream> source(new InMemoryInputStream(buffer));
    std::unique_ptr<SerializedPageReader> page_reader(
        new SerializedPageReader(std::move(source), Compression::UNCOMPRESSED));
    reader_.reset(new TypedColumnReader<TestType>(schema_.get(), std::move(page_reader)));
  }

  std::unique_ptr<TypedColumnWriter<TestType>> BuildWriter(int64_t output_size = 100) {
    sink_.reset(new InMemoryOutputStream());
    std::unique_ptr<SerializedPageWriter> pager(
        new SerializedPageWriter(sink_.get(), Compression::UNCOMPRESSED, &metadata_));
    return std::unique_ptr<TypedColumnWriter<TestType>>(
        new TypedColumnWriter<TestType>(schema_.get(), std::move(pager), output_size));
  }

  void SyncValuesOut();
  void ReadColumn() {
    BuildReader();
    reader_->ReadBatch(values_out_.size(), definition_levels_out_.data(),
        repetition_levels_out_.data(), values_out_ptr_, &values_read_);
    SyncValuesOut();
  }

 protected:
  int64_t values_read_;
  // Keep the reader alive as for ByteArray the lifetime of the ByteArray
  // content is bound to the reader.
  std::unique_ptr<TypedColumnReader<TestType>> reader_;

  // Input buffers
  std::vector<T> values_;
  std::vector<uint8_t> buffer_;
  // Pointer to the values, needed as we cannot use vector<bool>::data()
  T* values_ptr_;
  bool* bool_buffer_ = nullptr;

  // Output buffers
  std::vector<T> values_out_;
  bool* bool_buffer_out_ = nullptr;
  T* values_out_ptr_;
  std::vector<int16_t> definition_levels_out_;
  std::vector<int16_t> repetition_levels_out_;

 private:
  NodePtr node_;
  format::ColumnChunk metadata_;
  std::shared_ptr<ColumnDescriptor> schema_;
  std::unique_ptr<InMemoryOutputStream> sink_;
};

template <typename TestType>
void TestPrimitiveWriter<TestType>::SetupValuesOut() {
  values_out_.resize(100);
  values_out_ptr_ = values_out_.data();
}

template <>
void TestPrimitiveWriter<BooleanType>::SetupValuesOut() {
  values_out_.resize(100);
  bool_buffer_out_ = new bool[100];
  // Write once to all values so we can copy it without getting Valgrind errors
  // about uninitialised values.
  std::fill(bool_buffer_out_, bool_buffer_out_ + 100, true);
  values_out_ptr_ = bool_buffer_out_;
}

template <typename TestType>
void TestPrimitiveWriter<TestType>::SyncValuesOut() {}

template <>
void TestPrimitiveWriter<BooleanType>::SyncValuesOut() {
  std::copy(bool_buffer_out_, bool_buffer_out_ + values_out_.size(), values_out_.begin());
}

template <typename TestType>
void TestPrimitiveWriter<TestType>::GenerateData(int64_t num_values) {
  values_.resize(num_values);
  InitValues<T>(num_values, values_, buffer_);
  values_ptr_ = values_.data();
}

template <>
void TestPrimitiveWriter<BooleanType>::GenerateData(int64_t num_values) {
  values_.resize(num_values);
  InitValues<T>(num_values, values_, buffer_);
  bool_buffer_ = new bool[num_values];
  std::copy(values_.begin(), values_.end(), bool_buffer_);
  values_ptr_ = bool_buffer_;
}

typedef ::testing::Types<Int32Type, Int64Type, Int96Type, FloatType, DoubleType,
    BooleanType, ByteArrayType, FLBAType> TestTypes;

TYPED_TEST_CASE(TestPrimitiveWriter, TestTypes);

TYPED_TEST(TestPrimitiveWriter, RequiredNonRepeated) {
  this->GenerateData(100);

  // Test case 1: required and non-repeated, so no definition or repetition levels
  std::unique_ptr<TypedColumnWriter<TypeParam>> writer = this->BuildWriter();
  writer->WriteBatch(this->values_.size(), nullptr, nullptr, this->values_ptr_);
  writer->Close();

  this->ReadColumn();
  ASSERT_EQ(100, this->values_read_);
  ASSERT_EQ(this->values_, this->values_out_);
}

TYPED_TEST(TestPrimitiveWriter, OptionalNonRepeated) {
  // Optional and non-repeated, with definition levels
  // but no repetition levels
  this->SetUpSchemaOptional();

  this->GenerateData(100);
  std::vector<int16_t> definition_levels(100, 1);
  definition_levels[1] = 0;

  auto writer = this->BuildWriter();
  writer->WriteBatch(
      this->values_.size(), definition_levels.data(), nullptr, this->values_ptr_);
  writer->Close();

  this->ReadColumn();
  ASSERT_EQ(99, this->values_read_);
  this->values_out_.resize(99);
  this->values_.resize(99);
  ASSERT_EQ(this->values_, this->values_out_);
}

TYPED_TEST(TestPrimitiveWriter, OptionalRepeated) {
  // Optional and repeated, so definition and repetition levels
  this->SetUpSchemaRepeated();

  this->GenerateData(100);
  std::vector<int16_t> definition_levels(100, 1);
  definition_levels[1] = 0;
  std::vector<int16_t> repetition_levels(100, 0);

  auto writer = this->BuildWriter();
  writer->WriteBatch(this->values_.size(), definition_levels.data(),
      repetition_levels.data(), this->values_ptr_);
  writer->Close();

  this->ReadColumn();
  ASSERT_EQ(99, this->values_read_);
  this->values_out_.resize(99);
  this->values_.resize(99);
  ASSERT_EQ(this->values_, this->values_out_);
}

TYPED_TEST(TestPrimitiveWriter, RequiredTooFewRows) {
  this->GenerateData(99);

  auto writer = this->BuildWriter();
  writer->WriteBatch(this->values_.size(), nullptr, nullptr, this->values_ptr_);
  ASSERT_THROW(writer->Close(), ParquetException);
}

TYPED_TEST(TestPrimitiveWriter, RequiredTooMany) {
  this->GenerateData(200);

  auto writer = this->BuildWriter();
  ASSERT_THROW(
      writer->WriteBatch(this->values_.size(), nullptr, nullptr, this->values_ptr_),
      ParquetException);
}

TYPED_TEST(TestPrimitiveWriter, OptionalRepeatedTooFewRows) {
  // Optional and repeated, so definition and repetition levels
  this->SetUpSchemaRepeated();

  this->GenerateData(100);
  std::vector<int16_t> definition_levels(100, 1);
  definition_levels[1] = 0;
  std::vector<int16_t> repetition_levels(100, 0);
  repetition_levels[3] = 1;

  auto writer = this->BuildWriter();
  writer->WriteBatch(this->values_.size(), definition_levels.data(),
      repetition_levels.data(), this->values_ptr_);
  ASSERT_THROW(writer->Close(), ParquetException);
}

TYPED_TEST(TestPrimitiveWriter, RequiredNonRepeatedLargeChunk) {
  this->GenerateData(10000);

  // Test case 1: required and non-repeated, so no definition or repetition levels
  auto writer = this->BuildWriter(10000);
  writer->WriteBatch(this->values_.size(), nullptr, nullptr, this->values_ptr_);
  writer->Close();

  // Just read the first 100 to ensure we could read it back in
  this->ReadColumn();
  ASSERT_EQ(100, this->values_read_);
  this->values_.resize(100);
  ASSERT_EQ(this->values_, this->values_out_);
}

}  // namespace test
}  // namespace parquet
