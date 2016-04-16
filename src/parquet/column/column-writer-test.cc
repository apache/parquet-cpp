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

class TestPrimitiveWriter : public ::testing::Test {
 public:
  void SetUpSchemaRequiredNonRepeated() {
    node = PrimitiveNode::Make("int64", Repetition::REQUIRED, Type::INT64);  
    schema = std::make_shared<ColumnDescriptor>(node, 0, 0);
  }

  void SetUpSchemaOptionalNonRepeated() {
    node = PrimitiveNode::Make("int64", Repetition::REQUIRED, Type::INT64);
    schema = std::make_shared<ColumnDescriptor>(node, 1, 0);
  }

  void SetUp() {
    SetUpSchemaRequiredNonRepeated();
  }

  std::unique_ptr<Int64Reader> BuildReader(const std::shared_ptr<Buffer>& buffer) {
    std::unique_ptr<InMemoryInputStream> source(new InMemoryInputStream(buffer));
    std::unique_ptr<SerializedPageReader> page_reader(new SerializedPageReader(std::move(source), Compression::UNCOMPRESSED));
    return std::unique_ptr<Int64Reader>(new Int64Reader(schema.get(), std::move(page_reader)));
  }

  std::unique_ptr<Int64Writer> BuildWriter(OutputStream* sink) {
    std::unique_ptr<SerializedPageWriter> pager(new SerializedPageWriter(sink, Compression::UNCOMPRESSED));
    return std::unique_ptr<Int64Writer>(new Int64Writer(schema.get(), std::move(pager)));
  }

 private:
  NodePtr node;
  std::shared_ptr<ColumnDescriptor> schema;
};

TEST_F(TestPrimitiveWriter, WriteReadLoopSinglePage) {
  // Small dataset that should fit inside a single page
  std::vector<int64_t> values(100);
  std::fill(values.begin(), values.end(), 128);
  std::vector<int16_t> definition_levels(100);
  std::fill(definition_levels.begin(), definition_levels.end(), 1);
  definition_levels[1] = 0;
  std::vector<int16_t> repetition_levels(100);

  // Output buffers
  std::vector<int64_t> values_out(100);
  std::vector<int16_t> definition_levels_out(100);
  std::vector<int16_t> repetition_levels_out(100);
  
  // Test case 1: required and non-repeated, so no definition or repetition levels
  std::unique_ptr<InMemoryOutputStream> sink(new InMemoryOutputStream());
  std::unique_ptr<Int64Writer> writer = BuildWriter(sink.get());
  writer->WriteBatch(values.size(), nullptr, nullptr, values.data());
  writer->Close();

  std::unique_ptr<Int64Reader> reader = BuildReader(sink->GetBuffer());
  int64_t values_read = 0;
  reader->ReadBatch(values.size(), definition_levels_out.data(), repetition_levels_out.data(), values_out.data(), &values_read);
  ASSERT_EQ(values_read, 100);
  ASSERT_EQ(values_out, values);
  
  // Test case 2: optional and non-repeated, with definition level but not repetition levels
  SetUpSchemaOptionalNonRepeated();
  sink.reset(new InMemoryOutputStream());
  writer = BuildWriter(sink.get());
  // TODO: Implement definition_levels
  writer->WriteBatch(values.size(), definition_levels.data(), nullptr, values.data());
  writer->Close();
  
  reader = BuildReader(sink->GetBuffer());
  values_read = 0;
  reader->ReadBatch(values.size(), definition_levels_out.data(), repetition_levels_out.data(), values_out.data(), &values_read);
  ASSERT_EQ(values_read, 99);
  std::vector<int64_t> values_expected(99);
  std::fill(values_expected.begin(), values_expected.end(), 128);
  values_out.resize(99);
  ASSERT_EQ(values_out, values_expected);

  // Test case 3: optional and repeated, so definition and repetition levels 
  // TODO
}

} // namespace test
} // namespace parquet


