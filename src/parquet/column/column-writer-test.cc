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
  void SetUp() {
    node = PrimitiveNode::Make("int64", Repetition::REQUIRED, Type::INT64);  
    schema = std::make_shared<ColumnDescriptor>(node, 0, 0);
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
  
  // Test case 1: required and non-repeated, so no definition or repetition levels
  InMemoryOutputStream sink;
  std::unique_ptr<Int64Writer> writer = BuildWriter(&sink);
  writer->WriteBatch(values.size(), nullptr, nullptr, values.data());
  writer->Close();
  writer.reset();

  std::unique_ptr<Int64Reader> reader = BuildReader(sink.GetBuffer());
  std::vector<int64_t> values_out(100);
  std::vector<int16_t> definition_levels(100);
  std::vector<int16_t> repetition_levels(100);
  int64_t values_read = 0;
  reader->ReadBatch(values.size(), definition_levels.data(), repetition_levels.data(), values_out.data(), &values_read);
  reader.reset();
  ASSERT_EQ(values_read, 100);
  ASSERT_EQ(values_out, values);
  
  // Test case 2: optional and non-repeated, with definition level but not repetition levels
  // TODO

  // Test case 3: optional and repeated, so definition and repetition levels 
  // TODO
}

} // namespace test
} // namespace parquet


