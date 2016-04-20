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

#include "parquet/column/reader.h"
#include "parquet/column/writer.h"
#include "parquet/file/reader.h"
#include "parquet/file/writer.h"
#include "parquet/types.h"
#include "parquet/util/input.h"
#include "parquet/util/output.h"

namespace parquet {

using schema::GroupNode;
using schema::NodePtr;
using schema::PrimitiveNode;

namespace test {

class TestSerialize : public ::testing::Test {
 public:
  void SetUpSchemaRequiredNonRepeated() {
    auto pnode = PrimitiveNode::Make("int64", Repetition::REQUIRED, Type::INT64);
    node = GroupNode::Make("schema", Repetition::REQUIRED, std::vector<NodePtr>({pnode}));
    schema.Init(node);
  }

  void SetUpSchemaOptionalNonRepeated() {
    auto pnode = PrimitiveNode::Make("int64", Repetition::REQUIRED, Type::INT64);
    node = GroupNode::Make("schema", Repetition::REQUIRED, std::vector<NodePtr>({pnode}));
    schema.Init(node);
  }

  void SetUpSchemaOptionalRepeated() {
    auto pnode = PrimitiveNode::Make("int64", Repetition::REPEATED, Type::INT64);
    node = GroupNode::Make("schema", Repetition::REQUIRED, std::vector<NodePtr>({pnode}));
    schema.Init(node);
  }

  void SetUp() {
    SetUpSchemaRequiredNonRepeated();
  }

 protected:
  NodePtr node;
  SchemaDescriptor schema;
};


TEST_F(TestSerialize, SmallFile) {
  std::shared_ptr<InMemoryOutputStream> sink(new InMemoryOutputStream());
  auto gnode = std::static_pointer_cast<GroupNode>(node);
  auto file_writer = ParquetFileWriter::Open(sink, gnode);
  auto row_group_writer = file_writer->AppendRowGroup(100);
  auto column_writer = static_cast<Int64Writer*>(row_group_writer->NextColumn());
  std::vector<int64_t> values(100);
  std::fill(values.begin(), values.end(), 128);
  column_writer->WriteBatch(values.size(), nullptr, nullptr, values.data());
  column_writer->Close();
  row_group_writer->Close();
  file_writer->Close();

  auto buffer = sink->GetBuffer();
  std::unique_ptr<RandomAccessSource> source(new BufferReader(buffer));
  auto file_reader = ParquetFileReader::Open(std::move(source));
}

} // namespace test

} // namespace parquet
