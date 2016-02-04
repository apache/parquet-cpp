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

#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include "parquet/util/test-common.h"

#include "parquet/schema/types.h"

using std::string;
using std::vector;

namespace parquet_cpp {

namespace schema {

// ----------------------------------------------------------------------
// Primitive node

TEST(TestPrimitiveNode, TestAttrs) {
  PrimitiveNode node1("foo", Repetition::REPEATED, Type::INT32);

  PrimitiveNode node2("bar", Repetition::OPTIONAL, Type::BYTE_ARRAY,
      LogicalType::UTF8);

  ASSERT_EQ("foo", node1.name());

  ASSERT_TRUE(node1.is_primitive());
  ASSERT_FALSE(node1.is_group());

  ASSERT_EQ(Repetition::REPEATED, node1.repetition());
  ASSERT_EQ(Repetition::OPTIONAL, node2.repetition());

  ASSERT_EQ(Node::PRIMITIVE, node1.node_type());

  ASSERT_EQ(Type::INT32, node1.physical_type());
  ASSERT_EQ(Type::BYTE_ARRAY, node2.physical_type());

  // logical types
  ASSERT_EQ(LogicalType::NONE, node1.logical_type());
  ASSERT_EQ(LogicalType::UTF8, node2.logical_type());
}

TEST(TestPrimitiveNode, TestFixedLenByteArray) {
  PrimitiveNode t1("foo", Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY, 10);

  ASSERT_EQ(10, t1.type_length());
}

TEST(TestPrimitiveNode, TestDecimal) {
  // TODO(wesm): need to look more at Parquet spec
}

TEST(TestPrimitiveNode, TestIsRepetition) {
  PrimitiveNode node1("foo", Repetition::REQUIRED, Type::INT32);
  PrimitiveNode node2("foo", Repetition::OPTIONAL, Type::INT32);
  PrimitiveNode node3("foo", Repetition::REPEATED, Type::INT32);

  ASSERT_TRUE(node1.is_required());

  ASSERT_TRUE(node2.is_optional());
  ASSERT_FALSE(node2.is_required());

  ASSERT_TRUE(node3.is_repeated());
  ASSERT_FALSE(node3.is_optional());
}

TEST(TestPrimitiveNode, TestEquals) {
  PrimitiveNode node1("foo", Repetition::REQUIRED, Type::INT32);
  PrimitiveNode node2("foo", Repetition::REQUIRED, Type::INT64);
  PrimitiveNode node3("bar", Repetition::REQUIRED, Type::INT32);
  PrimitiveNode node4("foo", Repetition::OPTIONAL, Type::INT32);
  PrimitiveNode node5("foo", Repetition::REQUIRED, Type::INT32);

  ASSERT_TRUE(node1.Equals(&node1));
  ASSERT_FALSE(node1.Equals(&node2));
  ASSERT_FALSE(node1.Equals(&node3));
  ASSERT_FALSE(node1.Equals(&node4));
  ASSERT_TRUE(node1.Equals(&node5));
}

TEST(TestPrimitiveNode, TestFLBAEquals) {
  PrimitiveNode flba1("foo", Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY, 12);
  PrimitiveNode flba2("foo", Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY, 12);
  PrimitiveNode flba3("foo", Repetition::REQUIRED, Type::FIXED_LEN_BYTE_ARRAY, 16);

  ASSERT_TRUE(flba1.Equals(&flba2));
  ASSERT_FALSE(flba1.Equals(&flba3));
}

// ----------------------------------------------------------------------
// Group node

class TestGroupNode : public ::testing::Test {
 public:
  NodeVector Fields1() {
    NodeVector fields;

    fields.push_back(Int32("one", Repetition::REQUIRED));
    fields.push_back(Int64("two"));
    fields.push_back(Double("three"));

    return fields;
  }
};

TEST_F(TestGroupNode, TestAttrs) {
  NodeVector fields = Fields1();

  GroupNode node1("foo", Repetition::REPEATED, fields);
  GroupNode node2("bar", Repetition::OPTIONAL, fields, LogicalType::LIST);

  ASSERT_EQ("foo", node1.name());

  ASSERT_TRUE(node1.is_group());
  ASSERT_FALSE(node1.is_primitive());

  ASSERT_EQ(fields.size(), node1.field_count());

  ASSERT_TRUE(node1.is_repeated());
  ASSERT_TRUE(node2.is_optional());

  ASSERT_EQ(Repetition::REPEATED, node1.repetition());
  ASSERT_EQ(Repetition::OPTIONAL, node2.repetition());

  ASSERT_EQ(Node::GROUP, node1.node_type());

  // logical types
  ASSERT_EQ(LogicalType::NONE, node1.logical_type());
  ASSERT_EQ(LogicalType::LIST, node2.logical_type());
}

TEST_F(TestGroupNode, TestEquals) {
  NodeVector f1 = Fields1();
  NodeVector f2 = Fields1();

  GroupNode group1("group", Repetition::REPEATED, f1);
  GroupNode group2("group", Repetition::REPEATED, f2);
  GroupNode group3("group2", Repetition::REPEATED, f2);

  // This is copied in the GroupNode ctor, so this is okay
  f2.push_back(Float("four", Repetition::OPTIONAL));
  GroupNode group4("group", Repetition::REPEATED, f2);

  ASSERT_TRUE(group1.Equals(&group2));
  ASSERT_FALSE(group1.Equals(&group3));

  ASSERT_FALSE(group1.Equals(&group4));
}

// ----------------------------------------------------------------------
// Schema root node

TEST_F(TestGroupNode, TestRootSchema) {
  NodeVector fields = Fields1();

  std::string test_name = "parquet_cpp_schema";
  RootSchema schema(test_name, fields);
  ASSERT_EQ(test_name, schema.name());

  // convenience ctor, no need to supply a name
  RootSchema schema2(fields);
  ASSERT_EQ("schema", schema2.name());

  // ctor equivalence
  RootSchema schema3("schema", fields);
  ASSERT_TRUE(schema2.Equals(&schema3));
}

} // namespace schema

} // namespace parquet_cpp
