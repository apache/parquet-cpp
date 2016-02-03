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

#include "parquet/schema/converter.h"
#include "parquet/thrift/parquet_types.h"

using std::string;
using std::vector;

namespace parquet_cpp {

namespace schema {

// ----------------------------------------------------------------------
// Test converting leaf nodes to and from schema::Node data structures

TEST(TestConvertPrimitiveElement, TestBasics) {
  const std::string name = "name";

  parquet::SchemaElement elt;
  elt.__set_name(name);
  elt.__set_repetition_type(parquet::FieldRepetitionType::OPTIONAL);
  elt.__set_type(parquet::Type::INT32);

  std::unique_ptr<Node> node = ConvertPrimitive(&elt, 5);
  ASSERT_TRUE(node->is_primitive());

  PrimitiveNode* prim_node = static_cast<PrimitiveNode*>(node.get());

  ASSERT_EQ(name, prim_node->name());
  ASSERT_EQ(5, prim_node->id());
  ASSERT_EQ(Repetition::OPTIONAL, prim_node->repetition());
  ASSERT_EQ(Type::INT32, prim_node->physical_type());
  ASSERT_EQ(LogicalType::NONE, prim_node->logical_type());

  // Test a logical type
  elt.__set_repetition_type(parquet::FieldRepetitionType::REQUIRED);
  elt.__set_type(parquet::Type::BYTE_ARRAY);
  elt.__set_converted_type(parquet::ConvertedType::UTF8);

  node = ConvertPrimitive(&elt, 5);
  ASSERT_TRUE(node->is_primitive());
  prim_node = static_cast<PrimitiveNode*>(node.get());

  ASSERT_EQ(Repetition::REQUIRED, prim_node->repetition());
  ASSERT_EQ(Type::BYTE_ARRAY, prim_node->physical_type());
  ASSERT_EQ(LogicalType::UTF8, prim_node->logical_type());
}

TEST(TestConvertPrimitiveElement, TestFixedLenByteArray) {
}

TEST(TestConvertPrimitiveElement, TestDecimal) {
}

// ----------------------------------------------------------------------
// Schema tree flatten / unflatten

} // namespace schema

} // namespace parquet_cpp
