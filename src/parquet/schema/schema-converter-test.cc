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

using parquet::ConvertedType;
using parquet::FieldRepetitionType;
using parquet::SchemaElement;

namespace parquet_cpp {

namespace schema {

static SchemaElement NewPrimitive(const std::string& name,
    FieldRepetitionType::type repetition, parquet::Type::type type) {
  SchemaElement result;
  result.__set_name(name);
  result.__set_repetition_type(repetition);
  result.__set_type(type);

  return result;
}

// ----------------------------------------------------------------------
// Test converting leaf nodes to and from schema::Node data structures

class TestConvertPrimitive : public ::testing::Test {
 public:
  void setUp() {
    name_ = "name";
    id_ = 5;
  }

  void Convert(const parquet::SchemaElement* element) {
    node_ = ConvertPrimitive(element, id_);
    ASSERT_TRUE(node_->is_primitive());
    prim_node_ = static_cast<const PrimitiveNode*>(node_.get());
  }

 protected:
  std::string name_;
  const PrimitiveNode* prim_node_;

  int id_;
  std::unique_ptr<Node> node_;
};

TEST_F(TestConvertPrimitive, TestBasics) {
  SchemaElement elt = NewPrimitive(name_, FieldRepetitionType::OPTIONAL,
      parquet::Type::INT32);

  Convert(&elt);
  ASSERT_EQ(name_, prim_node_->name());
  ASSERT_EQ(id_, prim_node_->id());
  ASSERT_EQ(Repetition::OPTIONAL, prim_node_->repetition());
  ASSERT_EQ(Type::INT32, prim_node_->physical_type());
  ASSERT_EQ(LogicalType::NONE, prim_node_->logical_type());

  // Test a logical type
  elt = NewPrimitive(name_, FieldRepetitionType::REQUIRED, parquet::Type::BYTE_ARRAY);
  elt.__set_converted_type(ConvertedType::UTF8);

  Convert(&elt);
  ASSERT_EQ(Repetition::REQUIRED, prim_node_->repetition());
  ASSERT_EQ(Type::BYTE_ARRAY, prim_node_->physical_type());
  ASSERT_EQ(LogicalType::UTF8, prim_node_->logical_type());
}

TEST_F(TestConvertPrimitive, TestFixedLenByteArray) {
  SchemaElement elt = NewPrimitive(name_, FieldRepetitionType::OPTIONAL,
      parquet::Type::FIXED_LEN_BYTE_ARRAY);
  elt.__set_type_length(16);

  Convert(&elt);
  ASSERT_EQ(name_, prim_node_->name());
  ASSERT_EQ(id_, prim_node_->id());
  ASSERT_EQ(Repetition::OPTIONAL, prim_node_->repetition());
  ASSERT_EQ(Type::FIXED_LEN_BYTE_ARRAY, prim_node_->physical_type());
  ASSERT_EQ(16, prim_node_->type_length());
}

TEST_F(TestConvertPrimitive, TestDecimal) {
}

// ----------------------------------------------------------------------
// Test convert group

static SchemaElement NewGroup(const std::string& name,
    FieldRepetitionType::type repetition, size_t num_children) {
  SchemaElement result;
  result.__set_name(name);
  result.__set_repetition_type(repetition);
  result.__set_num_children(num_children);

  return result;
}

class TestSchemaConverter : public ::testing::Test {
 public:
  void setUp() {
    name_ = "bag";
  }

  void Convert(const parquet::SchemaElement* elements, size_t length) {
    FlatSchemaConverter converter(elements, length);
    node_ = converter.Convert();
    ASSERT_TRUE(node_->is_group());
    group_ = static_cast<const GroupNode*>(node_.get());
  }

 protected:
  std::string name_;
  const GroupNode* group_;
  std::unique_ptr<Node> node_;
};

TEST_F(TestSchemaConverter, NestedExample) {
  std::vector<SchemaElement> elements;
  elements.push_back(NewGroup("schema", FieldRepetitionType::REPEATED, 2));
}

TEST_F(TestSchemaConverter, InvalidRoot) {
  SchemaElement elements[2];
  elements[0] = NewPrimitive("not-a-group", FieldRepetitionType::REQUIRED,
      parquet::Type::INT32);
  elements[0].num_children = 0;
  ASSERT_THROW(Convert(elements, 2), ParquetException);

  elements[0] = NewGroup("not-repeated", FieldRepetitionType::REQUIRED, 1);
  ASSERT_THROW(Convert(elements, 2), ParquetException);

  elements[0] = NewGroup("not-repeated", FieldRepetitionType::OPTIONAL, 1);
  ASSERT_THROW(Convert(elements, 2), ParquetException);
}

TEST_F(TestSchemaConverter, LogicalTypes) {
}

// ----------------------------------------------------------------------
// Schema tree flatten / unflatten

} // namespace schema

} // namespace parquet_cpp
