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

#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include "parquet/exception.h"
#include "parquet/schema/converter.h"
#include "parquet/schema/test-util.h"
#include "parquet/schema/types.h"
#include "parquet/thrift/parquet_types.h"
#include "parquet/types.h"

using std::string;
using std::vector;

using parquet::format::ConvertedType;
using parquet::format::FieldRepetitionType;
using parquet::format::SchemaElement;

namespace parquet {

namespace schema {

// ----------------------------------------------------------------------
// Test convert group

class TestSchemaConverter : public ::testing::Test {
 public:
  void setUp() {
    name_ = "parquet_schema";
  }

  void Convert(const parquet::format::SchemaElement* elements, int length) {
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

bool check_for_parent_consistency(const GroupNode* node) {
  // Each node should have the group as parent
  for (int i = 0; i < node->field_count(); i++) {
    const NodePtr& field = node->field(i);
    if (field->parent() != node) {
      return false;
    }
    if (field->is_group()) {
      const GroupNode* group = static_cast<GroupNode*>(field.get());
      if (!check_for_parent_consistency(group)) {
        return false;
      }
    }
  }
  return true;
}

TEST_F(TestSchemaConverter, NestedExample) {
  SchemaElement elt;
  std::vector<SchemaElement> elements;
  elements.push_back(NewGroup(name_, FieldRepetitionType::REPEATED, 2, 0));

  // A primitive one
  elements.push_back(NewPrimitive("a", FieldRepetitionType::REQUIRED,
          format::Type::INT32, 1));

  // A group
  elements.push_back(NewGroup("bag", FieldRepetitionType::OPTIONAL, 1, 2));

  // 3-level list encoding, by hand
  elt = NewGroup("b", FieldRepetitionType::REPEATED, 1, 3);
  elt.__set_converted_type(ConvertedType::LIST);
  elements.push_back(elt);
  elements.push_back(NewPrimitive("item", FieldRepetitionType::OPTIONAL,
          format::Type::INT64, 4));

  Convert(&elements[0], elements.size());

  // Construct the expected schema
  NodeVector fields;
  fields.push_back(Int32("a", Repetition::REQUIRED));

  // 3-level list encoding
  NodePtr item = Int64("item");
  NodePtr list(GroupNode::Make("b", Repetition::REPEATED, {item}, LogicalType::LIST));
  NodePtr bag(GroupNode::Make("bag", Repetition::OPTIONAL, {list}));
  fields.push_back(bag);

  NodePtr schema = GroupNode::Make(name_, Repetition::REPEATED, fields);

  ASSERT_TRUE(schema->Equals(group_));

  // Check that the parent relationship in each node is consitent
  ASSERT_EQ(group_->parent(), nullptr);
  ASSERT_TRUE(check_for_parent_consistency(group_));
}

TEST_F(TestSchemaConverter, InvalidRoot) {
  // According to the Parquet specification, the first element in the
  // list<SchemaElement> is a group whose children (and their descendants)
  // contain all of the rest of the flattened schema elements. If the first
  // element is not a group, it is a malformed Parquet file.

  SchemaElement elements[2];
  elements[0] = NewPrimitive("not-a-group", FieldRepetitionType::REQUIRED,
      format::Type::INT32, 0);
  ASSERT_THROW(Convert(elements, 2), ParquetException);

  // While the Parquet spec indicates that the root group should have REPEATED
  // repetition type, some implementations may return REQUIRED or OPTIONAL
  // groups as the first element. These tests check that this is okay as a
  // practicality matter.
  elements[0] = NewGroup("not-repeated", FieldRepetitionType::REQUIRED, 1, 0);
  elements[1] = NewPrimitive("a", FieldRepetitionType::REQUIRED,
      format::Type::INT32, 1);
  Convert(elements, 2);

  elements[0] = NewGroup("not-repeated", FieldRepetitionType::OPTIONAL, 1, 0);
  Convert(elements, 2);
}

TEST_F(TestSchemaConverter, NotEnoughChildren) {
  // Throw a ParquetException, but don't core dump or anything
  SchemaElement elt;
  std::vector<SchemaElement> elements;
  elements.push_back(NewGroup(name_, FieldRepetitionType::REPEATED, 2, 0));
  ASSERT_THROW(Convert(&elements[0], 1), ParquetException);
}

// ----------------------------------------------------------------------
// Schema tree flatten / unflatten

class TestSchemaFlatten : public ::testing::Test {
 public:
  void setUp() {
    name_ = "parquet_schema";
  }

  void Flatten(const GroupNode* schema) {
    ToParquet(schema, &elements_);
  }

 protected:
  std::string name_;
  std::vector<format::SchemaElement> elements_;
};

TEST_F(TestSchemaFlatten, NestedExample) {
  SchemaElement elt;
  std::vector<SchemaElement> elements;
  elements.push_back(NewGroup(name_, FieldRepetitionType::REPEATED, 2, 0));

  // A primitive one
  elements.push_back(NewPrimitive("a", FieldRepetitionType::REQUIRED,
          format::Type::INT32, 1));

  // A group
  elements.push_back(NewGroup("bag", FieldRepetitionType::OPTIONAL, 1, 2));

  // 3-level list encoding, by hand
  elt = NewGroup("b", FieldRepetitionType::REPEATED, 1, 3);
  elt.__set_converted_type(ConvertedType::LIST);
  elements.push_back(elt);
  elements.push_back(NewPrimitive("item", FieldRepetitionType::OPTIONAL,
          format::Type::INT64, 4));

  // Construct the schema
  NodeVector fields;
  fields.push_back(Int32("a", Repetition::REQUIRED));

  // 3-level list encoding
  NodePtr item = Int64("item");
  NodePtr list(GroupNode::Make("b", Repetition::REPEATED, {item}, LogicalType::LIST));
  NodePtr bag(GroupNode::Make("bag", Repetition::OPTIONAL, {list}));
  fields.push_back(bag);

  NodePtr schema = GroupNode::Make(name_, Repetition::REPEATED, fields);

  Flatten(static_cast<GroupNode*>(schema.get()));
  ASSERT_EQ(elements_.size(), elements.size());
  for (size_t i = 0; i < elements_.size(); i++) {
    ASSERT_EQ(elements_[i], elements[i]);
  }
}

} // namespace schema

} // namespace parquet
