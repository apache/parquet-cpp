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

#include "parquet/schema/converter.h"

#include "parquet/exception.h"

using parquet::SchemaElement;

namespace parquet_cpp {

namespace schema {

class ParquetSchemaConverter {
 public:
  ParquetSchemaConverter(const SchemaElement* elements, size_t length) :
      elements_(elements),
      length_(length),
      pos_(0),
      current_id_(0) {}

  std::shared_ptr<SchemaInfo> Convert() {
    const SchemaElement& root = Next();

    // Validate the root node
    if (root.num_children == 0) {
      throw ParquetException("Parquet schema did not have a root group");
    }

    size_t root_id = next_id();

    NodeVector fields;
    for (size_t i = 0; i < root.num_children; ++i) {
      NodePtr field = NextNode();
      fields.push_back(field);
    }

    std::shared_ptr<Schema> result(new Schema(fields));
    return std::make_shared<SchemaInfo>(result);
  }

 private:
  size_t next_id() {
    return current_id_++;
  }

  NodePtr NextNode() {
    const SchemaElement& element = Next();

    size_t node_id = next_id();

    LogicalType::type logical_type = LogicalType::NONE;
    if (element.__isset.converted_type) {
      logical_type = LogicalType::FromParquet(element.converted_type);
    }

    Repetition::type repetition = Repetition::FromParquet(element.repetition_type);

    if (element.num_children == 0) {
      // Leaf (primitive) node
      Type::type primitive_type = Type::FromParquet(element.type);

      // TODO(wesm): FLBA metadata

      // TODO(wesm): Decimal metadata

      return NodePtr(new PrimitiveNode(element.name, repetition, primitive_type,
              logical_type, node_id));
    } else {
      // Group
      NodeVector fields;
      for (size_t i = 0; i < element.num_children; ++i) {
        NodePtr field = NextNode();
        fields.push_back(field);
      }

      return NodePtr(new GroupNode(element.name, repetition, fields,
              logical_type, node_id));
    }
  }
  bool HasNext() const {
    return pos_ < length_;
  }

  const SchemaElement& Peek() const {
    return elements_[pos_];
  }

  const SchemaElement& Next() {
    return elements_[pos_++];
  }

  const SchemaElement* elements_;
  size_t length_;
  size_t pos_;
  size_t current_id_;
};


std::shared_ptr<SchemaInfo> FromParquet(const std::vector<SchemaElement>& schema) {
  ParquetSchemaConverter converter(&schema[0], schema.size());
  return converter.Convert();
}


} // namespace schema

} // namespace parquet_cpp
