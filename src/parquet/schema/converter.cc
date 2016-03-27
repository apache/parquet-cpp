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
#include "parquet/schema/descriptor.h"
#include "parquet/schema/types.h"
#include "parquet/thrift/parquet_types.h"

using parquet::format::SchemaElement;

namespace parquet {

namespace schema {

std::unique_ptr<Node> FlatSchemaConverter::Convert() {
  const SchemaElement& root = elements_[0];

  // Validate the root node
  if (root.num_children == 0) {
    throw ParquetException("Root node did not have children");
  }

  // Relaxing this restriction as some implementations don't set this
  // if (root.repetition_type != FieldRepetitionType::REPEATED) {
  //   throw ParquetException("Root node was not FieldRepetitionType::REPEATED");
  // }

  return NextNode();
}

std::unique_ptr<Node> FlatSchemaConverter::NextNode() {
  const SchemaElement& element = Next();

  int node_id = next_id();

  const void* opaque_element = static_cast<const void*>(&element);

  if (element.num_children == 0) {
    // Leaf (primitive) node
    return PrimitiveNode::FromParquet(opaque_element, node_id);
  } else {
    // Group
    NodeVector fields;
    for (int i = 0; i < element.num_children; ++i) {
      std::unique_ptr<Node> field = NextNode();
      fields.push_back(NodePtr(field.release()));
    }
    return GroupNode::FromParquet(opaque_element, node_id, fields);
  }
}

const format::SchemaElement& FlatSchemaConverter::Next() {
  if (pos_ == length_) {
    throw ParquetException("Malformed schema: not enough SchemaElement values");
  }
  return elements_[pos_++];
}

std::shared_ptr<SchemaDescriptor> FromParquet(const std::vector<SchemaElement>& schema) {
  FlatSchemaConverter converter(&schema[0], schema.size());
  std::unique_ptr<Node> root = converter.Convert();

  std::shared_ptr<SchemaDescriptor> descr = std::make_shared<SchemaDescriptor>();
  descr->Init(std::shared_ptr<GroupNode>(
          static_cast<GroupNode*>(root.release())));

  return descr;
}

} // namespace schema

} // namespace parquet
