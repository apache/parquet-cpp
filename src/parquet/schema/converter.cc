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

#include <string>

#include "parquet/exception.h"

using parquet::SchemaElement;

namespace parquet_cpp {

namespace schema {

// Using operator overloading on these for now, can always refactor later
static Type::type FromParquet(parquet::Type::type type) {
  return static_cast<Type::type>(type);
}

static LogicalType::type FromParquet(parquet::ConvertedType::type type) {
  // item 0 is NONE
  return static_cast<LogicalType::type>(static_cast<int>(type) + 1);
}

static Repetition::type FromParquet(parquet::FieldRepetitionType::type type) {
  return static_cast<Repetition::type>(type);
}

// TODO: decide later what to do with these. When converting back only need to
// write into a parquet::SchemaElement

// parquet::FieldRepetitionType::type ToParquet(Repetition::type type) {
//   return static_cast<parquet::FieldRepetitionType::type>(type);
// }

// parquet::ConvertedType::type ToParquet(LogicalType::type type) {
//   // item 0 is NONE
//   return static_cast<parquet::ConvertedType::type>(static_cast<int>(type) - 1);
// }

// parquet::Type::type ToParquet(Type::type type) {
//   return static_cast<parquet::Type::type>(type);
// }

struct NodeParams {
  explicit NodeParams(const std::string& name) :
      name(name) {}

  const std::string& name;
  Repetition::type repetition;
  LogicalType::type logical_type;
};

static inline NodeParams GetNodeParams(const SchemaElement* element) {
  NodeParams params(element->name);

  params.repetition = FromParquet(element->repetition_type);
  if (element->__isset.converted_type) {
    params.logical_type = FromParquet(element->converted_type);
  } else {
    params.logical_type = LogicalType::NONE;
  }
  return params;
}

std::unique_ptr<Node> ConvertPrimitive(const SchemaElement* element, int node_id) {
  NodeParams params = GetNodeParams(element);

  // TODO(wesm): FLBA metadata

  // TODO(wesm): Decimal metadata

  return std::unique_ptr<Node>(new PrimitiveNode(params.name, params.repetition,
          FromParquet(element->type), params.logical_type, node_id));
}

std::unique_ptr<Node> ConvertGroup(const SchemaElement* element, int node_id,
    const NodeVector& fields) {
  NodeParams params = GetNodeParams(element);
  return std::unique_ptr<Node>(new GroupNode(params.name, params.repetition, fields,
          params.logical_type, node_id));
}

class GroupConverter {
 public:
  GroupConverter(const SchemaElement* elements, size_t length) :
      elements_(elements),
      length_(length),
      pos_(0),
      current_id_(0) {}

  std::unique_ptr<Node> Convert() {
    const SchemaElement& root = elements_[0];

    // Validate the root node
    if (root.num_children == 0) {
      throw ParquetException("Root node did not have children");
    }

    return NextNode();
  }

 private:
  const SchemaElement* elements_;
  size_t length_;
  size_t pos_;
  size_t current_id_;

  size_t next_id() {
    return current_id_++;
  }

  std::unique_ptr<Node> NextNode() {
    const SchemaElement& element = Next();

    size_t node_id = next_id();

    if (element.num_children == 0) {
      // Leaf (primitive) node
      return ConvertPrimitive(&element, node_id);
    } else {
      // Group
      NodeVector fields;
      for (size_t i = 0; i < element.num_children; ++i) {
        std::unique_ptr<Node> field = NextNode();
        fields.push_back(NodePtr(field.release()));
      }
      return ConvertGroup(&element, node_id, fields);
    }
  }

  const SchemaElement& Next() {
    return elements_[pos_++];
  }
};


std::shared_ptr<SchemaDescriptor> FromParquet(const std::vector<SchemaElement>& schema) {
  GroupConverter converter(&schema[0], schema.size());
  std::unique_ptr<Node> root = converter.Convert();

  std::shared_ptr<SchemaDescriptor> descr = std::make_shared<SchemaDescriptor>(
      std::shared_ptr<GroupNode>(static_cast<GroupNode*>(root.release())));
  descr->Init();

  return descr;
}


} // namespace schema

} // namespace parquet_cpp
