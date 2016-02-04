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

#include "parquet/schema/types.h"

namespace parquet_cpp {

namespace schema {

// ----------------------------------------------------------------------
// Base node

bool Node::EqualsInternal(const Node* other) const {
  return type_ == other->type_ &&
    name_ == other->name_ &&
    repetition_ == other->repetition_ &&
    logical_type_ == other->logical_type_;
}

// ----------------------------------------------------------------------
// Primitive node

bool PrimitiveNode::EqualsInternal(const PrimitiveNode* other) const {
  if (physical_type_ != other->physical_type_) {
    return false;
  } else if (logical_type_ == LogicalType::DECIMAL) {
    // TODO(wesm): metadata
    ParquetException::NYI("comparing decimals");
    return false;
  } else if (physical_type_ == Type::FIXED_LEN_BYTE_ARRAY) {
    return type_length_ == other->type_length_;
  }
  return true;
}

bool PrimitiveNode::Equals(const Node* other) const {
  if (!Node::EqualsInternal(other)) {
    return false;
  }
  return EqualsInternal(static_cast<const PrimitiveNode*>(other));
}

void PrimitiveNode::Visit(Node::Visitor* visitor) {
  visitor->Visit(this);
}

// ----------------------------------------------------------------------
// Group node

bool GroupNode::EqualsInternal(const GroupNode* other) const {
  if (this == other) {
    return true;
  }
  if (this->field_count() != other->field_count()) {
    return false;
  }
  for (size_t i = 0; i < this->field_count(); ++i) {
    const Node* other_field = static_cast<const Node*>(other->field(i).get());
    if (!this->field(i)->Equals(other_field)) {
      return false;
    }
  }
  return true;
}

bool GroupNode::Equals(const Node* other) const {
  if (!Node::EqualsInternal(other)) {
    return false;
  }
  return EqualsInternal(static_cast<const GroupNode*>(other));
}

void GroupNode::Visit(Node::Visitor* visitor) {
  // Depth-first traversal
  visitor->Visit(this);
  for (size_t i = 0; i < this->field_count(); ++i) {
    this->field(i)->Visit(visitor);
  }
}

} // namespace schema

} // namespace parquet_cpp
