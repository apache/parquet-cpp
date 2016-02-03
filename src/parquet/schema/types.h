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

#ifndef PARQUET_SCHEMA_TYPES_H
#define PARQUET_SCHEMA_TYPES_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "parquet/thrift/parquet_types.h"

namespace parquet_cpp {

namespace schema {

// Mirrors parquet::Type
struct Type {
  enum type {
    BOOLEAN = 0,
    INT32 = 1,
    INT64 = 2,
    INT96 = 3,
    FLOAT = 4,
    DOUBLE = 5,
    BYTE_ARRAY = 6,
    FIXED_LEN_BYTE_ARRAY = 7
  };

  static Type::type FromParquet(parquet::Type::type type);
  // static parquet::Type::type ToParquet(Type::type type);
};

// Mirrors parquet::ConvertedType
struct LogicalType {
  enum type {
    NONE,
    MAP,
    LIST,
    UTF8,
    MAP_KEY_VALUE,
    ENUM,
    DECIMAL,
    DATE,
    TIME_MILLIS,
    TIME_MICROS,
    TIMESTAMP_MILLIS,
    TIMESTAMP_MICROS,
    UINT_8,
    UINT_16,
    UINT_32,
    UINT_64,
    INT_8,
    INT_16,
    INT_32,
    INT_64,
    JSON,
    BSON,
    INTERVAL
  };

  static LogicalType::type FromParquet(parquet::ConvertedType::type type);
  // static parquet::ConvertedType::type ToParquet(LogicalType::type type);
};

// Mirrors parquet::FieldRepetitionType
struct Repetition {
  enum type {
    REQUIRED = 0,
    OPTIONAL = 1,
    REPEATED = 2
  };

  static Repetition::type FromParquet(parquet::FieldRepetitionType::type type);
  // static parquet::FieldRepetitionType::type ToParquet(Repetition::type type);
};

struct ArrayEncoding {
  enum type {
    ONE_LEVEL,
    TWO_LEVEL,
    THREE_LEVEL
  };
};

struct DecimalMetadata {
  int32_t scale;
  int32_t precision;
};

// Base class for logical schema types. A type has a name, repetition level,
// and optionally a logical type (ConvertedType in Parquet metadata parlance)
class Node {
 public:
  enum node_type {
    PRIMITIVE,
    GROUP
  };

  Node(Node::node_type type, const std::string& name,
      Repetition::type repetition,
      LogicalType::type logical_type = LogicalType::NONE,
      int id = -1) :
      type_(type),
      name_(name),
      repetition_(repetition),
      logical_type_(logical_type),
      id_(id) {}

  virtual ~Node() {}

  bool is_primitive() const {
    return type_ == Node::PRIMITIVE;
  }

  bool is_optional() const {
    return repetition_ == Repetition::OPTIONAL;
  }

  bool is_repeated() const {
    return repetition_ == Repetition::REPEATED;
  }

  bool is_required() const {
    return repetition_ == Repetition::REQUIRED;
  }

  virtual bool Equals(const Node* other) const = 0;

  const std::string& name() const {
    return name_;
  }

  Node::node_type type() const {
    return type_;
  }

  Repetition::type repetition() const {
    return repetition_;
  }

  LogicalType::type logical_type() const {
    return logical_type_;
  }

 protected:
  Node::node_type type_;
  std::string name_;
  Repetition::type repetition_;
  LogicalType::type logical_type_;
  int id_;

  bool EqualsInternal(const Node* other) const {
    return type_ == other->type_ &&
      name_ == other->name_ &&
      repetition_ == other->repetition_ &&
      logical_type_ == other->logical_type_;
  }
};

typedef std::shared_ptr<Node> NodePtr;
typedef std::vector<NodePtr> NodeVector;

// A type that is one of the primitive Parquet storage types. In addition to
// the other type metadata (name, repetition level, logical type), also has the
// physical storage type and their type-specific metadata (byte width, decimal
// parameters)
class PrimitiveNode : public Node {
 public:
  PrimitiveNode(const std::string& name, Repetition::type repetition,
      Type::type type,
      LogicalType::type logical_type = LogicalType::NONE,
      int id = -1) :
      Node(Node::PRIMITIVE, name, repetition, logical_type, id),
      physical_type_(type) {}

  virtual bool Equals(const Node* other) const {
    if (!Node::EqualsInternal(other)) {
      return false;
    }
    return EqualsInternal(static_cast<const PrimitiveNode*>(other));
  }

  // TODO FIXED_LEN_BYTE_ARRAY

  // TODO Decimal

 private:
  Type::type physical_type_;

  // For FIXED_LEN_BYTE_ARRAY
  size_t length_;

  // Precision and scale
  DecimalMetadata decimal_meta_;

  bool EqualsInternal(const PrimitiveNode* other) const {
    // TODO(wesm): metadata
    return (this == other) || (physical_type_ == other->physical_type_);
  }
};


class GroupNode : public Node {
 public:
  GroupNode(const std::string& name, Repetition::type repetition,
      const NodeVector& fields,
      LogicalType::type logical_type = LogicalType::NONE,
      int id = -1) :
      Node(Node::GROUP, name, repetition, logical_type, id),
      fields_(fields) {}

  virtual bool Equals(const Node* other) const {
    if (this->type() != other->type()) {
      return false;
    }
    return EqualsInternal(static_cast<const GroupNode*>(other));
  }

  const NodePtr& field(size_t i) const {
    return fields_[i];
  }

  size_t field_count() const {
    return fields_.size();
  }

 private:
  NodeVector fields_;

  bool EqualsInternal(const GroupNode* other) const {
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
};


// A group representing a top-level Parquet schema
class Schema : public GroupNode {
 public:
  Schema(const std::string& name, const NodeVector& fields) :
      GroupNode(name, Repetition::REPEATED, fields) {}

  explicit Schema(const NodeVector& fields) :
      Schema("schema", fields) {}
};

// The ColumnDescriptor encapsulates information necessary to interpret
// primitive column data in the context of
class ColumnDescriptor {
 public:
  ColumnDescriptor(const NodePtr& type, int16_t max_definition_level,
      int16_t max_repetition_level) :
      type_(type),
      max_definition_level_(max_definition_level),
      max_repetition_level_(max_repetition_level) {}

 private:
  NodePtr type_;
  int16_t max_definition_level_;
  int16_t max_repetition_level_;
};

// ----------------------------------------------------------------------
// Convenience primitive type factory functions

static inline NodePtr Boolean(const std::string& name,
    Repetition::type repetition = Repetition::OPTIONAL) {
  return NodePtr(new PrimitiveNode(name, repetition, Type::BOOLEAN));
}

static inline NodePtr Int32(const std::string& name,
    Repetition::type repetition = Repetition::OPTIONAL) {
  return NodePtr(new PrimitiveNode(name, repetition, Type::INT32));
}

static inline NodePtr Int64(const std::string& name,
    Repetition::type repetition = Repetition::OPTIONAL) {
  return NodePtr(new PrimitiveNode(name, repetition, Type::INT64));
}

static inline NodePtr Int96(const std::string& name,
    Repetition::type repetition = Repetition::OPTIONAL) {
  return NodePtr(new PrimitiveNode(name, repetition, Type::INT96));
}

static inline NodePtr Float(const std::string& name,
    Repetition::type repetition = Repetition::OPTIONAL) {
  return NodePtr(new PrimitiveNode(name, repetition, Type::FLOAT));
}

static inline NodePtr Double(const std::string& name,
    Repetition::type repetition = Repetition::OPTIONAL) {
  return NodePtr(new PrimitiveNode(name, repetition, Type::DOUBLE));
}

static inline NodePtr ByteArray(const std::string& name,
    Repetition::type repetition = Repetition::OPTIONAL) {
  return NodePtr(new PrimitiveNode(name, repetition, Type::BYTE_ARRAY));
}

static inline NodePtr FLBA(const std::string& name,
    Repetition::type repetition = Repetition::OPTIONAL) {
  return NodePtr(new PrimitiveNode(name, repetition,
          Type::FIXED_LEN_BYTE_ARRAY));
}

} // namespace schema

} // namespace parquet_cpp

#endif // PARQUET_SCHEMA_TYPES_H
