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

namespace parquet_cpp {

namespace schema {

// Mirrors parquet::Type
struct PhysicalType {
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
};

// Mirrors parquet::FieldRepetitionType
struct Repetition {
  enum type {
    REQUIRED = 0,
    OPTIONAL = 1,
    REPEATED = 2
  };
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
class Type {
 public:
  enum node_type {
    PRIMITIVE,
    GROUP
  };

  Type(Type::node_type type, const std::string& name,
      Repetition::type repetition, int id = -1,
      LogicalType::type logical_type = LogicalType::NONE) :
      type_(type),
      name_(name),
      repetition_(repetition),
      logical_type_(logical_type) {}

  virtual ~Type() {}

  bool is_primitive() const {
    return type_ == Type::PRIMITIVE;
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

  virtual bool Equals(const Type* other) const = 0;

  const std::string& name() const {
    return name_;
  }

  Type::node_type type() const {
    return type_;
  }

  Repetition::type repetition() const {
    return repetition_;
  }

  LogicalType::type logical_type() const {
    return logical_type_;
  }

 protected:
  Type::node_type type_;
  std::string name_;
  Repetition::type repetition_;

  int id_;

  LogicalType::type logical_type_;

  bool EqualsInternal(const Type* other) const {
    return type_ == other->type_ &&
      name_ == other->name_ &&
      repetition_ == other->repetition_ &&
      logical_type_ == other->logical_type_;
  }
};

typedef std::shared_ptr<Type> TypePtr;
typedef std::vector<TypePtr> TypeList;

// A type that is one of the primitive Parquet storage types. In addition to
// the other type metadata (name, repetition level, logical type), also has the
// physical storage type and their type-specific metadata (byte width, decimal
// parameters)
class PrimitiveType : public Type {
 public:
  PrimitiveType(const std::string& name, Repetition::type repetition,
      PhysicalType::type type,
      int id = -1,
      LogicalType::type logical_type = LogicalType::NONE) :
      Type(Type::PRIMITIVE, name, repetition, id, logical_type),
      physical_type_(type) {}

  virtual bool Equals(const Type* other) const {
    if (!Type::EqualsInternal(other)) {
      return false;
    }
    return EqualsInternal(static_cast<const PrimitiveType*>(other));
  }

  // TODO FIXED_LEN_BYTE_ARRAY

  // TODO Decimal

 private:
  PhysicalType::type physical_type_;

  // For FIXED_LEN_BYTE_ARRAY
  size_t length_;

  // Precision and scale
  DecimalMetadata decimal_meta_;

  bool EqualsInternal(const PrimitiveType* other) const {
    // TODO(wesm): metadata
    return (this == other) || (physical_type_ == other->physical_type_);
  }
};


class GroupType : public Type {
 public:
  GroupType(const std::string& name, Repetition::type repetition,
      const TypeList& fields,
      int id = -1,
      LogicalType::type logical_type = LogicalType::NONE) :
      Type(Type::GROUP, name, repetition, id, logical_type),
      fields_(fields) {}

  virtual bool Equals(const Type* other) const {
    if (this->type() != other->type()) {
      return false;
    }
    return EqualsInternal(static_cast<const GroupType*>(other));
  }

  const TypePtr& field(size_t i) const {
    return fields_[i];
  }

  size_t field_count() const {
    return fields_.size();
  }

 private:
  TypeList fields_;

  bool EqualsInternal(const GroupType* other) const {
    if (this == other) {
      return true;
    }
    if (this->field_count() != other->field_count()) {
      return false;
    }
    for (size_t i = 0; i < this->field_count(); ++i) {
      const Type* other_field = static_cast<const Type*>(other->field(i).get());
      if (!this->field(i)->Equals(other_field)) {
        return false;
      }
    }
    return true;
  }
};


// A group representing a top-level Parquet schema
class Schema : public GroupType {
 public:
  Schema(const std::string& name, const TypeList& fields) :
      GroupType(name, Repetition::REPEATED, fields) {}

  explicit Schema(const TypeList& fields) :
      Schema("schema", fields) {}
};


} // namespace schema

} // namespace parquet_cpp

#endif // PARQUET_SCHEMA_TYPES_H
