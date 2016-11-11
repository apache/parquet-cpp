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

#include "parquet/schema/printer.h"

#include <memory>
#include <string>

#include "parquet/schema/types.h"
#include "parquet/types.h"

namespace parquet {

namespace schema {

class SchemaPrinter : public Node::ConstVisitor {
 public:
  explicit SchemaPrinter(std::ostream& stream, int indent_width)
      : stream_(stream), indent_(0), indent_width_(2) {}

  void Visit(const Node* node) override;

 private:
  void Visit(const PrimitiveNode* node);
  void Visit(const GroupNode* node);

  void Indent();

  std::ostream& stream_;

  int indent_;
  int indent_width_;
};

static void PrintRepLevel(Repetition::type repetition, std::ostream& stream) {
  switch (repetition) {
    case Repetition::REQUIRED:
      stream << "required";
      break;
    case Repetition::OPTIONAL:
      stream << "optional";
      break;
    case Repetition::REPEATED:
      stream << "repeated";
      break;
    default:
      break;
  }
}

static void PrintType(const PrimitiveNode* node, std::ostream& stream) {
  switch (node->physical_type()) {
    case Type::BOOLEAN:
      stream << "boolean";
      break;
    case Type::INT32:
      stream << "int32";
      break;
    case Type::INT64:
      stream << "int64";
      break;
    case Type::INT96:
      stream << "int96";
      break;
    case Type::FLOAT:
      stream << "float";
      break;
    case Type::DOUBLE:
      stream << "double";
      break;
    case Type::BYTE_ARRAY:
      stream << "binary";
      break;
    case Type::FIXED_LEN_BYTE_ARRAY:
      stream << "fixed_len_byte_array(" << node->type_length() << ")";
      break;
    default:
      break;
  }
}

static void PrintLogicalType(LogicalType::type lt, std::ostream& stream) {
  switch (lt) {
    case LogicalType::UTF8:
      stream << " (UTF8)";
      break;
    case LogicalType::MAP:
      stream << " (MAP)";
      break;
    case LogicalType::MAP_KEY_VALUE:
      stream << " (MAP_KEY_VALUE)";
      break;
    case LogicalType::LIST:
      stream << " (LIST)";
      break;
    case LogicalType::ENUM:
      stream << " (ENUM)";
      break;
    case LogicalType::DATE:
      stream << " (DATE)";
      break;
    case LogicalType::TIME_MILLIS:
      stream << " (TIME_MILLIS)";
      break;
    case LogicalType::TIME_MICROS:
      stream << " (TIME_MICROS)";
      break;
    case LogicalType::TIMESTAMP_MILLIS:
      stream << " (TIMESTAMP_MILLIS)";
      break;
    case LogicalType::TIMESTAMP_MICROS:
      stream << " (TIMESTAMP_MICROS)";
      break;
    case LogicalType::UINT_8:
      stream << " (UINT_8)";
      break;
    case LogicalType::UINT_16:
      stream << " (UINT_16)";
      break;
    case LogicalType::UINT_32:
      stream << " (UINT_32)";
      break;
    case LogicalType::UINT_64:
      stream << " (UINT_64)";
      break;
    case LogicalType::INT_8:
      stream << " (INT_8)";
      break;
    case LogicalType::INT_16:
      stream << " (INT_16)";
      break;
    case LogicalType::INT_32:
      stream << " (INT_32)";
      break;
    case LogicalType::INT_64:
      stream << " (INT_64)";
      break;
    case LogicalType::JSON:
      stream << " (JSON)";
      break;
    case LogicalType::BSON:
      stream << " (BSON)";
      break;
    case LogicalType::INTERVAL:
      stream << " (INTERVAL)";
      break;
  default:
      break;
  }
}

static void PrintLogicalType(const PrimitiveNode* node, std::ostream& stream) {
  auto lt = node->logical_type();
  if (lt == LogicalType::DECIMAL) {
    stream << " (DECIMAL(" <<  node->decimal_metadata().precision << "," <<
      node->decimal_metadata().scale << "))";
  } else {
    PrintLogicalType(lt, stream);
  }
}

void SchemaPrinter::Visit(const PrimitiveNode* node) {
  PrintRepLevel(node->repetition(), stream_);
  stream_ << " ";
  PrintType(node, stream_);
  stream_ << " " << node->name();
  PrintLogicalType(node, stream_);
  stream_ << ";" << std::endl;
}

void SchemaPrinter::Visit(const GroupNode* node) {
  if (!node->parent()) {
    stream_ << "message " << node->name() << " {" << std::endl;
  } else {
    PrintRepLevel(node->repetition(), stream_);
    stream_ << " group " << node->name();
    PrintLogicalType(node->logical_type(), stream_);
    stream_  << " {" << std::endl;
  }

  indent_ += indent_width_;
  for (int i = 0; i < node->field_count(); ++i) {
    node->field(i)->VisitConst(this);
  }
  indent_ -= indent_width_;
  Indent();
  stream_ << "}" << std::endl;
}

void SchemaPrinter::Indent() {
  if (indent_ > 0) {
    std::string spaces(indent_, ' ');
    stream_ << spaces;
  }
}

void SchemaPrinter::Visit(const Node* node) {
  Indent();
  if (node->is_group()) {
    Visit(static_cast<const GroupNode*>(node));
  } else {
    // Primitive
    Visit(static_cast<const PrimitiveNode*>(node));
  }
}

void PrintSchema(const Node* schema, std::ostream& stream, int indent_width) {
  SchemaPrinter printer(stream, indent_width);
  printer.Visit(schema);
}

}  // namespace schema

}  // namespace parquet
