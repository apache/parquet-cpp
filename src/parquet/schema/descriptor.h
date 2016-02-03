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

// Conversion routines for converting to and from flat Parquet metadata

#ifndef PARQUET_SCHEMA_DESCRIPTOR_H
#define PARQUET_SCHEMA_DESCRIPTOR_H

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "parquet/schema/types.h"

namespace parquet_cpp {

namespace schema {

class SchemaDescriptor;

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

  // When this descriptor is part of a real schema (and not being used for
  // testing purposes), maintain a link back to the parent SchemaDescriptor to
  // enable reverse graph traversals
  SchemaDescriptor* schema_descr_;
};

// Container for the converted Parquet schema with a bunch of computed
// information from the schema analysis needed for file reading
// * Column index to Node
// * Max repetition / definition levels for each primitive type
//
// The ColumnDescriptor objects produced by this class can be used to assist in
// the reconstruction of fully materialized data structures from the
// repetition-definition level encoding of nested data
//
// TODO(wesm): this object can be recomputed from a Schema
class SchemaDescriptor {
 public:
  explicit SchemaDescriptor(std::shared_ptr<Schema> schema) :
      schema_(schema) {}
  ~SchemaDescriptor() {}

  ColumnDescriptor Column(size_t i) const;

  // The number of physical columns appearing in the file
  size_t num_columns() const;

 private:
  friend class ColumnDescriptor;

  std::shared_ptr<Schema> schema_;

  // TODO(wesm): mapping between leaf nodes and root group of leaf (first node
  // below the schema's root group)
  //
  // For example, the leaf `a.b.c.d` would have a link back to `a`
  //
  // -- a  <------
  // -- -- b     |
  // -- -- -- c  |
  // -- -- -- -- d
};

} // namespace schema

} // namespace parquet_cpp

#endif // PARQUET_SCHEMA_DESCRIPTOR_H
