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

#ifndef PARQUET_SCHEMA_DESCRIPTOR_H
#define PARQUET_SCHEMA_DESCRIPTOR_H

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "parquet/schema/types.h"

namespace parquet_cpp {

class SchemaDescriptor;

// The ColumnDescriptor encapsulates information necessary to interpret
// primitive column data in the context of a particular schema. We have to
// examine the node structure of a column's path to the root in the schema tree
// to be able to reassemble the nested structure from the repetition and
// definition levels.
class ColumnDescriptor {
 public:
  ColumnDescriptor(const schema::NodePtr& node, int16_t max_definition_level,
      int16_t max_repetition_level, const SchemaDescriptor* schema_descr = nullptr);

  int16_t max_definition_level() const {
    return max_definition_level_;
  }

  int16_t max_repetition_level() const {
    return max_repetition_level_;
  }

  Type::type physical_type() const {
    return primitive_node_->physical_type();
  }

  const std::string& name() const {
    return primitive_node_->name();
  }

  int type_length() const;

 private:
  schema::NodePtr node_;
  const schema::PrimitiveNode* primitive_node_;

  int16_t max_definition_level_;
  int16_t max_repetition_level_;

  // When this descriptor is part of a real schema (and not being used for
  // testing purposes), maintain a link back to the parent SchemaDescriptor to
  // enable reverse graph traversals
  const SchemaDescriptor* schema_descr_;
};

// Container for the converted Parquet schema with a computed information from
// the schema analysis needed for file reading
//
// * Column index to Node
// * Max repetition / definition levels for each primitive node
//
// The ColumnDescriptor objects produced by this class can be used to assist in
// the reconstruction of fully materialized data structures from the
// repetition-definition level encoding of nested data
//
// TODO(wesm): this object can be recomputed from a Schema
class SchemaDescriptor {
 public:
  SchemaDescriptor() {}
  ~SchemaDescriptor() {}

  // Analyze the schema
  void Init(std::unique_ptr<schema::Node> schema);
  void Init(const schema::NodePtr& schema);

  const ColumnDescriptor* Column(size_t i) const;

  // The number of physical columns appearing in the file
  size_t num_columns() const {
    return leaves_.size();
  }

 private:
  friend class ColumnDescriptor;

  schema::NodePtr schema_;
  const schema::GroupNode* group_;

  void FindLeaves(const schema::NodePtr& node, int16_t max_def_level,
      int16_t max_rep_level);

  // Result of leaf node / tree analysis
  std::vector<ColumnDescriptor> leaves_;

  // Mapping between leaf nodes and root group of leaf (first node
  // below the schema's root group)
  //
  // For example, the leaf `a.b.c.d` would have a link back to `a`
  //
  // -- a  <------
  // -- -- b     |
  // -- -- -- c  |
  // -- -- -- -- d
  std::unordered_map<int, schema::NodePtr> leaf_to_base_;
};

} // namespace parquet_cpp

#endif // PARQUET_SCHEMA_DESCRIPTOR_H
