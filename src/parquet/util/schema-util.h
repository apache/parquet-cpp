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

#ifndef PARQUET_SCHEMA_UTIL_H
#define PARQUET_SCHEMA_UTIL_H

#include <string>
#include <vector>
#include <algorithm>

#include "parquet/exception.h"
#include "parquet/schema.h"
#include "parquet/types.h"
#include "parquet/util/logging.h"

using parquet::ParquetException;
using parquet::SchemaDescriptor;
using parquet::schema::GroupNode;
using parquet::schema::NodePtr;
using parquet::LogicalType;

inline bool str_endswith_tuple(const std::string& str) {
  if (str.size() >= 6) { return str.substr(str.size() - 6, 6) == "_tuple"; }
  return false;
}

// Special case mentioned in the format spec:
//   If the name is array or ends in _tuple, this should be a list of struct
//   even for single child elements.
inline bool HasStructListName(const GroupNode* node) {
  return (node->name() == "array" ||
          str_endswith_tuple(node->name()));
}

// TODO(itaiin): This aux. function is to be deleted once repeated structs are supported
inline bool IsSimpleStruct(const NodePtr& node) {
  if (!node->is_group()) return false;
  if (node->is_repeated()) return false;
  if (node->logical_type() == LogicalType::LIST) return false;
  // Special case mentioned in the format spec:
    //   If the name is array or ends in _tuple, this should be a list of struct
    //   even for single child elements.
  GroupNode* group = static_cast<GroupNode*>(node.get());
  if (group->field_count() == 1 && HasStructListName(group)) return false;

  return true;
}

// Coalesce a list of schema fields indices which are the roots of the
// columns referred by a list of column indices
inline bool ColumnIndicesToFieldIndices(const SchemaDescriptor& descr,
    const std::vector<int>& column_indices, std::vector<int>* out) {
  auto group = descr.group_node();
  auto num_fields = group->field_count();
  std::vector<bool> fields_to_read(descr.group_node()->field_count(), false);
  // Copy the indices vector and then sort it.
  // This ensures the corresponding schema field indices of the columns are
  // monotonic too which enables a single-pass.
  std::vector<int> sorted_column_indices = column_indices;
  std::sort(sorted_column_indices.begin(), sorted_column_indices.end());
  auto field_idx = 0;
  for (auto& column_index : sorted_column_indices) {
    auto field_node = descr.GetColumnRoot(column_index);
    // look for the corresponding field index
    while ((field_idx < num_fields) &&
           (group->field(field_idx) != field_node)) {
      field_idx++;
    }

    if (field_idx == num_fields) {
      // not found
      return false;
    }
    fields_to_read[field_idx] = true;
  }

  // Gather the indices of the relevant fields
  out->clear();
  for (uint i = 0; i < fields_to_read.size(); i++) {
    if (fields_to_read[i]) {
      out->push_back(i);
    }
  }

  return true;
}

#endif  // PARQUET_SCHEMA_UTIL_H
