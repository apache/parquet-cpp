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

#include "parquet/exception.h"
#include "parquet/schema.h"
#include "parquet/util/logging.h"

using parquet::ParquetException;
using parquet::SchemaDescriptor;
using parquet::schema::GroupNode;

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

// Find the index of the field node under the schema root of a leaf node
// return a negative result if not found
inline int ColumnIndexToSchemaFieldIndex(const SchemaDescriptor* schema, int i) {
  auto field_node = schema->GetColumnRoot(i);
  auto group = schema->group_node();
  for (int field_idx = 0; field_idx < group->field_count(); field_idx++) {
    if (schema->group_node()->field(field_idx) == field_node) {
      return field_idx;
    }
  }

  return -1;
}

#endif  // PARQUET_SCHEMA_UTIL_H
