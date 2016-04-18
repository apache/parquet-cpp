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

// This module defines an abstract interface for iterating through pages in a
// Parquet column chunk within a row group. It could be extended in the future
// to iterate through all data pages in all chunks in a file.

#ifndef PARQUET_SCHEMA_TEST_UTIL_H
#define PARQUET_SCHEMA_TEST_UTIL_H

#include <string>

#include "parquet/schema/types.h"
#include "parquet/thrift/parquet_types.h"

using parquet::format::ConvertedType;
using parquet::format::FieldRepetitionType;
using parquet::format::SchemaElement;

namespace parquet {

namespace schema {

static inline SchemaElement NewPrimitive(const std::string& name,
    FieldRepetitionType::type repetition, format::Type::type type, int id = 0) {
  SchemaElement result;
  result.__set_name(name);
  result.__set_repetition_type(repetition);
  result.__set_type(type);
  result.__set_num_children(0);
  result.__set_field_id(id);
  // Set default (non-set) values
  result.__set_type_length(-1);
  result.__set_precision(-1);
  result.__set_scale(-1);

  return result;
}

static inline SchemaElement NewGroup(const std::string& name,
    FieldRepetitionType::type repetition, int num_children, int id = 0) {
  SchemaElement result;
  result.__set_name(name);
  result.__set_repetition_type(repetition);
  result.__set_num_children(num_children);
  result.__set_field_id(id);

  return result;
}

} // namespace schema

} // namespace parquet

#endif // PARQUET_COLUMN_TEST_UTIL_H
