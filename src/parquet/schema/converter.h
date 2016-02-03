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

// Conversion routines for converting to and from flat Parquet metadata. Among
// other things, this limits the exposure of the internals of the Thrift
// metadata structs to the rest of the library.

// NB: This file is not part of the schema public API and only used internally
// for converting to and from Parquet Thrift metadata

#ifndef PARQUET_SCHEMA_CONVERTER_H
#define PARQUET_SCHEMA_CONVERTER_H

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "parquet/schema/descriptor.h"
#include "parquet/schema/types.h"

#include "parquet/thrift/parquet_types.h"

namespace parquet_cpp {

namespace schema {

class GroupConverter {
 public:
  GroupConverter(const parquet::SchemaElement* elements, size_t length) :
      elements_(elements),
      length_(length),
      pos_(0),
      current_id_(0) {}

  std::unique_ptr<Node> Convert();

 private:
  const parquet::SchemaElement* elements_;
  size_t length_;
  size_t pos_;
  size_t current_id_;

  size_t next_id() {
    return current_id_++;
  }

  const parquet::SchemaElement& Next() {
    return elements_[pos_++];
  }

  std::unique_ptr<Node> NextNode();
};

std::shared_ptr<SchemaDescriptor> FromParquet(
    const std::vector<parquet::SchemaElement>& schema);

void ToParquet(RootSchema* schema, std::vector<parquet::SchemaElement>* out);

// Helper functions that we can also test
std::unique_ptr<Node> ConvertPrimitive(const parquet::SchemaElement* element,
    int node_id = -1);
std::unique_ptr<Node> ConvertGroup(const parquet::SchemaElement* element, int node_id,
    const NodeVector& fields);

} // namespace schema

} // namespace parquet_cpp

#endif // PARQUET_SCHEMA_CONVERTER_H
