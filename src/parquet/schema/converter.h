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

#ifndef PARQUET_SCHEMA_CONVERTER_H
#define PARQUET_SCHEMA_CONVERTER_H

#include <cstdint>
#include <memory>
#include <vector>

#include "parquet/schema/types.h"

#include "parquet/thrift/parquet_types.h"

namespace parquet_cpp {

namespace schema {

// Container for the converted Parquet schema with a bunch of computed
// information from the schema analysis needed for file reading
// * Column index to Type
// * Max repetition / definition levels for each primitive type
//
// TODO(wesm): this object can be recomputed from a Schema
class SchemaInfo {
 public:
  explicit SchemaInfo(std::shared_ptr<Schema> schema);
  ~SchemaInfo() {}

  ColumnDescriptor Column(size_t i) const;

  // The number of physical columns appearing in the file
  size_t num_columns() const;

 private:
  std::shared_ptr<Schema> schema_;
};

std::shared_ptr<SchemaInfo> FromParquet(
    const std::vector<parquet::SchemaElement>& schema);

void ToParquet(Schema* schema, std::vector<parquet::SchemaElement>* out);

} // namespace schema

} // namespace parquet_cpp

#endif // PARQUET_SCHEMA_CONVERTER_H
