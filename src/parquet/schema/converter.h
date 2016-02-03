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
#include <unordered_map>
#include <vector>

#include "parquet/schema/descriptor.h"
#include "parquet/schema/types.h"

#include "parquet/thrift/parquet_types.h"

namespace parquet_cpp {

namespace schema {

std::shared_ptr<SchemaDescriptor> FromParquet(
    const std::vector<parquet::SchemaElement>& schema);

void ToParquet(RootSchema* schema, std::vector<parquet::SchemaElement>* out);

} // namespace schema

} // namespace parquet_cpp

#endif // PARQUET_SCHEMA_CONVERTER_H
