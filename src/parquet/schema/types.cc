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

#include "parquet/schema/types.h"

namespace parquet_cpp {

namespace schema {

Type::type Type::FromParquet(parquet::Type::type type) {
  return static_cast<Type::type>(type);
}

LogicalType::type LogicalType::FromParquet(parquet::ConvertedType::type type) {
  // item 0 is NONE
  return static_cast<LogicalType::type>(static_cast<int>(type) + 1);
}

Repetition::type Repetition::FromParquet(parquet::FieldRepetitionType::type type) {
  return static_cast<Repetition::type>(type);
}

// TODO: decide later what to do with these. When converting back only need to
// write into a parquet::SchemaElement

// parquet::FieldRepetitionType::type Repetition::ToParquet(Repetition::type type) {
//   return static_cast<parquet::FieldRepetitionType::type>(type);
// }

// parquet::ConvertedType::type LogicalType::ToParquet(LogicalType::type type) {
//   // item 0 is NONE
//   return static_cast<parquet::ConvertedType::type>(static_cast<int>(type) - 1);
// }

// parquet::Type::type Type::ToParquet(Type::type type) {
//   return static_cast<parquet::Type::type>(type);
// }

} // namespace schema

} // namespace parquet_cpp
