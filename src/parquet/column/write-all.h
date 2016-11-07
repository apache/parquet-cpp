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

#ifndef PARQUET_WRITE_ALL_H
#define PARQUET_WRITE_ALL_H

#include "parquet/column/writer.h"

namespace parquet {

template <typename RType>
void WriteAll(int64_t batch_size, const int16_t* def_levels, const int16_t* rep_levels,
    const uint8_t* values, parquet::ColumnWriter* writer) {
  typedef typename RType::T Type;
  auto typed_writer = static_cast<RType*>(writer);
  auto vals = reinterpret_cast<const Type*>(&values[0]);
  typed_writer->WriteBatch(batch_size, def_levels, rep_levels, vals);
}

void PARQUET_EXPORT WriteAllValues(int64_t batch_size, const int16_t* def_levels,
    const int16_t* rep_levels, const uint8_t* values, parquet::ColumnWriter* Writer);

}  // namespace parquet

#endif  // PARQUET_WRITE_ALL_H
