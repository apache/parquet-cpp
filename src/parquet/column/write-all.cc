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

#include "parquet/column/write-all.h"

namespace parquet {

void WriteAllValues(int64_t batch_size, const int16_t* def_levels, const int16_t* rep_levels, const uint8_t* values, parquet::ColumnWriter* writer) {
  switch (writer->type()) {
    case parquet::Type::BOOLEAN:
        WriteAll<parquet::BoolWriter>(batch_size, def_levels, rep_levels, values, writer);
        break;
    case parquet::Type::INT32:
        WriteAll<parquet::Int32Writer>(batch_size, def_levels, rep_levels, values, writer);
        break;
    case parquet::Type::INT64:
        WriteAll<parquet::Int64Writer>(batch_size, def_levels, rep_levels, values, writer);
        break;
    case parquet::Type::INT96:
        WriteAll<parquet::Int96Writer>(batch_size, def_levels, rep_levels, values, writer);
        break;
    case parquet::Type::FLOAT:
        WriteAll<parquet::FloatWriter>(batch_size, def_levels, rep_levels, values, writer);
        break;
    case parquet::Type::DOUBLE:
        WriteAll<parquet::DoubleWriter>(batch_size, def_levels, rep_levels, values, writer);
        break;
    case parquet::Type::BYTE_ARRAY:
        WriteAll<parquet::ByteArrayWriter>(batch_size, def_levels, rep_levels, values, writer);
        break;
    case parquet::Type::FIXED_LEN_BYTE_ARRAY:
        WriteAll<parquet::FixedLenByteArrayWriter>(batch_size, def_levels, rep_levels, values, writer);
        break;
    default:
        parquet::ParquetException::NYI("type writer not implemented");
        break;
  }
}

}  // namespace parquet
