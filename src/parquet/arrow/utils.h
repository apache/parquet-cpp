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

#ifndef PARQUET_ARROW_UTILS_H
#define PARQUET_ARROW_UTILS_H

#include <sstream>

#include "arrow/util/status.h"
#include "parquet/exception.h"

namespace parquet {
namespace arrow {

#define PARQUET_CATCH_NOT_OK(s)                    \
  try {                                            \
    (s);                                           \
  } catch (const ::parquet::ParquetException& e) { \
    return ::arrow::Status::Invalid(e.what());     \
  }

#define PARQUET_IGNORE_NOT_OK(s) \
  try {                          \
    (s);                         \
  } catch (const ::parquet::ParquetException& e) {}

#define PARQUET_THROW_NOT_OK(s)                    \
  do {                                             \
    ::arrow::Status _s = (s);                      \
    if (!_s.ok()) {                                \
      std::stringstream ss;                        \
      ss << "Arrow error: " << _s.ToString();      \
      throw ::parquet::ParquetException(ss.str()); \
    }                                              \
  } while (0);

}  // namespace arrow
}  // namespace parquet

#endif  // PARQUET_ARROW_UTILS_H
