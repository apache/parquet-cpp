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

#include "parquet/arrow/io.h"

#include <cstdint>
#include <memory>

#include "arrow/status.h"
#include "parquet/api/io.h"

using arrow::Status;
using arrow::MemoryPool;

namespace parquet {
namespace arrow {

// ----------------------------------------------------------------------
// ParquetAllocator

ParquetAllocator::ParquetAllocator() : pool_(::arrow::default_memory_pool()) {}

ParquetAllocator::ParquetAllocator(MemoryPool* pool) : pool_(pool) {}

ParquetAllocator::~ParquetAllocator() {}

Status ParquetAllocator::Allocate(int64_t size, uint8_t** out) {
  return pool_->Allocate(size, &out));
}

void ParquetAllocator::Free(uint8_t* buffer, int64_t size) {
  // Does not report Status
  pool_->Free(buffer, size);
}

}  // namespace arrow
}  // namespace parquet
