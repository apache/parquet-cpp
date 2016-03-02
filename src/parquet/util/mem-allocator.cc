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

#include "parquet/util/mem-allocator.h"

#include <cstdlib>

#include "parquet/exception.h"

namespace parquet_cpp {

MemoryAllocator::~MemoryAllocator() {}

uint8_t* TrackingAllocator::Malloc(int64_t size) {
  if (0 == size) {
    return nullptr;
  }

  // store the size at the beginning of the block
  int64_t* p = static_cast<int64_t*>(std::malloc(size + OFFSET));
  if (!p) {
    throw ParquetException("OOM: memory allocation failed");
  }
  *p = size + OFFSET;
  total_memory_ += size;
  if (total_memory_ > max_memory_) {
    max_memory_ = total_memory_;
  }

  return reinterpret_cast<uint8_t*>(++p);
}

void TrackingAllocator::Free(uint8_t* p) {
  if (nullptr != p) {
      int64_t* p1 = reinterpret_cast<int64_t*>(p);
      --p1;
      total_memory_ -= *p1-OFFSET;
      std::free(p1);
  }
}

TrackingAllocator::~TrackingAllocator() {
  if (0 != total_memory_) {
    throw ParquetException("Memory has not been deallocation");
  }
}

MemoryAllocator* default_allocator() {
  static TrackingAllocator default_allocator;
  return &default_allocator;
}

} // namespace parquet_cpp
