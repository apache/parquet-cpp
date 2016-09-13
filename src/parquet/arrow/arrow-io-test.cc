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

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>

#include "gtest/gtest.h"

#include "arrow/test-util.h"
#include "arrow/util/memory-pool.h"
#include "arrow/util/status.h"

#include "parquet/api/io.h"
#include "parquet/arrow/io.h"

using arrow::MemoryPool;
using arrow::Status;

namespace parquet {
namespace arrow {

// Allocator tests

TEST(TestParquetAllocator, DefaultCtor) {
  ParquetAllocator allocator;

  const int buffer_size = 10;

  uint8_t* buffer = nullptr;
  ASSERT_NO_THROW(buffer = allocator.Malloc(buffer_size););

  // valgrind will complain if we write into nullptr
  memset(buffer, 0, buffer_size);

  allocator.Free(buffer, buffer_size);
}

// Pass through to the default memory pool
class TrackingPool : public MemoryPool {
 public:
  TrackingPool() : pool_(::arrow::default_memory_pool()), bytes_allocated_(0) {}

  Status Allocate(int64_t size, uint8_t** out) override {
    RETURN_NOT_OK(pool_->Allocate(size, out));
    bytes_allocated_ += size;
    return Status::OK();
  }

  void Free(uint8_t* buffer, int64_t size) override {
    pool_->Free(buffer, size);
    bytes_allocated_ -= size;
  }

  int64_t bytes_allocated() const override { return bytes_allocated_; }

 private:
  MemoryPool* pool_;
  int64_t bytes_allocated_;
};

TEST(TestParquetAllocator, CustomPool) {
  TrackingPool pool;

  ParquetAllocator allocator(&pool);

  ASSERT_EQ(&pool, allocator.pool());

  const int buffer_size = 10;

  uint8_t* buffer = nullptr;
  ASSERT_NO_THROW(buffer = allocator.Malloc(buffer_size););

  ASSERT_EQ(buffer_size, pool.bytes_allocated());

  // valgrind will complain if we write into nullptr
  memset(buffer, 0, buffer_size);

  allocator.Free(buffer, buffer_size);

  ASSERT_EQ(0, pool.bytes_allocated());
}

// ----------------------------------------------------------------------
// Read source tests

class BufferReader : public ::arrow::io::RandomAccessFile {
 public:
  BufferReader(const uint8_t* buffer, int buffer_size)
      : buffer_(buffer), buffer_size_(buffer_size), position_(0) {}

  Status Close() override {
    // no-op
    return Status::OK();
  }

  Status Tell(int64_t* position) override {
    *position = position_;
    return Status::OK();
  }

  Status ReadAt(
      int64_t position, int64_t nbytes, int64_t* bytes_read, uint8_t* buffer) override {
    RETURN_NOT_OK(Seek(position));
    return Read(nbytes, bytes_read, buffer);
  }

  Status Read(int64_t nbytes, int64_t* bytes_read, uint8_t* buffer) override {
    memcpy(buffer, buffer_ + position_, nbytes);
    *bytes_read = std::min(nbytes, buffer_size_ - position_);
    position_ += *bytes_read;
    return Status::OK();
  }

  Status GetSize(int64_t* size) override {
    *size = buffer_size_;
    return Status::OK();
  }

  Status Seek(int64_t position) override {
    if (position < 0 || position >= buffer_size_) {
      return Status::IOError("position out of bounds");
    }

    position_ = position;
    return Status::OK();
  }

 private:
  const uint8_t* buffer_;
  int buffer_size_;
  int64_t position_;
};

TEST(TestParquetReadSource, Basics) {
  std::string data = "this is the data";
  auto data_buffer = reinterpret_cast<const uint8_t*>(data.c_str());

  ParquetAllocator allocator(::arrow::default_memory_pool());

  auto file = std::make_shared<BufferReader>(data_buffer, data.size());
  auto source = std::make_shared<ParquetReadSource>(&allocator);

  ASSERT_OK(source->Open(file));

  ASSERT_EQ(0, source->Tell());
  ASSERT_NO_THROW(source->Seek(5));
  ASSERT_EQ(5, source->Tell());
  ASSERT_NO_THROW(source->Seek(0));

  // Seek out of bounds
  ASSERT_THROW(source->Seek(100), ::parquet::ParquetException);

  uint8_t buffer[50];

  ASSERT_NO_THROW(source->Read(4, buffer));
  ASSERT_EQ(0, std::memcmp(buffer, "this", 4));
  ASSERT_EQ(4, source->Tell());

  std::shared_ptr<::parquet::Buffer> pq_buffer;

  ASSERT_NO_THROW(pq_buffer = source->Read(7));

  auto expected_buffer = std::make_shared<::parquet::Buffer>(data_buffer + 4, 7);

  ASSERT_TRUE(expected_buffer->Equals(*pq_buffer.get()));
}

}  // namespace arrow
}  // namespace parquet
