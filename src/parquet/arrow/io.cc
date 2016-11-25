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

#include "parquet/api/io.h"
#include "parquet/arrow/utils.h"

#include "arrow/util/status.h"

using arrow::Status;
using arrow::MemoryPool;

// To assist with readability
using ArrowROFile = arrow::io::ReadableFileInterface;

namespace parquet {
namespace arrow {

// ----------------------------------------------------------------------
// ParquetAllocator

ParquetAllocator::ParquetAllocator() : pool_(::arrow::default_memory_pool()) {}

ParquetAllocator::ParquetAllocator(MemoryPool* pool) : pool_(pool) {}

ParquetAllocator::~ParquetAllocator() {}

uint8_t* ParquetAllocator::Malloc(int64_t size) {
  uint8_t* result;
  PARQUET_THROW_NOT_OK(pool_->Allocate(size, &result));
  return result;
}

void ParquetAllocator::Free(uint8_t* buffer, int64_t size) {
  // Does not report Status
  pool_->Free(buffer, size);
}

// ----------------------------------------------------------------------
// ParquetReadSource

ParquetReadSource::ParquetReadSource(ParquetAllocator* allocator)
    : file_(nullptr), allocator_(allocator) {}

Status ParquetReadSource::Open(const std::shared_ptr<ArrowROFile>& file) {
  int64_t file_size;
  RETURN_NOT_OK(file->GetSize(&file_size));

  file_ = file;
  size_ = file_size;
  return Status::OK();
}

void ParquetReadSource::Close() {
  // TODO(wesm): Make this a no-op for now. This leaves Python wrappers for
  // these classes in a borked state. Probably better to explicitly close.

  // PARQUET_THROW_NOT_OK(file_->Close());
}

int64_t ParquetReadSource::Tell() const {
  int64_t position;
  PARQUET_THROW_NOT_OK(file_->Tell(&position));
  return position;
}

void ParquetReadSource::Seek(int64_t position) {
  PARQUET_THROW_NOT_OK(file_->Seek(position));
}

int64_t ParquetReadSource::Read(int64_t nbytes, uint8_t* out) {
  int64_t bytes_read;
  PARQUET_THROW_NOT_OK(file_->Read(nbytes, &bytes_read, out));
  return bytes_read;
}

std::shared_ptr<Buffer> ParquetReadSource::Read(int64_t nbytes) {
  // TODO(wesm): This code is duplicated from parquet/util/input.cc; suggests
  // that there should be more code sharing amongst file-like sources
  auto result = std::make_shared<OwnedMutableBuffer>(0, allocator_);
  result->Resize(nbytes);

  int64_t bytes_read = Read(nbytes, result->mutable_data());
  if (bytes_read < nbytes) { result->Resize(bytes_read); }
  return result;
}

ParquetWriteSink::ParquetWriteSink(
    const std::shared_ptr<::arrow::io::OutputStream>& stream)
    : stream_(stream) {}

ParquetWriteSink::~ParquetWriteSink() {}

void ParquetWriteSink::Close() {
  PARQUET_THROW_NOT_OK(stream_->Close());
}

int64_t ParquetWriteSink::Tell() {
  int64_t position;
  PARQUET_THROW_NOT_OK(stream_->Tell(&position));
  return position;
}

void ParquetWriteSink::Write(const uint8_t* data, int64_t length) {
  PARQUET_THROW_NOT_OK(stream_->Write(data, length));
}

}  // namespace arrow
}  // namespace parquet
