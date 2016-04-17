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

#include "parquet/file/writer.h"

#include "parquet/file/writer-internal.h"
#include "parquet/util/output.h"

using parquet::schema::GroupNode;

namespace parquet {

RowGroupWriter::RowGroupWriter(std::unique_ptr<Contents> contents,
    MemoryAllocator* allocator):
  contents_(std::move(contents)), allocator_(allocator) {
  schema_ = contents_->schema();
}

void RowGroupWriter::Close() {
  contents_->Close();
}

// ----------------------------------------------------------------------
// ParquetFileWriter public API

ParquetFileWriter::ParquetFileWriter() {}

ParquetFileWriter::~ParquetFileWriter() {
  Close();
}

std::unique_ptr<ParquetFileWriter> ParquetFileWriter::Open(
    std::unique_ptr<OutputStream> sink, std::shared_ptr<GroupNode>& schema,
    MemoryAllocator* allocator) {
  auto contents = FileSerializer::Open(std::move(sink), schema, allocator);

  std::unique_ptr<ParquetFileWriter> result(new ParquetFileWriter());
  result->Open(std::move(contents));

  return result;
}

void ParquetFileWriter::Open(std::unique_ptr<ParquetFileWriter::Contents> contents) {
  contents_ = std::move(contents);
  schema_ = contents_->schema();
}

void ParquetFileWriter::Close() {
  if (contents_) {
    contents_->Close();
  }
}

} // namespace parquet
