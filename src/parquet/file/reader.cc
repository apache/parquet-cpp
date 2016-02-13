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

#include "parquet/file/reader.h"

#include <cstdio>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "parquet/column/page.h"
#include "parquet/column/reader.h"
#include "parquet/column/scanner.h"
#include "parquet/exception.h"
#include "parquet/file/reader-internal.h"
#include "parquet/util/input.h"
#include "parquet/types.h"

using std::string;
using std::vector;

namespace parquet_cpp {

// ----------------------------------------------------------------------
// RowGroupReader public API

RowGroupReader::RowGroupReader(const SchemaDescriptor* schema,
    std::unique_ptr<Contents> contents) :
    schema_(schema),
    contents_(std::move(contents)) {}

int RowGroupReader::num_columns() const {
  return contents_->num_columns();
}

std::shared_ptr<ColumnReader> RowGroupReader::Column(int i) {
  // TODO: boundschecking
  auto it = column_readers_.find(i);
  if (it !=  column_readers_.end()) {
    // Already have constructed the ColumnReader
    return it->second;
  }

  const ColumnDescriptor* descr = schema_->Column(i);

  std::unique_ptr<PageReader> page_reader = contents_->GetColumnPageReader(i);
  std::shared_ptr<ColumnReader> reader = ColumnReader::Make(descr,
      std::move(page_reader));
  column_readers_[i] = reader;
  return reader;
}

RowGroupStatistics RowGroupReader::GetColumnStats(int i) const {
  return contents_->GetColumnStats(i);
}

// ----------------------------------------------------------------------
// ParquetFileReader public API

ParquetFileReader::ParquetFileReader() : schema_(nullptr) {}
ParquetFileReader::~ParquetFileReader() {}

std::unique_ptr<ParquetFileReader> ParquetFileReader::OpenFile(const std::string& path) {
  std::unique_ptr<LocalFileSource> file(new LocalFileSource());
  file->Open(path);

  auto contents = SerializedFile::Open(std::move(file));

  std::unique_ptr<ParquetFileReader> result(new ParquetFileReader());
  result->Open(std::move(contents));

  return result;
}

void ParquetFileReader::Open(std::unique_ptr<ParquetFileReader::Contents> contents) {
  contents_ = std::move(contents);
  schema_ = contents_->schema();
}

void ParquetFileReader::Close() {
  contents_->Close();
}

int ParquetFileReader::num_row_groups() const {
  return contents_->num_row_groups();
}

int64_t ParquetFileReader::num_rows() const {
  return contents_->num_rows();
}

int ParquetFileReader::num_columns() const {
  return schema_->num_columns();
}

RowGroupReader* ParquetFileReader::RowGroup(int i) {
  if (i >= num_row_groups()) {
    std::stringstream ss;
    ss << "The file only has " << num_row_groups()
       << "row groups, requested reader for: "
       << i;
    throw ParquetException(ss.str());
  }

  auto it = row_group_readers_.find(i);
  if (it != row_group_readers_.end()) {
    // Constructed the RowGroupReader already
    return it->second.get();
  }

  row_group_readers_[i] = contents_->GetRowGroup(i);
  return row_group_readers_[i].get();
}

// ----------------------------------------------------------------------
// ParquetFileReader::DebugPrint

// the fixed initial size is just for an example
#define COL_WIDTH "20"


void ParquetFileReader::DebugPrint(std::ostream& stream, bool print_values) {
  stream << "File statistics:\n";
  stream << "Total rows: " << this->num_rows() << "\n";

  for (int i = 0; i < num_columns(); ++i) {
    const ColumnDescriptor* descr = schema_->Column(i);
    stream << "Column " << i << ": "
           << descr->name()
           << " ("
           << type_to_string(descr->physical_type())
           << ")" << std::endl;
  }

  for (int r = 0; r < num_row_groups(); ++r) {
    stream << "--- Row Group " << r << " ---\n";

    RowGroupReader* group_reader = RowGroup(r);

    // Print column metadata
    size_t num_columns = group_reader->num_columns();

    for (int i = 0; i < num_columns; ++i) {
      RowGroupStatistics stats = group_reader->GetColumnStats(i);

      stream << "Column " << i << ": "
             << stats.num_values << " rows, "
             << stats.null_count << " null values, "
             << stats.distinct_count << " distinct values, "
             << std::endl;
    }

    if (!print_values) {
      continue;
    }

    static constexpr size_t bufsize = 25;
    char buffer[bufsize];

    // Create readers for all columns and print contents
    vector<std::shared_ptr<Scanner> > scanners(num_columns, NULL);
    for (int i = 0; i < num_columns; ++i) {
      std::shared_ptr<ColumnReader> col_reader = group_reader->Column(i);
      Type::type col_type = col_reader->type();

      std::stringstream ss;
      ss << "%-" << COL_WIDTH << "s";
      std::string fmt = ss.str();

      snprintf(buffer, bufsize, fmt.c_str(), column_schema(i)->name().c_str());
      stream << buffer;

      // This is OK in this method as long as the RowGroupReader does not get
      // deleted
      scanners[i] = Scanner::Make(col_reader);
    }
    stream << "\n";

    bool hasRow;
    do {
      hasRow = false;
      for (int i = 0; i < num_columns; ++i) {
        if (scanners[i]->HasNext()) {
          hasRow = true;
          scanners[i]->PrintNext(stream, 17);
        }
      }
      stream << "\n";
    } while (hasRow);
  }
}

} // namespace parquet_cpp
