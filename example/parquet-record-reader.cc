// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <parquet/generic-record.h>
#include <parquet/parquet.h>
#include <parquet/schema.h>
#include <iostream>
#include <stdio.h>

#include "example_util.h"

using namespace boost;
using namespace parquet;
using namespace parquet_cpp;
using namespace std;

void ReadParquet(char* filename, vector<int> columns) {
  FILE* file = fopen(filename, "r");
  if (file == NULL) {
    cerr << "Could not open file: " << filename << endl;
    return;
  }

  FileMetaData metadata;
  if (!GetFileMetadata(filename, &metadata)) return;
  shared_ptr<Schema> schema = Schema::FromParquet(metadata.schema);

  if (columns.empty()) {
    for (int i = 0; i < schema->leaves().size(); ++i) {
      columns.push_back(i);
    }
  }

  for (int i = 0; i < metadata.row_groups.size(); ++i) {
    const RowGroup& row_group = metadata.row_groups[i];

    vector<InputStream*> streams;
    vector<const Schema::Element*> projected_cols;
    vector<const ColumnMetaData*> col_metadata;
    vector<uint8_t*> col_buffers;

    // Read all the columns we are interested in.
    for (int c = 0; c < columns.size(); ++c) {
      int col_idx = columns[c];
      if (col_idx >= row_group.columns.size()) {
        cerr << "Invalid col idx." << endl;
        return;
      }

      const ColumnChunk& col = row_group.columns[col_idx];
      size_t col_start = col.meta_data.data_page_offset;
      if (col.meta_data.__isset.dictionary_page_offset) {
        if (col_start > col.meta_data.dictionary_page_offset) {
          col_start = col.meta_data.dictionary_page_offset;
        }
      }
      fseek(file, col_start, SEEK_SET);
      col_buffers.push_back(new uint8_t[col.meta_data.total_compressed_size]);
      size_t num_read = fread(
          col_buffers[c], 1, col.meta_data.total_compressed_size, file);
      if (num_read != col.meta_data.total_compressed_size) {
        cerr << "Could not read column data." << endl;
        continue;
      }

      streams.push_back(new InMemoryInputStream(col_buffers[c], num_read));
      projected_cols.push_back(schema->leaves()[col_idx]);
      col_metadata.push_back(&col.meta_data);
    }

    RecordReader reader(schema.get(), &metadata, i,
        projected_cols, col_metadata, streams);
    printf("Total rows: %ld\n", reader.rows_left());

    while (reader.rows_left() > 0) {
      vector<shared_ptr<GenericStruct> > records = reader.GetNext();
      for (int j = 0; j < records.size(); ++j) {
        printf("%s\n", records[j]->ToString(schema->root()).c_str());
      }
    }

    for (int j = 0; j < streams.size(); ++j) {
      delete streams[j];
      delete col_buffers[j];
    }
  }

  fclose(file);
}

void PrintUsage() {
  cerr << "Usage: parquet_reader <file> [col_idx1 col_idx2 etc]" << endl;
}

// Simple example which prints out the content of the Parquet file
int main(int argc, char** argv) {
  if (argc < 2) {
    PrintUsage();
    return -1;
  }
  vector<int> cols;
  for (int i = 2; i < argc; ++i) {
    cols.push_back(atoi(argv[i]));
  }

  ReadParquet(argv[1], cols);
  return 0;
}

