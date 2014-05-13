#include <parquet/parquet.h>
#include <iostream>
#include <stdio.h>

#include "example_util.h"

using namespace parquet;
using namespace parquet_cpp;
using namespace std;

int main(int argc, char** argv) {
  int col_idx = 0;
  if (argc < 2) {
    cerr << "Usage: compute_stats <file> [col_idx]" << endl;
    return -1;
  }
  if (argc == 3) {
    col_idx = atoi(argv[2]);
  }
  FileMetaData metadata;
  if (!GetFileMetadata(argv[1], &metadata)) return -1;
  //cout << apache::thrift::ThriftDebugString(metadata);

  FILE* file = fopen(argv[1], "r");
  if (file == NULL) {
    cerr << "Could not open file: " << argv[1] << endl;
    return -1;
  }

  for (int i = 0; i < metadata.row_groups.size(); ++i) {
    const RowGroup& row_group = metadata.row_groups[i];
    for (int c = 0; c < row_group.columns.size(); ++c) {
      if (col_idx != -1 && col_idx != c) continue;
      const ColumnChunk& col = row_group.columns[c];
      cout << "Reading column " << c << endl;
      size_t col_start = col.meta_data.data_page_offset;
      if (col.meta_data.__isset.dictionary_page_offset) {
        if (col_start > col.meta_data.dictionary_page_offset) {
          col_start = col.meta_data.dictionary_page_offset;
        }
      }
      fseek(file, col_start, SEEK_SET);
      vector<uint8_t> column_buffer;
      column_buffer.resize(col.meta_data.total_compressed_size);
      size_t num_read = fread(&column_buffer[0], 1, column_buffer.size(), file);
      if (num_read != column_buffer.size()) {
        cerr << "Could not read column data." << endl;
        continue;
      }
      InMemoryInputStream input(&column_buffer[0], column_buffer.size());
      ColumnReader reader(&metadata.schema[c + 1], &input);
      while (reader.HasNext()) {
        int32_t val;
        bool is_null = reader.GetInt32(&val);
        if (!is_null) cout << val << endl;
      }
    }
  }
  fclose(file);
  return 0;
}
