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

#include <parquet/parquet.h>
#include <iostream>
#include <stdio.h>

#include "example_util.h"

using namespace parquet;
using namespace parquet_cpp;
using namespace std;

struct AnyType {
  union {
    bool bool_val;
    int32_t int32_val;
    int64_t int64_val;
    float float_val;
    double double_val;
    ByteArray byte_array_val;
  };
};

static string ByteArrayToString(const ByteArray& a) {
  return string(reinterpret_cast<const char*>(a.ptr), a.len);
}

int ByteCompare(const ByteArray& x1, const ByteArray& x2) {
  int len = ::min(x1.len, x2.len);
  int cmp = memcmp(x1.ptr, x2.ptr, len);
  if (cmp != 0) return cmp;
  if (len < x1.len) return 1;
  if (len < x2.len) return -1;
  return 0;
}

// Simple example which reads all the values in the file and outputs the number of
// values, number of nulls and min/max for each column.
int main(int argc, char** argv) {
  int col_idx = -1;
  if (argc < 2) {
    cerr << "Usage: compute_stats <file> [col_idx]" << endl;
    return -1;
  }
  if (argc == 3) col_idx = atoi(argv[2]);
  FileMetaData metadata;
  if (!GetFileMetadata(argv[1], &metadata)) return -1;

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
      cout << "Reading column " << metadata.schema[c + 1].name << " (idx=" << c << ")\n";
      if (col.meta_data.type == Type::INT96) {
        cout << "  Skipping unsupported column" << endl;
        continue;
      }

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
      ColumnReader reader(&col.meta_data, &metadata.schema[c + 1], &input);

      bool first_val = true;
      AnyType min, max;
      int num_values = 0;
      int num_nulls = 0;

      int def_level, rep_level;
      while (reader.HasNext()) {
        switch (col.meta_data.type) {
          case Type::BOOLEAN: {
            bool val = reader.GetBool(&def_level, &rep_level);
            if (def_level < rep_level) break;
            if (first_val) {
              min.bool_val = max.bool_val = val;
              first_val = false;
            } else {
              min.bool_val = ::min(val, min.bool_val);
              max.bool_val = ::max(val, max.bool_val);
            }
            break;
          }
          case Type::INT32: {
            int32_t val = reader.GetInt32(&def_level, &rep_level);;
            if (def_level < rep_level) break;
            if (first_val) {
              min.int32_val = max.int32_val = val;
              first_val = false;
            } else {
              min.int32_val = ::min(val, min.int32_val);
              max.int32_val = ::max(val, max.int32_val);
            }
            break;
          }
          case Type::INT64: {
            int64_t val = reader.GetInt64(&def_level, &rep_level);;
            if (def_level < rep_level) break;
            if (first_val) {
              min.int64_val = max.int64_val = val;
              first_val = false;
            } else {
              min.int64_val = ::min(val, min.int64_val);
              max.int64_val = ::max(val, max.int64_val);
            }
            break;
          }
          case Type::FLOAT: {
            float val = reader.GetFloat(&def_level, &rep_level);;
            if (def_level < rep_level) break;
            if (first_val) {
              min.float_val = max.float_val = val;
              first_val = false;
            } else {
              min.float_val = ::min(val, min.float_val);
              max.float_val = ::max(val, max.float_val);
            }
            break;
          }
          case Type::DOUBLE: {
            double val = reader.GetDouble(&def_level, &rep_level);;
            if (def_level < rep_level) break;
            if (first_val) {
              min.double_val = max.double_val = val;
              first_val = false;
            } else {
              min.double_val = ::min(val, min.double_val);
              max.double_val = ::max(val, max.double_val);
            }
            break;
          }
          case Type::BYTE_ARRAY: {
            ByteArray val = reader.GetByteArray(&def_level, &rep_level);;
            if (def_level < rep_level) break;
            if (first_val) {
              min.byte_array_val = max.byte_array_val = val;
              first_val = false;
            } else {
              if (ByteCompare(val, min.byte_array_val) < 0) {
                min.byte_array_val = val;
              }
              if (ByteCompare(val, max.byte_array_val) > 0) {
                max.byte_array_val = val;
              }
            }
            break;
          }
          default:
            continue;
        }

        if (def_level < rep_level) ++num_nulls;
        ++num_values;
      }

      cout << "  Num Values: " << num_values << endl;
      cout << "  Num Nulls: " << num_nulls << endl;
      switch (col.meta_data.type) {
        case Type::BOOLEAN:
          cout << "  Min: " << min.bool_val << endl;
          cout << "  Max: " << max.bool_val << endl;
          break;
        case Type::INT32:
          cout << "  Min: " << min.int32_val << endl;
          cout << "  Max: " << max.int32_val << endl;
          break;
        case Type::INT64:
          cout << "  Min: " << min.int64_val << endl;
          cout << "  Max: " << max.int64_val << endl;
          break;
        case Type::FLOAT:
          cout << "  Min: " << min.float_val << endl;
          cout << "  Max: " << max.float_val << endl;
          break;
        case Type::DOUBLE:
          cout << "  Min: " << min.double_val << endl;
          cout << "  Max: " << max.double_val << endl;
          break;
        case Type::BYTE_ARRAY:
          cout << "  Min: " << ByteArrayToString(min.byte_array_val) << endl;
          cout << "  Max: " << ByteArrayToString(max.byte_array_val) << endl;
          break;
        default:
          continue;
      }
    }
  }
  fclose(file);
  return 0;
}
