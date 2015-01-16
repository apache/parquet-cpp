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

// the fixed initial size is just for an example
#define INIT_SIZE 100
#define COL_WIDTH "17"

using namespace boost;
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

void* read_parquet(char* filename);

// Simple example which prints out the content of the Parquet file
int main(int argc, char** argv) {

  if (argc < 2) {
    cerr << "Usage: parquet_reader <file>" << endl;
    return -1;
  }

  void *column_ptr = read_parquet(argv[1]);

  // an example to use the returned column_ptr
  // printf("%-"COL_WIDTH"d\n",((int32_t *)(((int32_t **)column_ptr)[0]))[0]);

  return 0;
}


void* read_parquet(char* filename) {

  unsigned int total_row_number = 0;

  FileMetaData metadata;
  if (!GetFileMetadata(filename, &metadata)) return NULL;

  FILE* file = fopen(filename, "r");
  if (file == NULL) {
    cerr << "Could not open file: " << filename << endl;
    return NULL;
  }

  shared_ptr<Schema> schema = Schema::FromParquet(metadata.schema);

  for (int i = 0; i < metadata.row_groups.size(); ++i) {
    const RowGroup& row_group = metadata.row_groups[i];

    Type::type* type_array = (Type::type*)malloc(
        row_group.columns.size() * sizeof(Type::type));
    assert(type_array);

    void** column_ptr = (void**)malloc(row_group.columns.size() * sizeof(void*));
    assert(column_ptr);

    for (int c = 0; c < row_group.columns.size(); ++c) {

      const ColumnChunk& col = row_group.columns[c];
      if (col.meta_data.type == Type::INT96 ||
          col.meta_data.type == Type::FIXED_LEN_BYTE_ARRAY) {
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
      ColumnReader reader(&col.meta_data, schema->leaves()[c], &input);

      AnyType min, max;
      int num_values = 0;
      int num_nulls = 0;

      switch (col.meta_data.type) {
        case Type::BOOLEAN: {
          ((bool**)column_ptr)[c] = (bool*)malloc(sizeof(bool) * INIT_SIZE);
          type_array[c] = Type::BOOLEAN;
          break;
        }
        case Type::INT32: {
          ((int32_t**)column_ptr)[c] = (int32_t*)malloc(sizeof(int32_t) * INIT_SIZE);
          type_array[c] = Type::INT32;
          break;
        }
        case Type::INT64: {
          ((int64_t**)column_ptr)[c] = (int64_t*)malloc(sizeof(int64_t) * INIT_SIZE);
          type_array[c] = Type::INT64;
          break;
        }
        case Type::FLOAT: {
          ((float**)column_ptr)[c] = (float*)malloc(sizeof(float) * INIT_SIZE);
          type_array[c] = Type::FLOAT;

          break;
        }
        case Type::DOUBLE: {
          ((double**)column_ptr)[c] = (double*)malloc(sizeof(double) * INIT_SIZE);
          type_array[c] = Type::DOUBLE;
          break;
        }
        case Type::BYTE_ARRAY: {
          ((ByteArray**)column_ptr)[c] =
              (ByteArray*)malloc(sizeof(ByteArray) * INIT_SIZE);
          type_array[c] = Type::BYTE_ARRAY;
          break;
        }
        case Type::FIXED_LEN_BYTE_ARRAY:
        case Type::INT96:
          assert(false);
          break;
      }

      bool is_null;
      int def_level = 0, rep_level = 0;
      while (reader.HasNext()) {
        switch (col.meta_data.type) {
          case Type::BOOLEAN: {
            bool val = reader.GetBool(&is_null, &def_level, &rep_level);
            ((bool*)(column_ptr[c]))[num_values] = val;
            break;
          }
          case Type::INT32: {
            int32_t val = reader.GetInt32(&is_null, &def_level, &rep_level);
            ((int32_t*)(column_ptr[c]))[num_values] = val;
            break;
          }
          case Type::INT64: {
            int64_t val = reader.GetInt64(&is_null, &def_level, &rep_level);
            ((int64_t*)(column_ptr[c]))[num_values] = val;
            break;
          }
          case Type::FLOAT: {
            float val = reader.GetFloat(&is_null, &def_level, &rep_level);
            ((float*)(column_ptr[c]))[num_values] = val;
            break;
          }
          case Type::DOUBLE: {
            double val = reader.GetDouble(&is_null, &def_level, &rep_level);
            ((double*)(column_ptr[c]))[num_values] = val;
            break;
          }
          case Type::BYTE_ARRAY: {
            ByteArray val = reader.GetByteArray(&is_null, &def_level, &rep_level);
            ByteArray* dst = &((ByteArray*)(column_ptr[c]))[num_values];
            if (is_null) {
              dst->ptr = NULL;
              dst->len = 0;
            } else {
              dst->len = val.len;
              dst->ptr = new uint8_t[val.len];
              memcpy((char*)dst->ptr, val.ptr, val.len);
            }
            break;
          }

          default:
            continue;
        }

        if (def_level < rep_level) ++num_nulls;
        ++num_values;
      }

      total_row_number = num_values;
    }

    // prints out the table
    cout << "=========================================================================\n";

    // j is the row, k is the column
    int k = 0, j = 0;

    // prints column name
    for (j = 0; j < row_group.columns.size(); ++j) {
      char* str = (char*)malloc(50);
      assert(str);
      strcpy(str, metadata.schema[j+1].name.c_str());
      printf("%-"COL_WIDTH"s", str);
      free(str);
    }

    cout << "\n";


    for (j = 0;j < row_group.columns.size(); ++j) {
      switch(type_array[j]) {
        case Type::BOOLEAN:
          printf("%-"COL_WIDTH"s","BOOLEAN");
          break;
        case Type::INT32:
          printf("%-"COL_WIDTH"s","INT32");
          break;
        case Type::INT64:
          printf("%-"COL_WIDTH"s","INT64");
          break;
        case Type::FLOAT:
          printf("%-"COL_WIDTH"s","FLOAT");
          break;
        case Type::DOUBLE:
          printf("%-"COL_WIDTH"s","DOUBLE");
          break;
        case Type::BYTE_ARRAY:
          printf("%-"COL_WIDTH"s","BYTE_ARRAY");
          break;
        default:
          continue;
      }
    }

    cout << "\n";

    string result;
    for (k = 0; k < total_row_number; ++k) {
      for (j = 0; j < row_group.columns.size(); ++j) {
        switch(type_array[j]) {
          case Type::BOOLEAN:
            printf("%-"COL_WIDTH"d",((bool*)(column_ptr[j]))[k]);
            break;
          case Type::INT32:
            printf("%-"COL_WIDTH"d",((int32_t *)(column_ptr[j]))[k]);
            break;
          case Type::INT64:
            printf("%-"COL_WIDTH"ld",((int64_t *)(column_ptr[j]))[k]);
            break;
          case Type::FLOAT:
            printf("%-"COL_WIDTH"f",((float*)(column_ptr[j]))[k]);
            break;
          case Type::DOUBLE:
            printf("%-"COL_WIDTH"lf",((double*)(column_ptr[j]))[k]);
            break;
          case Type::BYTE_ARRAY:
            result = ByteArrayToString(((ByteArray*)(column_ptr[j]))[k]);
            printf("%-"COL_WIDTH"s", result.c_str());
            break;
          default:
            continue;
        }
      }
      cout << "\n";
      // print ends
    }

    return column_ptr;
  }

  fclose(file);
  return NULL;
}
