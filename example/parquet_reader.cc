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
#include "example_util.h"

#include <iostream>

// the fixed initial size is just for an example
#define  COL_WIDTH "17"

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

string type2String(Type::type t) {
  switch(t) {
    case Type::BOOLEAN:
      return "BOOLEAN";
      break;
    case Type::INT32:
      return "INT32";
      break;
    case Type::INT64:
      return "INT64";
      break;
    case Type::FLOAT:
      return "FLOAT";
      break;
    case Type::DOUBLE:
      return "DOUBLE";
      break;
    case Type::BYTE_ARRAY:
      return "BYTE_ARRAY";
      break;
    case Type::INT96:
      return "INT96";
      break;
    case Type::FIXED_LEN_BYTE_ARRAY:
      return "FIXED_LEN_BYTE_ARRAY";
      break;
    default:
      return "UNKNOWN";
      break;
  }
}

void readParquet(const string& filename, const bool printValues) {
  InputFile file(filename);
  if (!file.isOpen()) {
    cerr << "Could not open file " << file.getFilename() << endl;
    return;
  }

  FileMetaData metadata;
  if (!GetFileMetadata(file.getFilename().c_str(), &metadata)) {
    cerr << "Could not read metadata from file " << file.getFilename() << endl;
    return;
  }

  cout << "File statistics:\n" ;
  cout << "Total rows: " << metadata.num_rows << "\n";
  for (int c = 1; c < metadata.schema.size(); ++c) {
    cout << "Column " << c-1 << ": " << metadata.schema[c].name << " ("
         << type2String(metadata.schema[c].type);
    if (metadata.schema[c].type == Type::INT96 ||
        metadata.schema[c].type == Type::FIXED_LEN_BYTE_ARRAY) {
      cout << " - not supported";
    }
    cout << ")\n";
  }

  for (int i = 0; i < metadata.row_groups.size(); ++i) {
    cout << "--- Row Group " << i << " ---\n";

    // Print column metadata
    const RowGroup& row_group = metadata.row_groups[i];
    size_t nColumns = row_group.columns.size();

    for (int c = 0; c < nColumns; ++c) {
      const ColumnMetaData& meta_data = row_group.columns[c].meta_data;
      cout << "Column " << c
           << ": " << meta_data.num_values << " rows, "
           << meta_data.statistics.null_count << " null values, "
           << meta_data.statistics.distinct_count << " distinct values, "
           << "min value: " << (meta_data.statistics.min.length()>0 ?
                                meta_data.statistics.min : "N/A")
           << ", max value: " << (meta_data.statistics.max.length()>0 ?
                                  meta_data.statistics.max : "N/A") << ".\n";
    }

    if (!printValues) {
      continue;
    }

    // Create readers for all columns and print contents
    vector<ColumnReader*> readers(nColumns, NULL);
    try {
      for (int c = 0; c < nColumns; ++c) {
        const ColumnChunk& col = row_group.columns[c];
        printf("%-" COL_WIDTH"s", metadata.schema[c+1].name.c_str());

        if (col.meta_data.type == Type::INT96 ||
            col.meta_data.type == Type::FIXED_LEN_BYTE_ARRAY) {
          continue;
        }

        size_t col_start = col.meta_data.data_page_offset;
        if (col.meta_data.__isset.dictionary_page_offset &&
            col_start > col.meta_data.dictionary_page_offset) {
          col_start = col.meta_data.dictionary_page_offset;
        }

        std::unique_ptr<ScopedInMemoryInputStream> input(
             new ScopedInMemoryInputStream(col.meta_data.total_compressed_size));
         fseek(file.getFileHandle(), col_start, SEEK_SET);
         size_t num_read = fread(input->data(),
                                 1,
                                 input->size(),
                                 file.getFileHandle());
         if (num_read != input->size()) {
           cerr << "Could not read column data." << endl;
           continue;
         }

        readers[c] = new ColumnReader(&col.meta_data,
                                      &metadata.schema[c+1],
                                      input.release());
      }
      cout << "\n";

      vector<int> def_level(nColumns, 0);
      vector<int> rep_level(nColumns, 0);

      bool hasRow;
      do {
        hasRow = false;
        for (int c = 0; c < nColumns; ++c) {
          if (readers[c] == NULL) {
            printf("%-" COL_WIDTH"s", " ");
            continue;
          }
          const ColumnChunk& col = row_group.columns[c];
          if (readers[c]->HasNext()) {
            hasRow = true;
            switch (col.meta_data.type) {
              case Type::BOOLEAN: {
                bool val = readers[c]->GetBool(&def_level[c], &rep_level[c]);
                if (def_level[c] >= rep_level[c]) {
                  printf("%-" COL_WIDTH"d",val);
                }
                break;
             }
              case Type::INT32: {
                int32_t val = readers[c]->GetInt32(&def_level[c], &rep_level[c]);
                if (def_level[c] >= rep_level[c]) {
                  printf("%-" COL_WIDTH"d",val);
                }
                break;
              }
              case Type::INT64: {
                int64_t val = readers[c]->GetInt64(&def_level[c], &rep_level[c]);
                if (def_level[c] >= rep_level[c]) {
                  printf("%-" COL_WIDTH"ld",val);
                }
                break;
              }
              case Type::FLOAT: {
                float val = readers[c]->GetFloat(&def_level[c], &rep_level[c]);
                if (def_level[c] >= rep_level[c]) {
                  printf("%-" COL_WIDTH"f",val);
                }
                break;
              }
              case Type::DOUBLE: {
                double val = readers[c]->GetDouble(&def_level[c], &rep_level[c]);
                if (def_level[c] >= rep_level[c]) {
                  printf("%-" COL_WIDTH"lf",val);
                }
                break;
              }
              case Type::BYTE_ARRAY: {
                ByteArray val = readers[c]->GetByteArray(&def_level[c], &rep_level[c]);
                if (def_level[c] >= rep_level[c]) {
                  string result = ByteArrayToString(val);
                  printf("%-" COL_WIDTH"s", result.c_str());
                }
                break;
              }
              default:
                continue;
            }
          }
        }
        cout << "\n";
      } while (hasRow);
    } catch (exception& e) {
      cout << "Caught an exception: " << e.what() << "\n";
    } catch (...) {
      cout << "Caught an exception.\n";
    }

    for(vector<ColumnReader*>::iterator it = readers.begin(); it != readers.end(); it++) {
      delete *it;
    }
  }
}

int main(int argc, char** argv) {
  if (argc > 3) {
    cerr << "Usage: parquet_reader [--only-stats] <file>" << endl;
    return -1;
  }

  string filename;
  bool printContents = true;

  // Read command-line options
  char *param, *value;
  for (int i = 1; i < argc; i++) {
    if ( (param = std::strstr(argv[i], "--only-stats")) ) {
      printContents = false;
    } else {
      filename = argv[i];
    }
  }

  readParquet(filename, printContents);

  return 0;
}

