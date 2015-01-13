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
#include <iomanip>
#include <iostream>
#include <stdio.h>

#include "example_util.h"

using namespace boost;
using namespace parquet;
using namespace parquet_cpp;
using namespace std;

string PrintType(const SchemaElement& e) {
  switch (e.type) {
    case Type::BOOLEAN: return "bool";
    case Type::INT32: return "int32";
    case Type::INT64: return "int64";
    case Type::INT96: return "int96";
    case Type::FLOAT: return "float";
    case Type::DOUBLE: return "double";
    case Type::BYTE_ARRAY:
      return (e.__isset.converted_type && e.converted_type == ConvertedType::UTF8)
            ? "string" : "uint8[]";
    default: return "unknown";
  }
}

string DumpSchema(const vector<SchemaElement>& schema, const string& prefix, int* idx) {
  stringstream ss;
  int num_children = schema[*idx].num_children;
  if (num_children > 0) {
    if (idx == 0) {
      ss << prefix << "{\n";
    } else {
      ss << prefix << "struct " << schema[*idx].name << " {\n";
    }
    ++*idx;
    for (int i = 1; i <= num_children; ++i) {
      ss << DumpSchema(schema, prefix + "  ", idx);
    }
    ss << prefix << "};\n";
  } else {
    ss << prefix << PrintType(schema[*idx]) << " " << schema[*idx].name << ";\n";
    ++*idx;
  }
  return ss.str();
}

void DumpMetadata(char* filename) {
  FILE* file = fopen(filename, "r");
  if (file == NULL) {
    cerr << "Could not open file: " << filename << endl;
    return;
  }

  FileMetaData metadata;
  if (!GetFileMetadata(filename, &metadata)) {
    cerr << "Could not parse file metadata.";
    fclose(file);
    return;
  }

  cout << "Metadata for file: " << filename << endl;
  cout << "  Version: " << metadata.version << endl;
  if (metadata.__isset.created_by) {
    cout << "  Written by: " << metadata.created_by << endl;
  }
  cout << "  Total rows: " << metadata.num_rows << endl;
  cout << "  Num row groups: " << metadata.row_groups.size() << endl;
  if (metadata.key_value_metadata.size() > 0) {
    cout << "  Optional metadata: " << endl;
    for (int i = 0; i < metadata.key_value_metadata.size(); ++i) {
      cout << "    " << metadata.key_value_metadata[i].key;
      if (metadata.key_value_metadata[i].__isset.value) {
        cout << " = " << metadata.key_value_metadata[i].value;
      }
      cout << endl;
    }
  }
  int idx = 0;
  cout << "  Schema:\n" << DumpSchema(metadata.schema, "    ", &idx) << endl;
}

// Utility that dumps the metadata of a parquet file as a "c-struct"
int main(int argc, char** argv) {
  if (argc < 2) {
    cerr << "Usage: dump_metadata <file>" << endl;
    return -1;
  }
  DumpMetadata(argv[1]);
  return 0;
}

