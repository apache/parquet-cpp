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
#include <parquet/schema.h>

#include <iomanip>
#include <iostream>
#include <stdio.h>

#include "example_util.h"

using namespace boost;
using namespace parquet;
using namespace parquet_cpp;
using namespace std;

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

  shared_ptr<Schema> schema = Schema::FromParquet(metadata.schema);
  cout << "Schema:\n" << schema->ToString() << endl;
  cout << "Num leaf columns: "  << schema->leaves().size() << endl;
  cout << "Max depth: " << schema->max_def_level() << endl;
  for (int i = 0; i < schema->leaves().size(); ++i) {
    Schema::Element* e = schema->leaves()[i];
    cout << PrintType(e->parquet_schema()) << " "  << e->full_name() << endl
         << "  Max definition level: " << e->max_def_level() << endl
         << "  Max repetition level: " << e->max_rep_level() << endl
         << "  Ordinal path: ";
    const vector<int>& path = e->ordinal_path();
    for (int j = 0; j < path.size(); ++j) {
      cout << path[j] << " ";
    }
    cout << endl;
  }
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

