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

int main(int argc, char** argv) {
  if (argc > 3) {
    std::cerr << "Usage: parquet_reader [--only-stats] <file>"
              << std::endl;
    return -1;
  }

  std::string filename;
  bool print_values = true;

  // Read command-line options
  char *param, *value;
  for (int i = 1; i < argc; i++) {
    if ( (param = std::strstr(argv[i], "--only-stats")) ) {
      print_values = false;
    } else {
      filename = argv[i];
    }
  }

  parquet_cpp::ParquetFileReader reader;
  parquet_cpp::LocalFile file;

  file.Open(filename);
  if (!file.is_open()) {
    std::cerr << "Could not open file " << file.path()
              << std::endl;
    return -1;
  }

  try {
    reader.Open(&file);
    reader.ParseMetaData();
    reader.DebugPrint(std::cout, print_values);
  } catch (const std::exception& e) {
    std::cerr << "Parquet error: "
              << e.what()
              << std::endl;
    return -1;
  }

  return 0;
}
