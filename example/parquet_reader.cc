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

#include <iostream>
#include <memory>

#include <parquet/api/reader.h>

using namespace parquet_cpp;

int main(int argc, char** argv) {
  if (argc > 3) {
    std::cerr << "Usage: parquet_reader [--only-stats] [--no-memory-map] <file>"
              << std::endl;
    return -1;
  }

  std::string filename;
  bool print_values = true;
  bool memory_map = true;

  // Read command-line options
  char *param, *value;
  for (int i = 1; i < argc; i++) {
    if ((param = std::strstr(argv[i], "--only-stats"))) {
      print_values = false;
    } else if ((param = std::strstr(argv[i], "--no-memory-map"))) {
      memory_map = false;
    } else {
      filename = argv[i];
    }
  }

  try {
    std::unique_ptr<ParquetFileReader> reader = ParquetFileReader::OpenFile(filename,
        memory_map);
    reader->DebugPrint(std::cout, print_values);
  } catch (const std::exception& e) {
    std::cerr << "Parquet error: "
              << e.what()
              << std::endl;
    return -1;
  }

  return 0;
}
