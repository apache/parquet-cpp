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

#include <parquet/reader.h>
#include <parquet/schema/converter.h>
#include <parquet/schema/printer.h>

#include <iostream>

using namespace parquet_cpp;

void DumpSchema(const ParquetFileReader* reader) {
  auto schema = reader->metadata().schema;
  schema::FlatSchemaConverter converter(&schema[0], schema.size());
  std::unique_ptr<schema::Node> root = converter.Convert();

  PrintSchema(root.get(), std::cout);
}

int main(int argc, char** argv) {
  std::string filename = argv[1];

  parquet_cpp::ParquetFileReader reader;
  parquet_cpp::LocalFileSource file;

  file.Open(filename);
  if (!file.is_open()) {
    std::cerr << "Could not open file " << file.path()
              << std::endl;
    return -1;
  }

  try {
    reader.Open(&file);
    reader.ParseMetaData();
    DumpSchema(&reader);
  } catch (const std::exception& e) {
    std::cerr << "Parquet error: "
              << e.what()
              << std::endl;
    return -1;
  }

  return 0;
}
