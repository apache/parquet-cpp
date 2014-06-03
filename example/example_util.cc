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

#include "example_util.h"
#include <iostream>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

using namespace parquet;
using namespace parquet_cpp;
using namespace std;

// 4 byte constant + 4 byte metadata len
const uint32_t FOOTER_SIZE = 8;
const uint8_t PARQUET_MAGIC[4] = {'P', 'A', 'R', '1'};

struct ScopedFile {
 public:
  ScopedFile(FILE* f) : file_(f) { }
  ~ScopedFile() { fclose(file_); }

 private:
  FILE* file_;
};

bool GetFileMetadata(const string& path, FileMetaData* metadata) {
  FILE* file = fopen(path.c_str(), "r");
  if (!file) {
    cerr << "Could not open file: " << path << endl;
    return false;
  }
  ScopedFile cleanup(file);
  fseek(file, 0L, SEEK_END);
  size_t file_len = ftell(file);
  if (file_len < FOOTER_SIZE) {
    cerr << "Invalid parquet file. Corrupt footer." << endl;
    return false;
  }

  uint8_t footer_buffer[FOOTER_SIZE];
  fseek(file, file_len - FOOTER_SIZE, SEEK_SET);
  size_t bytes_read = fread(footer_buffer, 1, FOOTER_SIZE, file);
  if (bytes_read != FOOTER_SIZE) {
    cerr << "Invalid parquet file. Corrupt footer." << endl;
    return false;
  }
  if (memcmp(footer_buffer + 4, PARQUET_MAGIC, 4) != 0) {
    cerr << "Invalid parquet file. Corrupt footer." << endl;
    return false;
  }

  uint32_t metadata_len = *reinterpret_cast<uint32_t*>(footer_buffer);
  size_t metadata_start = file_len - FOOTER_SIZE - metadata_len;
  if (metadata_start < 0) {
    cerr << "Invalid parquet file. File is less than file metadata size." << endl;
    return false;
  }

  fseek(file, metadata_start, SEEK_SET);
  uint8_t metadata_buffer[metadata_len];
  bytes_read = fread(metadata_buffer, 1, metadata_len, file);
  if (bytes_read != metadata_len) {
    cerr << "Invalid parquet file. Could not read metadata bytes." << endl;
    return false;
  }

  DeserializeThriftMsg(metadata_buffer, &metadata_len, metadata);
  return true;

}
