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

#ifndef PARQUET_EXAMPLE_UTIL_H
#define PARQUET_EXAMPLE_UTIL_H

#include <string>
#include <parquet/parquet.h>
#include <stdio.h>

bool GetFileMetadata(const std::string& path, parquet::FileMetaData* metadata);

class InputFile {
private:
  FILE* file;
  std::string filename;

public:
  InputFile(const std::string& _filename): filename(_filename) {
    file = fopen(_filename.c_str(), "r");
  }
  ~InputFile() {
    if (file != NULL) {
      fclose(file);
    }
  }

  FILE* getFileHandle() { return file; }
  bool isOpen() { return file != NULL; }
  std::string getFilename()  { return filename; }
};

#endif  // PARQUET_EXAMPLE_UTIL_H
