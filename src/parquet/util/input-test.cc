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

#include <gtest/gtest.h>

#include <string>

#include "parquet/exception.h"
#include "parquet/util/input.h"
#include "parquet/util/test-common.h"

namespace parquet_cpp {

namespace test {

void create_test_file(const std::string& filename, const std::string& contents) {
  FILE* file = fopen(filename.c_str(), "w");
  fwrite(contents.data(), sizeof(char), contents.length(), file);
  fclose(file);
}

TEST(LocalFileSource, TestNoop) {
  LocalFileSource file;
  ASSERT_THROW(file.Open("/samnvjhdsaf/nonexistent_file"), ParquetException);
}

TEST(LocalFileSource, TestOperations) {
  std::string filename("/tmp/LocalFileSource.test");
  std::string contents("abc");
  create_test_file(filename, contents);

  LocalFileSource file;
  file.Open(filename);
  ASSERT_EQ(3, file.Size());

  file.Seek(0);
  ASSERT_EQ(0, file.Tell());
  file.Seek(2);
  ASSERT_EQ(2, file.Tell());
  ASSERT_THROW(file.Seek(3), ParquetException);

  file.Seek(0);
  std::vector<uint8_t> output(4, 'X');
  ASSERT_EQ(3, file.Read(5, output.data()));
  ASSERT_EQ(contents[0], output[0]);
  ASSERT_EQ(contents[1], output[1]);
  ASSERT_EQ(contents[2], output[2]);
  ASSERT_EQ('X', output[3]);
}

} // namespace test

} // namespace parquet_cpp
