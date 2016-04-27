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

#include "parquet/column/properties.h"
#include "parquet/file/reader.h"

namespace parquet {

namespace test {

TEST(TestReaderProperties, Basics) {
  ReaderProperties opts(true, 1024);

  ASSERT_EQ(1024, opts.buffer_size());
  ASSERT_EQ(true, opts.is_buffered_stream_enabled());

  std::string smin;
  std::string smax;
  int32_t int_min = 1024;
  int32_t int_max = 2048;
  smin = std::string(reinterpret_cast<char*>(&int_min), sizeof(int32_t));
  smax = std::string(reinterpret_cast<char*>(&int_max), sizeof(int32_t));
  ASSERT_STREQ("1024", opts.type_printer(Type::INT32, smin.c_str(), 0).c_str());
  ASSERT_STREQ("2048", opts.type_printer(Type::INT32, smax.c_str(), 0).c_str());

  int64_t int64_min = 10240000000000;
  int64_t int64_max = 20480000000000;
  smin = std::string(reinterpret_cast<char*>(&int64_min), sizeof(int64_t));
  smax = std::string(reinterpret_cast<char*>(&int64_max), sizeof(int64_t));
  ASSERT_STREQ("10240000000000", opts.type_printer(Type::INT64, smin.c_str(), 0).c_str());
  ASSERT_STREQ("20480000000000", opts.type_printer(Type::INT64, smax.c_str(), 0).c_str());

  float float_min = 1.024;
  float float_max = 2.048;
  smin = std::string(reinterpret_cast<char*>(&float_min), sizeof(float));
  smax = std::string(reinterpret_cast<char*>(&float_max), sizeof(float));
  ASSERT_STREQ("1.024", opts.type_printer(Type::FLOAT, smin.c_str(), 0).c_str());
  ASSERT_STREQ("2.048", opts.type_printer(Type::FLOAT, smax.c_str(), 0).c_str());

  double double_min = 1.0245;
  double double_max = 2.0489;
  smin = std::string(reinterpret_cast<char*>(&double_min), sizeof(double));
  smax = std::string(reinterpret_cast<char*>(&double_max), sizeof(double));
  ASSERT_STREQ("1.0245", opts.type_printer(Type::DOUBLE, smin.c_str(), 0).c_str());
  ASSERT_STREQ("2.0489", opts.type_printer(Type::DOUBLE, smax.c_str(), 0).c_str());

  Int96 Int96_min = {{1024, 2048, 4096}};
  Int96 Int96_max = {{2048, 4096, 8192}};
  smin = std::string(reinterpret_cast<char*>(&Int96_min), sizeof(Int96));
  smax = std::string(reinterpret_cast<char*>(&Int96_max), sizeof(Int96));
  ASSERT_STREQ("1024 2048 4096 ",
      opts.type_printer(Type::INT96, smin.c_str(), 0).c_str());
  ASSERT_STREQ("2048 4096 8192 ",
      opts.type_printer(Type::INT96, smax.c_str(), 0).c_str());

  ByteArray BA_min;
  ByteArray BA_max;
  BA_min.ptr = reinterpret_cast<const uint8_t*>("abcdef");
  BA_min.len = 6;
  BA_max.ptr = reinterpret_cast<const uint8_t*>("ijklmnop");
  BA_max.len = 8;
  smin = std::string(reinterpret_cast<char*>(&BA_min), sizeof(ByteArray));
  smax = std::string(reinterpret_cast<char*>(&BA_max), sizeof(ByteArray));
  ASSERT_STREQ("a b c d e f ",
      opts.type_printer(Type::BYTE_ARRAY, smin.c_str(), 0).c_str());
  ASSERT_STREQ("i j k l m n o p ",
      opts.type_printer(Type::BYTE_ARRAY, smax.c_str(), 0).c_str());

  FLBA FLBA_min;
  FLBA FLBA_max;
  FLBA_min.ptr = reinterpret_cast<const uint8_t*>("abcdefgh");
  FLBA_max.ptr = reinterpret_cast<const uint8_t*>("ijklmnop");
  int len = 8;
  smin = std::string(reinterpret_cast<char*>(&FLBA_min), sizeof(FLBA));
  smax = std::string(reinterpret_cast<char*>(&FLBA_max), sizeof(FLBA));
  ASSERT_STREQ("a b c d e f g h ",
      opts.type_printer(Type::FIXED_LEN_BYTE_ARRAY, smin.c_str(), len).c_str());
  ASSERT_STREQ("i j k l m n o p ",
      opts.type_printer(Type::FIXED_LEN_BYTE_ARRAY, smax.c_str(), len).c_str());
}

TEST(TestWriterProperties, Basics) {
  WriterProperties opts(1024, 2048, true);

  ASSERT_EQ(1024, opts.data_pagesize());
  ASSERT_EQ(2048, opts.dictionary_pagesize());
  ASSERT_EQ(true, opts.is_dictionary_enabled());
}

} // namespace test
} // namespace parquet
