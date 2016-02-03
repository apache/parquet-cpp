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

#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include "parquet/util/test-common.h"

#include "parquet/schema/types.h"

using std::string;
using std::vector;

namespace parquet_cpp {

namespace schema {

TEST(TestPrimitiveType, TestIsRepetition) {
  PrimitiveType type1("foo", Repetition::REQUIRED, PhysicalType::INT32);
  PrimitiveType type2("foo", Repetition::OPTIONAL, PhysicalType::INT32);
  PrimitiveType type3("foo", Repetition::REPEATED, PhysicalType::INT32);

  ASSERT_TRUE(type1.is_required());

  ASSERT_TRUE(type2.is_optional());
  ASSERT_FALSE(type2.is_required());

  ASSERT_TRUE(type3.is_repeated());
  ASSERT_FALSE(type3.is_optional());
}

TEST(TestPrimitiveType, TestEquals) {
  PrimitiveType type1("foo", Repetition::REQUIRED, PhysicalType::INT32);
  PrimitiveType type2("foo", Repetition::REQUIRED, PhysicalType::INT64);
  PrimitiveType type3("bar", Repetition::REQUIRED, PhysicalType::INT32);
  PrimitiveType type4("foo", Repetition::OPTIONAL, PhysicalType::INT32);
  PrimitiveType type5("foo", Repetition::REQUIRED, PhysicalType::INT32);

  ASSERT_TRUE(type1.Equals(&type1));
  ASSERT_FALSE(type1.Equals(&type2));
  ASSERT_FALSE(type1.Equals(&type3));
  ASSERT_FALSE(type1.Equals(&type4));
  ASSERT_TRUE(type1.Equals(&type5));
}

} // namespace schema

} // namespace parquet_cpp
