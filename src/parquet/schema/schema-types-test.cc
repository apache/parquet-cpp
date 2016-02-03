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

TEST(TestPrimitiveNode, TestIsRepetition) {
  PrimitiveNode type1("foo", Repetition::REQUIRED, Type::INT32);
  PrimitiveNode type2("foo", Repetition::OPTIONAL, Type::INT32);
  PrimitiveNode type3("foo", Repetition::REPEATED, Type::INT32);

  ASSERT_TRUE(type1.is_required());

  ASSERT_TRUE(type2.is_optional());
  ASSERT_FALSE(type2.is_required());

  ASSERT_TRUE(type3.is_repeated());
  ASSERT_FALSE(type3.is_optional());
}

TEST(TestPrimitiveNode, TestEquals) {
  PrimitiveNode type1("foo", Repetition::REQUIRED, Type::INT32);
  PrimitiveNode type2("foo", Repetition::REQUIRED, Type::INT64);
  PrimitiveNode type3("bar", Repetition::REQUIRED, Type::INT32);
  PrimitiveNode type4("foo", Repetition::OPTIONAL, Type::INT32);
  PrimitiveNode type5("foo", Repetition::REQUIRED, Type::INT32);

  ASSERT_TRUE(type1.Equals(&type1));
  ASSERT_FALSE(type1.Equals(&type2));
  ASSERT_FALSE(type1.Equals(&type3));
  ASSERT_FALSE(type1.Equals(&type4));
  ASSERT_TRUE(type1.Equals(&type5));
}

class TestGroupNode : public ::testing::Test {
 public:
  NodeVector Fields1() {
    NodeVector fields;

    fields.push_back(Int32("one", Repetition::REQUIRED));
    fields.push_back(Int64("two"));
    fields.push_back(Double("three"));

    return fields;
  }
};

TEST_F(TestGroupNode, TestEquals) {
  NodeVector f1 = Fields1();
  NodeVector f2 = Fields1();

  GroupNode group1("group", Repetition::REPEATED, f1);
  GroupNode group2("group", Repetition::REPEATED, f2);

  ASSERT_TRUE(group1.Equals(&group2));
}

} // namespace schema

} // namespace parquet_cpp
