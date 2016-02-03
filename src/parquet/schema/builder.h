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

// TODO: Think about a fluent schema builder interface in C++

// WM: NOT YET COMPLETED OR TESTED

#ifndef PARQUET_SCHEMA_BUILDER_H
#define PARQUET_SCHEMA_BUILDER_H

#include <memory>
#include <string>
#include <vector>

#include "parquet/schema/schema.h"

namespace parquet_cpp {

namespace schema {

template <typename T>
class Visitor {
 public:
  virtual void visit(T* what) = 0;
};

class NodeBuilder {
 private:
  void Visit(Visitor* visitor) {
    visitor->visit(this);
  }
  virtual void AppendTo(vector<parquet::SchemaElement>* out) = 0;

  std::vector<Schema::Node> children_;
};

class ListBuilder;

class GroupBuilder : public NodeBuilder {
 public:
  GroupBuilder* NewGroup();
  ListBuilder* NewList(const std::string& name,
      bool optional = true, bool optional_values = true);

 private:
  std::vector<Schema::Node> children_;
};


// A NodeBuilder to simplify the mechanics of different array encodings. See
// ListEncoding declaration for description of the different types of
// encodings.
class ListBuilder  : public NodeBuilder {
 public:
  explicit ListBuilder(const std::string& name) :
      NodeBuilder(name),
      optional_(true),
      optional_values_(true) {}

  // Fluid interface
  //
  // builder->NewList("foo")
  //        ->Optional(false)
  //        ->OptionalValues(true)->Finish();
  ListBuilder* Encoding(ListEncoding encoding) {
    encoding_ = encoding;
    return this;
  }

  ListBuilder* Optional(bool optional = true) {
    optional_ = optional;
    return this;
  }

  ListBuilder* OptionalValues(bool optional = true) {
    optional_values_ = optional;
    return this;
  }

 private:
  ListEncoding encoding_;
  bool optional_;
  bool optional_values_;

  Schema::Node array_item_;
};

// A GroupBuilder
class SchemaBuilder : GroupBuilder {
 public:
  SchemaBuilder();
  ~SchemaBuilder() {}

 private:
  GroupBuilder root_;
}

} // namespace schema

} // namespace parquet_cpp

#endif // PARQUET_SCHEMA_BUILDER_H
