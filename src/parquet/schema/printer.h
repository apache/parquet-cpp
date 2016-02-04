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

// A simple Schema printer using the visitor pattern

#ifndef PARQUET_SCHEMA_PRINTER_H
#define PARQUET_SCHEMA_PRINTER_H

#include "parquet/schema/types.h"

#include <ostream>

namespace parquet_cpp {

namespace schema {

class SchemaPrinter : public Node::Visitor {
 public:
  explicit SchemaPrinter(std::ostream& stream, size_t indent_width = 2) :
      stream_(stream),
      indent_(0),
      indent_width_(2) {}

  virtual void Visit(const Node* node);

 private:
  void Visit(const PrimitiveNode* node);
  void Visit(const GroupNode* node);

  void Indent();

  std::ostream& stream_;

  size_t indent_;
  size_t indent_width_;
};

} // namespace schema

} // namespace parquet_cpp

#endif // PARQUET_SCHEMA_PRINTER_H
