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

#ifndef PARQUET_SCHEMA_H
#define PARQUET_SCHEMA_H

#include <exception>
#include <sstream>
#include <boost/cstdint.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/unordered_map.hpp>
#include "gen-cpp/parquet_constants.h"
#include "gen-cpp/parquet_types.h"

namespace parquet_cpp {

// Tree representation of a parquet schema (which is encoded as a flattened tree
// in thrift).
class Schema {
 public:
  class Element {
   public:
    const parquet::SchemaElement& parquet_schema() const { return parquet_schema_; }
    const Element* parent() const { return parent_; }
    int max_def_level() const { return max_def_level_; }
    int max_rep_level() const { return max_rep_level_; }
    bool is_root() const { return parent_ == NULL; }

    const std::string& name() const { return parquet_schema_.name; }
    const std::string& full_name() const { return full_name_; }
    int num_children() const { return children_.size(); }
    const Schema::Element* child(int idx) const { return children_[idx].get(); }

    bool is_repeated() const {
      return parquet_schema_.repetition_type == parquet::FieldRepetitionType::REPEATED;
    }

    // Returns the ordinal of child with 'child_name'.
    // e.g. for a schema like
    // struct S {
    //   int a;
    //   int b;
    // };
    // If Element is for 'S', IndexOf('a') returns 0 and IndexOf('b') returns 1.
    int IndexOf(const std::string& child_name) const;

    // Returns the index this elemnt is in the parent.
    // Equivalent to parent()->IndexOf(this->name())
    // Computed in Compile().
    int index_in_parent() const { return index_in_parent_; }

    // Returns the ordinal (aka index_in_parent) path up to the root, starting
    // with the root.
    // e.g. for a schema
    // struct root {
    //   struct a {}
    //   struct b {
    //     struct c {
    //       int x;
    //       int y; <-- This element maps to root.c.y
    //     }
    //   }
    // this would 1 (b's index in root), 0 (c's index in b)
    // Computed in Compile().
    const std::vector<int> ordinal_path() const { return ordinal_path_; }
    const std::vector<const Element*> schema_path() const { return schema_path_; }

    std::string ToString(const std::string& prefix = "") const;

   private:
    friend class Schema;

    Element(const parquet::SchemaElement& e, Element* parent);
    std::string ComputeFullName() const;

    // Must be called after the schema is fully constructed.
    void Compile();

    const parquet::SchemaElement parquet_schema_;
    const Element* parent_;
    std::vector<boost::shared_ptr<Element> > children_;
    const int max_def_level_;
    const int max_rep_level_;

    int index_in_parent_;
    std::vector<int> ordinal_path_;
    std::vector<const Element*> schema_path_;
    std::string full_name_;
  };

  static boost::shared_ptr<Schema> FromParquet(
      const std::vector<parquet::SchemaElement>& root);

  const Element* root() { return root_.get(); }
  const std::vector<Element*> leaves() const { return leaves_; }
  int max_def_level() const { return max_def_level_; }

  std::string ToString() const { return root_->ToString(); }

 private:
  Schema() : max_def_level_(0) {}
  Schema(const Schema&);
  Schema& operator=(const Schema&);

  boost::scoped_ptr<Element> root_;
  std::vector<Element*> leaves_;
  int max_def_level_;

  void Parse(const std::vector<parquet::SchemaElement>& nodes,
      Element* parent, int* idx);
};

}

#endif
