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

#include "parquet/schema.h"

#include "parquet/parquet.h"

using namespace boost;
using namespace parquet;
using namespace std;

namespace parquet_cpp {

Schema::Element::Element(const SchemaElement& s, Element* parent) :
  parquet_schema_(s),
  parent_(parent),
  max_def_level_(parent == NULL ? 0 : parent->max_def_level() +
      (s.repetition_type != FieldRepetitionType::REQUIRED)),
  max_rep_level_(parent == NULL ? 0 : parent->max_rep_level() +
      (s.repetition_type == FieldRepetitionType::REPEATED)),
  index_in_parent_(-1),
  full_name_(ComputeFullName()) {
}

string Schema::Element::ToString(const string& prefix) const {
  stringstream ss;
  ss << prefix;
  if (num_children() == 0) {
    ss << index_in_parent_ << ": " << PrintRepetitionType(parquet_schema_) << " "
       << PrintType(parquet_schema_) << " " << name() << ";" << endl;
  } else {
    if (parent_ == NULL) {
      ss << "{" << endl;
    } else {
      ss << index_in_parent_ << ": "
         << PrintRepetitionType(parquet_schema_) << " struct " << name()
         << " {" << endl;
    }
    for (int i = 0; i < num_children(); ++i) {
      ss << children_[i]->ToString(prefix + "  ");
    }
    ss << prefix << "};" << endl;
  }
  return ss.str();
}

void Schema::Element::Compile() {
  if (parent_ != NULL) {
    index_in_parent_ = parent_->IndexOf(name());
  }
  for (int i = 0; i < children_.size(); ++i) {
    children_[i]->Compile();
  }

  const Schema::Element* current = this;
  while (!current->is_root()) {
    ordinal_path_.insert(ordinal_path_.begin(), current->index_in_parent());
    schema_path_.insert(schema_path_.begin(), current);
    current = current->parent();
  }
}

int Schema::Element::IndexOf(const string& name) const {
  for (int i = 0; i < children_.size(); ++i) {
    if (children_[i]->name() == name) return i;
  }
  throw ParquetException("Invalid child.");
}

string Schema::Element::ComputeFullName() const {
  stringstream ss;
  vector<string> paths;
  const Element* n = this;
  while (n->parent_ != NULL) {
    paths.push_back(n->parquet_schema().name);
    n = n->parent();
  }
  for (int i = paths.size() - 1; i >= 0; --i) {
    if (i == paths.size() - 1) {
      ss << paths[i];
    } else {
      ss << "." << paths[i];
    }
  }
  return ss.str();
}

void Schema::Parse(const vector<SchemaElement>& nodes, Element* parent, int* idx) {
  int cur_idx = *idx;
  for (int i = 0; i < nodes[cur_idx].num_children; ++i) {
    ++*idx;
    shared_ptr<Element> child(new Element(nodes[*idx], parent));
    parent->children_.push_back(child);
    Parse(nodes, child.get(), idx);
  }
  if (nodes[cur_idx].num_children == 0) {
    max_def_level_ = std::max(max_def_level_, parent->max_def_level());
    leaves_.push_back(parent);
  }
}

shared_ptr<Schema> Schema::FromParquet(const vector<SchemaElement>& nodes) {
  shared_ptr<Schema> schema(new Schema());
  if (nodes.size() == 0) {
    throw ParquetException("Invalid empty schema");
  }
  schema->root_.reset(new Element(nodes[0], NULL));
  int idx = 0;
  schema->Parse(nodes, schema->root_.get(), &idx);
  schema->root_->Compile();
  return schema;
}

}
