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

#include "parquet/generic-record.h"

using namespace boost;
using namespace parquet;
using namespace std;

namespace parquet_cpp {

string GenericList::ToString(const Schema::Element* schema,
      const string& prefix) const {
  stringstream ss;
  ss << prefix;
  if (schema == NULL || 1) {
    ss << "[";
    bool first = true;
    for (int i = 0; i < elements_.size(); ++i) {
      if (!first) ss << ", ";
      first = false;
      ss << elements_[i]->ToString(schema, "");
    }
    ss << "]";
  }
  return ss.str();
}

string GenericStruct::ToString(const Schema::Element* schema,
    const string& prefix) const {
  stringstream ss;
  if (schema == NULL) {
    ss << "{ ";
    bool first = true;
    for (int i = 0; i < fields_.size(); ++i) {
      if (!first) ss << ", ";
      first = false;
      ss << (fields_[i] == NULL ? "NULL" : fields_[i]->ToString(NULL, prefix));
    }
    ss << " }";
  } else {
    if (schema->is_root()) {
      ss << "{" << endl;
    } else {
      ss << prefix << schema->name() << " {" << endl;
    }
    for (int i = 0; i < schema->num_children(); ++i) {
      // TODO: not right. Handle projection.
      int idx = schema->child(i)->index_in_parent();
      if (idx >= fields_.size()) continue;
      if (fields_[idx] == NULL) {
        ss << prefix << "  " << schema->child(i)->name() << ": NULL" << endl;
      } else {
        ss << fields_[idx]->ToString(schema->child(i), prefix + "  ") << endl;
      }
    }
    ss << prefix << "};";
  }
  return ss.str();
}

RecordReader::RecordReader(
    const Schema* schema,
    const FileMetaData* metadata,
    int row_group_idx,
    const vector<const Schema::Element*>& projected_columns,
    const vector<const ColumnMetaData*>& col_metadata,
    const vector<InputStream*>& streams)
  : schema_(schema),
    metadata_(metadata),
    row_group_idx_(row_group_idx),
    rows_returned_(0) {
  if (projected_columns.size() != streams.size()) {
    throw ParquetException(
        "Invalid input. projected_columns.size() != streams.size()");
  }
  if (col_metadata.size() != streams.size()) {
    throw ParquetException("Invalid input. col_metadata.size() != streams.size()");
  }
  if (row_group_idx < 0 || row_group_idx >= metadata->row_groups.size()) {
    throw ParquetException("Invalid row group index.");
  }

  readers_.resize(projected_columns.size());
  for (int i = 0; i < projected_columns.size(); ++i) {
    readers_[i].reader =
        new ColumnReader(col_metadata[i], projected_columns[i], streams[i]);
  }
  // TODO: verify schema, handle schema resolution.

  // Initialize all columns with the first value.
  for (int i = 0; i < readers_.size(); ++i) {
    readers_[i].ReadNext();
  }
}

RecordReader::~RecordReader() {
  for (int i = 0; i < readers_.size(); ++i) {
    delete readers_[i].reader;
  }
}

void RecordReader::ColumnState::ReadNext() {
  if (UNLIKELY(!reader->HasNext())) {
    // When we are done with the column, set rep_level to 0. This indicates a new
    // record/end of previous record.
    rep_level = 0;
    return;
  }

  datum.reset();
  bool is_null;
  switch (reader->type()) {
    case Type::BOOLEAN: {
      bool v = reader->GetBool(&is_null, &def_level, &rep_level);
      if (!is_null) datum = BoolDatum::Create(v);
      break;
    }
    case Type::INT32: {
      int32_t v = reader->GetInt32(&is_null, &def_level, &rep_level);
      if (!is_null) datum = Int32Datum::Create(v);
      break;
    }
    case Type::INT64: {
      int64_t v = reader->GetInt64(&is_null, &def_level, &rep_level);
      if (!is_null) datum = Int64Datum::Create(v);
      break;
    }
    case Type::FLOAT: {
      float v = reader->GetFloat(&is_null, &def_level, &rep_level);
      if (!is_null) datum = FloatDatum::Create(v);
      break;
    }
    case Type::DOUBLE: {
      double v = reader->GetDouble(&is_null, &def_level, &rep_level);
      if (!is_null) datum = DoubleDatum::Create(v);
      break;
    }
    case Type::BYTE_ARRAY: {
      ByteArray v = reader->GetByteArray(&is_null, &def_level, &rep_level);
      if (!is_null) datum = ByteArrayDatum::Create(v);
      break;
    }
    default:
      PARQUET_NOT_YET_IMPLEMENTED("Unsupported type");
  }
}

vector<shared_ptr<GenericStruct> > RecordReader::GetNext() {
  vector<shared_ptr<GenericStruct> > results;
  while (rows_left() > 0) {
    shared_ptr<GenericStruct> record = GenericStruct::Create();

    // Loop through each projected column and assemble the record.
    // TODO: use def/rep level
    for (int c = 0; c < readers_.size(); ++c) {
      Schema::Element* schema = schema_->leaves()[c];
      const vector<int>& path = schema->ordinal_path();

      // Put the current value in readers_[c] into the record. Keep reading
      // from readers_[c] until we hit rep_level = 0.
      do {
        // This is the core of the reconstruction algorithm. readers_[c] contains
        // the triple (rep, def, value) of the current value. From the def levels,
        // we construct the path of objects up to value. For example, with a schema
        // struct x {
        //  struct y {
        //   int a;
        //   list<int> b;
        //   }
        // }
        //
        // As a reminder, readers_ are *only* for leaf columns. We need to use
        // the path to the column and the rep/def levels to reconstruct the internal
        // structure.
        //
        // When we read the first 'a' (def=2, rep=1), we will create the path to
        // 'a' (the 'x' and 'y' structs) and then insert 'a' into 'y'. The depth
        // of the path we need to create is the definition level. The objects are
        // created only if they are not there so when we get to col 'b', the 'x'
        // and 'y' structs for this record could have already been created.
        //
        // TODO: this is not right for repetition levels. How do they work?
        // Right now, this code only cares if rep level == 0 vs != 0.
        //
        GenericList* l = NULL;
        GenericStruct* s = record.get();
        for (int i = 0; i < path.size() - 1; ++i) {
          if (readers_[c].def_level < i) {
            // In this case, the NULL happened before the leaf. In the above
            // example, this would mean the entire 'x' struct is NULL. In this
            // case we don't want to construct 'y' at all (and not insert a datum
            // anywhere).
            // TODO: generate a schema to check this? should this be <= i?
            goto next_value;
          }

          if (schema->schema_path()[i]->is_repeated()) {
            if (s->Get(path[i]) == NULL) {
              s->Put(path[i], GenericList::Create());
            }
            l = (GenericList*)s->Get(path[i]);
          } else {
            if (s->Get(path[i]) == NULL) {
              s->Put(path[i], GenericStruct::Create());
            }
            s = (GenericStruct*) s->Get(path[i]);
          }
        }
        if (l == NULL) {
          s->Put(path.back(), readers_[c].datum);
        } else {
          l->Put(readers_[c].datum);
        }

        // Read the next value to check if we are done with this column. We only
        // know *after* reading the next value (for rep_level == 0).
next_value:
        readers_[c].ReadNext();
      } while (readers_[c].rep_level != 0);
    }

    results.push_back(record);
    ++rows_returned_;
  }

  return results;
}

}
