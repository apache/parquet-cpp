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

string GenericStruct::ToString() const {
  stringstream ss;
  ss << "{ ";
  bool first = true;
  for (map<string, shared_ptr<GenericDatum> >::const_iterator it = fields_.begin();
      it != fields_.end(); ++it) {
    if (!first) ss << ", ";
    first = false;
    ss << it->first << "=" << it->second->ToString();
  }
  ss << " }";
  return ss.str();
}

RecordReader::RecordReader(const FileMetaData* metadata,
    int row_group_idx,
    const vector<const SchemaElement*>& projected_columns,
    const vector<const ColumnMetaData*>& col_metadata,
    const vector<InputStream*>& streams)
  : metadata_(metadata),
    row_group_idx_(row_group_idx),
    rows_returned_(0) {
  if (projected_columns.size() != streams.size()) {
    throw ParquetException("Invalid input. projected_columns.size() != streams.size()");
  }
  if (col_metadata.size() != streams.size()) {
    throw ParquetException("Invalid input. col_metadata.size() != streams.size()");
  }
  if (row_group_idx < 0 || row_group_idx >= metadata->row_groups.size()) {
    throw ParquetException("Invalid row group index.");
  }

  for (int i = 0; i < projected_columns.size(); ++i) {
    readers_.push_back(new ColumnReader(col_metadata[i], projected_columns[i], streams[i]));
  }

  // TODO: verify schema, handle schema resolution.
}

RecordReader::~RecordReader() {
  for (int i = 0; i < readers_.size(); ++i) {
    delete readers_[i];
  }
}

vector<shared_ptr<GenericStruct> > RecordReader::GetNext() {
  vector<shared_ptr<GenericStruct> > results;
  while (rows_left() > 0) {
    shared_ptr<GenericStruct> r = GenericStruct::Create();

    // Loop through each projected column and assemble the record.
    // TODO: use def/rep level
    for (int c = 0; c < readers_.size(); ++c) {
      int def_level, rep_level;
      switch (readers_[c]->type()) {
        case Type::BOOLEAN: {
          bool v = readers_[c]->GetBool(&def_level, &rep_level);
          r->Put(readers_[c]->name(), BoolDatum::Create(v));
          break;
        }
        case Type::INT32: {
          int32_t v = readers_[c]->GetInt32(&def_level, &rep_level);
          r->Put(readers_[c]->name(), Int32Datum::Create(v));
          break;
        }
        case Type::INT64: {
          int64_t v = readers_[c]->GetInt64(&def_level, &rep_level);
          r->Put(readers_[c]->name(), Int64Datum::Create(v));
          break;
        }
        case Type::FLOAT: {
          float v = readers_[c]->GetFloat(&def_level, &rep_level);
          r->Put(readers_[c]->name(), FloatDatum::Create(v));
          break;
        }
        case Type::DOUBLE: {
          double v = readers_[c]->GetDouble(&def_level, &rep_level);
          r->Put(readers_[c]->name(), DoubleDatum::Create(v));
          break;
        }
        case Type::BYTE_ARRAY: {
          ByteArray v = readers_[c]->GetByteArray(&def_level, &rep_level);
          r->Put(readers_[c]->name(), ByteArrayDatum::Create(v));
          break;
        }
        default:
          PARQUET_NOT_YET_IMPLEMENTED("Unsupported type");
      }
    }
    results.push_back(r);
    ++rows_returned_;
  }

  return results;
}

}
