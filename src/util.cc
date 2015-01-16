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

string PrintRepetitionType(const SchemaElement& e) {
  switch (e.repetition_type) {
    case FieldRepetitionType::REQUIRED: return "required";
    case FieldRepetitionType::OPTIONAL: return "optional";
    case FieldRepetitionType::REPEATED: return "repeated";
    default: return "unknown ";
  }
}

string PrintType(const SchemaElement& e) {
  stringstream ss;
  switch (e.type) {
    case Type::BOOLEAN: ss << "bool"; break;
    case Type::INT32: ss << "int32"; break;
    case Type::INT64: ss << "int64"; break;
    case Type::INT96: ss << "int96"; break;
    case Type::FLOAT: ss << "float"; break;
    case Type::DOUBLE: ss << "double"; break;
    case Type::BYTE_ARRAY:
      ss << (e.__isset.converted_type && e.converted_type == ConvertedType::UTF8)
            ? "string" : "uint8[]";
      break;
    default: ss << "unknown";
  }
  return ss.str();
}



}
