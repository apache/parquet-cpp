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

#include <parquet/generic-record.h>
#include <iostream>
#include <stdio.h>

#include <assert.h>

using namespace boost;
using namespace parquet_cpp;

void TestStruct() {
  shared_ptr<GenericStruct> r = GenericStruct::Create();
  const GenericDatum* v;

  v = r->Get("Doesn't exist");
  assert(v == NULL);

  r->Put("f", Int32Datum::Create(3));
  v = r->Get("f");
  printf("%d\n", v->GetInt32());
}

int main(int argc, char** argv) {
  printf("Done.\n");
  try {
    TestStruct();
  } catch (const ParquetException& e) {
    printf("Failed: %s\n", e.what());
  }

  return 0;
}
