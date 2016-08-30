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

#include <gtest/gtest.h>
#include "parquet/file/metadata.h"
#include "parquet/schema/descriptor.h"
#include "parquet/schema/types.h"
#include "parquet/types.h"

namespace parquet {

namespace metadata {

TEST(Metadata, TestBuildAccess) {
  parquet::schema::NodeVector fields;
  parquet::schema::NodePtr root;
  parquet::SchemaDescriptor schema;

  std::shared_ptr<WriterProperties> props = WriterProperties::Builder().build();

  fields.push_back(parquet::schema::Int32("int_col", Repetition::REQUIRED));
  fields.push_back(parquet::schema::Float("float_col", Repetition::REQUIRED));
  root = parquet::schema::GroupNode::Make("schema", Repetition::REPEATED, fields);
  schema.Init(root);

  int64_t nrows = 1000;
  ColumnStatistics stats_int;
  stats_int.null_count = 0;
  stats_int.distinct_count = nrows;
  std::string int_min = std::string("100");
  std::string int_max = std::string("200");
  stats_int.min = &int_min;
  stats_int.max = &int_max;
  ColumnStatistics stats_float;
  stats_float.null_count = 0;
  stats_float.distinct_count = nrows;
  std::string float_min = std::string("100.100");
  std::string float_max = std::string("200.200");
  stats_float.min = &float_min;
  stats_float.max = &float_max;

  auto f_builder = FileMetaDataBuilder::Make(&schema, props);
  auto rg1_builder = f_builder->AppendRowGroupMetaData();
  auto rg2_builder = f_builder->AppendRowGroupMetaData();

  // Write the metadata
  // rowgroup1 metadata
  auto col1_builder = rg1_builder->NextColumnMetaData();
  auto col2_builder = rg1_builder->NextColumnMetaData();
  // column metadata
  col1_builder->SetStatistics(stats_int);
  col2_builder->SetStatistics(stats_float);
  col1_builder->Finish(nrows / 2, 0, 0, 4, 512, 600, false);
  col2_builder->Finish(nrows / 2, 0, 0, 4, 512, 600, false);
  rg1_builder->Finish(nrows / 2);

  // rowgroup2 metadata
  col1_builder = rg2_builder->NextColumnMetaData();
  col2_builder = rg2_builder->NextColumnMetaData();
  // column metadata
  col1_builder->SetStatistics(stats_int);
  col2_builder->SetStatistics(stats_float);
  col1_builder->Finish(nrows / 2, 0, 0, 6, 512, 600, false);
  col2_builder->Finish(nrows / 2, 0, 0, 516, 512, 600, false);
  rg2_builder->Finish(nrows / 2);

  // Read the metadata
  auto f_accessor = f_builder->Finish();

  // file metadata
  ASSERT_EQ(nrows, f_accessor->num_rows());
  ASSERT_EQ(2, f_accessor->num_row_groups());
  ASSERT_EQ(DEFAULT_WRITER_VERSION, f_accessor->version());
  ASSERT_EQ(DEFAULT_CREATED_BY, f_accessor->created_by());
  ASSERT_EQ(3, f_accessor->num_schema_elements());

  // row group1 metadata
  auto rg1_accessor = f_accessor->RowGroup(0);
  ASSERT_EQ(2, rg1_accessor->num_columns());
  ASSERT_EQ(nrows / 2, rg1_accessor->num_rows());
  ASSERT_EQ(1024, rg1_accessor->total_byte_size());

  auto rg1_column1 = rg1_accessor->Column(0);
  auto rg1_column2 = rg1_accessor->Column(1);
  ASSERT_EQ(true, rg1_column1->is_stats_set());
  ASSERT_EQ(true, rg1_column2->is_stats_set());
  ASSERT_EQ("100.100", *rg1_column2->Statistics().min);
  ASSERT_EQ("200.200", *rg1_column2->Statistics().max);
  ASSERT_EQ("100", *rg1_column1->Statistics().min);
  ASSERT_EQ("200", *rg1_column1->Statistics().max);
  ASSERT_EQ(0, rg1_column1->Statistics().null_count);
  ASSERT_EQ(0, rg1_column2->Statistics().null_count);
  ASSERT_EQ(nrows, rg1_column1->Statistics().distinct_count);
  ASSERT_EQ(nrows, rg1_column2->Statistics().distinct_count);
  ASSERT_EQ(DEFAULT_COMPRESSION_TYPE, rg1_column1->compression());
  ASSERT_EQ(DEFAULT_COMPRESSION_TYPE, rg1_column2->compression());
  ASSERT_EQ(nrows / 2, rg1_column1->num_values());
  ASSERT_EQ(nrows / 2, rg1_column2->num_values());
  ASSERT_EQ(2, rg1_column1->Encodings().size());
  ASSERT_EQ(2, rg1_column2->Encodings().size());
  ASSERT_EQ(512, rg1_column1->total_compressed_size());
  ASSERT_EQ(512, rg1_column2->total_compressed_size());
  ASSERT_EQ(600, rg1_column1->total_uncompressed_size());
  ASSERT_EQ(600, rg1_column2->total_uncompressed_size());

  auto rg2_accessor = f_accessor->RowGroup(1);
  ASSERT_EQ(2, rg2_accessor->num_columns());
  ASSERT_EQ(nrows / 2, rg2_accessor->num_rows());
  ASSERT_EQ(1024, rg2_accessor->total_byte_size());

  auto rg2_column1 = rg2_accessor->Column(0);
  auto rg2_column2 = rg2_accessor->Column(1);
  ASSERT_EQ(true, rg2_column1->is_stats_set());
  ASSERT_EQ(true, rg2_column2->is_stats_set());
  ASSERT_EQ("100.100", *rg2_column2->Statistics().min);
  ASSERT_EQ("200.200", *rg2_column2->Statistics().max);
  ASSERT_EQ("100", *rg2_column1->Statistics().min);
  ASSERT_EQ("200", *rg2_column1->Statistics().max);
  ASSERT_EQ(0, rg2_column1->Statistics().null_count);
  ASSERT_EQ(0, rg2_column2->Statistics().null_count);
  ASSERT_EQ(nrows, rg2_column1->Statistics().distinct_count);
  ASSERT_EQ(nrows, rg2_column2->Statistics().distinct_count);
  ASSERT_EQ(nrows / 2, rg2_column1->num_values());
  ASSERT_EQ(nrows / 2, rg2_column2->num_values());
  ASSERT_EQ(DEFAULT_COMPRESSION_TYPE, rg2_column1->compression());
  ASSERT_EQ(DEFAULT_COMPRESSION_TYPE, rg2_column2->compression());
  ASSERT_EQ(2, rg2_column1->Encodings().size());
  ASSERT_EQ(2, rg2_column2->Encodings().size());
  ASSERT_EQ(512, rg2_column1->total_compressed_size());
  ASSERT_EQ(512, rg2_column2->total_compressed_size());
  ASSERT_EQ(600, rg2_column1->total_uncompressed_size());
  ASSERT_EQ(600, rg2_column2->total_uncompressed_size());
}
}  // namespace metadata
}  // namespace parquet
