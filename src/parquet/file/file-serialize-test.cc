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

#include "parquet/column_reader.h"
#include "parquet/column_writer.h"
#include "parquet/file/reader.h"
#include "parquet/file/writer.h"
#include "parquet/test-specialization.h"
#include "parquet/test-util.h"
#include "parquet/types.h"
#include "parquet/util/memory.h"

namespace parquet {

using schema::GroupNode;
using schema::NodePtr;
using schema::PrimitiveNode;

namespace test {

template <typename TestType>
class TestSerialize : public PrimitiveTypedTest<TestType> {
 public:
  typedef typename TestType::c_type T;

  void SetUp() {
    num_columns_ = 4;
    num_rowgroups_ = 2;
    rows_per_rowgroup_ = 50;
    this->SetUpSchema(Repetition::OPTIONAL, num_columns_);
  }

 protected:
  int num_columns_;
  int num_rowgroups_;
  int rows_per_rowgroup_;

  void FileSerializeTest_(Compression::type codec_type, bool row_count_known) {
    std::shared_ptr<InMemoryOutputStream> sink(new InMemoryOutputStream());
    auto gnode = std::static_pointer_cast<GroupNode>(this->node_);

    WriterProperties::Builder prop_builder;

    for (int i = 0; i < num_columns_; ++i) {
      prop_builder.compression(this->schema_.Column(i)->name(), codec_type);
    }
    std::shared_ptr<WriterProperties> writer_properties = prop_builder.build();

    auto file_writer = ParquetFileWriter::Open(sink, gnode, writer_properties);
    for (int rg = 0; rg < num_rowgroups_; ++rg) {
      RowGroupWriter* row_group_writer;
      if (row_count_known) {
        row_group_writer = file_writer->AppendRowGroup(rows_per_rowgroup_);
      } else {
        row_group_writer = file_writer->AppendRowGroup();
      }
      this->GenerateData(rows_per_rowgroup_);
      for (int col = 0; col < num_columns_; ++col) {
        auto column_writer =
          static_cast<TypedColumnWriter<TestType>*>(row_group_writer->NextColumn());
        column_writer->WriteBatch(rows_per_rowgroup_, this->def_levels_.data(), nullptr,
                                  this->values_ptr_);
        column_writer->Close();
      }

      row_group_writer->Close();
    }
    file_writer->Close();

    auto buffer = sink->GetBuffer();
    int num_rows_ = num_rowgroups_ * rows_per_rowgroup_;


    auto source = std::make_shared<::arrow::io::BufferReader>(buffer);
    auto file_reader = ParquetFileReader::Open(source);
    ASSERT_EQ(num_columns_, file_reader->metadata()->num_columns());
    ASSERT_EQ(num_rowgroups_, file_reader->metadata()->num_row_groups());
    ASSERT_EQ(num_rows_, file_reader->metadata()->num_rows());

    for (int rg = 0; rg < num_rowgroups_; ++rg) {
      auto rg_reader = file_reader->RowGroup(rg);
      ASSERT_EQ(num_columns_, rg_reader->metadata()->num_columns());
      ASSERT_EQ(rows_per_rowgroup_, rg_reader->metadata()->num_rows());
      // Check that the specified compression was actually used.
      ASSERT_EQ(codec_type, rg_reader->metadata()->ColumnChunk(0)->compression());

      int64_t values_read;

      for (int i = 0; i < num_columns_; ++i) {
        std::vector<int16_t> def_levels_out(rows_per_rowgroup_);
        std::vector<int16_t> rep_levels_out(rows_per_rowgroup_);
        auto col_reader =
          std::static_pointer_cast<TypedColumnReader<TestType>>(rg_reader->Column(i));
        this->SetupValuesOut(rows_per_rowgroup_);
        col_reader->ReadBatch(rows_per_rowgroup_, def_levels_out.data(),
                              rep_levels_out.data(),
                              this->values_out_ptr_,
                              &values_read);
        this->SyncValuesOut();
        ASSERT_EQ(rows_per_rowgroup_, values_read);
        ASSERT_EQ(this->values_, this->values_out_);
        ASSERT_EQ(this->def_levels_, def_levels_out);
      }
    }
  }
  void FileSerializeTest(Compression::type codec_type) {
    FileSerializeTest_(codec_type, true);
  }

  void FileSerializeRowCountUnknownTest(Compression::type codec_type) {
    FileSerializeTest_(codec_type, false);
  }
};

typedef ::testing::Types<Int32Type, Int64Type, Int96Type, FloatType, DoubleType,
                         BooleanType, ByteArrayType, FLBAType>
    TestTypes;

TYPED_TEST_CASE(TestSerialize, TestTypes);

TYPED_TEST(TestSerialize, WriteRowGroupUnknownSize) {
  this->FileSerializeRowCountUnknownTest(Compression::UNCOMPRESSED);
}

TYPED_TEST(TestSerialize, SmallFileUncompressed) {
  this->FileSerializeTest(Compression::UNCOMPRESSED);
}

TYPED_TEST(TestSerialize, SmallFileSnappy) {
  this->FileSerializeTest(Compression::SNAPPY);
}

TYPED_TEST(TestSerialize, SmallFileBrotli) {
  this->FileSerializeTest(Compression::BROTLI);
}

TYPED_TEST(TestSerialize, SmallFileGzip) { this->FileSerializeTest(Compression::GZIP); }

}  // namespace test

}  // namespace parquet
