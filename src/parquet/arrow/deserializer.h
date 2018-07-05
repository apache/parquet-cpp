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
#ifndef PARQUET_ARROW_DESERIALIZER_H
#define PARQUET_ARROW_DESERIALIZER_H

#include <memory>
#include <vector>

#include "arrow/status.h"
#include "parquet/util/visibility.h"

namespace arrow {

class Array;
class MemoryPool;
class Schema;
class Table;

}  // namespace arrow

namespace parquet {

class ParquetFileReader;
class RowGroupReader;

namespace arrow {

class DeserializerNode;

//
// This header file contains classes that can deserialize from Parquet
// files into Arrow arrays and tables. It supports nested structures
// such as this (taken from the Dremel paper):
//
// message root {
//   required int32 DocId;
//   optional group Links {
//     required group Forward_Outer (LIST) {
//       repeated group Forward_Inner {
//         required int32 Forward;
//       }
//     }
//     required group Backward_Outer (LIST) {
//       repeated group Backward_Inner {
//         required int32 Backward;
//       }
//     }
//   }
//   required group Name_Outer (LIST) {
//     repeated group Name_Inner {
//       required group Name_Element {
//         required group Language_Outer (LIST) {
//           repeated group Language_Inner {
//             required group Language_Element {
//               required binary Code (UTF8);
//               optional binary Country (UTF8);
//             }
//           }
//         }
//         optional binary Url (UTF8);
//       }
//     }
//   }
// }
//
// There are two types of serializers, one for arrays and one for tables.
// To obtain a deserializer, first construct a DeserializerBuilder and
// then call one of its Build* methods.
//
// The contract of the ArrayDeserializer::DeserializeArray and
// TableDeserializer::DeserializeTable is that they should be called once
// and only once.

//
// Interface for deserializing a Parquet node into an Arrow Array. A node can
// contain multiple columns in the case of a Parquet group node.
//
class PARQUET_EXPORT ArrayDeserializer {
 public:
  virtual ~ArrayDeserializer();

  // \brief Deserializes the node for which this deserializer was constructed
  // \param[out] array to deserialize into
  // \return error status if there is a problem reading the data from the
  //         Parquet file or in constructing the array
  virtual ::arrow::Status DeserializeArray(std::shared_ptr<::arrow::Array>& array) = 0;

 protected:
  ArrayDeserializer(std::unique_ptr<DeserializerNode>&& node);

  std::unique_ptr<DeserializerNode> node_;
};

//
// Interface for deserializing a Parquet node into Arrow Arrays in batches. A node
// can contain multiple columns in the case of a Parquet group node.
//
class PARQUET_EXPORT ArrayBatchedDeserializer {
 public:
  virtual ~ArrayBatchedDeserializer();

  // \brief Deserializes a batch of Parquet rows
  // \param[in] number of records to deserialize in this batch
  // \param[out] array to deserialize into
  // \return error status if there is a problem reading the data from the
  //         Parquet file or in constructing the array. The number of rows
  //         will be the minimum of num_records and the remaining records
  //         available for deserialization
  virtual ::arrow::Status DeserializeBatch(int64_t num_records,
                                           std::shared_ptr<::arrow::Array>& array) = 0;
};

//
// Interface for deserializing Parquet nodes in an Arrow table. The schema
// of the Table will depend on which method was invoked in DeserializerBuilder.
// For example, if column indices are specified in the opposite order of how
// they appear in the Parquet file, the resulting Arrow schema will have its
// fields in the order in which the indices are specified.
//
class PARQUET_EXPORT TableDeserializer {
 public:
  virtual ~TableDeserializer();

  // \brief Deserializes Parquet nodes into an Arrow Table
  // \param[out] table to deserialize into
  // \return error status if there is a problem reading the data from the
  //         Parquet file or in constructing the Table.
  virtual ::arrow::Status DeserializeTable(std::shared_ptr<::arrow::Table>& result) = 0;

 protected:
  TableDeserializer(std::shared_ptr<::arrow::Schema> const& schema);

  // Schema, which was most likely obtained from a call to
  // FromParquetSchema
  std::shared_ptr<::arrow::Schema> schema_;
};

//
// A builder of deserializers. This is the main entry point to obtaining
// Parquet deserializers.
//
class PARQUET_EXPORT DeserializerBuilder {
 public:
  //
  // \brief Create a new deserializer builder
  // \param[in] the reader that is the source of the data to deserialize
  // \param[in] the memory pool to use for builders and buffers that
  //            are created for reading primitive values
  // \param[in] the deserializers use buffers for reading Parquet definition
  //            and repetition levels, as well as values. This denotes how
  //            many values will be buffered when reading from primitive
  //            columns.
  DeserializerBuilder(std::shared_ptr<::parquet::ParquetFileReader> const& file_reader,
                      ::arrow::MemoryPool* pool,
                      // TODO: make this configurable from a reader
                      // properties object
                      int64_t buffer_size = 1024);

  // Defined so that we can hide the impl and use a unique_ptr
  ~DeserializerBuilder();

  //
  // \brief builds an array deserializer that will deserialize a particular
  //        node in the Parquet schema.
  // \param[in] the index which refers to one of the top
  //        	level nodes in the Parquet schema. For exmaple, in the example schema
  //        	defined at the top of this file, index 0 would be the DocId, index 1
  //        	would be the Links, and index 2 would be Name_Outer.
  // \param[out] the deserializer
  // \return error status if there is a problem building the appropriate
  //         deserializers for various column types or if the schema_index
  //         is out of bounds
  //
  ::arrow::Status BuildSchemaNodeDeserializer(int schema_index,
                                              std::unique_ptr<ArrayDeserializer>& result);

  //
  // \brief builds an array deserializer that will deserialize a particular
  //        node in the Parquet schema.
  // \param[in] the index which refers to a primitive node in the Parquet
  //            schema. In the example schema at the top of this file,
  //            DocId would have index 0, Forward index 1, Backward index 2,
  //            Code index 3, and so forth.
  // \param[out] the deserializer
  // \return error status if there is a problem building the appropriate
  //         deserializers for various column types or if the column_index
  //         is out of bounds.
  //
  ::arrow::Status BuildColumnDeserializer(int column_index,
                                          std::unique_ptr<ArrayDeserializer>& result);

  //
  // \brief builds an array deserializer that cant deserialize a particular
  //        node in the Parquet schema in batches.
  // \param[in] the index which refers to a primitive node in the Parquet
  //            schema. In the example schema at the top of this file,
  //            DocId would have index 0, Forward index 1, Backward index 2,
  //            Code index 3, and so forth.
  // \param[out] the deserializer
  // \return error status if there is a problem building the appropriate
  //         deserializers for various column types or if the column_index
  //         is out of bounds.
  //
  ::arrow::Status BuildArrayBatchedDeserializer(
      int column_index, std::unique_ptr<ArrayBatchedDeserializer>& result);

  //
  // \brief builds an array deserializer that will deserialize a particular
  //        node in the Parquet schema for a particular row group. This
  //        is known as a ColumnChunk in the Parquet metadata.
  // \param[in] the index which refers to a primitive node in the Parquet
  //            schema. In the example schema at the top of this file,
  //            DocId would have index 0, Forward index 1, Backward index 2,
  //            Code index 3, and so forth.
  // \param[in] the row group to deserialize
  // \param[out] the deserializer
  // \return error status if there is a problem building the appropriate
  //         deserializers for various column types or if the column_index
  //         or row_group_index is out of bounds.
  //
  ::arrow::Status BuildColumnChunkDeserializer(
      int column_index, int row_group_index, std::unique_ptr<ArrayDeserializer>& result);

  //
  // \brief builds a table deserializer that will deserialize all of the
  //        nodes in the Parquet schema for a particular row group.
  // \param[in] the row group to deserialize
  // \param[in] the column indices to use for filtering the Parquet schema.
  //            For example, if column_index 2 is specified for the example schema
  //            at the top of this file, it will include Code and all of its
  //            ancestors in the resulting table.
  // \param[in] the number of threads to use when deserializing. This
  //            will cause the top level columns to be read in parallel
  // \param[out] the deserializer
  // \return error status if there is a problem building the appropriate
  //         deserializers for various column types or if the row_group_index
  //         is out of bounds.
  //
  ::arrow::Status BuildRowGroupDeserializer(int row_group_index,
                                            const std::vector<int>& indices,
                                            int num_threads,
                                            std::unique_ptr<TableDeserializer>& result);

  // \brief build a table deserializer that will only deserialize nodes that
  //        contain the specified column indices.
  // \param[in] the column indices to use for filtering the Parquet schema.
  //            For example, if column_index 2 is specified for the example schema
  //            at the top of this file, it will include Code and all of its
  //            ancestors in the resulting table.
  // \param[in] the number of threads to use when deserializing. This
  //            will cause the top level columns to be read in parallel
  // \param[out] the deserializer
  // \return error status if there is a problem building the appropriate
  //         deserializers for various column types or if all of the
  //         column indices are out of bounds.
  ::arrow::Status BuildFileDeserializer(const std::vector<int>& column_indices,
                                        int num_threads,
                                        std::unique_ptr<TableDeserializer>& result);

 private:
  // Hiding implementation due to a large number of member template methods
  // with SFINAE.
  class PARQUET_NO_EXPORT Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace arrow
}  // namespace parquet

#endif  // PARQUET_ARROW_DESERIALIZER_H
