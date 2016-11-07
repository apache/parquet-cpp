// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include <cassert>
#include <fstream>
#include <iostream>
#include <list>
#include <memory>

#include <parquet/api/writer.h>
#include <parquet/api/reader.h>

/*
 * This example describes writing and reading Parquet Files in C++ and serves as a reference to the API.
 * The file contains all the physical data types supported by Parquet.
**/

/* Parquet is a structured columnar file format
 * Parquet File = "Parquet data" + "Parquet Metadata"
 * "Parquet data" is simply a vector of RowGroups. Each RowGroup is a batch of rows in a columnar layout
 * "Parquet Metadata" contains the "file schema" and attributes of the RowGroups and their Columns
 * "file schema" is a tree where each node is either a primitive type (leaf nodes) or a complex (nested) type (internal nodes)
 * For specific details, please refer the format here:
 * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
**/

constexpr int NUM_ROW_GROUPS = 10;
constexpr int NUM_ROWS_PER_ROW_GROUP = 512;
constexpr int NUM_COLUMNS = 8;
constexpr int FIXED_LENGTH = 10;
const std::string PARQUET_FILENAME = "parquet_cpp_example.parquet";

class ParquetWriter {
 public:
  ParquetWriter() {
      ostream_.reset(new parquet::InMemoryOutputStream());
  }

  void SetupSchema() {
    parquet::schema::NodeVector fields;
    // Create a primitive node named 'boolean_field' with type:BOOLEAN, repetition:REQUIRED
    fields.push_back(parquet::schema::PrimitiveNode::Make("boolean_field", parquet::Repetition::REQUIRED, parquet::Type::BOOLEAN, parquet::LogicalType::NONE));
    // Create a primitive node named 'int32_field' with type:INT32, repetition:REQUIRED, logical type:TIME_MILLIS
    fields.push_back(parquet::schema::PrimitiveNode::Make("int32_field", parquet::Repetition::REQUIRED, parquet::Type::INT32, parquet::LogicalType::TIME_MILLIS));
    // Create a primitive node named 'int64_field' with type:INT64, repetition:REPEATED
    fields.push_back(parquet::schema::PrimitiveNode::Make("int64_field", parquet::Repetition::REPEATED, parquet::Type::INT64, parquet::LogicalType::NONE));
    // Create a primitive node named 'int96_field' with type:INT96, repetition:REQUIRED
    fields.push_back(parquet::schema::PrimitiveNode::Make("int96_field", parquet::Repetition::REQUIRED, parquet::Type::INT96, parquet::LogicalType::NONE));
    // Create a primitive node named 'float_field' with type:FLOAT, repetition:REQUIRED
    fields.push_back(parquet::schema::PrimitiveNode::Make("float_field", parquet::Repetition::REQUIRED, parquet::Type::FLOAT, parquet::LogicalType::NONE));
    // Create a primitive node named 'double_field' with type:DOUBLE, repetition:REQUIRED
    fields.push_back(parquet::schema::PrimitiveNode::Make("double_field", parquet::Repetition::REQUIRED, parquet::Type::DOUBLE, parquet::LogicalType::NONE));
    // Create a primitive node named 'ba_field' with type:BYTE_ARRAY, repetition:OPTIONAL
    fields.push_back(parquet::schema::PrimitiveNode::Make("ba_field", parquet::Repetition::OPTIONAL, parquet::Type::BYTE_ARRAY, parquet::LogicalType::NONE));
    // Create a primitive node named 'flba_field' with type:FIXED_LEN_BYTE_ARRAY, repetition:REQUIRED
    fields.push_back(parquet::schema::PrimitiveNode::Make("flba_field", parquet::Repetition::REQUIRED, parquet::Type::FIXED_LEN_BYTE_ARRAY, parquet::LogicalType::NONE, FIXED_LENGTH));

    // Create a GroupNode named 'schema' using the primitive nodes defined above
    // This GroupNode is the root node of the schema tree
    schema_ = std::static_pointer_cast<parquet::schema::GroupNode>(parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));
  }

  parquet::ParquetFileWriter* GetWriter() {
    if (!writer_) {
      // Create a parquet file writer using the schema and the output stream.
      writer_ = parquet::ParquetFileWriter::Open(ostream_, schema_);
    }
    return writer_.get();
  }

  void WriteToFile(std::string filename) {
    std::ofstream parquetFile;
    parquetFile.open(filename, std::ios::out | std::ios::binary);
    std::shared_ptr<parquet::Buffer> buffer = ostream_->GetBuffer();
    parquetFile.write(reinterpret_cast<const char*>(buffer->data()), buffer->size());
    parquetFile.close();
  }

 private:
  std::shared_ptr<parquet::schema::GroupNode> schema_;
  std::shared_ptr<parquet::InMemoryOutputStream> ostream_;
  std::unique_ptr<parquet::ParquetFileWriter> writer_;
};

int main(int argc, char** argv) {

/**********************************************************************************
                           PARQUET WRITER EXAMPLE
**********************************************************************************/
  // parquet::REQUIRED fields do not need definition and repetition level values
  // parquet::OPTIONAL fields require only definition level values
  // parquet::REPEATED fields require both definition and repetition level values
  try {
    // Create a ParquetWriter instance
    auto parquet_writer = std::unique_ptr<ParquetWriter>(new ParquetWriter());

    // Setup the schema
    parquet_writer->SetupSchema();

    // Get a handle to the ParquetFileWriter
    parquet::ParquetFileWriter* file_writer = parquet_writer->GetWriter();

    // Append a RowGroup with a specific number of rows.
    parquet::RowGroupWriter* rgWriter = file_writer->AppendRowGroup(NUM_ROWS_PER_ROW_GROUP);

    // Write the Bool column 
    parquet::ColumnWriter* colWriter = rgWriter->NextColumn();
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
       bool value = ((i % 2) == 0) ? true : false;
       WriteAllValues(1, nullptr, nullptr, reinterpret_cast<const uint8_t*>(&value), colWriter);
    }
    colWriter->Close();
    
    // Write the Int32 column 
    colWriter = rgWriter->NextColumn();
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
       int32_t value = i;
       WriteAllValues(1, nullptr, nullptr, reinterpret_cast<const uint8_t*>(&value), colWriter);
    }
    colWriter->Close();
    
    // Write the Int64 column. Each row has repeats twice. 
    colWriter = rgWriter->NextColumn();
    for (int i = 0; i < 2 * NUM_ROWS_PER_ROW_GROUP; i++) {
       int64_t value = i * 1000 * 1000;
       value *= 1000 * 1000;
       int16_t definition_level = 1;
       int16_t repetition_level = 0;
       if ((i % 2) == 0) {
           repetition_level = 1; // start of a new record
       }
       WriteAllValues(1, &definition_level, &repetition_level, reinterpret_cast<const uint8_t*>(&value), colWriter);
    }
    colWriter->Close();

    // Write the INT96 column. 
    colWriter = rgWriter->NextColumn();
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
       parquet::Int96 value;
       value.value[0] = i;
       value.value[1] = i + 1;
       value.value[2] = i + 2;
       WriteAllValues(1, nullptr, nullptr, reinterpret_cast<const uint8_t*>(&value), colWriter);
    }
    colWriter->Close();
    
    // Write the Float column 
    colWriter = rgWriter->NextColumn();
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
       float value = i * 1.1;
       WriteAllValues(1, nullptr, nullptr, reinterpret_cast<const uint8_t*>(&value), colWriter);
    }
    colWriter->Close();
    
    // Write the Double column 
    colWriter = rgWriter->NextColumn();
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
       double value = i * 1.1111111;
       WriteAllValues(1, nullptr, nullptr, reinterpret_cast<const uint8_t*>(&value), colWriter);
    }
    colWriter->Close();
       
    // Write the ByteArray column. Make every alternate values NULL 
    colWriter = rgWriter->NextColumn();
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
       parquet::ByteArray value;
       char hello[FIXED_LENGTH] = "parquet";
       hello[7] = '0' + i / 100;
       hello[8] = '0' + (i / 10) % 10;
       hello[9] = '0' + i % 10;
       if ( i % 2 == 0) {
          int16_t definition_level = 1;
          value.ptr = reinterpret_cast<const uint8_t*>(&hello[0]);
          value.len = FIXED_LENGTH;
          WriteAllValues(1, &definition_level, nullptr, reinterpret_cast<const uint8_t*>(&value), colWriter);
       } else {
          int16_t definition_level = 0;
          WriteAllValues(1, &definition_level, nullptr, nullptr, colWriter);
       }
    }
    colWriter->Close();
    
    // Write the FixedLengthByteArray column 
    colWriter = rgWriter->NextColumn();
    for (int i = 0; i < NUM_ROWS_PER_ROW_GROUP; i++) {
       parquet::FixedLenByteArray value;
       char v = static_cast<char>(i);
       char flba[FIXED_LENGTH] = {v, v, v, v, v, v, v, v, v, v};
       value.ptr = reinterpret_cast<const uint8_t*>(&flba[0]);

       WriteAllValues(1, nullptr, nullptr, reinterpret_cast<const uint8_t*>(&value), colWriter);
    }
    colWriter->Close();

    // Close the RowGroup Writer
    rgWriter->Close();
    
    // Close the ParquetFileWriter
    file_writer->Close();

    // Write the in-memory bytes to file
    parquet_writer->WriteToFile(PARQUET_FILENAME);
    
  } catch (const std::exception& e) {
      std::cerr << "Parquet write error: " << e.what() << std::endl; return -1;
  }


/**********************************************************************************
                           PARQUET READER EXAMPLE
**********************************************************************************/

  try {
    // Create a ParquetReader instance
    std::unique_ptr<parquet::ParquetFileReader> parquet_reader = parquet::ParquetFileReader::OpenFile(PARQUET_FILENAME, false);
    // Get the File MetaData
    const parquet::FileMetaData* file_metadata = parquet_reader->metadata();

    // Get the number of RowGroups
    int num_row_groups = file_metadata->num_row_groups();
    assert(num_row_groups == 1); 

    // Get the number of Columns
    int num_columns = file_metadata->num_columns();
    assert(num_columns == 8); 

    // Iterate over all the RowGroups in the file
    for (int r = 0; r < num_row_groups; ++r) {
        // Get the RowGroup Reader
        std::shared_ptr<parquet::RowGroupReader> row_group_reader = parquet_reader->RowGroup(r);

        int64_t values_read = 0;
        int64_t rows_read = 0;
        int16_t definition_level;
        int16_t repetition_level;
        int i;
        std::shared_ptr<parquet::ColumnReader> column_reader;

        // Get the Column Reader for the boolean column
        column_reader = row_group_reader->Column(0);

        // Read all the rows in the column
        i = 0;
        while (column_reader->HasNext()) {
          bool value;
          // Read one value at a time. The number of rows read is returned. values_read contains the number of non-null rows
          rows_read = parquet::ScanAllValues(1, nullptr, nullptr, reinterpret_cast<uint8_t*>(&value), &values_read, column_reader.get());
          // Ensure only one value is read
          assert(rows_read == 1);
          // There are no NULL values in the rows written
          assert(values_read == 1);
          // Verify the value written
          bool expected_value = ((i % 2) == 0) ? true : false;
          assert(value ==  expected_value);
          i++;
        }

        // Get the Column Reader for the Int32 column
        column_reader = row_group_reader->Column(1);
        // Read all the rows in the column
        i = 0;
        while (column_reader->HasNext()) {
          int32_t value;
          // Read one value at a time. The number of rows read is returned. values_read contains the number of non-null rows
          rows_read = parquet::ScanAllValues(1, nullptr, nullptr, reinterpret_cast<uint8_t*>(&value), &values_read, column_reader.get());
          // Ensure only one value is read
          assert(rows_read == 1);
          // There are no NULL values in the rows written
          assert(values_read == 1);
          // Verify the value written
          assert(value ==  i);
          i++;
        }

        // Get the Column Reader for the Int64 column
        column_reader = row_group_reader->Column(2);
        // Read all the rows in the column
        i = 0;
        while (column_reader->HasNext()) {
          int64_t value;
          // Read one value at a time. The number of rows read is returned. values_read contains the number of non-null rows
          rows_read = parquet::ScanAllValues(1, &definition_level, &repetition_level, reinterpret_cast<uint8_t*>(&value), &values_read, column_reader.get());
          // Ensure only one value is read
          assert(rows_read == 1);
          // There are no NULL values in the rows written
          assert(values_read == 1);
          // Verify the value written
          int64_t expected_value = i * 1000 * 1000;
          expected_value *= 1000 * 1000;
          assert(value ==  expected_value);
          if ((i % 2) == 0) {
              assert(repetition_level ==  1);
          } else {
              assert(repetition_level ==  0);
          }
          i++;
        }

        // Get the Column Reader for the Int96 column
        column_reader = row_group_reader->Column(3);
        // Read all the rows in the column
        i = 0;
        while (column_reader->HasNext()) {
          parquet::Int96 value;
          // Read one value at a time. The number of rows read is returned. values_read contains the number of non-null rows
          rows_read = parquet::ScanAllValues(1, nullptr, nullptr, reinterpret_cast<uint8_t*>(&value), &values_read, column_reader.get());
          // Ensure only one value is read
          assert(rows_read == 1);
          // There are no NULL values in the rows written
          assert(values_read == 1);
          // Verify the value written
          parquet::Int96 expected_value;
          expected_value.value[0] = i;
          expected_value.value[1] = i + 1;
          expected_value.value[2] = i + 2;
          for (int j = 0; j < 3; j++) {
            assert(value.value[j] ==  expected_value.value[j]);
          }
          i++;
        }

        // Get the Column Reader for the Float column
        column_reader = row_group_reader->Column(4);
        // Read all the rows in the column
        i = 0;
        while (column_reader->HasNext()) {
          float value;
          // Read one value at a time. The number of rows read is returned. values_read contains the number of non-null rows
          rows_read = parquet::ScanAllValues(1, nullptr, nullptr, reinterpret_cast<uint8_t*>(&value), &values_read, column_reader.get());
          // Ensure only one value is read
          assert(rows_read == 1);
          // There are no NULL values in the rows written
          assert(values_read == 1);
          // Verify the value written
          float expected_value = i * 1.1;
          assert(value == expected_value);
          i++;
        }

        // Get the Column Reader for the Double column
        column_reader = row_group_reader->Column(5);
        // Read all the rows in the column
        i = 0;
        while (column_reader->HasNext()) {
          double value;
          // Read one value at a time. The number of rows read is returned. values_read contains the number of non-null rows
          rows_read = parquet::ScanAllValues(1, nullptr, nullptr, reinterpret_cast<uint8_t*>(&value), &values_read, column_reader.get());
          // Ensure only one value is read
          assert(rows_read == 1);
          // There are no NULL values in the rows written
          assert(values_read == 1);
          // Verify the value written
          double expected_value = i * 1.1111111;
          assert(value == expected_value);
          i++;
        }

        // Get the Column Reader for the ByteArray column
        column_reader = row_group_reader->Column(6);
        // Read all the rows in the column
        i = 0;
        while (column_reader->HasNext()) {
          parquet::ByteArray value;
          // Read one value at a time. The number of rows read is returned. values_read contains the number of non-null rows
          rows_read = parquet::ScanAllValues(1, &definition_level, nullptr, reinterpret_cast<uint8_t*>(&value), &values_read, column_reader.get());
          // Ensure only one value is read
          assert(rows_read == 1);
          // Verify the value written
          char expected_value[FIXED_LENGTH] = "parquet";
          expected_value[7] = '0' + i / 100;
          expected_value[8] = '0' + (i / 10) % 10;
          expected_value[9] = '0' + i % 10;
          if (i % 2 == 0) {// only alternate values exist
            // There are no NULL values in the rows written
            assert(values_read == 1);
            assert(value.len == FIXED_LENGTH);
            assert(memcmp(value.ptr, &expected_value[0], FIXED_LENGTH) == 0);
            assert(definition_level == 1);
          } else {
            // There are NULL values in the rows written
            assert(values_read == 0);
            assert(definition_level == 0);
          }
          i++;
        }

        // Get the Column Reader for the FixedLengthByteArray column
        column_reader = row_group_reader->Column(7);
        // Read all the rows in the column
        i = 0;
        while (column_reader->HasNext()) {
          parquet::FixedLenByteArray value;
          // Read one value at a time. The number of rows read is returned. values_read contains the number of non-null rows
          rows_read = parquet::ScanAllValues(1, nullptr, nullptr, reinterpret_cast<uint8_t*>(&value), &values_read, column_reader.get());
          // Ensure only one value is read
          assert(rows_read == 1);
          // There are no NULL values in the rows written
          assert(values_read == 1);
          // Verify the value written
          char v = static_cast<char>(i);
          char expected_value[FIXED_LENGTH] = {v, v, v, v, v, v, v, v, v, v};
          assert(memcmp(value.ptr, &expected_value[0], FIXED_LENGTH) == 0);
          i++;
        }
    }
 
  } catch (const std::exception& e) {
      std::cerr << "Parquet read error: " << e.what() << std::endl; return -1;
  }

  return 0;
}
