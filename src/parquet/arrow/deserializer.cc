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

#include "parquet/arrow/deserializer.h"

#include <set>
#include <tuple>

#include "arrow/builder.h"
#include "arrow/memory_pool.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/util/bit-util.h"
#include "arrow/util/decimal.h"
#include "arrow/util/logging.h"
#include "parquet/arrow/schema.h"
#include "parquet/file_reader.h"
#include "parquet/types.h"

namespace arrow {

class Array;
}

using arrow::Array;
using arrow::Status;
using arrow::Table;

using parquet::schema::GroupNode;
using parquet::schema::Node;
using parquet::schema::PrimitiveNode;

namespace parquet {

namespace arrow {

constexpr int64_t kJulianToUnixEpochDays = 2440588LL;
constexpr int64_t kMillisecondsInADay = 86400000LL;
constexpr int64_t kNanosecondsInADay = kMillisecondsInADay * 1000LL * 1000LL;

static inline int64_t impala_timestamp_to_nanoseconds(const Int96& impala_timestamp) {
  int64_t days_since_epoch = impala_timestamp.value[2] - kJulianToUnixEpochDays;
  int64_t nanoseconds = *(reinterpret_cast<const int64_t*>(&(impala_timestamp.value)));
  return days_since_epoch * kNanosecondsInADay + nanoseconds;
}

// This is a "tagged" integer type for definition/repetition levels.
// The tag gives us strong typing so that we can differentiate between
// definition and repeition level arguments in the DeserializerBuilder
// build function calls.
template <typename Tag>
class Level {
 public:
  Level() : value_(0) {}

  Level& operator++() {
    ++value_;
    return *this;
  }

  int16_t operator*() const { return value_; }

 private:
  int16_t value_;
};

struct Repetition;
struct Definition;

using RepetitionLevel = Level<Repetition>;
using DefinitionLevel = Level<Definition>;

//
// This is a utility class that is used by primitive node deserializer
// nodes. It's similar to a std::list, but stores its data as a contiguous
// array of memory because it's passed to calls to ReadBatch.
template <typename T>
class PrimitiveDeserializerArray {
 public:
  PrimitiveDeserializerArray(::arrow::MemoryPool* pool, int64_t capacity)
      : pool_(pool), capacity_(capacity), size_(0), index_(0) {}

  ~PrimitiveDeserializerArray() {
    pool_->Free(reinterpret_cast<uint8_t*>(data_), capacity_);
  }

  Status Initialize() {
    uint8_t* out;
    RETURN_NOT_OK(pool_->Allocate(sizeof(T) * capacity_, &out));
    data_ = reinterpret_cast<T*>(out);
    return Status::OK();
  }

  bool const Empty() const { return index_ >= size_; }

  T Front() const { return data_[index_]; }

  void PopFront() { ++index_; }

  void Reset() {
    index_ = 0;
    size_ = 0;
  }

  ::arrow::MemoryPool* pool_;

  T* data_;

  int64_t capacity_;

  int64_t size_;

  int64_t index_;
};

//
// Interface for implementations that are able to deserialize
// Parquet primitive and group nodes. Successive calls to
// Deserialize will cause this node to append values or nulls
// to its builder.
//
class DeserializerNode {
 public:
  using LevelSet = std::set<int16_t>;

  //
  // \brief Deserializes the next set of values.
  // \param[in] the current repeition level. When this is first called
  //            when deserializing a row, it should be zero. It may be
  //            non-zero when a list node is calling a child node
  //            during a repetition. In those cases, a child node may
  //            do nothing because it's next value doesn't repeat at
  //            the current repeition level.
  // \param[out] this is used to find out what the next set of
  //            repetitions will be. This is used by list
  //            (aka - repeated) nodes in their Deserialize method
  //            to determine whether or not they need to repeat
  //            and call Deserialize for their child nodes
  //            again before popping back to their parent node.
  // \return error status if there is no more data for this node
  virtual Status Deserialize(RepetitionLevel current_repetition_level,
                             LevelSet& next_repetitions) = 0;

  // \brief Skips deserialization. This is used by group and list
  //        nodes when they are nullable, in which they call this
  //        method on their child nodes so that they do not append
  //        any values to their builders.
  // \param[out] this is used to find out what the next set of
  //            repetitions will be. This is used by list
  //            (aka - repeated) nodes in their Deserialize method
  //            to determine whether or not they need to repeat
  //            and call Deserialize for their child nodes
  //            again before popping back to their parent node.
  // \return error status if there is no more data for this node
  virtual Status Skip(LevelSet& next_repetitions) = 0;

  // \brief returns the maximum definition level of all of this
  //        node's descendants. In the case of a primitive node
  //        it will be the next definition level in the file (if
  //        one exists) or zero otherwise. In the case of a group
  //        node, it will simply take the maximum of all of its
  //        child nodes.
  // \param[out] the maximum definition level for this nodes and its
  //             descendants
  // \return error status if there is no more data for this node
  virtual Status CurrentMaxDefinitionLevel(int16_t& level) = 0;

  // \brief returns the array buidler that was used by this node
  //        to append values and null in calls to Deserialize
  // \return the array builder
  virtual std::shared_ptr<::arrow::ArrayBuilder> GetArrayBuilder() = 0;

  // \brief initializes this node for a new row group. This is used
  //        by primitive node deserializers to obtain a column reader
  //        for that row group
  // \return status error if this node is a primitive node and the
  //         column index is out of bounds
  virtual Status InitializeRowGroup(
      std::shared_ptr<::parquet::RowGroupReader> const& row_group_reader) = 0;
};

//
// The PrimitiveAppender class is used adapt the reading of parquet
// values from a ColumnReader to calls to a builder's append method.
// The reason this is needed is that the signature of the builder's
// Append method varies by type. For example, byte arrays take
// a pointer and a length whereas primitive types take just a value.
// By using partial specialization for various types, the deserializer
// code for primitive types does not need specialization but instead
// delegates to these set of classes to handle it.
//
template <typename ParquetType, typename BuilderType>
class PrimitiveAppender {
 public:
  template <typename ParquetValueType>
  Status Append(const ColumnDescriptor* descriptor, BuilderType& builder,
                ParquetValueType value) {
    using value_type = typename BuilderType::value_type;
    return builder.Append(static_cast<value_type>(value));
  }
};

template <typename BuilderType>
class PrimitiveAppender<::parquet::ByteArray, BuilderType> {
 public:
  template <typename ParquetValueType>
  Status Append(const ColumnDescriptor* descriptor, BuilderType& builder,
                ParquetValueType value) {
    return builder.Append(value.ptr, value.len);
  }
};

template <typename BuilderType>
class PrimitiveAppender<FLBA, BuilderType> {
 public:
  template <typename ParquetValueType>
  Status Append(const ColumnDescriptor* descriptor, BuilderType& builder,
                ParquetValueType value) {
    return builder.Append(value.ptr);
  }
};

template <>
class PrimitiveAppender<ByteArray, ::arrow::Decimal128Builder> {
 public:
  template <typename ParquetValueType>
  Status Append(const ColumnDescriptor* descriptor, ::arrow::DecimalBuilder& builder,
                ParquetValueType value) {
    ::arrow::Decimal128 converted;
    RETURN_NOT_OK(::arrow::Decimal128::FromBigEndian(value.ptr, value.len, &converted));
    return builder.Append(converted);
  }
};

template <>
class PrimitiveAppender<FLBA, ::arrow::Decimal128Builder> {
 public:
  template <typename ParquetValueType>
  Status Append(const ColumnDescriptor* descriptor, ::arrow::DecimalBuilder& builder,
                ParquetValueType value) {
    ::arrow::Decimal128 converted;
    RETURN_NOT_OK(::arrow::Decimal128::FromBigEndian(value.ptr, descriptor->type_length(),
                                                     &converted));
    return builder.Append(converted);
  }
};

template <typename ParquetType>
class PrimitiveAppender<ParquetType, ::arrow::Decimal128Builder> {
 public:
  template <typename ParquetValueType>
  Status Append(const ColumnDescriptor* descriptor, ::arrow::DecimalBuilder& builder,
                ParquetValueType value) {
    return builder.Append(::arrow::Decimal128(value));
  }
};

template <>
class PrimitiveAppender<Int96, ::arrow::TimestampBuilder> {
 public:
  template <typename ParquetValueType>
  Status Append(const ColumnDescriptor* descriptor, ::arrow::TimestampBuilder& builder,
                ParquetValueType value) {
    return builder.Append(impala_timestamp_to_nanoseconds(value));
  }
};

class NullPrimitiveAppender {
 public:
  template <typename ParquetValueType>
  Status Append(const ColumnDescriptor* descriptor, ::arrow::NullBuilder& builder,
                ParquetValueType value) {
    std::ostringstream stream;
    stream << "Unexpected call to NullPrimitiveAppender::Append";
    return Status::Invalid(stream.str());
  }
};

template <typename ParquetType>
class PrimitiveAppender<ParquetType, ::arrow::NullBuilder>
    : public NullPrimitiveAppender {};

// These specializations avoid ambiguity with other ones where FLBA is
// specified but the builder type isn't (ambiguous with the class right above)
template <>
class PrimitiveAppender<FLBA, ::arrow::NullBuilder> : public NullPrimitiveAppender {};

template <>
class PrimitiveAppender<ByteArray, ::arrow::NullBuilder> : public NullPrimitiveAppender {
};

//
// This is the base class for primitive types deserializers.
// The ColumnReaderType should be an instantiation of
// TypeColumnReader (see column_reader.h). The ArrayBuilderType
// should be an instantiation of one of the Arrow builders
// (e.g. - Int32Builder). This class is used for initializing
// the column reader and appending values, whereas the sub-classes
// handle reading of repetition and definition levels and implement
// the DeserializerNode virtual functions.
//
template <typename ColumnReaderType, typename ArrayBuilderType>
class PrimitiveDeserializerNode : public DeserializerNode {
 public:
  PrimitiveDeserializerNode(const ColumnDescriptor* const descriptor, int column_index,
                            int64_t buffer_size,
                            std::shared_ptr<ArrayBuilderType> const& builder,
                            ::arrow::MemoryPool* pool)
      : descriptor_(descriptor),
        column_index_(column_index),
        values_(pool, buffer_size),
        builder_(builder) {}

  std::shared_ptr<::arrow::ArrayBuilder> GetArrayBuilder() { return builder_; }

  // \brief initializes the column reader for this deserializer
  // \param[in] the reader for the row group from which the column reader
  //            is obtained
  // \return error status if the column index is out of bounds
  Status InitializeRowGroup(
      std::shared_ptr<RowGroupReader> const& row_group_reader) final {
    if (column_index_ < row_group_reader->metadata()->num_columns()) {
      column_reader_ = std::static_pointer_cast<ColumnReaderType>(
          row_group_reader->Column(column_index_));
    } else {
      std::ostringstream stream;
      stream << "Invalid column index " << column_index_ << " for row group ";
      return Status::Invalid(stream.str());
    }
    return Status::OK();
  }

  virtual Status Initialize() { return values_.Initialize(); }

 protected:
  // \brief appends the next value in this node's values array. It is
  //        assumed that the caller has checked to make sure that the
  //        values buffer is not empty
  // \return error status if the call to the builder's Append function fails
  Status Append() {
    auto status = appender_.Append(descriptor_, *builder_, values_.Front());
    values_.PopFront();
    return status;
  }

  const ColumnDescriptor* const descriptor_;

  int const column_index_;

  PrimitiveDeserializerArray<typename ColumnReaderType::T> values_;

  std::shared_ptr<ArrayBuilderType> builder_;

  PrimitiveAppender<typename ColumnReaderType::T, ArrayBuilderType> appender_;

  std::shared_ptr<ColumnReaderType> column_reader_;
};

// This enum is used to differentiate between the three
// different types of primitive deserializers
enum PrimitiveSerializerType {
  // This means that the node in the tree has no ancestors (including
  // itself) that are repeated or optional
  REQUIRED,
  // This means that the node in the tree has only ancestors
  // (including itself) that are required or optional, and therefore
  // doesn't need to store repetition levels but does need to store
  // definition levels
  OPTIONAL,
  // This means that the node in the tree has at least one ancestor
  // (including itself)
  REPEATED
};

//
// The TypedPrimitiveDeserializerNode does the heavy lifting of
// reading repetition and definition levels and implementing
// the DeserializerNode interface for primitive types. There are
// three specializations based on whether or not the field
// has definition levels or repetition levels (e.g. - a required
// field at depth 1 doesn't have repeition or definition levels
// and they are not stored in the parquet file). By using
// specialization in this manner, we save space and also don't
// have to do things like check if the max repetition or
// max definition level is zero. Unfortunately, this pattern is not
// yet being used in the column reader classes, so those checks
// still happen in calls to ReadBatch.
//
template <typename ColumnReaderType, typename ArrayBuilderType,
          PrimitiveSerializerType Serializertype>
class TypedPrimitiveDeserializerNode;

// Specialization for a field that has definition levels but no
// repetition levels. For example, the following schemas would
// create this type of node:
//
// message root {
//    optional int32 field;
// }
//
// message root {
//    optional group contact {
//       required bytearray name (UTF8);
//       optional int32 phone_number;
//    }
//
// Both name and phone_number would be serialized with this type of
// node because they have a max definition level of 1 and 2 respectively.
//
template <typename ColumnReaderType, typename ArrayBuilderType>
class TypedPrimitiveDeserializerNode<ColumnReaderType, ArrayBuilderType,
                                     PrimitiveSerializerType::OPTIONAL>
    : public PrimitiveDeserializerNode<ColumnReaderType, ArrayBuilderType> {
 public:
  TypedPrimitiveDeserializerNode(const ColumnDescriptor* const descriptor,
                                 int const column_index, std::size_t buffer_size,
                                 std::shared_ptr<ArrayBuilderType> const& builder,
                                 ::arrow::MemoryPool* pool)
      : PrimitiveDeserializerNode<ColumnReaderType, ArrayBuilderType>(
            descriptor, column_index, buffer_size, builder, pool),
        definition_levels_(pool, buffer_size) {}

  Status Initialize() final {
    RETURN_NOT_OK(definition_levels_.Initialize());
    return PrimitiveDeserializerNode<ColumnReaderType, ArrayBuilderType>::Initialize();
  }

  Status Skip(DeserializerNode::LevelSet& next_repetitions) final {
    if (!Fill()) {
      std::ostringstream stream;
      stream << "Unable to skip data";
      return Status::IOError(stream.str());
    }
    if (definition_levels_.Front() == this->descriptor_->max_definition_level()) {
      this->values_.PopFront();
    }
    definition_levels_.PopFront();
    return Status::OK();
  }

  Status CurrentMaxDefinitionLevel(int16_t& level) final {
    if (!Fill()) {
      std::ostringstream stream;
      stream << "Unable to read current max definition level";
      return Status::IOError(stream.str());
    }
    level = definition_levels_.Front();
    return Status::OK();
  }

  Status Deserialize(RepetitionLevel current_repetition_level,
                     DeserializerNode::LevelSet& next_repetitions) final {
    if (!Fill()) {
      std::ostringstream stream;
      stream << "Unable to deserialize value";
      return Status::IOError(stream.str());
    }
    if (definition_levels_.Front() == this->descriptor_->max_definition_level()) {
      // The value is not null
      RETURN_NOT_OK(this->Append());
    } else {
      RETURN_NOT_OK(this->builder_->AppendNull());
    }
    definition_levels_.PopFront();
    return Status::OK();
  }

 private:
  bool Fill() {
    if (!definition_levels_.Empty()) {
      return true;
    }
    definition_levels_.Reset();
    this->values_.Reset();
    definition_levels_.size_ = this->column_reader_->ReadBatch(
        definition_levels_.capacity_, definition_levels_.data_, 0, this->values_.data_,
        &this->values_.size_);
    return !definition_levels_.Empty();
  }

  PrimitiveDeserializerArray<int16_t> definition_levels_;
};

//
// Specialization for a field that has both definition and repetition levels.
// For example, the following schema would create this type of node:
//
// message root {
//    required group list_outer (LIST) {
//        repeated group list_inner {
//            required int32 value;
//        }
//    }
// }
template <typename ColumnReaderType, typename ArrayBuilderType>
class TypedPrimitiveDeserializerNode<ColumnReaderType, ArrayBuilderType,
                                     PrimitiveSerializerType::REPEATED>
    : public PrimitiveDeserializerNode<ColumnReaderType, ArrayBuilderType> {
 public:
  TypedPrimitiveDeserializerNode(const ColumnDescriptor* const descriptor,
                                 int column_index, std::size_t buffer_size,
                                 std::shared_ptr<ArrayBuilderType> const& builder,
                                 ::arrow::MemoryPool* pool)
      : PrimitiveDeserializerNode<ColumnReaderType, ArrayBuilderType>(
            descriptor, column_index, buffer_size, builder, pool),
        definition_levels_(pool, buffer_size),
        repetition_levels_(pool, buffer_size) {}

  Status Initialize() final {
    RETURN_NOT_OK(definition_levels_.Initialize());
    RETURN_NOT_OK(repetition_levels_.Initialize());
    return PrimitiveDeserializerNode<ColumnReaderType, ArrayBuilderType>::Initialize();
  }

  Status CurrentMaxDefinitionLevel(int16_t& level) final {
    if (!Fill()) {
      std::ostringstream stream;
      stream << "Unable to determine current max definition level";
      return Status::IOError(stream.str());
    }
    level = definition_levels_.Front();
    return Status::OK();
  }

  Status Skip(DeserializerNode::LevelSet& next_repetitions) final {
    if (!Fill()) {
      std::ostringstream stream;
      stream << "Unable to determine skip data";
      return Status::IOError(stream.str());
    }
    if (definition_levels_.Front() == this->descriptor_->max_definition_level()) {
      this->values_.PopFront();
    }
    definition_levels_.PopFront();
    AddRepetitions(next_repetitions);
    return Status::OK();
  }

  Status Deserialize(RepetitionLevel current_repetition_level,
                     DeserializerNode::LevelSet& next_repetitions) final {
    if (!Fill()) {
      std::ostringstream stream;
      stream << "Unable to fill values when deserializing";
      return Status::IOError(stream.str());
    }
    // See Dremel paper: second entry for Links.Backward would
    // apply in this case and return
    if (repetition_levels_.Front() != *current_repetition_level) {
      return Status::OK();
    }

    if (definition_levels_.Front() == this->descriptor_->max_definition_level()) {
      // The value is not null
      RETURN_NOT_OK(this->Append());
    } else {
      // Insert a null
      RETURN_NOT_OK(this->builder_->AppendNull());
    }
    definition_levels_.PopFront();
    AddRepetitions(next_repetitions);
    return Status::OK();
  }

 private:
  // This is factored into its own method because it's used in both
  // Skip and Deserialize
  void AddRepetitions(DeserializerNode::LevelSet& next_repetitions) {
    repetition_levels_.PopFront();
    if (Fill()) {
      next_repetitions.insert(repetition_levels_.Front());
    }
  }

  bool Fill() {
    if (!repetition_levels_.Empty()) {
      return true;
    }
    definition_levels_.Reset();
    repetition_levels_.Reset();
    this->values_.Reset();
    auto levels_read = this->column_reader_->ReadBatch(
        definition_levels_.capacity_, definition_levels_.data_, repetition_levels_.data_,
        this->values_.data_, &this->values_.size_);
    definition_levels_.size_ = levels_read;
    repetition_levels_.size_ = levels_read;

    return !definition_levels_.Empty();
  }

  PrimitiveDeserializerArray<int16_t> definition_levels_;

  PrimitiveDeserializerArray<int16_t> repetition_levels_;
};

//
// Specialization for a field that has neither definition nor repetition levels.
// For example, all of the primitive fields in the following schema
// would create this type of node:
//
// message root {
//    required group contact {
//        required bytearray name (UTF8);
//        required group address {
//            required bytearray street (UTF8);
//            required int32 number;
//            required bytearray city (UTF8);
//            required int8 zip;
//        }
//    }
// }
template <typename ColumnReaderType, typename ArrayBuilderType>
class TypedPrimitiveDeserializerNode<ColumnReaderType, ArrayBuilderType,
                                     PrimitiveSerializerType::REQUIRED>
    : public PrimitiveDeserializerNode<ColumnReaderType, ArrayBuilderType> {
 public:
  TypedPrimitiveDeserializerNode(const ColumnDescriptor* const descriptor,
                                 int column_index, std::size_t buffer_size,
                                 std::shared_ptr<ArrayBuilderType> const& builder,
                                 ::arrow::MemoryPool* pool)
      : PrimitiveDeserializerNode<ColumnReaderType, ArrayBuilderType>(
            descriptor, column_index, buffer_size, builder, pool) {}

  Status CurrentMaxDefinitionLevel(int16_t& level) final {
    level = this->descriptor_->max_definition_level();
    return Status::OK();
  }

  Status Skip(DeserializerNode::LevelSet& next_repetitions) final {
    std::ostringstream stream;
    stream << "Call to Skip made when it shouldn't have";
    return Status::Invalid(stream.str());
  }

  Status Deserialize(RepetitionLevel current_repetition_level,
                     DeserializerNode::LevelSet& next_repetitions) final {
    if (!Fill()) {
      std::ostringstream stream;
      stream << "Unable to load more values in call to Deserialize";
      return Status::IOError(stream.str());
    }
    return this->Append();
  }

 private:
  bool Fill() {
    if (!this->values_.Empty()) {
      return true;
    }
    this->values_.Reset();
    // Ignore the return value here
    this->column_reader_->ReadBatch(this->values_.capacity_, 0, 0, this->values_.data_,
                                    &this->values_.size_);
    return !this->values_.Empty();
  }
};

//
// Deserializer for lists that are required. The Parquet spec on list
// types requires that the schema for a required list looks likes this:
//
// required group list_outer (LIST) {
//     repeated group list_inner {
//         (required|optional) <type> element ...
//     }
// }
//
// The reason for the outer level of nesting is that without it
// you wouldn't be able to differentiate a list of a single null
// from an empty list. By having this extra level, an empty list
// is indicated when the max definition level of the list_outer's
// descendants are equal to list_outer's max definition level, and
// a list of null would be indicated by a definition level of
// list_inner's descendants that is equal to list_inner's max
// definition level.
//
// The constructor of this class takes the max_definition_level of
// list_outer and the max_repetition_level of list_inner
class ListDeserializerNode : public DeserializerNode {
 public:
  ListDeserializerNode(std::unique_ptr<DeserializerNode>&& node_reader,
                       ::arrow::MemoryPool* pool, RepetitionLevel max_repetition_level,
                       DefinitionLevel max_definition_level)
      : node_reader_(std::move(node_reader)),
        max_repetition_level_(max_repetition_level),
        max_definition_level_(max_definition_level),
        builder_(std::make_shared<::arrow::ListBuilder>(
            pool, node_reader_->GetArrayBuilder())) {}

  Status CurrentMaxDefinitionLevel(int16_t& level) final {
    return node_reader_->CurrentMaxDefinitionLevel(level);
  }

  Status Skip(LevelSet& next_repetitions) final {
    return node_reader_->Skip(next_repetitions);
  }

  Status Deserialize(RepetitionLevel current_repetition_level,
                     LevelSet& next_repetitions) {
    RETURN_NOT_OK(builder_->Append());
    int16_t current_max_definition_level;
    RETURN_NOT_OK(node_reader_->CurrentMaxDefinitionLevel(current_max_definition_level));
    if (current_max_definition_level == *max_definition_level_) {
      // This is just an empty list
      return node_reader_->Skip(next_repetitions);
    } else {
      // Deserialize the first one
      RETURN_NOT_OK(
          node_reader_->Deserialize(current_repetition_level, next_repetitions));
      // Deserialize the repetitions
      return DeserializeRepetitions(next_repetitions);
    }
  }

  std::shared_ptr<::arrow::ArrayBuilder> GetArrayBuilder() final { return builder_; }

  Status InitializeRowGroup(std::shared_ptr<RowGroupReader> const& row_group_reader) {
    return node_reader_->InitializeRowGroup(row_group_reader);
  }

 protected:
  // \brief keeps appending elements to the current list as long as
  //        the descendants of this node have repetitions at this list's
  //        repetition level
  // \param[in] the set of next repetitions from the last call to
  //            children's Deserialize functions.
  // \return status error if calls to child node Deserialize functions
  //         produce an error
  Status DeserializeRepetitions(LevelSet& next_repetitions) {
    bool repetition_at_this_level = true;
    while (!next_repetitions.empty() && repetition_at_this_level) {
      // next_repetitions is a sorted set, so end
      // is the highest value. If it's lower than our
      // max_repetition_level_, we just return from this
      // function. Otherwise, we erase highest, descend again,
      // and then re-check if we repeated at this level
      auto highest = next_repetitions.end();
      --highest;
      repetition_at_this_level = (*highest == *max_repetition_level_);
      if (repetition_at_this_level) {
        next_repetitions.erase(highest);
        RETURN_NOT_OK(node_reader_->Deserialize(max_repetition_level_, next_repetitions));
      }
    }
    return Status::OK();
  }

  std::unique_ptr<DeserializerNode> node_reader_;

  RepetitionLevel max_repetition_level_;

  DefinitionLevel max_definition_level_;

  std::shared_ptr<::arrow::ListBuilder> builder_;
};

//
// Deserializer for lists that are optional. The Parquet spec on list
// types requires that the schema for an optional list looks likes this:
//
// optional group list_outer (LIST) {
//     repeated group list_inner {
//         (required|optional) <type> element ...
//     }
// }
//
// When deserializing, if the max definition level of this node's
// children is less than list_outer's max definition level, then
// we append a null. If it's equal to it, then we create an empty
// list. Otherwise, we create a non-empty list.
//
// The constructor of this class takes the max_definition_level of
// list_outer and the max_repetition_level of list_inner
class NullableListDeserializerNode : public ListDeserializerNode {
 public:
  NullableListDeserializerNode(std::unique_ptr<DeserializerNode>&& node_reader,
                               ::arrow::MemoryPool* pool,
                               RepetitionLevel max_repetition_level,
                               DefinitionLevel max_definition_level)
      : ListDeserializerNode(std::forward<std::unique_ptr<DeserializerNode>>(node_reader),
                             pool, max_repetition_level, max_definition_level) {}

  Status Deserialize(RepetitionLevel current_repetition_level,
                     LevelSet& next_repetitions) final {
    int16_t current_max_definition_level;
    RETURN_NOT_OK(node_reader_->CurrentMaxDefinitionLevel(current_max_definition_level));
    if (current_max_definition_level < *max_definition_level_) {
      // It's null
      RETURN_NOT_OK(builder_->AppendNull());
      RETURN_NOT_OK(node_reader_->Skip(next_repetitions));
    } else if (current_max_definition_level == *max_definition_level_) {
      // Empty list
      RETURN_NOT_OK(builder_->Append());
      RETURN_NOT_OK(node_reader_->Skip(next_repetitions));
    } else {
      RETURN_NOT_OK(builder_->Append());
      RETURN_NOT_OK(
          node_reader_->Deserialize(current_repetition_level, next_repetitions));
      RETURN_NOT_OK(DeserializeRepetitions(next_repetitions));
    }
    return Status::OK();
  }
};

//
// Serializer for a group (aka struct) node. The schema for such a node
// would look like this;
//
// (repeated|required) group pair {
//    required int32 key;
//    required bytearray value (UTF8);
// }
//
// This has an Optional template parameter so that we can remove unnecessary checks
// for required nodes at compile time.
//
template <bool Optional>
class NestedDeserializerNode : public DeserializerNode {
 public:
  NestedDeserializerNode(DefinitionLevel max_definition_level,
                         std::vector<std::unique_ptr<DeserializerNode>>&& children,
                         std::vector<std::shared_ptr<::arrow::Field>>& fields,
                         ::arrow::MemoryPool* pool)
      : children_(std::move(children)), max_definition_level_(max_definition_level) {
    std::vector<std::shared_ptr<::arrow::ArrayBuilder>> child_builders;
    for (auto const& child : children_) {
      child_builders.emplace_back(child->GetArrayBuilder());
    }
    auto arrow_type = std::make_shared<::arrow::StructType>(fields);
    builder_ = std::make_shared<::arrow::StructBuilder>(arrow_type, pool,
                                                        std::move(child_builders));
  }

  Status Skip(LevelSet& next_repetitions) {
    for (auto& child : children_) {
      RETURN_NOT_OK(child->Skip(next_repetitions));
    }
    return Status::OK();
  }

  Status CurrentMaxDefinitionLevel(int16_t& max) final {
    max = 0;
    for (auto& child : children_) {
      int16_t current;
      RETURN_NOT_OK(child->CurrentMaxDefinitionLevel(current));
      if (current > max) {
        max = current;
      }
    }
    return Status::OK();
  }

  Status Deserialize(RepetitionLevel current_repetition_level,
                     LevelSet& next_repetitions) final {
    // TODO: detect this at tree construction time. If all of
    // the fields inside this nested node are required, then
    // there is no need to do this check.
    auto has_data = !Optional;
    if (Optional) {
      int16_t current_max_definition_level;
      RETURN_NOT_OK(CurrentMaxDefinitionLevel(current_max_definition_level));
      has_data = current_max_definition_level >= *max_definition_level_;
    }
    if (has_data) {
      RETURN_NOT_OK(builder_->Append());
    } else {
      RETURN_NOT_OK(builder_->AppendNull());
    }
    // For structs, we call Deserialize even when they don't have data.
    // This ensures that the length of the child arrays match
    // the length of the struct bitmap.
    for (auto& child : children_) {
      RETURN_NOT_OK(child->Deserialize(current_repetition_level, next_repetitions));
    }
    return Status::OK();
  }

  std::shared_ptr<::arrow::ArrayBuilder> GetArrayBuilder() final { return builder_; }

  Status InitializeRowGroup(
      std::shared_ptr<RowGroupReader> const& row_group_reader) final {
    for (auto& child : children_) {
      RETURN_NOT_OK(child->InitializeRowGroup(row_group_reader));
    }
    return Status::OK();
  }

 private:
  std::vector<std::unique_ptr<DeserializerNode>> children_;

  DefinitionLevel max_definition_level_;

  std::shared_ptr<::arrow::StructBuilder> builder_;
};

// The following set of classes are used to drive the
// iteration over the input data in a particular manner.
//
// TODO: except for the batched array serializer, it might
//       be possible to collapse these classes through the
//       use of iterators.
ArrayDeserializer::ArrayDeserializer(std::unique_ptr<DeserializerNode>&& node)
    : node_(std::move(node)) {}

ArrayDeserializer::~ArrayDeserializer() {}

// This class will deserialize a single array from the entire
// file. The node that is passed to the constructor may
// contain multiple primitive nodes as its descendants.
class FileArrayDeserializer : public ArrayDeserializer {
 public:
  FileArrayDeserializer(std::unique_ptr<DeserializerNode>&& node,
                        std::shared_ptr<ParquetFileReader> const& file_reader)
      : ArrayDeserializer(std::forward<std::unique_ptr<DeserializerNode>>(node)),
        file_reader_(file_reader) {}

  ~FileArrayDeserializer() {}

  ::arrow::Status DeserializeArray(std::shared_ptr<::arrow::Array>& array) final {
    DeserializerNode::LevelSet repetitions;
    auto const num_row_groups = file_reader_->metadata()->num_row_groups();
    for (int ii = 0; ii < num_row_groups; ++ii) {
      auto row_group_reader = file_reader_->RowGroup(ii);
      RETURN_NOT_OK(node_->InitializeRowGroup(row_group_reader));
      auto const num_rows = row_group_reader_->metadata()->num_rows();
      for (int jj = 0; jj < num_rows; ++jj) {
        RETURN_NOT_OK(node_->Deserialize(RepetitionLevel(), repetitions));
        repetitions.clear();
      }
    }
    return node_->GetArrayBuilder()->Finish(&array);
  }

 private:
  std::shared_ptr<ParquetFileReader> file_reader_;

  std::shared_ptr<::parquet::RowGroupReader> row_group_reader_;
};

ArrayBatchedDeserializer::~ArrayBatchedDeserializer() {}

// This class will deserialize a single array from the entire
// file in batches. The node that is passed to the constructor may
// contain multiple primitive nodes as its descendants.
class FileArrayBatchedDeserializer : public ArrayBatchedDeserializer {
 public:
  FileArrayBatchedDeserializer(std::unique_ptr<DeserializerNode>&& node,
                               std::shared_ptr<ParquetFileReader> const& file_reader)
      : node_(std::move(node)),
        file_reader_(file_reader),
        current_row_group_(file_reader->RowGroup(0)),
        current_row_group_index_(0),
        current_row_(0) {}

  ~FileArrayBatchedDeserializer() {}

  Status Initialize() { return node_->InitializeRowGroup(current_row_group_); }

  ::arrow::Status DeserializeBatch(int64_t num_records,
                                   std::shared_ptr<::arrow::Array>& array) final {
    DeserializerNode::LevelSet repetitions;
    int64_t records_read = 0;
    auto const num_row_groups = file_reader_->metadata()->num_row_groups();
    while (records_read < num_records && current_row_group_index_ < num_row_groups) {
      auto const num_rows = current_row_group_->metadata()->num_rows();
      for (; current_row_ < num_rows && records_read < num_records;
           ++current_row_, ++records_read) {
        RETURN_NOT_OK(node_->Deserialize(RepetitionLevel(), repetitions));
        repetitions.clear();
      }
      if (current_row_ >= num_rows) {
        ++current_row_group_index_;
        if (current_row_group_index_ < num_row_groups) {
          current_row_group_ = file_reader_->RowGroup(current_row_group_index_);
          RETURN_NOT_OK(node_->InitializeRowGroup(current_row_group_));
          current_row_ = 0;
        }
      }
    }
    return node_->GetArrayBuilder()->Finish(&array);
  }

 private:
  std::unique_ptr<DeserializerNode> node_;

  std::shared_ptr<::parquet::ParquetFileReader> file_reader_;

  std::shared_ptr<::parquet::RowGroupReader> current_row_group_;

  int current_row_group_index_;

  int current_row_;
};

// This class will deserialize a single array from a single
// row group for a single node. The DeserializeBuilder only
// constructs this for a node that has at most one primitive
// descendant, but it could just as well be used for nodes that
// contain multiple primitive descendants.
class ColumnChunkDeserializer : public ArrayDeserializer {
 public:
  ColumnChunkDeserializer(
      std::unique_ptr<DeserializerNode>&& node,
      std::shared_ptr<::parquet::RowGroupReader> const& row_group_reader)
      : ArrayDeserializer(std::move(node)), row_group_reader_(row_group_reader) {}

  Status Initialize() { return node_->InitializeRowGroup(row_group_reader_); }

  ::arrow::Status DeserializeArray(std::shared_ptr<::arrow::Array>& array) final {
    DeserializerNode::LevelSet repetitions;
    auto const num_rows = row_group_reader_->metadata()->num_rows();
    for (int ii = 0; ii < num_rows; ++ii) {
      RETURN_NOT_OK(node_->Deserialize(RepetitionLevel(), repetitions));
      repetitions.clear();
    }
    return node_->GetArrayBuilder()->Finish(&array);
  }

 private:
  std::shared_ptr<::parquet::RowGroupReader> row_group_reader_;
};

TableDeserializer::TableDeserializer(
    std::vector<std::unique_ptr<DeserializerNode>>&& nodes,
    std::shared_ptr<::arrow::Schema> const& schema)
    : nodes_(std::move(nodes)), schema_(schema) {}

TableDeserializer::~TableDeserializer() {}

::arrow::Status TableDeserializer::MakeTable(std::shared_ptr<::arrow::Table>& result) {
  std::vector<std::shared_ptr<::arrow::Array>> arrays;
  for (auto const& node : nodes_) {
    std::shared_ptr<::arrow::Array> array;
    RETURN_NOT_OK(node->GetArrayBuilder()->Finish(&array));
    arrays.emplace_back(std::move(array));
  }
  result = Table::Make(schema_, arrays);
  return Status::OK();
}

Status TableDeserializer::DeserializeRowGroup(
    std::shared_ptr<::parquet::RowGroupReader> const& row_group_reader) {
  for (auto& node : nodes_) {
    RETURN_NOT_OK(node->InitializeRowGroup(row_group_reader));
  }
  DeserializerNode::LevelSet repetitions;
  auto const num_rows = row_group_reader->metadata()->num_rows();
  for (int ii = 0; ii < num_rows; ++ii) {
    for (auto& node : nodes_) {
      RETURN_NOT_OK(node->Deserialize(RepetitionLevel(), repetitions));
      repetitions.clear();
    }
  }
  return Status::OK();
}

// This class will deserialize all of the specified nodes from a single row group.
class RowGroupDeserializer : public TableDeserializer {
 public:
  RowGroupDeserializer(std::vector<std::unique_ptr<DeserializerNode>>&& nodes,
                       std::shared_ptr<::arrow::Schema> const& schema,
                       std::shared_ptr<::parquet::RowGroupReader> const& row_group_reader)
      : TableDeserializer(
            std::forward<std::vector<std::unique_ptr<DeserializerNode>>>(nodes), schema),
        row_group_reader_(row_group_reader) {}

  ~RowGroupDeserializer() {}

  ::arrow::Status DeserializeTable(std::shared_ptr<::arrow::Table>& table) final {
    DeserializeRowGroup(row_group_reader_);
    return MakeTable(table);
  }

 private:
  std::shared_ptr<::parquet::RowGroupReader> row_group_reader_;
};

// This class wil deserialize all of the specified nodes from a Parquet file
class FileTableDeserializer : public TableDeserializer {
 public:
  FileTableDeserializer(std::vector<std::unique_ptr<DeserializerNode>>&& nodes,
                        std::shared_ptr<::arrow::Schema> const& schema,
                        std::shared_ptr<::parquet::ParquetFileReader> const& file_reader)
      : TableDeserializer(
            std::forward<std::vector<std::unique_ptr<DeserializerNode>>>(nodes), schema),
        file_reader_(file_reader) {}

  ~FileTableDeserializer() {}

  ::arrow::Status DeserializeTable(std::shared_ptr<::arrow::Table>& table) {
    auto num_row_groups = file_reader_->metadata()->num_row_groups();
    for (int ii = 0; ii < num_row_groups; ++ii) {
      DeserializeRowGroup(file_reader_->RowGroup(ii));
    }
    return MakeTable(table);
  }

 private:
  std::shared_ptr<::parquet::ParquetFileReader> file_reader_;
};

// This class does the main work of constructing deserializer
// nodes from Parquet schema nodes.
class DeserializerBuilder::Impl {
 public:
  Impl(std::shared_ptr<::parquet::ParquetFileReader> const& file_reader,
       ::arrow::MemoryPool* pool, int64_t buffer_size);

  ::arrow::Status BuildSchemaNodeDeserializer(int schema_index,
                                              std::unique_ptr<ArrayDeserializer>& result);

  ::arrow::Status BuildColumnDeserializer(int column_index,
                                          std::unique_ptr<ArrayDeserializer>& result);

  ::arrow::Status BuildColumnChunkDeserializer(
      int column_index, int row_group_index, std::unique_ptr<ArrayDeserializer>& result);

  ::arrow::Status BuildRowGroupDeserializer(int row_group_index,
                                            const std::vector<int>& column_indices,
                                            std::unique_ptr<TableDeserializer>& result);

  ::arrow::Status BuildFileDeserializer(const std::vector<int>& column_indices,
                                        std::unique_ptr<TableDeserializer>& result);

  ::arrow::Status BuildArrayBatchedDeserializer(
      int column_index, std::unique_ptr<ArrayBatchedDeserializer>& result);

 private:
  std::vector<int> const FillColumnIndices() const {
    auto const num_columns = file_reader_->metadata()->num_columns();
    std::vector<int> indices(num_columns);
    for (int ii = 0; ii < num_columns; ++ii) {
      indices[ii] = ii;
    }
    return indices;
  }

  // \brief calls BuildNode for each node in the parquet file's
  //        schema
  // \param[in] indices that are used to filter indicate which
  //            primitive columns to include. The indices correspond
  //            to the index you would obtain from calling
  //            SchemaDescriptor::Column
  // \param[out] the deserializer nodes
  // \param[in] if specified, the nodes will appear in the result
  //            list based on the order in which they appeared in
  //            column_indices. For example, if the Parquet schema has two
  //            primitive nodes and the column_indices given is {1, 0},
  //            then the resulting table will have the fields reversed
  //            in the Arrow schema.
  // \return error status if all of the column indices were out of bounds
  //         or any of the converted/physical type specifications in the
  //         Parquet schema are unsupported
  Status BuildNodes(std::vector<int> const& column_indices,
                    std::vector<std::unique_ptr<DeserializerNode>>& nodes,
                    bool in_index_order = true);

  // \brief builds a deserializer node based on the Parquet node.
  // \param[in] the parquet node
  // \param[in] indices that are used to filter indicate which
  //            primitive columns to include. The indices correspond
  //            to the index you would obtain from calling
  //            SchemaDescriptor::Column
  // \param[in] these indicate which indices were used in building
  //            this node. This is used to re-order the arrays in
  //            the schema of the deserialized Table for that it
  //            matches the order in which the indices were specified.
  // \param[out] the deserializer node
  // \param[in] repetition levels seen so far. Since this function is
  //            called recursively for group nodes, the value is
  //            incremented as the tree is traversed. Not specifying
  //            this value in the call should only be done when the
  //            node is the root node.
  // \param[in] definition levels seen so far. Used in the same manner
  //            as the repetition levels.
  // \return error status if all of the column indices were out of bounds
  //         or any of the converted/physical type specifications in the
  //         Parquet schema are unsupported
  Status BuildNode(std::shared_ptr<const Node> const& node,
                   std::vector<int> const& indices, std::vector<int>& indices_seen,
                   std::unique_ptr<DeserializerNode>& result,
                   RepetitionLevel repetition_levels = RepetitionLevel(),
                   DefinitionLevel definition_levels = DefinitionLevel());

  // \brief builds a primitive deserializer node based on the Parquet node.
  // \param[in] the parquet node, which must point to a PrimitiveNode
  // \param[in] indices that are used to filter indicate which
  //            primitive columns to include. The indices correspond
  //            to the index you would obtain from calling
  //            SchemaDescriptor::Column
  // \param[in] these indicate which indices were used in building
  //            this node. This is used to re-order the arrays in
  //            the schema of the deserialized Table for that it
  //            matches the order in which the indices were specified.
  // \param[out] the deserializer node
  // \param[in] repetition levels seen so far. Since this function is
  //            called recursively for group nodes, the value is
  //            incremented as the tree is traversed. Not specifying
  //            this value in the call should only be done when the
  //            node is the root node.
  // \param[in] definition levels seen so far. Used in the same manner
  //            as the repetition levels.
  // \return error status if all of the column indices were out of bounds
  //         or any of the converted/physical type specifications in the
  //         Parquet schema are unsupported
  Status BuildPrimitiveNode(std::shared_ptr<const Node> const& node,
                            RepetitionLevel repetition_levels,
                            DefinitionLevel definition_levels,
                            std::vector<int> const& indices,
                            std::vector<int>& indices_seen,
                            std::unique_ptr<DeserializerNode>& result);

  // \brief builds a group deserializer node based on the Parquet node.
  // \param[in] the parquet node, which must point to a GroupNode
  // \param[in] indices that are used to filter indicate which
  //            primitive columns to include. The indices correspond
  //            to the index you would obtain from calling
  //            SchemaDescriptor::Column
  // \param[in] these indicate which indices were used in building
  //            this node. This is used to re-order the arrays in
  //            the schema of the deserialized Table for that it
  //            matches the order in which the indices were specified.
  // \param[out] the deserializer node
  // \param[in] repetition levels seen so far. Since this function is
  //            called recursively for group nodes, the value is
  //            incremented as the tree is traversed. Not specifying
  //            this value in the call should only be done when the
  //            node is the root node.
  // \param[in] definition levels seen so far. Used in the same manner
  //            as the repetition levels.
  // \return error status if all of the column indices were out of bounds
  //         or any of the converted/physical type specifications in the
  //         Parquet schema are unsupported
  Status BuildGroupNode(std::shared_ptr<const Node> const& node,
                        RepetitionLevel repetition_levels,
                        DefinitionLevel definition_levels,
                        std::vector<int> const& indices, std::vector<int>& indices_seen,
                        std::unique_ptr<DeserializerNode>& result);

  // \brief builds a list deserializer node based on the Parquet node.
  // \param[in] the parquet node, which must point to a GroupNode which
  //            has a LIST annotation.
  // \param[in] indices that are used to filter indicate which
  //            primitive columns to include. The indices correspond
  //            to the index you would obtain from calling
  //            SchemaDescriptor::Column
  // \param[in] these indicate which indices were used in building
  //            this node. This is used to re-order the arrays in
  //            the schema of the deserialized Table for that it
  //            matches the order in which the indices were specified.
  // \param[out] the deserializer node
  // \param[in] repetition levels seen so far. Since this function is
  //            called recursively for group nodes, the value is
  //            incremented as the tree is traversed. Not specifying
  //            this value in the call should only be done when the
  //            node is the root node.
  // \param[in] definition levels seen so far. Used in the same manner
  //            as the repetition levels.
  // \return error status if all of the column indices were out of bounds
  //         or any of the converted/physical type specifications in the
  //         Parquet schema are unsupported
  Status BuildListNode(std::shared_ptr<const GroupNode> const& node,
                       RepetitionLevel repetition_levels,
                       DefinitionLevel definition_levels, std::vector<int> const& indices,
                       std::vector<int>& indices_seen,
                       std::unique_ptr<DeserializerNode>& result);

  // \brief builds a typed primitive node. This is the main entry point
  //        for a number of SFINAE based calls.
  // \param[in] the parquet node, which must point to a PrimitiveNode
  // \param[in] the column descriptor, which is used in error
  //            messages to report what node had an unsupported type
  // \parma[in] the index of the column, which is stored in the node
  //            and then later used to lookup the column reader from
  //            the row group reader
  // \param[out] the deserializer node
  // \return error status if any of the converted/physical type
  //         specifications in the Parquet schema are unsupported
  template <PrimitiveSerializerType SerializerType>
  Status MakeTypedPrimitiveDeserializerNode(
      std::shared_ptr<const Node> const& node,
      const ColumnDescriptor* const column_descriptor, int column_index,
      std::unique_ptr<DeserializerNode>& result) {
    auto primitive_node = std::static_pointer_cast<const PrimitiveNode>(node);
    switch (primitive_node->physical_type()) {
      case Type::BOOLEAN:
        return MakeLogicalPrimitiveDeserializerNode<parquet::BooleanType, SerializerType>(
            primitive_node, column_descriptor, column_index, result);
      case Type::INT32:
        return MakeLogicalPrimitiveDeserializerNode<parquet::Int32Type, SerializerType>(
            primitive_node, column_descriptor, column_index, result);
      case Type::INT64:
        return MakeLogicalPrimitiveDeserializerNode<parquet::Int64Type, SerializerType>(
            primitive_node, column_descriptor, column_index, result);
      case Type::INT96:
        return MakeLogicalPrimitiveDeserializerNode<parquet::Int96Type, SerializerType>(
            primitive_node, column_descriptor, column_index, result);
      case Type::FLOAT:
        return MakeLogicalPrimitiveDeserializerNode<parquet::FloatType, SerializerType>(
            primitive_node, column_descriptor, column_index, result);
      case Type::DOUBLE:
        return MakeLogicalPrimitiveDeserializerNode<parquet::DoubleType, SerializerType>(
            primitive_node, column_descriptor, column_index, result);
      case Type::BYTE_ARRAY:
        return MakeLogicalPrimitiveDeserializerNode<parquet::ByteArrayType,
                                                    SerializerType>(
            primitive_node, column_descriptor, column_index, result);
      case Type::FIXED_LEN_BYTE_ARRAY:
        return MakeLogicalPrimitiveDeserializerNode<parquet::FLBAType, SerializerType>(
            primitive_node, column_descriptor, column_index, result);
      default:
        std::ostringstream stream;
        stream << "Unrecognized type when creating a primitive node reader";
        return Status::Invalid(stream.str());
    }
  }

  // \brief builds a typed primitive node, taking into account it's
  //        logical (aka converted) type.
  // \param[in] the parquet node, which must point to a PrimitiveNode
  // \param[in] the column descriptor, which is used in error
  //            messages to report what node had an unsupported type
  // \parma[in] the index of the column, which is stored in the node
  //            and then later used to lookup the column reader from
  //            the row group reader
  // \param[out] the deserializer node
  // \return error status if any of the converted/physical type
  //         specifications in the Parquet schema are unsupported
  template <typename ParquetType, PrimitiveSerializerType SerializerType>
  Status MakeLogicalPrimitiveDeserializerNode(
      std::shared_ptr<const PrimitiveNode> const& node,
      const ColumnDescriptor* const column_descriptor, int column_index,
      std::unique_ptr<DeserializerNode>& result) {
    switch (node->logical_type()) {
      case parquet::LogicalType::DATE:
        return MakeDateDeserializerNode<ParquetType, SerializerType>(
            column_descriptor, column_index, result);
      case parquet::LogicalType::TIME_MILLIS:
        return MakeTimeMillisDeserializerNode<ParquetType, SerializerType>(
            column_descriptor, column_index, result);
      case parquet::LogicalType::TIME_MICROS:
        return MakeTimeMicrosDeserializerNode<ParquetType, SerializerType>(
            column_descriptor, column_index, result);
      case parquet::LogicalType::TIMESTAMP_MILLIS:
        return MakeTimestampDeserializerNode<ParquetType, SerializerType>(
            column_descriptor, column_index, ::arrow::TimeUnit::MILLI, result);
      case parquet::LogicalType::TIMESTAMP_MICROS:
        return MakeTimestampDeserializerNode<ParquetType, SerializerType>(
            column_descriptor, column_index, ::arrow::TimeUnit::MICRO, result);
      case parquet::LogicalType::DECIMAL:
        return MakeDecimalDeserializerNode<ParquetType, SerializerType>(
            column_descriptor, column_index, result);
      case parquet::LogicalType::UINT_8:
        return MakeIntegerDeserializerNode<ParquetType, ::arrow::UInt8Builder,
                                           SerializerType>(column_descriptor,
                                                           column_index, result);
      case parquet::LogicalType::UINT_16:
        return MakeIntegerDeserializerNode<ParquetType, ::arrow::UInt16Builder,
                                           SerializerType>(column_descriptor,
                                                           column_index, result);
      case parquet::LogicalType::UINT_32:
        return MakeIntegerDeserializerNode<ParquetType, ::arrow::UInt32Builder,
                                           SerializerType>(column_descriptor,
                                                           column_index, result);
      case parquet::LogicalType::UINT_64:
        return MakeIntegerDeserializerNode<ParquetType, ::arrow::UInt64Builder,
                                           SerializerType>(column_descriptor,
                                                           column_index, result);
      case parquet::LogicalType::INT_8:
        return MakeIntegerDeserializerNode<ParquetType, ::arrow::Int8Builder,
                                           SerializerType>(column_descriptor,
                                                           column_index, result);
      case parquet::LogicalType::INT_16:
        return MakeIntegerDeserializerNode<ParquetType, ::arrow::Int16Builder,
                                           SerializerType>(column_descriptor,
                                                           column_index, result);
      case parquet::LogicalType::INT_32:
        return MakeIntegerDeserializerNode<ParquetType, ::arrow::Int32Builder,
                                           SerializerType>(column_descriptor,
                                                           column_index, result);
      case parquet::LogicalType::INT_64:
        return MakeIntegerDeserializerNode<ParquetType, ::arrow::Int64Builder,
                                           SerializerType>(column_descriptor,
                                                           column_index, result);
      case parquet::LogicalType::UTF8:
        return MakeUTF8DeserializerNode<ParquetType, SerializerType>(
            column_descriptor, column_index, result);
      case parquet::LogicalType::NA:
        return MakeNullDeserializerNode<ParquetType, SerializerType>(
            column_descriptor, column_index, result);
      default:
        switch (node->physical_type()) {
          case Type::BOOLEAN:
            return MakeBooleanDeserializerNode<ParquetType, SerializerType>(
                column_descriptor, column_index, result);
          case Type::INT32:
            return MakeIntegerDeserializerNode<ParquetType, ::arrow::Int32Builder,
                                               SerializerType>(column_descriptor,
                                                               column_index, result);
          case Type::INT64:
            return MakeIntegerDeserializerNode<ParquetType, ::arrow::Int64Builder,
                                               SerializerType>(column_descriptor,
                                                               column_index, result);
          case Type::FLOAT:
            return MakeFloatDeserializerNode<ParquetType, SerializerType>(
                column_descriptor, column_index, result);
          case Type::DOUBLE:
            return MakeDoubleDeserializerNode<ParquetType, SerializerType>(
                column_descriptor, column_index, result);
          case Type::BYTE_ARRAY:
            return MakeByteArrayDeserializerNode<ParquetType, SerializerType>(
                column_descriptor, column_index, result);
          case Type::FIXED_LEN_BYTE_ARRAY:
            return MakeFixedLengthByteArrayDeserializerNode<ParquetType, SerializerType>(
                column_descriptor, column_index, result);
          case Type::INT96:
            return MakeImpalaTimestampDeserializerNode<ParquetType, SerializerType>(
                column_descriptor, column_index, result);
          default:
            std::ostringstream stream;
            stream << "Unsupported physical type " << node->physical_type() << std::endl;
            return Status::Invalid(stream.str());
        }
    }
  }

  //
  // The functions below create the various logical types of primitive nodes.
  // Since the ParquetType template parameter is determined at run-time and we
  // bind that earlier in the call stack, we can use SFINAE to limit the
  // allowable types for a particular field (e.g. - Timestamp types can only
  // be constructed from an int64 physical type).
  //
  // Most of the code is boiler-plate, except for the fact that there are
  // different builders for the various types.

  template <typename ParquetType, PrimitiveSerializerType SerializerType>
  Status MakeNullDeserializerNode(const ColumnDescriptor* const column_descriptor,
                                  int column_index,
                                  std::unique_ptr<DeserializerNode>& result) {
    return MakeTypedPrimitiveNodeWithBuilder<ParquetType, ::arrow::NullBuilder,
                                             SerializerType>(column_descriptor,
                                                             column_index, result, pool_);
  }

  template <typename ParquetType, PrimitiveSerializerType SerializerType>
  typename std::enable_if<std::is_same<ParquetType, parquet::Int96Type>::value,
                          Status>::type
  MakeImpalaTimestampDeserializerNode(const ColumnDescriptor* const column_descriptor,
                                      int column_index,
                                      std::unique_ptr<DeserializerNode>& result) {
    auto arrow_type = std::make_shared<::arrow::TimestampType>(::arrow::TimeUnit::NANO);
    return MakeTypedPrimitiveNodeWithBuilder<ParquetType, ::arrow::TimestampBuilder,
                                             SerializerType>(
        column_descriptor, column_index, result, arrow_type, pool_);
  }

  template <typename ParquetType, PrimitiveSerializerType SerializerType>
  typename std::enable_if<!std::is_same<ParquetType, parquet::Int96Type>::value,
                          Status>::type
  MakeImpalaTimestampDeserializerNode(const ColumnDescriptor* const column_descriptor,
                                      int column_index,
                                      std::unique_ptr<DeserializerNode>& result) {
    std::ostringstream stream;
    stream << "Type mismatch: attempt to make a nano timestamp field for "
           << column_descriptor->path()->ToDotString();
    stream << ", but it was not an int96";
    return Status::Invalid(stream.str());
  }

  template <typename ParquetType>
  struct SupportsDecimal {
    static constexpr auto value =
        (std::is_same<ParquetType, parquet::Int32Type>::value ||
         std::is_same<ParquetType, parquet::Int64Type>::value ||
         std::is_same<ParquetType, parquet::ByteArrayType>::value ||
         std::is_same<ParquetType, parquet::FLBAType>::value);
  };

  template <typename ParquetType, PrimitiveSerializerType SerializerType>
  typename std::enable_if<SupportsDecimal<ParquetType>::value, Status>::type
  MakeDecimalDeserializerNode(const ColumnDescriptor* const column_descriptor,
                              int column_index,
                              std::unique_ptr<DeserializerNode>& result) {
    auto arrow_type = std::make_shared<::arrow::Decimal128Type>(
        column_descriptor->type_precision(), column_descriptor->type_scale());
    return MakeTypedPrimitiveNodeWithBuilder<ParquetType, ::arrow::Decimal128Builder,
                                             SerializerType>(
        column_descriptor, column_index, result, arrow_type, pool_);
  }

  template <typename ParquetType, PrimitiveSerializerType SerializerType>
  typename std::enable_if<!SupportsDecimal<ParquetType>::value, Status>::type
  MakeDecimalDeserializerNode(const ColumnDescriptor* const column_descriptor,
                              int column_index,
                              std::unique_ptr<DeserializerNode>& result) {
    std::ostringstream stream;
    stream << "Type mismatch: attempt to make a decimal field for "
           << column_descriptor->path()->ToDotString();
    stream << ", but it was not an int32, int64, or fixed length byte array ";
    return Status::Invalid(stream.str());
  }

  template <typename ParquetType, PrimitiveSerializerType SerializerType>
  typename std::enable_if<std::is_same<ParquetType, parquet::BooleanType>::value,
                          Status>::type
  MakeBooleanDeserializerNode(const ColumnDescriptor* const column_descriptor,
                              int column_index,
                              std::unique_ptr<DeserializerNode>& result) {
    return MakeTypedPrimitiveNodeWithBuilder<ParquetType, ::arrow::BooleanBuilder,
                                             SerializerType>(column_descriptor,
                                                             column_index, result, pool_);
  }

  template <typename ParquetType, PrimitiveSerializerType SerializerType>
  typename std::enable_if<!std::is_same<ParquetType, parquet::BooleanType>::value,
                          Status>::type
  MakeBooleanDeserializerNode(const ColumnDescriptor* const column_descriptor,
                              int column_index,
                              std::unique_ptr<DeserializerNode>& result) {
    std::ostringstream stream;
    stream << "Type mismatch: attempt to make a boolean field for "
           << column_descriptor->path()->ToDotString();
    stream << ", but it was not an boolean ";
    return Status::Invalid(stream.str());
  }

  template <typename ParquetType, PrimitiveSerializerType SerializerType>
  typename std::enable_if<std::is_same<ParquetType, parquet::Int32Type>::value,
                          Status>::type
  MakeDateDeserializerNode(const ColumnDescriptor* const column_descriptor,
                           int column_index, std::unique_ptr<DeserializerNode>& result) {
    return MakeTypedPrimitiveNodeWithBuilder<ParquetType, ::arrow::Date32Builder,
                                             SerializerType>(column_descriptor,
                                                             column_index, result, pool_);
  }

  template <typename ParquetType, PrimitiveSerializerType SerializerType>
  typename std::enable_if<!std::is_same<ParquetType, parquet::Int32Type>::value,
                          Status>::type
  MakeDateDeserializerNode(const ColumnDescriptor* const column_descriptor,
                           int column_index, std::unique_ptr<DeserializerNode>& result) {
    std::ostringstream stream;
    stream << "Type mismatch: attempt to make a TIME_MILLIS field for "
           << column_descriptor->path()->ToDotString();
    stream << ", but it was not an int32 ";
    return Status::Invalid(stream.str());
  }

  template <typename ParquetType, PrimitiveSerializerType SerializerType>
  typename std::enable_if<std::is_same<ParquetType, parquet::Int32Type>::value,
                          Status>::type
  MakeTimeMillisDeserializerNode(const ColumnDescriptor* const column_descriptor,
                                 int column_index,
                                 std::unique_ptr<DeserializerNode>& result) {
    auto arrow_type = std::make_shared<::arrow::Time32Type>(::arrow::TimeUnit::MILLI);
    return MakeTypedPrimitiveNodeWithBuilder<ParquetType, ::arrow::Time32Builder,
                                             SerializerType>(
        column_descriptor, column_index, result, arrow_type, pool_);
  }

  template <typename ParquetType, PrimitiveSerializerType SerializerType>
  typename std::enable_if<!std::is_same<ParquetType, parquet::Int32Type>::value,
                          Status>::type
  MakeTimeMillisDeserializerNode(const ColumnDescriptor* const column_descriptor,
                                 int column_index,
                                 std::unique_ptr<DeserializerNode>& result) {
    std::ostringstream stream;
    stream << "Type mismatch: attempt to make a TIME_MILLIS field for "
           << column_descriptor->path()->ToDotString();
    stream << ", but it was not an int32 ";
    return Status::Invalid(stream.str());
  }

  template <typename ParquetType, PrimitiveSerializerType SerializerType>
  typename std::enable_if<std::is_same<ParquetType, parquet::Int64Type>::value,
                          Status>::type
  MakeTimeMicrosDeserializerNode(const ColumnDescriptor* const column_descriptor,
                                 int column_index,
                                 std::unique_ptr<DeserializerNode>& result) {
    auto arrow_type = std::make_shared<::arrow::Time64Type>(::arrow::TimeUnit::MICRO);
    return MakeTypedPrimitiveNodeWithBuilder<ParquetType, ::arrow::Time64Builder,
                                             SerializerType>(
        column_descriptor, column_index, result, arrow_type, pool_);
  }

  template <typename ParquetType, PrimitiveSerializerType SerializerType>
  typename std::enable_if<!std::is_same<ParquetType, parquet::Int64Type>::value,
                          Status>::type
  MakeTimeMicrosDeserializerNode(const ColumnDescriptor* const column_descriptor,
                                 int column_index,
                                 std::unique_ptr<DeserializerNode>& result) {
    std::ostringstream stream;
    stream << "Type mismatch: attempt to make a TIME_MICROS field for "
           << column_descriptor->path()->ToDotString();
    stream << ", but it was not an int64";
    return Status::Invalid(stream.str());
  }

  template <typename ParquetType, PrimitiveSerializerType SerializerType>
  typename std::enable_if<std::is_same<ParquetType, parquet::Int64Type>::value,
                          Status>::type
  MakeTimestampDeserializerNode(const ColumnDescriptor* const column_descriptor,
                                int column_index, ::arrow::TimeUnit::type time_unit,
                                std::unique_ptr<DeserializerNode>& result) {
    auto arrow_type = std::make_shared<::arrow::TimestampType>(time_unit);
    return MakeTypedPrimitiveNodeWithBuilder<ParquetType, ::arrow::TimestampBuilder,
                                             SerializerType>(
        column_descriptor, column_index, result, arrow_type, pool_);
  }

  template <typename ParquetType, PrimitiveSerializerType SerializerType>
  typename std::enable_if<!std::is_same<ParquetType, parquet::Int64Type>::value,
                          Status>::type
  MakeTimestampDeserializerNode(const ColumnDescriptor* const column_descriptor,
                                int column_index, ::arrow::TimeUnit::type time_unit,
                                std::unique_ptr<DeserializerNode>& result) {
    std::ostringstream stream;
    stream << "Type mismatch: attempt to make a TIMESTAMP field for "
           << column_descriptor->path()->ToDotString();
    stream << ", but it was not an int64";
    return Status::Invalid(stream.str());
  }

  template <typename ParquetType, typename ArrayBuilderType>
  struct CanCastInteger {
    static constexpr auto parquet_is_int =
        std::is_integral<typename ParquetType::c_type>::value;

    static constexpr auto arrow_is_int =
        std::is_integral<typename ArrayBuilderType::value_type>::value;

    static constexpr auto parquet_size = sizeof(typename ParquetType::c_type);

    static constexpr auto arrow_size = sizeof(typename ArrayBuilderType::value_type);

    static constexpr auto both_ints = parquet_is_int && arrow_is_int;

    static constexpr auto value = (both_ints && parquet_size >= arrow_size);
  };

  template <typename ParquetType, typename ArrayBuilderType,
            PrimitiveSerializerType SerializerType>
  typename std::enable_if<CanCastInteger<ParquetType, ArrayBuilderType>::value,
                          Status>::type
  MakeIntegerDeserializerNode(const ColumnDescriptor* const column_descriptor,
                              int column_index, std::unique_ptr<DeserializerNode>& result)

  {
    return MakeTypedPrimitiveNodeWithBuilder<ParquetType, ArrayBuilderType,
                                             SerializerType>(column_descriptor,
                                                             column_index, result, pool_);
  }

  template <typename ParquetType, typename ArrayBuilderType,
            PrimitiveSerializerType SerializerType>
  typename std::enable_if<!CanCastInteger<ParquetType, ArrayBuilderType>::value,
                          Status>::type
  MakeIntegerDeserializerNode(const ColumnDescriptor* const column_descriptor,
                              int column_index,
                              std::unique_ptr<DeserializerNode>& result) {
    std::ostringstream stream;
    stream << "Type mismatch: attempt to make an integer field for "
           << column_descriptor->path()->ToDotString();
    stream << ", but it was not an integer or not the correct size ";
    return Status::Invalid(stream.str());
  }

  template <typename ParquetType, PrimitiveSerializerType SerializerType>
  typename std::enable_if<std::is_same<ParquetType, parquet::ByteArrayType>::value,
                          Status>::type
  MakeUTF8DeserializerNode(const ColumnDescriptor* const column_descriptor,
                           int column_index, std::unique_ptr<DeserializerNode>& result)

  {
    return MakeTypedPrimitiveNodeWithBuilder<ParquetType, ::arrow::StringBuilder,
                                             SerializerType>(column_descriptor,
                                                             column_index, result, pool_);
  }

  template <typename ParquetType, PrimitiveSerializerType SerializerType>
  typename std::enable_if<!std::is_same<ParquetType, parquet::ByteArrayType>::value,
                          Status>::type
  MakeUTF8DeserializerNode(const ColumnDescriptor* const column_descriptor,
                           int column_index, std::unique_ptr<DeserializerNode>& result) {
    std::ostringstream stream;
    stream << "Type mismatch: attempt to make an UTF8 field for "
           << column_descriptor->path()->ToDotString();
    stream << ", but it was not a byte array";
    return Status::Invalid(stream.str());
  }

  template <typename ParquetType, PrimitiveSerializerType SerializerType>
  typename std::enable_if<std::is_same<ParquetType, parquet::ByteArrayType>::value,
                          Status>::type
  MakeByteArrayDeserializerNode(const ColumnDescriptor* const column_descriptor,
                                int column_index,
                                std::unique_ptr<DeserializerNode>& result)

  {
    return MakeTypedPrimitiveNodeWithBuilder<ParquetType, ::arrow::BinaryBuilder,
                                             SerializerType>(column_descriptor,
                                                             column_index, result, pool_);
  }

  template <typename ParquetType, PrimitiveSerializerType SerializerType>
  typename std::enable_if<!std::is_same<ParquetType, parquet::ByteArrayType>::value,
                          Status>::type
  MakeByteArrayDeserializerNode(const ColumnDescriptor* const column_descriptor,
                                int column_index,
                                std::unique_ptr<DeserializerNode>& result) {
    std::ostringstream stream;
    stream << "Type mismatch: attempt to make a byte array field for "
           << column_descriptor->path()->ToDotString();
    stream << ", but it was not a byte array";
    return Status::Invalid(stream.str());
  }

  template <typename ParquetType, PrimitiveSerializerType SerializerType>
  typename std::enable_if<std::is_same<ParquetType, parquet::FLBAType>::value,
                          Status>::type
  MakeFixedLengthByteArrayDeserializerNode(
      const ColumnDescriptor* const column_descriptor, int column_index,
      std::unique_ptr<DeserializerNode>& result)

  {
    auto arrow_type =
        std::make_shared<::arrow::FixedSizeBinaryType>(column_descriptor->type_length());
    return MakeTypedPrimitiveNodeWithBuilder<ParquetType, ::arrow::FixedSizeBinaryBuilder,
                                             SerializerType>(
        column_descriptor, column_index, result, arrow_type, pool_);
  }

  template <typename ParquetType, PrimitiveSerializerType SerializerType>
  typename std::enable_if<!std::is_same<ParquetType, parquet::FLBAType>::value,
                          Status>::type
  MakeFixedLengthByteArrayDeserializerNode(
      const ColumnDescriptor* const column_descriptor, int column_index,
      std::unique_ptr<DeserializerNode>& result) {
    std::ostringstream stream;
    stream << "Type mismatch: attempt to make a byte array field for "
           << column_descriptor->path()->ToDotString();
    stream << ", but it was not a fixed length byte array";
    return Status::Invalid(stream.str());
  }

  template <typename ParquetType, PrimitiveSerializerType SerializerType>
  typename std::enable_if<std::is_same<ParquetType, parquet::DoubleType>::value,
                          Status>::type
  MakeDoubleDeserializerNode(const ColumnDescriptor* const column_descriptor,
                             int column_index, std::unique_ptr<DeserializerNode>& result)

  {
    return MakeTypedPrimitiveNodeWithBuilder<ParquetType, ::arrow::DoubleBuilder,
                                             SerializerType>(column_descriptor,
                                                             column_index, result, pool_);
  }

  template <typename ParquetType, PrimitiveSerializerType SerializerType>
  typename std::enable_if<!std::is_same<ParquetType, parquet::DoubleType>::value,
                          Status>::type
  MakeDoubleDeserializerNode(const ColumnDescriptor* const column_descriptor,
                             int column_index,
                             std::unique_ptr<DeserializerNode>& result) {
    std::ostringstream stream;
    stream << "Type mismatch: attempt to make a double field for "
           << column_descriptor->path()->ToDotString();
    stream << ", but it was not a double type";
    return Status::Invalid(stream.str());
  }

  template <typename ParquetType, PrimitiveSerializerType SerializerType>
  typename std::enable_if<std::is_same<ParquetType, parquet::FloatType>::value,
                          Status>::type
  MakeFloatDeserializerNode(const ColumnDescriptor* const column_descriptor,
                            int column_index, std::unique_ptr<DeserializerNode>& result)

  {
    return MakeTypedPrimitiveNodeWithBuilder<ParquetType, ::arrow::FloatBuilder,
                                             SerializerType>(column_descriptor,
                                                             column_index, result, pool_);
  }

  template <typename ParquetType, PrimitiveSerializerType SerializerType>
  typename std::enable_if<!std::is_same<ParquetType, parquet::FloatType>::value,
                          Status>::type
  MakeFloatDeserializerNode(const ColumnDescriptor* const column_descriptor,
                            int column_index, std::unique_ptr<DeserializerNode>& result) {
    std::ostringstream stream;
    stream << "Type mismatch: attempt to make a float field for "
           << column_descriptor->path()->ToDotString();
    stream << ", but it was not a float type";
    return Status::Invalid(stream.str());
  }

  // \brief The primary function for creating a primitive node. This will
  //        create an array builder of the specified type, passing
  //        args to its constructor, and finally pass that builder
  //        to the TypedPrimitiveDeserializerNode constructor
  // \param[in] the column descriptor, which is used in error
  //            messages to report what node had an unsupported type
  // \parma[in] the index of the column, which is stored in the node
  //            and then later used to lookup the column reader from
  //            the row group reader
  // \param[in] arguments that are passed to the BuilderType's
  //            constructor
  // \param[out] the deserializer node
  // \return error status if any of the converted/physical type
  //         specifications in the Parquet schema are unsupported
  template <typename ParquetType, typename ArrayBuilderType,
            PrimitiveSerializerType SerializerType, typename... Args>
  Status MakeTypedPrimitiveNodeWithBuilder(
      const ColumnDescriptor* const column_descriptor, int column_index,
      std::unique_ptr<DeserializerNode>& result, Args&&... args) {
    auto builder = std::make_shared<ArrayBuilderType>(std::forward<Args>(args)...);
    using ReturnType = TypedPrimitiveDeserializerNode<TypedColumnReader<ParquetType>,
                                                      ArrayBuilderType, SerializerType>;
    auto node = std::unique_ptr<ReturnType>(
        new ReturnType(column_descriptor, column_index, buffer_size_, builder, pool_));
    RETURN_NOT_OK(node->Initialize());
    result = std::move(node);
    return Status::OK();
  }

  // This method is pulled out here instead of inside BuildNode because
  // we skip the "middle" node when constructing List nodes.
  void IncrementLevels(std::shared_ptr<const Node> const& node,
                       RepetitionLevel& repetition_levels,
                       DefinitionLevel& definition_levels) {
    if (!node->is_required()) {
      ++definition_levels;
    }
    if (node->is_repeated()) {
      ++repetition_levels;
    }
  }

  std::shared_ptr<::parquet::ParquetFileReader> file_reader_;

  ::arrow::MemoryPool* pool_;

  int64_t buffer_size_;
};

DeserializerBuilder::Impl::Impl(
    std::shared_ptr<::parquet::ParquetFileReader> const& file_reader,
    ::arrow::MemoryPool* pool, int64_t buffer_size)
    : file_reader_(file_reader), pool_(pool), buffer_size_(buffer_size) {}

::arrow::Status DeserializerBuilder::Impl::BuildSchemaNodeDeserializer(
    int schema_index, std::unique_ptr<ArrayDeserializer>& result) {
  std::vector<std::unique_ptr<DeserializerNode>> nodes;
  // Pass false to in_index_order so that the nodes are created
  // in schema order so we can look it up that way
  auto const column_indices = FillColumnIndices();
  RETURN_NOT_OK(BuildNodes(column_indices, nodes, false /* in_index_order */));
  if (static_cast<decltype(nodes.size())>(schema_index) >= nodes.size()) {
    std::ostringstream stream;
    stream << "Could not find schema node with index ";
    stream << schema_index << ", there were only " << nodes.size();
    stream << "nodes";
    return Status::Invalid(stream.str());
  }
  // nodes is guaranteed to be non-empty after calling BuildNodes
  result.reset(new FileArrayDeserializer(std::move(nodes[schema_index]), file_reader_));
  return Status::OK();
}

::arrow::Status DeserializerBuilder::Impl::BuildColumnDeserializer(
    int column_index, std::unique_ptr<ArrayDeserializer>& result) {
  std::vector<std::unique_ptr<DeserializerNode>> nodes;
  RETURN_NOT_OK(BuildNodes({column_index}, nodes));
  // nodes is guaranteed to be non-empty after calling BuildNodes
  if (nodes.size() > 1) {
    std::ostringstream stream;
    stream << "Unable to build a deserializer for column index ";
    stream << column_index << ", multiple deserializer nodes ";
    stream << "were created";
    return Status::Invalid(stream.str());
  }
  result.reset(new FileArrayDeserializer(std::move(nodes[0]), file_reader_));
  return Status::OK();
}

::arrow::Status DeserializerBuilder::Impl::BuildArrayBatchedDeserializer(
    int column_index, std::unique_ptr<ArrayBatchedDeserializer>& result) {
  std::vector<std::unique_ptr<DeserializerNode>> nodes;
  RETURN_NOT_OK(BuildNodes({column_index}, nodes));
  // nodes is guaranteed to be non-empty after calling BuildNodes
  if (nodes.size() > 1) {
    std::ostringstream stream;
    stream << "Unable to build an array batched deserializer for column index ";
    stream << column_index << ", multiple deserializer nodes ";
    stream << "were created";
    return Status::Invalid(stream.str());
  }
  std::unique_ptr<FileArrayBatchedDeserializer> deserializer(
      new FileArrayBatchedDeserializer(std::move(nodes[0]), file_reader_));
  RETURN_NOT_OK(deserializer->Initialize());
  result = std::move(deserializer);
  return Status::OK();
}

::arrow::Status DeserializerBuilder::Impl::BuildColumnChunkDeserializer(
    int column_index, int row_group_index, std::unique_ptr<ArrayDeserializer>& result) {
  std::vector<std::unique_ptr<DeserializerNode>> nodes;
  RETURN_NOT_OK(BuildNodes({column_index}, nodes));
  // nodes is guaranteed to be non-empty after calling BuildNodes
  if (nodes.size() > 1) {
    std::ostringstream stream;
    stream << "Unable to build a deserializer for column chunk ";
    stream << "with column index " << column_index << " and ";
    stream << "row group " << row_group_index << ", multiple deserializer nodes ";
    stream << "were created";
    return Status::Invalid(stream.str());
  }
  std::unique_ptr<ColumnChunkDeserializer> deserializer(new ColumnChunkDeserializer(
      std::move(nodes[0]), file_reader_->RowGroup(row_group_index)));
  RETURN_NOT_OK(deserializer->Initialize());
  result = std::move(deserializer);
  return Status::OK();
}

::arrow::Status DeserializerBuilder::Impl::BuildRowGroupDeserializer(
    int row_group_index, const std::vector<int>& indices,
    std::unique_ptr<TableDeserializer>& result) {
  std::vector<std::unique_ptr<DeserializerNode>> nodes;
  RETURN_NOT_OK(BuildNodes(indices, nodes));
  std::shared_ptr<::arrow::Schema> schema;
  RETURN_NOT_OK(FromParquetSchema(file_reader_->metadata()->schema(), indices, &schema));
  result.reset(new RowGroupDeserializer(std::move(nodes), schema,
                                        file_reader_->RowGroup(row_group_index)));
  return Status::OK();
}

::arrow::Status DeserializerBuilder::Impl::BuildFileDeserializer(
    const std::vector<int>& column_indices, std::unique_ptr<TableDeserializer>& result) {
  std::vector<std::unique_ptr<DeserializerNode>> nodes;
  RETURN_NOT_OK(BuildNodes(column_indices, nodes));
  std::shared_ptr<::arrow::Schema> schema;
  RETURN_NOT_OK(
      FromParquetSchema(file_reader_->metadata()->schema(), column_indices, &schema));
  result.reset(new FileTableDeserializer(std::move(nodes), schema, file_reader_));
  return Status::OK();
}

Status DeserializerBuilder::Impl::BuildNodes(
    std::vector<int> const& indices,
    std::vector<std::unique_ptr<DeserializerNode>>& result, bool in_index_order) {
  auto node = std::const_pointer_cast<const Node>(
      file_reader_->metadata()->schema()->schema_root());
  if (!node->is_group()) {
    std::ostringstream stream;
    stream << "Expected a group node at the root of the schema, ";
    stream << "but did not encounter one";
    return Status::Invalid(stream.str());
  }
  auto group_node = std::static_pointer_cast<const GroupNode>(node);
  using location = decltype(indices.begin() - indices.end());
  using reader_and_indices = std::tuple<std::unique_ptr<DeserializerNode>, location>;
  std::vector<reader_and_indices> deserializers;
  for (int ii = 0; ii < group_node->field_count(); ++ii) {
    std::vector<int> indices_seen;
    std::unique_ptr<DeserializerNode> deserializer;
    RETURN_NOT_OK(BuildNode(group_node->field(ii), indices, indices_seen, deserializer));
    if (!deserializer) {
      continue;
    }
    auto earliest = indices.end();
    for (auto index_seen : indices_seen) {
      auto current = std::find(indices.begin(), indices.end(), index_seen);
      if (current < earliest) {
        earliest = current;
      }
    }
    if (earliest == indices.end()) {
      std::ostringstream stream;
      stream << "Could not determine the ordering of the fields";
      stream << " for " << group_node->field(ii)->path()->ToDotString();
      stream << " because the indices for the columns seen did not ";
      stream << " match any that were given";
      return Status::Invalid(stream.str());
    }
    deserializers.emplace_back(
        std::make_tuple(std::move(deserializer), earliest - indices.begin()));
  }
  if (deserializers.empty()) {
    std::ostringstream stream;
    stream << "Unable to match any of the columns to the specified indices: [";
    bool first = true;
    for (auto index : indices) {
      if (!first) {
        stream << ", ";
      } else {
        first = false;
      }
      stream << index;
    }
    stream << "]";
    return Status::Invalid(stream.str());
  }
  if (in_index_order) {
    std::sort(deserializers.begin(), deserializers.end(),
              [](reader_and_indices const& first, reader_and_indices const& second) {
                return std::get<1>(first) < std::get<1>(second);
              });
  }
  for (auto& current : deserializers) {
    result.emplace_back(std::move(std::get<0>(current)));
  }
  return Status::OK();
}

Status DeserializerBuilder::Impl::BuildNode(std::shared_ptr<const Node> const& node,
                                            std::vector<int> const& indices,
                                            std::vector<int>& indices_seen,
                                            std::unique_ptr<DeserializerNode>& result,
                                            RepetitionLevel repetition_levels,
                                            DefinitionLevel definition_levels) {
  IncrementLevels(node, repetition_levels, definition_levels);
  if (node->is_group()) {
    return BuildGroupNode(node, repetition_levels, definition_levels, indices,
                          indices_seen, result);
  } else {
    return BuildPrimitiveNode(node, repetition_levels, definition_levels, indices,
                              indices_seen, result);
  }
}

Status DeserializerBuilder::Impl::BuildPrimitiveNode(
    std::shared_ptr<const Node> const& node, RepetitionLevel repetition_levels,
    DefinitionLevel definition_levels, std::vector<int> const& indices,
    std::vector<int>& indices_seen, std::unique_ptr<DeserializerNode>& result) {
  auto const schema = file_reader_->metadata()->schema();
  auto column_index = schema->ColumnIndex(*node);
  auto found = std::find(indices.begin(), indices.end(), column_index);
  if (found == indices.end()) {
    // This was not specified in the list of indices, so it doesn't
    // show up in the resulting deserializer. Parent's will check for
    // null and possibly filter themselves out (e.g. - a struct with
    // no fields that match nodes with columns in indices)
    return Status::OK();
  }
  indices_seen.emplace_back(column_index);
  auto column_descriptor = schema->Column(column_index);
  if (*definition_levels > 0) {
    if (*repetition_levels > 0) {
      // The path to the node either contains only repeated nodes or
      // a mix of repeated and optional nodes.
      return MakeTypedPrimitiveDeserializerNode<PrimitiveSerializerType::REPEATED>(
          node, column_descriptor, column_index, result);
    } else {
      // A field that is either itself optional or a descendant of
      // an optional field (e.g. - member of a struct that is optional)
      // but does not have any ancestors in the tree that are repeated.
      // It therefore won't have any repetition levels.
      return MakeTypedPrimitiveDeserializerNode<PrimitiveSerializerType::OPTIONAL>(
          node, column_descriptor, column_index, result);
    }
  } else if (*repetition_levels == 0) {
    // A field that is always present (max_definition_level ==
    // 0 && max_repetition_level == 0) and therefore doesn't
    // need repetition levels or definition levels
    return MakeTypedPrimitiveDeserializerNode<PrimitiveSerializerType::REQUIRED>(
        node, column_descriptor, column_index, result);
  } else {
    // This should never happen since a non-zero max repetition level
    // should have a non-zero definition level
    std::ostringstream stream;
    stream << "Primitive node " << node->path()->ToDotString();
    stream << " has a max_repetition_level or ";
    stream << *definition_levels;
    stream << " and a max_definition_level of ";
    stream << *repetition_levels;
    return Status::Invalid(stream.str());
  }
  if (node->is_repeated()) {
    result.reset(new ListDeserializerNode(std::move(result), pool_, repetition_levels,
                                          definition_levels));
  }
  return Status::OK();
}

Status DeserializerBuilder::Impl::BuildGroupNode(
    std::shared_ptr<const Node> const& node, RepetitionLevel repetition_levels,
    DefinitionLevel definition_levels, std::vector<int> const& indices,
    std::vector<int>& indices_seen, std::unique_ptr<DeserializerNode>& result) {
  // TODO: implement a proper Visitor interface in Node
  auto group_node = std::static_pointer_cast<const GroupNode>(node);
  if (node->logical_type() == LogicalType::LIST) {
    return BuildListNode(group_node, repetition_levels, definition_levels, indices,
                         indices_seen, result);
  }
  std::vector<std::unique_ptr<DeserializerNode>> children;
  std::vector<std::shared_ptr<::arrow::Field>> fields;
  for (int ii = 0; ii < group_node->field_count(); ++ii) {
    auto child_node = group_node->field(static_cast<int>(ii));
    std::unique_ptr<DeserializerNode> child;
    RETURN_NOT_OK(BuildNode(child_node, indices, indices_seen, child, repetition_levels,
                            definition_levels));
    // Check to see if the child is valid. If so,
    // it or at least one of its descendants was in
    // the list of indices.
    if (child) {
      // TODO: deal with metadata args
      fields.emplace_back(std::make_shared<::arrow::Field>(
          child_node->name(), child->GetArrayBuilder()->type(),
          // If the child node is required,
          // then it's not nullable
          !child_node->is_required()));
      children.emplace_back(std::move(child));
    }
  }
  if (children.empty()) {
    // All of the descendants have been filtered out because
    // of the indices
    return Status::OK();
  }
  if (node->is_optional()) {
    result.reset(new NestedDeserializerNode<true>(definition_levels, std::move(children),
                                                  fields, pool_));
  } else {
    result.reset(new NestedDeserializerNode<false>(definition_levels, std::move(children),
                                                   fields, pool_));
  }
  if (node->is_repeated()) {
    std::ostringstream stream;
    stream << "Deserializing from Parquet repeated nodes to Arrow ";
    stream << "is not supported without a LIST annotation";
    return Status::Invalid(stream.str());
  }
  return Status::OK();
}

Status DeserializerBuilder::Impl::BuildListNode(
    std::shared_ptr<const GroupNode> const& node, RepetitionLevel repetition_levels,
    DefinitionLevel definition_levels, std::vector<int> const& indices,
    std::vector<int>& indices_seen, std::unique_ptr<DeserializerNode>& result) {
  //
  // A list node must have this schema:
  //
  // (required|optional) group outer {
  //     repeated group inner {
  //         (required|optiona) (group|primitive) element [{ children }]
  //     }
  //  }
  //
  // We construct a single node from outer and inner, using
  // the definition level from outer and the repetition level
  // from inner. This allows us to determine null lists when
  // outer is optional, and empty lists in both cases.
  if (node->is_repeated()) {
    std::ostringstream stream;
    stream << "List node " << node->path()->ToDotString();
    stream << " is repeated. List nodes can only be ";
    stream << " optional or required, and must contain ";
    stream << " a single child repeated group child node";
    return Status::Invalid(stream.str());
  }
  if (node->field_count() != 1) {
    std::ostringstream stream;
    stream << "List node " << node->path()->ToDotString();
    stream << " has multiple children, it should have";
    stream << " a single repeated group child node";
    return Status::Invalid(stream.str());
  }
  auto child_node = node->field(0);
  if (!child_node->is_repeated()) {
    std::ostringstream stream;
    stream << "List node " << node->path()->ToDotString();
    stream << " should have a single repeated group child node, but ";
    stream << " its child is not a repeated node";
    return Status::Invalid(stream.str());
  }
  if (!child_node->is_group()) {
    std::ostringstream stream;
    stream << "List node " << node->path()->ToDotString();
    stream << " should have a single repeated group child node, but ";
    stream << " its child is not a repeated primitive node";
    return Status::Invalid(stream.str());
  }

  // Since we've obtained the child node without calling
  // BuildNode, we need to call IncrementLevels before
  // building its children. We also save the definition level
  // before we increment them, so that we can use it if this
  // node is nullable.
  auto current_max_definition_level = definition_levels;
  IncrementLevels(child_node, repetition_levels, definition_levels);
  auto group_node = std::static_pointer_cast<const GroupNode>(child_node);
  if (group_node->field_count() > 1) {
    std::ostringstream stream;
    stream << "List node " << node->path()->ToDotString();
    stream << " should have a repeated group node as its child,";
    stream << " which has one child. However, the repeated group";
    stream << " node has " << group_node->field_count() << "children";
    return Status::Invalid(stream.str());
  }
  std::unique_ptr<DeserializerNode> child;
  RETURN_NOT_OK(BuildNode(group_node->field(0), indices, indices_seen, child,
                          repetition_levels, definition_levels));
  if (!child) {
    // The descendants have been filtered out based on indices
    // so we retrun OK with a null result so that caller will
    // know that this list has been filtered
    return Status::OK();
  }
  // If this node is required, then we defer the list building
  // to the child node, since the list cannot be nullable and
  // this node is effectively a pass through to the repeated
  // child node. However, if this node is optional, we need to
  // make a NullableListDeserializerNode, which will first check to
  // see if there are any primitives in its tree that have
  // a definition level equal to its max_definition_level. If
  // there aren't, then it writes a null and skips writing
  // any of its children.
  if (node->is_optional()) {
    result.reset(new NullableListDeserializerNode(
        std::move(child), pool_, repetition_levels, current_max_definition_level));
  } else {
    result.reset(new ListDeserializerNode(std::move(child), pool_, repetition_levels,
                                          current_max_definition_level));
  }
  return Status::OK();
}

DeserializerBuilder::DeserializerBuilder(
    std::shared_ptr<::parquet::ParquetFileReader> const& file_reader,
    ::arrow::MemoryPool* pool, int64_t buffer_size)
    : impl_(new Impl(file_reader, pool, buffer_size)) {}

DeserializerBuilder::~DeserializerBuilder() {}

::arrow::Status DeserializerBuilder::BuildSchemaNodeDeserializer(
    int schema_index, std::unique_ptr<ArrayDeserializer>& result) {
  return impl_->BuildSchemaNodeDeserializer(schema_index, result);
}

::arrow::Status DeserializerBuilder::BuildColumnDeserializer(
    int column_index, std::unique_ptr<ArrayDeserializer>& result) {
  return impl_->BuildColumnDeserializer(column_index, result);
}

::arrow::Status DeserializerBuilder::BuildColumnChunkDeserializer(
    int column_index, int row_group_index, std::unique_ptr<ArrayDeserializer>& result) {
  return impl_->BuildColumnChunkDeserializer(column_index, row_group_index, result);
}

::arrow::Status DeserializerBuilder::BuildRowGroupDeserializer(
    int row_group_index, const std::vector<int>& indices,
    std::unique_ptr<TableDeserializer>& result) {
  return impl_->BuildRowGroupDeserializer(row_group_index, indices, result);
}

::arrow::Status DeserializerBuilder::BuildFileDeserializer(
    const std::vector<int>& column_indices, std::unique_ptr<TableDeserializer>& result) {
  return impl_->BuildFileDeserializer(column_indices, result);
}

::arrow::Status DeserializerBuilder::BuildArrayBatchedDeserializer(
    int column_index, std::unique_ptr<ArrayBatchedDeserializer>& result) {
  return impl_->BuildArrayBatchedDeserializer(column_index, result);
}

}  // namespace arrow
}  // namespace parquet
