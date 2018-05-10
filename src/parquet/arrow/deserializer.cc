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

// Start: copied from reader.cc
static uint64_t BytesToInteger(const uint8_t* bytes, int32_t start, int32_t stop) {
  using ::arrow::BitUtil::FromBigEndian;

  const int32_t length = stop - start;

  DCHECK_GE(length, 0);
  DCHECK_LE(length, 8);

  switch (length) {
    case 0:
      return 0;
    case 1:
      return bytes[start];
    case 2:
      return FromBigEndian(*reinterpret_cast<const uint16_t*>(bytes + start));
    case 3: {
      const uint64_t first_two_bytes =
          FromBigEndian(*reinterpret_cast<const uint16_t*>(bytes + start));
      const uint64_t last_byte = bytes[stop - 1];
      return first_two_bytes << 8 | last_byte;
    }
    case 4:
      return FromBigEndian(*reinterpret_cast<const uint32_t*>(bytes + start));
    case 5: {
      const uint64_t first_four_bytes =
          FromBigEndian(*reinterpret_cast<const uint32_t*>(bytes + start));
      const uint64_t last_byte = bytes[stop - 1];
      return first_four_bytes << 8 | last_byte;
    }
    case 6: {
      const uint64_t first_four_bytes =
          FromBigEndian(*reinterpret_cast<const uint32_t*>(bytes + start));
      const uint64_t last_two_bytes =
          FromBigEndian(*reinterpret_cast<const uint16_t*>(bytes + start + 4));
      return first_four_bytes << 16 | last_two_bytes;
    }
    case 7: {
      const uint64_t first_four_bytes =
          FromBigEndian(*reinterpret_cast<const uint32_t*>(bytes + start));
      const uint64_t second_two_bytes =
          FromBigEndian(*reinterpret_cast<const uint16_t*>(bytes + start + 4));
      const uint64_t last_byte = bytes[stop - 1];
      return first_four_bytes << 24 | second_two_bytes << 8 | last_byte;
    }
    case 8:
      return FromBigEndian(*reinterpret_cast<const uint64_t*>(bytes + start));
    default: {
      DCHECK(false);
      return UINT64_MAX;
    }
  }
}

static constexpr int32_t kMinDecimalBytes = 1;
static constexpr int32_t kMaxDecimalBytes = 16;

/// \brief Convert a sequence of big-endian bytes to one int64_t (high bits) and one
/// uint64_t (low bits).
static void BytesToIntegerPair(const uint8_t* bytes,
                               const int32_t total_number_of_bytes_used, int64_t* high,
                               uint64_t* low) {
  DCHECK_GE(total_number_of_bytes_used, kMinDecimalBytes);
  DCHECK_LE(total_number_of_bytes_used, kMaxDecimalBytes);

  /// Bytes are coming in big-endian, so the first byte is the MSB and therefore holds the
  /// sign bit.
  const bool is_negative = static_cast<int8_t>(bytes[0]) < 0;

  /// Sign extend the low bits if necessary
  *low = UINT64_MAX * (is_negative && total_number_of_bytes_used < 8);
  *high = -1 * (is_negative && total_number_of_bytes_used < kMaxDecimalBytes);

  /// Stop byte of the high bytes
  const int32_t high_bits_offset = std::max(0, total_number_of_bytes_used - 8);

  /// Shift left enough bits to make room for the incoming int64_t
  *high <<= high_bits_offset * CHAR_BIT;

  /// Preserve the upper bits by inplace OR-ing the int64_t
  *high |= BytesToInteger(bytes, 0, high_bits_offset);

  /// Stop byte of the low bytes
  const int32_t low_bits_offset = std::min(total_number_of_bytes_used, 8);

  /// Shift left enough bits to make room for the incoming uint64_t
  *low <<= low_bits_offset * CHAR_BIT;

  /// Preserve the upper bits by inplace OR-ing the uint64_t
  *low |= BytesToInteger(bytes, high_bits_offset, total_number_of_bytes_used);
}

constexpr int64_t kJulianToUnixEpochDays = 2440588LL;
constexpr int64_t kMillisecondsInADay = 86400000LL;
constexpr int64_t kNanosecondsInADay = kMillisecondsInADay * 1000LL * 1000LL;

static inline int64_t impala_timestamp_to_nanoseconds(const Int96& impala_timestamp) {
  int64_t days_since_epoch = impala_timestamp.value[2] - kJulianToUnixEpochDays;
  int64_t nanoseconds = *(reinterpret_cast<const int64_t*>(&(impala_timestamp.value)));
  return days_since_epoch * kNanosecondsInADay + nanoseconds;
}
// End: copied from reader.cc

//
// This is a utility class that is used by primitive node deserializer
// nodes. It's similar to a std::list, but stores its data as a contiguous
// array of memory because it's passed to calls to ReadBatch.
template <typename T>
class PrimitiveDeserializerArray {
 public:
  PrimitiveDeserializerArray(int64_t capacity)
      :  // TODO: use memory pools and buffers
        data_(new T[capacity]),
        capacity_(capacity),
        size_(0),
        index_(0) {}

  ~PrimitiveDeserializerArray() { delete[] data_; }

  bool const Empty() const { return index_ >= size_; }

  T Front() const { return data_[index_]; }

  void PopFront() { ++index_; }

  void Reset() {
    index_ = 0;
    size_ = 0;
  }

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
  // \param[in] this is used to find out what the next set of
  //            repetitions will be. This is used by list
  //            (aka - repeated) nodes in their Deserialize method
  //            to determine whether or not they need to repeat
  //            and call Deserialize for their child nodes
  //            again before popping back to their parent node.
  // \param[in] the current repeition level. When this is first called
  //            when deserializing a row, it should be zero. It may be
  //            non-zero when a list node is calling a child node
  //            during a repetition. In those cases, a child node may
  //            do nothing because it's next value doesn't repeat at
  //            the current repeition level.
  virtual void Deserialize(LevelSet& next_repetitions,
                           int16_t current_repetition_level) = 0;

  // \brief Skips deserialization. This is used by group and list
  //        nodes when they are nullable, in which they call this
  //        method on their child nodes so that they do not append
  //        any values to their builders.
  virtual void Skip(LevelSet& next_repetitions) = 0;

  // \brief returns the maximum definition level of all of this
  //        node's descendants. In the case of a primitive node
  //        it will be the next definition level in the file (if
  //        one exists) or zero otherwise. In the case of a group
  //        node, it will simply take the maximum of all of its
  //        child nodes.
  // \return the maximum definition level for this nodes and its
  //         descendants
  virtual int16_t CurrentMaxDefinitionLevel() = 0;

  // \brief returns the array buidler that was used by this node
  //        to append values and null in calls to Deserialize
  // \return the array builder
  virtual std::shared_ptr<::arrow::ArrayBuilder> GetArrayBuilder() = 0;

  // \brief initializes this node for a new row group. This is used
  //        by primitive node deserializers to obtain a column reader
  //        for that row group
  virtual void InitializeRowGroup(
      std::shared_ptr<::parquet::RowGroupReader> const& row_group_reader) = 0;
};

template <typename ParquetType, typename BuilderType>
class PrimitiveAppender {
 public:
  template <typename ParquetValueType>
  void Append(const ColumnDescriptor* descriptor, BuilderType& builder,
              ParquetValueType value) {
    using value_type = typename BuilderType::value_type;
    builder.Append(static_cast<value_type>(value));
  }
};

template <typename BuilderType>
class PrimitiveAppender<::parquet::ByteArray, BuilderType> {
 public:
  template <typename ParquetValueType>
  void Append(const ColumnDescriptor* descriptor, BuilderType& builder,
              ParquetValueType value) {
    builder.Append(value.ptr, value.len);
  }
};

template <typename BuilderType>
class PrimitiveAppender<FLBA, BuilderType> {
 public:
  template <typename ParquetValueType>
  void Append(const ColumnDescriptor* descriptor, BuilderType& builder,
              ParquetValueType value) {
    builder.Append(value.ptr);
  }
};

template <>
class PrimitiveAppender<ByteArray, ::arrow::Decimal128Builder> {
 public:
  template <typename ParquetValueType>
  void Append(const ColumnDescriptor* descriptor, ::arrow::DecimalBuilder& builder,
              ParquetValueType value) {
    int64_t high;
    uint64_t low;
    BytesToIntegerPair(value.ptr, value.len, &high, &low);
    builder.Append(::arrow::Decimal128(high, low));
  }
};

template <>
class PrimitiveAppender<FLBA, ::arrow::Decimal128Builder> {
 public:
  template <typename ParquetValueType>
  void Append(const ColumnDescriptor* descriptor, ::arrow::DecimalBuilder& builder,
              ParquetValueType value) {
    int64_t high;
    uint64_t low;
    BytesToIntegerPair(value.ptr, descriptor->type_length(), &high, &low);
    builder.Append(::arrow::Decimal128(high, low));
  }
};

template <typename ParquetType>
class PrimitiveAppender<ParquetType, ::arrow::Decimal128Builder> {
 public:
  template <typename ParquetValueType>
  void Append(const ColumnDescriptor* descriptor, ::arrow::DecimalBuilder& builder,
              ParquetValueType value) {
    builder.Append(::arrow::Decimal128(value));
  }
};

template <>
class PrimitiveAppender<Int96, ::arrow::TimestampBuilder> {
 public:
  template <typename ParquetValueType>
  void Append(const ColumnDescriptor* descriptor, ::arrow::TimestampBuilder& builder,
              ParquetValueType value) {
    builder.Append(impala_timestamp_to_nanoseconds(value));
  }
};

class NullPrimitiveAppender {
 public:
  template <typename ParquetValueType>
  void Append(const ColumnDescriptor* descriptor, ::arrow::NullBuilder& builder,
              ParquetValueType value) {
    // This should never be called.
    // TODO: add error values and have this return
    // Status::Invalid.
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

template <typename ColumnReaderType, typename ArrayBuilderType>
class PrimitiveDeserializerNode : public DeserializerNode {
 public:
  PrimitiveDeserializerNode(const ColumnDescriptor* const descriptor, int column_index,
                            int64_t buffer_size,
                            std::shared_ptr<ArrayBuilderType> const& builder)
      : descriptor_(descriptor),
        column_index_(column_index),
        values_(buffer_size),
        builder_(builder) {}

  std::shared_ptr<::arrow::ArrayBuilder> GetArrayBuilder() { return builder_; }

  void InitializeRowGroup(std::shared_ptr<RowGroupReader> const& row_group_reader) final {
    column_reader_ = std::static_pointer_cast<ColumnReaderType>(
        row_group_reader->Column(column_index_));
  }

 protected:
  void Append() {
    appender_.Append(descriptor_, *builder_, values_.Front());
    values_.PopFront();
  }

  const ColumnDescriptor* const descriptor_;

  int const column_index_;

  PrimitiveDeserializerArray<typename ColumnReaderType::T> values_;

  std::shared_ptr<ArrayBuilderType> builder_;

  PrimitiveAppender<typename ColumnReaderType::T, ArrayBuilderType> appender_;

  std::shared_ptr<ColumnReaderType> column_reader_;
};

template <typename ColumnReaderType, typename ArrayBuilderType, bool HasDefinitionLevels,
          bool HasRepetitionLevels>
class TypedPrimitiveDeserializerNode;

template <typename ColumnReaderType, typename ArrayBuilderType>
class TypedPrimitiveDeserializerNode<ColumnReaderType, ArrayBuilderType, true, false>
    : public PrimitiveDeserializerNode<ColumnReaderType, ArrayBuilderType> {
 public:
  TypedPrimitiveDeserializerNode(const ColumnDescriptor* const descriptor,
                                 int const column_index, std::size_t buffer_size,
                                 std::shared_ptr<ArrayBuilderType> const& builder)
      : PrimitiveDeserializerNode<ColumnReaderType, ArrayBuilderType>(
            descriptor, column_index, buffer_size, builder),
        definition_levels_(buffer_size) {}

  void Skip(DeserializerNode::LevelSet& next_repetitions) final {
    if (!Fill()) {
      return;
    }
    if (definition_levels_.Front() == this->descriptor_->max_definition_level()) {
      this->values_.PopFront();
    }
    definition_levels_.PopFront();
  }

  int16_t CurrentMaxDefinitionLevel() final {
    if (!Fill()) {
      // TODO: change the return type to bool when there is
      // better error handling for not filling values
      return 0;
    }
    return definition_levels_.Front();
  }

  void Deserialize(DeserializerNode::LevelSet& next_repetitions,
                   int16_t current_repetition_level) final {
    if (!Fill()) {
      // TODO: check the current_repetition_level against
      // the max repetition level and throw an exception
      // if necessary?
      return;
    }
    if (definition_levels_.Front() == this->descriptor_->max_definition_level()) {
      // The value is not null
      this->Append();
    } else {
      this->builder_->AppendNull();
    }
    definition_levels_.PopFront();
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

template <typename ColumnReaderType, typename ArrayBuilderType>
class TypedPrimitiveDeserializerNode<ColumnReaderType, ArrayBuilderType, true, true>
    : public PrimitiveDeserializerNode<ColumnReaderType, ArrayBuilderType> {
 public:
  TypedPrimitiveDeserializerNode(const ColumnDescriptor* const descriptor,
                                 int column_index, std::size_t buffer_size,
                                 std::shared_ptr<ArrayBuilderType> const& builder)
      : PrimitiveDeserializerNode<ColumnReaderType, ArrayBuilderType>(
            descriptor, column_index, buffer_size, builder),
        definition_levels_(buffer_size),
        repetition_levels_(buffer_size) {}

  int16_t CurrentMaxDefinitionLevel() final {
    if (!Fill()) {
      return false;
    }
    return definition_levels_.Front();
  }

  void Skip(DeserializerNode::LevelSet& next_repetitions) final {
    if (!Fill()) {
      return;
    }
    if (definition_levels_.Front() == this->descriptor_->max_definition_level()) {
      this->values_.PopFront();
    }
    definition_levels_.PopFront();
    AddRepetitions(next_repetitions);
  }

  void Deserialize(DeserializerNode::LevelSet& next_repetitions,
                   int16_t current_repetition_level) final {
    if (!Fill()) {
      // TODO: check the current_repetition_level against
      // the max repetition level and throw an exception
      // if necessary?
      return;
    }
    // See Dremel paper: second entry for Links.Backward would
    // apply in this case and return
    if (repetition_levels_.Front() != current_repetition_level) {
      return;
    }

    if (definition_levels_.Front() == this->descriptor_->max_definition_level()) {
      // The value is not null
      this->Append();
    } else {
      // Insert a null
      this->builder_->AppendNull();
    }
    definition_levels_.PopFront();
    AddRepetitions(next_repetitions);
  }

 private:
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

template <typename ColumnReaderType, typename ArrayBuilderType>
class TypedPrimitiveDeserializerNode<ColumnReaderType, ArrayBuilderType, false, false>
    : public PrimitiveDeserializerNode<ColumnReaderType, ArrayBuilderType> {
 public:
  TypedPrimitiveDeserializerNode(const ColumnDescriptor* const descriptor,
                                 int column_index, std::size_t buffer_size,
                                 std::shared_ptr<ArrayBuilderType> const& builder)
      : PrimitiveDeserializerNode<ColumnReaderType, ArrayBuilderType>(
            descriptor, column_index, buffer_size, builder) {}

  int16_t CurrentMaxDefinitionLevel() final {
    return this->descriptor_->max_definition_level();
  }

  void Skip(DeserializerNode::LevelSet& next_repetitions) final {
    // This should only happen if there is some problem
    // with the reading of the data.
  }

  void Deserialize(DeserializerNode::LevelSet& next_repetitions,
                   int16_t current_repetition_level) final {
    if (!Fill()) {
      // TODO: check the current_repetition_level against
      // the max repetition level and throw an exception
      // if necessary?
      return;
    }
    this->Append();
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

class ListDeserializerNode : public DeserializerNode {
 public:
  ListDeserializerNode(std::unique_ptr<DeserializerNode>&& node_reader,
                       int16_t max_repetition_level, int16_t max_definition_level)
      : node_reader_(std::move(node_reader)),
        max_repetition_level_(max_repetition_level),
        max_definition_level_(max_definition_level),
        builder_(std::make_shared<::arrow::ListBuilder>(
            ::arrow::default_memory_pool(), node_reader_->GetArrayBuilder())) {}

  int16_t CurrentMaxDefinitionLevel() final {
    return node_reader_->CurrentMaxDefinitionLevel();
  }

  void Skip(LevelSet& next_repetitions) final { node_reader_->Skip(next_repetitions); }

  void Deserialize(LevelSet& next_repetitions, int16_t current_repetition_level) {
    builder_->Append();
    if (node_reader_->CurrentMaxDefinitionLevel() == max_definition_level_) {
      // This is just an empty list
      node_reader_->Skip(next_repetitions);
    } else {
      // Deserialize the first one
      node_reader_->Deserialize(next_repetitions, current_repetition_level);
      // Deserialize the repetitions
      DeserializeRepetitions(next_repetitions);
    }
  }

  std::shared_ptr<::arrow::ArrayBuilder> GetArrayBuilder() final { return builder_; }

  void InitializeRowGroup(std::shared_ptr<RowGroupReader> const& row_group_reader) {
    node_reader_->InitializeRowGroup(row_group_reader);
  }

 protected:
  void DeserializeRepetitions(LevelSet& next_repetitions) {
    bool repetition_at_this_level = true;
    while (!next_repetitions.empty() && repetition_at_this_level) {
      // next_repetitions is a sorted set, so end
      // is the highest value. If it's lower than our
      // max_repetition_level_, we just return from this
      // function. Otherwise, we erase highest, descend again,
      // and then re-check if we repeated at this level
      auto highest = next_repetitions.end();
      --highest;
      repetition_at_this_level = (*highest == max_repetition_level_);
      if (repetition_at_this_level) {
        next_repetitions.erase(highest);
        node_reader_->Deserialize(next_repetitions, max_repetition_level_);
      }
    }
  }

  std::unique_ptr<DeserializerNode> node_reader_;

  int16_t max_repetition_level_;

  int16_t max_definition_level_;

  std::shared_ptr<::arrow::ListBuilder> builder_;
};

class NullableListDeserializerNode : public ListDeserializerNode {
 public:
  NullableListDeserializerNode(std::unique_ptr<DeserializerNode>&& node_reader,
                               int16_t max_repetition_level, int16_t max_definition_level)
      : ListDeserializerNode(std::forward<std::unique_ptr<DeserializerNode>>(node_reader),
                             max_repetition_level, max_definition_level) {}

  void Deserialize(LevelSet& next_repetitions, int16_t current_repetition_level) final {
    auto max_definition_level = node_reader_->CurrentMaxDefinitionLevel();
    if (max_definition_level < max_definition_level_) {
      // It's null
      builder_->AppendNull();
      node_reader_->Skip(next_repetitions);
    } else if (max_definition_level == max_definition_level_) {
      // Empty list
      builder_->Append();
      node_reader_->Skip(next_repetitions);
    } else {
      builder_->Append();
      node_reader_->Deserialize(next_repetitions, current_repetition_level);
    }
    // TODO: should this be in the else block above?
    DeserializeRepetitions(next_repetitions);
  }
};

template <bool Optional>
class NestedDeserializerNode : public DeserializerNode {
 public:
  NestedDeserializerNode(std::string const& name, int16_t max_definition_level,
                         std::vector<std::unique_ptr<DeserializerNode>>&& children,
                         std::vector<std::shared_ptr<::arrow::Field>>& fields)
      : name_(name),
        children_(std::move(children)),
        max_definition_level_(max_definition_level) {
    std::vector<std::shared_ptr<::arrow::ArrayBuilder>> child_builders;
    for (auto const& child : children_) {
      child_builders.emplace_back(child->GetArrayBuilder());
    }
    auto arrow_type = std::make_shared<::arrow::StructType>(fields);
    builder_ = std::make_shared<::arrow::StructBuilder>(
        arrow_type, ::arrow::default_memory_pool(), std::move(child_builders));
  }

  void Skip(LevelSet& next_repetitions) {
    for (auto& child : children_) {
      child->Skip(next_repetitions);
    }
  }

  int16_t CurrentMaxDefinitionLevel() final {
    int16_t max = 0;
    for (auto& child : children_) {
      auto current = child->CurrentMaxDefinitionLevel();
      if (current > max) {
        max = current;
      }
    }
    return max;
  }

  void Deserialize(LevelSet& next_repetitions, int16_t current_repetition_level) final {
    // TODO: detect this at tree construction time. If all of
    // the fields inside this nested node are required, then
    // there is no need to do this check.
    auto has_data = !Optional;
    if (Optional) {
      has_data = CurrentMaxDefinitionLevel() >= max_definition_level_;
    }
    if (has_data) {
      builder_->Append();
    } else {
      builder_->AppendNull();
    }
    // For structs, we call Deserialize even when they don't have data.
    // This ensures that the length of the child arrays match
    // the length of the struct bitmap.
    for (auto& child : children_) {
      child->Deserialize(next_repetitions, current_repetition_level);
    }
  }

  std::shared_ptr<::arrow::ArrayBuilder> GetArrayBuilder() final { return builder_; }

  void InitializeRowGroup(std::shared_ptr<RowGroupReader> const& row_group_reader) final {
    for (auto& child : children_) {
      child->InitializeRowGroup(row_group_reader);
    }
  }

 private:
  std::string name_;

  std::vector<std::unique_ptr<DeserializerNode>> children_;

  int16_t max_definition_level_;

  std::shared_ptr<::arrow::StructBuilder> builder_;
};

ArrayDeserializer::ArrayDeserializer(std::unique_ptr<DeserializerNode>&& node)
    : node_(std::move(node)) {}

ArrayDeserializer::~ArrayDeserializer() {}

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
      node_->InitializeRowGroup(row_group_reader);
      auto const num_rows = row_group_reader_->metadata()->num_rows();
      for (int jj = 0; jj < num_rows; ++jj) {
        node_->Deserialize(repetitions, /* repetition level */ 0);
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

class FileArrayBatchedDeserializer : public ArrayBatchedDeserializer {
 public:
  FileArrayBatchedDeserializer(std::unique_ptr<DeserializerNode>&& node,
                               std::shared_ptr<ParquetFileReader> const& file_reader)
      : node_(std::move(node)),
        file_reader_(file_reader),
        current_row_group_(file_reader->RowGroup(0)),
        current_row_group_index_(0),
        current_row_(0) {
    node_->InitializeRowGroup(current_row_group_);
  }

  ~FileArrayBatchedDeserializer() {}

  ::arrow::Status DeserializeBatch(int64_t num_records,
                                   std::shared_ptr<::arrow::Array>& array) final {
    DeserializerNode::LevelSet repetitions;
    int64_t records_read = 0;
    auto const num_row_groups = file_reader_->metadata()->num_row_groups();
    while (records_read < num_records && current_row_group_index_ < num_row_groups) {
      auto const num_rows = current_row_group_->metadata()->num_rows();
      for (; current_row_ < num_rows && records_read < num_records;
           ++current_row_, ++records_read) {
        node_->Deserialize(repetitions, /* repetition_level */ 0);
        repetitions.clear();
      }
      if (current_row_ >= num_rows) {
        ++current_row_group_index_;
        if (current_row_group_index_ < num_row_groups) {
          current_row_group_ = file_reader_->RowGroup(current_row_group_index_);
          node_->InitializeRowGroup(current_row_group_);
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

class ColumnChunkDeserializer : public ArrayDeserializer {
 public:
  ColumnChunkDeserializer(
      std::unique_ptr<DeserializerNode>&& node,
      std::shared_ptr<::parquet::RowGroupReader> const& row_group_reader)
      : ArrayDeserializer(std::move(node)), row_group_reader_(row_group_reader) {
    node_->InitializeRowGroup(row_group_reader_);
  }

  ::arrow::Status DeserializeArray(std::shared_ptr<::arrow::Array>& array) final {
    DeserializerNode::LevelSet repetitions;
    auto const num_rows = row_group_reader_->metadata()->num_rows();
    for (int ii = 0; ii < num_rows; ++ii) {
      node_->Deserialize(repetitions, /* repetition level */ 0);
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

class RowGroupDeserializer : public TableDeserializer {
 public:
  RowGroupDeserializer(std::vector<std::unique_ptr<DeserializerNode>>&& nodes,
                       std::shared_ptr<::arrow::Schema> const& schema,
                       std::shared_ptr<::parquet::RowGroupReader> const& row_group_reader)
      : TableDeserializer(
            std::forward<std::vector<std::unique_ptr<DeserializerNode>>>(nodes), schema),
        row_group_reader_(row_group_reader) {
    for (auto& node : nodes_) {
      node->InitializeRowGroup(row_group_reader);
    }
  }

  ~RowGroupDeserializer() {}

  ::arrow::Status DeserializeTable(std::shared_ptr<::arrow::Table>& table) final {
    DeserializerNode::LevelSet repetitions;
    auto const num_rows = row_group_reader_->metadata()->num_rows();
    for (int ii = 0; ii < num_rows; ++ii) {
      for (auto& node : nodes_) {
        node->Deserialize(repetitions, /* repetition level */ 0);
        repetitions.clear();
      }
    }
    return MakeTable(table);
  }

 private:
  std::shared_ptr<::parquet::RowGroupReader> row_group_reader_;
};

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
    DeserializerNode::LevelSet repetitions;
    auto num_row_groups = file_reader_->metadata()->num_row_groups();
    for (int ii = 0; ii < num_row_groups; ++ii) {
      auto row_group_reader = file_reader_->RowGroup(ii);
      for (auto& node : nodes_) {
        node->InitializeRowGroup(row_group_reader);
      }
      auto const num_rows = row_group_reader->metadata()->num_rows();
      for (int ii = 0; ii < num_rows; ++ii) {
        for (auto& node : nodes_) {
          node->Deserialize(repetitions, /* repetition level */ 0);
          repetitions.clear();
        }
      }
    }
    return MakeTable(table);
  }

 private:
  std::shared_ptr<::parquet::ParquetFileReader> file_reader_;
};

class DeserializerBuilder::Impl {
 public:
  Impl(std::shared_ptr<::parquet::ParquetFileReader> const& file_reader,
       int64_t buffer_size);

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

  Status BuildNodes(std::vector<int> const& column_indices,
                    std::vector<std::unique_ptr<DeserializerNode>>& nodes,
                    bool in_index_order = true);

  // TODO: make repetition_levels and definition_levels
  // tagged/typed integers:
  //
  // struct RepetitionTag;
  //
  // template <typename Tag, typename ValueType>
  // TaggedType;
  //
  // using Repetition = TaggedType<RepetitionTag, int16_t>
  Status BuildNode(std::shared_ptr<const Node> const& node,
                   std::vector<int> const& indices, std::vector<int>& indices_seen,
                   std::unique_ptr<DeserializerNode>& result,
                   int16_t repetition_levels = 0, int16_t definition_levels = 0);

  Status BuildPrimitiveNode(std::shared_ptr<const Node> const& node,
                            int16_t repetition_levels, int16_t definition_levels,
                            std::vector<int> const& indices,
                            std::vector<int>& indices_seen,
                            std::unique_ptr<DeserializerNode>& result);

  Status BuildGroupNode(std::shared_ptr<const Node> const& node,
                        int16_t repetition_levels, int16_t definition_levels,
                        std::vector<int> const& indices, std::vector<int>& indices_seen,
                        std::unique_ptr<DeserializerNode>& result);

  Status BuildListNode(std::shared_ptr<const GroupNode> const& node,
                       int16_t repetition_levels, int16_t definition_levels,
                       std::vector<int> const& indices, std::vector<int>& indices_seen,
                       std::unique_ptr<DeserializerNode>& result);

  template <bool HasDefinition, bool HasRepetition>
  Status MakeTypedPrimitiveDeserializerNode(
      std::shared_ptr<const Node> const& node,
      const ColumnDescriptor* const column_descriptor, int column_index,
      std::unique_ptr<DeserializerNode>& result) {
    auto primitive_node = std::static_pointer_cast<const PrimitiveNode>(node);
    switch (primitive_node->physical_type()) {
      case Type::BOOLEAN:
        return MakeLogicalPrimitiveDeserializerNode<parquet::BooleanType, HasDefinition,
                                                    HasRepetition>(
            primitive_node, column_descriptor, column_index, result);
      case Type::INT32:
        return MakeLogicalPrimitiveDeserializerNode<parquet::Int32Type, HasDefinition,
                                                    HasRepetition>(
            primitive_node, column_descriptor, column_index, result);
      case Type::INT64:
        return MakeLogicalPrimitiveDeserializerNode<parquet::Int64Type, HasDefinition,
                                                    HasRepetition>(
            primitive_node, column_descriptor, column_index, result);
      case Type::INT96:
        return MakeLogicalPrimitiveDeserializerNode<parquet::Int96Type, HasDefinition,
                                                    HasRepetition>(
            primitive_node, column_descriptor, column_index, result);
      case Type::FLOAT:
        return MakeLogicalPrimitiveDeserializerNode<parquet::FloatType, HasDefinition,
                                                    HasRepetition>(
            primitive_node, column_descriptor, column_index, result);
      case Type::DOUBLE:
        return MakeLogicalPrimitiveDeserializerNode<parquet::DoubleType, HasDefinition,
                                                    HasRepetition>(
            primitive_node, column_descriptor, column_index, result);
      case Type::BYTE_ARRAY:
        return MakeLogicalPrimitiveDeserializerNode<parquet::ByteArrayType, HasDefinition,
                                                    HasRepetition>(
            primitive_node, column_descriptor, column_index, result);
      case Type::FIXED_LEN_BYTE_ARRAY:
        return MakeLogicalPrimitiveDeserializerNode<parquet::FLBAType, HasDefinition,
                                                    HasRepetition>(
            primitive_node, column_descriptor, column_index, result);
      default:
        std::ostringstream stream;
        stream << "Unrecognized type when creating a primitive node reader";
        return Status::Invalid(stream.str());
    }
  }

  template <typename ParquetType, bool HasDefinition, bool HasRepetition>
  Status MakeLogicalPrimitiveDeserializerNode(
      std::shared_ptr<const PrimitiveNode> const& node,
      const ColumnDescriptor* const column_descriptor, int column_index,
      std::unique_ptr<DeserializerNode>& result) {
    switch (node->logical_type()) {
      case parquet::LogicalType::DATE:
        return MakeDateDeserializerNode<ParquetType, HasDefinition, HasRepetition>(
            column_descriptor, column_index, result);
      case parquet::LogicalType::TIME_MILLIS:
        return MakeTimeMillisDeserializerNode<ParquetType, HasDefinition, HasRepetition>(
            column_descriptor, column_index, result);
      case parquet::LogicalType::TIME_MICROS:
        return MakeTimeMicrosDeserializerNode<ParquetType, HasDefinition, HasRepetition>(
            column_descriptor, column_index, result);
      case parquet::LogicalType::TIMESTAMP_MILLIS:
        return MakeTimestampDeserializerNode<ParquetType, HasDefinition, HasRepetition>(
            column_descriptor, column_index, ::arrow::TimeUnit::MILLI, result);
      case parquet::LogicalType::TIMESTAMP_MICROS:
        return MakeTimestampDeserializerNode<ParquetType, HasDefinition, HasRepetition>(
            column_descriptor, column_index, ::arrow::TimeUnit::MICRO, result);
      case parquet::LogicalType::DECIMAL:
        return MakeDecimalDeserializerNode<ParquetType, HasDefinition, HasRepetition>(
            column_descriptor, column_index, result);
      case parquet::LogicalType::UINT_8:
        return MakeIntegerDeserializerNode<ParquetType, ::arrow::UInt8Builder,
                                           HasDefinition, HasRepetition>(
            column_descriptor, column_index, result);
      case parquet::LogicalType::UINT_16:
        return MakeIntegerDeserializerNode<ParquetType, ::arrow::UInt16Builder,
                                           HasDefinition, HasRepetition>(
            column_descriptor, column_index, result);
      case parquet::LogicalType::UINT_32:
        return MakeIntegerDeserializerNode<ParquetType, ::arrow::UInt32Builder,
                                           HasDefinition, HasRepetition>(
            column_descriptor, column_index, result);
      case parquet::LogicalType::UINT_64:
        return MakeIntegerDeserializerNode<ParquetType, ::arrow::UInt64Builder,
                                           HasDefinition, HasRepetition>(
            column_descriptor, column_index, result);
      case parquet::LogicalType::INT_8:
        return MakeIntegerDeserializerNode<ParquetType, ::arrow::Int8Builder,
                                           HasDefinition, HasRepetition>(
            column_descriptor, column_index, result);
      case parquet::LogicalType::INT_16:
        return MakeIntegerDeserializerNode<ParquetType, ::arrow::Int16Builder,
                                           HasDefinition, HasRepetition>(
            column_descriptor, column_index, result);
      case parquet::LogicalType::INT_32:
        return MakeIntegerDeserializerNode<ParquetType, ::arrow::Int32Builder,
                                           HasDefinition, HasRepetition>(
            column_descriptor, column_index, result);
      case parquet::LogicalType::INT_64:
        return MakeIntegerDeserializerNode<ParquetType, ::arrow::Int64Builder,
                                           HasDefinition, HasRepetition>(
            column_descriptor, column_index, result);
      case parquet::LogicalType::UTF8:
        return MakeUTF8DeserializerNode<ParquetType, HasDefinition, HasRepetition>(
            column_descriptor, column_index, result);
      case parquet::LogicalType::NA:
        return MakeNullDeserializerNode<ParquetType, HasDefinition, HasRepetition>(
            column_descriptor, column_index, result);
      default:
        switch (node->physical_type()) {
          case Type::BOOLEAN:
            return MakeBooleanDeserializerNode<ParquetType, HasDefinition, HasRepetition>(
                column_descriptor, column_index, result);
          case Type::INT32:
            return MakeIntegerDeserializerNode<ParquetType, ::arrow::Int32Builder,
                                               HasDefinition, HasRepetition>(
                column_descriptor, column_index, result);
          case Type::INT64:
            return MakeIntegerDeserializerNode<ParquetType, ::arrow::Int64Builder,
                                               HasDefinition, HasRepetition>(
                column_descriptor, column_index, result);
          case Type::FLOAT:
            return MakeFloatDeserializerNode<ParquetType, HasDefinition, HasRepetition>(
                column_descriptor, column_index, result);
          case Type::DOUBLE:
            return MakeDoubleDeserializerNode<ParquetType, HasDefinition, HasRepetition>(
                column_descriptor, column_index, result);
          case Type::BYTE_ARRAY:
            return MakeByteArrayDeserializerNode<ParquetType, HasDefinition,
                                                 HasRepetition>(column_descriptor,
                                                                column_index, result);
          case Type::FIXED_LEN_BYTE_ARRAY:
            return MakeFixedLengthByteArrayDeserializerNode<ParquetType, HasDefinition,
                                                            HasRepetition>(
                column_descriptor, column_index, result);
          case Type::INT96:
            return MakeImpalaTimestampDeserializerNode<ParquetType, HasDefinition,
                                                       HasRepetition>(
                column_descriptor, column_index, result);
          default:
            std::ostringstream stream;
            stream << "Unsupported physical type " << node->physical_type() << std::endl;
            return Status::Invalid(stream.str());
        }
    }
  }

  template <typename ParquetType, bool HasDefinition, bool HasRepetition>
  Status MakeNullDeserializerNode(const ColumnDescriptor* const column_descriptor,
                                  int column_index,
                                  std::unique_ptr<DeserializerNode>& result) {
    return MakeTypedPrimitiveNodeWithBuilder<ParquetType, ::arrow::NullBuilder,
                                             HasDefinition, HasRepetition>(
        column_descriptor, column_index, result);
  }

  template <typename ParquetType, bool HasDefinition, bool HasRepetition>
  typename std::enable_if<std::is_same<ParquetType, parquet::Int96Type>::value,
                          Status>::type
  MakeImpalaTimestampDeserializerNode(const ColumnDescriptor* const column_descriptor,
                                      int column_index,
                                      std::unique_ptr<DeserializerNode>& result) {
    auto arrow_type = std::make_shared<::arrow::TimestampType>(::arrow::TimeUnit::NANO);
    return MakeTypedPrimitiveNodeWithBuilder<ParquetType, ::arrow::TimestampBuilder,
                                             HasDefinition, HasRepetition>(
        column_descriptor, column_index, result, arrow_type,
        ::arrow::default_memory_pool());
  }

  template <typename ParquetType, bool HasDefinition, bool HasRepetition>
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

  template <typename ParquetType, bool HasDefinition, bool HasRepetition>
  typename std::enable_if<SupportsDecimal<ParquetType>::value, Status>::type
  MakeDecimalDeserializerNode(const ColumnDescriptor* const column_descriptor,
                              int column_index,
                              std::unique_ptr<DeserializerNode>& result) {
    auto arrow_type = std::make_shared<::arrow::Decimal128Type>(
        column_descriptor->type_precision(), column_descriptor->type_scale());
    return MakeTypedPrimitiveNodeWithBuilder<ParquetType, ::arrow::Decimal128Builder,
                                             HasDefinition, HasRepetition>(
        column_descriptor, column_index, result, arrow_type,
        ::arrow::default_memory_pool());
  }

  template <typename ParquetType, bool HasDefinition, bool HasRepetition>
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

  template <typename ParquetType, bool HasDefinition, bool HasRepetition>
  typename std::enable_if<std::is_same<ParquetType, parquet::BooleanType>::value,
                          Status>::type
  MakeBooleanDeserializerNode(const ColumnDescriptor* const column_descriptor,
                              int column_index,
                              std::unique_ptr<DeserializerNode>& result) {
    return MakeTypedPrimitiveNodeWithBuilder<ParquetType, ::arrow::BooleanBuilder,
                                             HasDefinition, HasRepetition>(
        column_descriptor, column_index, result);
  }

  template <typename ParquetType, bool HasDefinition, bool HasRepetition>
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

  template <typename ParquetType, bool HasDefinition, bool HasRepetition>
  typename std::enable_if<std::is_same<ParquetType, parquet::Int32Type>::value,
                          Status>::type
  MakeDateDeserializerNode(const ColumnDescriptor* const column_descriptor,
                           int column_index, std::unique_ptr<DeserializerNode>& result) {
    return MakeTypedPrimitiveNodeWithBuilder<ParquetType, ::arrow::Date32Builder,
                                             HasDefinition, HasRepetition>(
        column_descriptor, column_index, result);
  }

  template <typename ParquetType, bool HasDefinition, bool HasRepetition>
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

  template <typename ParquetType, bool HasDefinition, bool HasRepetition>
  typename std::enable_if<std::is_same<ParquetType, parquet::Int32Type>::value,
                          Status>::type
  MakeTimeMillisDeserializerNode(const ColumnDescriptor* const column_descriptor,
                                 int column_index,
                                 std::unique_ptr<DeserializerNode>& result) {
    auto arrow_type = std::make_shared<::arrow::Time32Type>(::arrow::TimeUnit::MILLI);
    return MakeTypedPrimitiveNodeWithBuilder<ParquetType, ::arrow::Time32Builder,
                                             HasDefinition, HasRepetition>(
        column_descriptor, column_index, result, arrow_type,
        ::arrow::default_memory_pool());
  }

  template <typename ParquetType, bool HasDefinition, bool HasRepetition>
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

  template <typename ParquetType, bool HasDefinition, bool HasRepetition>
  typename std::enable_if<std::is_same<ParquetType, parquet::Int64Type>::value,
                          Status>::type
  MakeTimeMicrosDeserializerNode(const ColumnDescriptor* const column_descriptor,
                                 int column_index,
                                 std::unique_ptr<DeserializerNode>& result) {
    auto arrow_type = std::make_shared<::arrow::Time64Type>(::arrow::TimeUnit::MICRO);
    return MakeTypedPrimitiveNodeWithBuilder<ParquetType, ::arrow::Time64Builder,
                                             HasDefinition, HasRepetition>(
        column_descriptor, column_index, result, arrow_type,
        ::arrow::default_memory_pool());
  }

  template <typename ParquetType, bool HasDefinition, bool HasRepetition>
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

  template <typename ParquetType, bool HasDefinition, bool HasRepetition>
  typename std::enable_if<std::is_same<ParquetType, parquet::Int64Type>::value,
                          Status>::type
  MakeTimestampDeserializerNode(const ColumnDescriptor* const column_descriptor,
                                int column_index, ::arrow::TimeUnit::type time_unit,
                                std::unique_ptr<DeserializerNode>& result) {
    auto arrow_type = std::make_shared<::arrow::TimestampType>(time_unit);
    return MakeTypedPrimitiveNodeWithBuilder<ParquetType, ::arrow::TimestampBuilder,
                                             HasDefinition, HasRepetition>(
        column_descriptor, column_index, result, arrow_type,
        ::arrow::default_memory_pool());
  }

  template <typename ParquetType, bool HasDefinition, bool HasRepetition>
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

  template <typename ParquetType, typename ArrayBuilderType, bool HasDefinition,
            bool HasRepetition>
  typename std::enable_if<CanCastInteger<ParquetType, ArrayBuilderType>::value,
                          Status>::type
  MakeIntegerDeserializerNode(const ColumnDescriptor* const column_descriptor,
                              int column_index, std::unique_ptr<DeserializerNode>& result)

  {
    return MakeTypedPrimitiveNodeWithBuilder<ParquetType, ArrayBuilderType, HasDefinition,
                                             HasRepetition>(column_descriptor,
                                                            column_index, result);
  }

  template <typename ParquetType, typename ArrayBuilderType, bool HasDefinition,
            bool HasRepetition>
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

  template <typename ParquetType, bool HasDefinition, bool HasRepetition>
  typename std::enable_if<std::is_same<ParquetType, parquet::ByteArrayType>::value,
                          Status>::type
  MakeUTF8DeserializerNode(const ColumnDescriptor* const column_descriptor,
                           int column_index, std::unique_ptr<DeserializerNode>& result)

  {
    return MakeTypedPrimitiveNodeWithBuilder<ParquetType, ::arrow::BinaryBuilder,
                                             HasDefinition, HasRepetition>(
        column_descriptor, column_index, result, ::arrow::utf8(),
        ::arrow::default_memory_pool());
  }

  template <typename ParquetType, bool HasDefinition, bool HasRepetition>
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

  template <typename ParquetType, bool HasDefinition, bool HasRepetition>
  typename std::enable_if<std::is_same<ParquetType, parquet::ByteArrayType>::value,
                          Status>::type
  MakeByteArrayDeserializerNode(const ColumnDescriptor* const column_descriptor,
                                int column_index,
                                std::unique_ptr<DeserializerNode>& result)

  {
    return MakeTypedPrimitiveNodeWithBuilder<ParquetType, ::arrow::BinaryBuilder,
                                             HasDefinition, HasRepetition>(
        column_descriptor, column_index, result);
  }

  template <typename ParquetType, bool HasDefinition, bool HasRepetition>
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

  template <typename ParquetType, bool HasDefinition, bool HasRepetition>
  typename std::enable_if<std::is_same<ParquetType, parquet::FLBAType>::value,
                          Status>::type
  MakeFixedLengthByteArrayDeserializerNode(
      const ColumnDescriptor* const column_descriptor, int column_index,
      std::unique_ptr<DeserializerNode>& result)

  {
    auto arrow_type =
        std::make_shared<::arrow::FixedSizeBinaryType>(column_descriptor->type_length());
    return MakeTypedPrimitiveNodeWithBuilder<ParquetType, ::arrow::FixedSizeBinaryBuilder,
                                             HasDefinition, HasRepetition>(
        column_descriptor, column_index, result, arrow_type,
        ::arrow::default_memory_pool());
  }

  template <typename ParquetType, bool HasDefinition, bool HasRepetition>
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

  template <typename ParquetType, bool HasDefinition, bool HasRepetition>
  typename std::enable_if<std::is_same<ParquetType, parquet::DoubleType>::value,
                          Status>::type
  MakeDoubleDeserializerNode(const ColumnDescriptor* const column_descriptor,
                             int column_index, std::unique_ptr<DeserializerNode>& result)

  {
    return MakeTypedPrimitiveNodeWithBuilder<ParquetType, ::arrow::DoubleBuilder,
                                             HasDefinition, HasRepetition>(
        column_descriptor, column_index, result);
  }

  template <typename ParquetType, bool HasDefinition, bool HasRepetition>
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

  template <typename ParquetType, bool HasDefinition, bool HasRepetition>
  typename std::enable_if<std::is_same<ParquetType, parquet::FloatType>::value,
                          Status>::type
  MakeFloatDeserializerNode(const ColumnDescriptor* const column_descriptor,
                            int column_index, std::unique_ptr<DeserializerNode>& result)

  {
    return MakeTypedPrimitiveNodeWithBuilder<ParquetType, ::arrow::FloatBuilder,
                                             HasDefinition, HasRepetition>(
        column_descriptor, column_index, result);
  }

  template <typename ParquetType, bool HasDefinition, bool HasRepetition>
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

  template <typename ParquetType, typename ArrayBuilderType, bool HasDefinition,
            bool HasRepetition, typename... Args>
  Status MakeTypedPrimitiveNodeWithBuilder(
      const ColumnDescriptor* const column_descriptor, int column_index,
      std::unique_ptr<DeserializerNode>& result, Args&&... args) {
    auto builder = std::make_shared<ArrayBuilderType>(std::forward<Args>(args)...);
    using ReturnType =
        TypedPrimitiveDeserializerNode<TypedColumnReader<ParquetType>, ArrayBuilderType,
                                       HasDefinition, HasRepetition>;
    result = std::unique_ptr<DeserializerNode>(
        new ReturnType(column_descriptor, column_index, buffer_size_, builder));
    return Status::OK();
  }

  void IncrementLevels(std::shared_ptr<const Node> const& node,
                       int16_t& repetition_levels, int16_t& definition_levels) {
    if (!node->is_required()) {
      ++definition_levels;
    }
    if (node->is_repeated()) {
      ++repetition_levels;
    }
  }

  std::shared_ptr<::parquet::ParquetFileReader> file_reader_;

  int64_t buffer_size_;
};

DeserializerBuilder::Impl::Impl(
    std::shared_ptr<::parquet::ParquetFileReader> const& file_reader, int64_t buffer_size)
    : file_reader_(file_reader), buffer_size_(buffer_size) {}

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
  result.reset(new FileArrayBatchedDeserializer(std::move(nodes[0]), file_reader_));
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
  result.reset(new ColumnChunkDeserializer(std::move(nodes[0]),
                                           file_reader_->RowGroup(row_group_index)));
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
                                            int16_t repetition_levels,
                                            int16_t definition_levels) {
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
    std::shared_ptr<const Node> const& node, int16_t repetition_levels,
    int16_t definition_levels, std::vector<int> const& indices,
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
  if (definition_levels > 0) {
    if (repetition_levels > 0) {
      // The path to the node either contains only repeated nodes or
      // a mix of repeated and optional nodes.
      return MakeTypedPrimitiveDeserializerNode<true, true>(node, column_descriptor,
                                                            column_index, result);
    } else {
      // A field that is either itself optional or a descendant of
      // an optional field (e.g. - member of a struct that is optional)
      // but does not have any ancestors in the tree that are repeated.
      // It therefore won't have any repetition levels.
      return MakeTypedPrimitiveDeserializerNode<true, false>(node, column_descriptor,
                                                             column_index, result);
    }
  } else if (repetition_levels == 0) {
    // A field that is always present (max_definition_level ==
    // 0 && max_repetition_level == 0) and therefore doesn't
    // need repetition levels or definition levels
    return MakeTypedPrimitiveDeserializerNode<false, false>(node, column_descriptor,
                                                            column_index, result);
  } else {
    // This should never happen since a non-zero max repetition level
    // should have a non-zero definition level
    std::ostringstream stream;
    stream << "Primitive node " << node->path()->ToDotString();
    stream << " has a max_repetition_level or ";
    stream << definition_levels;
    stream << " and a max_definition_level of ";
    stream << repetition_levels;
    return Status::Invalid(stream.str());
  }
  if (node->is_repeated()) {
    result.reset(new ListDeserializerNode(std::move(result), repetition_levels,
                                          definition_levels));
  }
  return Status::OK();
}

Status DeserializerBuilder::Impl::BuildGroupNode(
    std::shared_ptr<const Node> const& node, int16_t repetition_levels,
    int16_t definition_levels, std::vector<int> const& indices,
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
    result.reset(new NestedDeserializerNode<true>(node->name(), definition_levels,
                                                  std::move(children), fields));
  } else {
    result.reset(new NestedDeserializerNode<false>(node->name(), definition_levels,
                                                   std::move(children), fields));
  }
  if (node->is_repeated()) {
    // TODO: do we even test this? It may actually be implemented
    // incorrectly because it doesn't contain the nesting that
    // exists in LIST nodes. For example, this code path would
    // be run for a node like this:
    //
    // repeated int32 element
    //
    // But that's the same code which is used for this:
    //
    // required group (LIST) {
    //    repated group list_group {
    //         required int32 element
    //    }
    // }
    //
    // The danger of using the same implementation is that in this
    // path we can't differentiate between an empty list and a list
    // with a null
    result.reset(new ListDeserializerNode(std::move(result), repetition_levels,
                                          definition_levels));
  }
  return Status::OK();
}

Status DeserializerBuilder::Impl::BuildListNode(
    std::shared_ptr<const GroupNode> const& node, int16_t repetition_levels,
    int16_t definition_levels, std::vector<int> const& indices,
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
    result.reset(new NullableListDeserializerNode(std::move(child), repetition_levels,
                                                  current_max_definition_level));
  } else {
    result.reset(new ListDeserializerNode(std::move(child), repetition_levels,
                                          current_max_definition_level));
  }
  return Status::OK();
}

DeserializerBuilder::DeserializerBuilder(
    std::shared_ptr<::parquet::ParquetFileReader> const& file_reader, int64_t buffer_size)
    : impl_(new Impl(file_reader, buffer_size)) {}

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
