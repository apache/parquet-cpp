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

#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/test-util.h"
#include "arrow/type_traits.h"
#include "arrow/util/decimal.h"

namespace parquet {
namespace arrow {

using ::arrow::Array;
using ::arrow::Status;

template <typename ArrowType>
using is_arrow_float = std::is_floating_point<typename ArrowType::c_type>;

template <typename ArrowType>
using is_arrow_int = std::is_integral<typename ArrowType::c_type>;

template <typename ArrowType>
using is_arrow_date = std::is_same<ArrowType, ::arrow::Date64Type>;

template <typename ArrowType>
using is_arrow_string = std::is_same<ArrowType, ::arrow::StringType>;

template <typename ArrowType>
using is_arrow_binary = std::is_same<ArrowType, ::arrow::BinaryType>;

template <typename ArrowType>
using is_arrow_fixed_size_binary = std::is_same<ArrowType, ::arrow::FixedSizeBinaryType>;

template <typename ArrowType>
using is_arrow_decimal = std::is_same<ArrowType, ::arrow::DecimalType>;

template <typename ArrowType>
using is_arrow_bool = std::is_same<ArrowType, ::arrow::BooleanType>;

template <class ArrowType>
typename std::enable_if<is_arrow_float<ArrowType>::value, Status>::type NonNullArray(
    size_t size, std::shared_ptr<Array>* out) {
  std::vector<typename ArrowType::c_type> values;
  ::arrow::test::random_real<typename ArrowType::c_type>(size, 0, 0, 1, &values);
  ::arrow::NumericBuilder<ArrowType> builder;
  RETURN_NOT_OK(builder.Append(values.data(), values.size()));
  return builder.Finish(out);
}

template <class ArrowType>
typename std::enable_if<
    is_arrow_int<ArrowType>::value && !is_arrow_date<ArrowType>::value, Status>::type
NonNullArray(size_t size, std::shared_ptr<Array>* out) {
  std::vector<typename ArrowType::c_type> values;
  ::arrow::test::randint<typename ArrowType::c_type>(size, 0, 64, &values);

  // Passing data type so this will work with TimestampType too
  ::arrow::NumericBuilder<ArrowType> builder(std::make_shared<ArrowType>(),
                                             ::arrow::default_memory_pool());
  RETURN_NOT_OK(builder.Append(values.data(), values.size()));
  return builder.Finish(out);
}

template <class ArrowType>
typename std::enable_if<is_arrow_date<ArrowType>::value, Status>::type NonNullArray(
    size_t size, std::shared_ptr<Array>* out) {
  std::vector<typename ArrowType::c_type> values;
  ::arrow::test::randint<typename ArrowType::c_type>(size, 0, 64, &values);
  for (size_t i = 0; i < size; i++) {
    values[i] *= 86400000;
  }

  // Passing data type so this will work with TimestampType too
  ::arrow::NumericBuilder<ArrowType> builder(std::make_shared<ArrowType>(),
                                             ::arrow::default_memory_pool());
  builder.Append(values.data(), values.size());
  return builder.Finish(out);
}

template <class ArrowType>
typename std::enable_if<
    is_arrow_string<ArrowType>::value || is_arrow_binary<ArrowType>::value, Status>::type
NonNullArray(size_t size, std::shared_ptr<Array>* out) {
  using BuilderType = typename ::arrow::TypeTraits<ArrowType>::BuilderType;
  BuilderType builder;
  for (size_t i = 0; i < size; i++) {
    RETURN_NOT_OK(builder.Append("test-string"));
  }
  return builder.Finish(out);
}

template <typename ArrowType>
typename std::enable_if<is_arrow_fixed_size_binary<ArrowType>::value, Status>::type
NonNullArray(size_t size, std::shared_ptr<Array>* out) {
  using BuilderType = typename ::arrow::TypeTraits<ArrowType>::BuilderType;
  // set byte_width to the length of "fixed": 5
  // todo: find a way to generate test data with more diversity.
  BuilderType builder(::arrow::fixed_size_binary(5));
  for (size_t i = 0; i < size; i++) {
    RETURN_NOT_OK(builder.Append("fixed"));
  }
  return builder.Finish(out);
}

static void DecimalRange(int32_t precision, ::arrow::Decimal128* min_decimal,
                         ::arrow::Decimal128* max_decimal) {
  DCHECK_GE(precision, 1) << "decimal precision must be greater than or equal to 1, got "
                          << precision;
  DCHECK_LE(precision, 38) << "decimal precision must be less than or equal to 38, got "
                           << precision;
  DCHECK_NE(min_decimal, NULLPTR);
  DCHECK_NE(max_decimal, NULLPTR);

  *max_decimal = 1;
  for (int32_t i = 0; i < precision; ++i) {
    *max_decimal *= 10;
  }
  *max_decimal -= 1;
  *min_decimal = -(*max_decimal);
}

class UniformDecimalDistribution {
 public:
  explicit UniformDecimalDistribution(int32_t precision) {
    ::arrow::Decimal128 min_decimal;
    ::arrow::Decimal128 max_decimal;

    DecimalRange(precision, &min_decimal, &max_decimal);

    const auto min_lower = static_cast<int64_t>(min_decimal.low_bits());
    const auto max_lower = static_cast<int64_t>(max_decimal.low_bits());

    lower_bits_ = std::uniform_int_distribution<int64_t>(min_lower, max_lower);
    upper_bits_ = std::uniform_int_distribution<int64_t>(min_decimal.high_bits(),
                                                         max_decimal.high_bits());
  }

  template <typename Generator>
  ::arrow::Decimal128 operator()(Generator& gen) {
    return ::arrow::Decimal128(upper_bits_(gen), static_cast<uint64_t>(lower_bits_(gen)));
  }

 private:
  std::uniform_int_distribution<int64_t> lower_bits_;
  std::uniform_int_distribution<int64_t> upper_bits_;
};

template <typename ArrowType>
typename std::enable_if<is_arrow_decimal<ArrowType>::value, Status>::type NonNullArray(
    size_t size, std::shared_ptr<Array>* out) {
  constexpr int32_t kDecimalPrecision = 4;
  constexpr int32_t kDecimalScale = 2;

  // todo: find a way to generate test data with more diversity.
  const auto type = ::arrow::decimal(kDecimalPrecision, kDecimalScale);

  using BuilderType = typename ::arrow::TypeTraits<ArrowType>::BuilderType;
  BuilderType builder(type);

  constexpr int32_t seed = 0;
  std::mt19937 gen(seed);
  UniformDecimalDistribution decimal_dist(kDecimalPrecision);

  for (size_t i = 0; i < size; i++) {
    const ::arrow::Decimal128 value(decimal_dist(gen));
    RETURN_NOT_OK(builder.Append(value));
  }
  return builder.Finish(out);
}

template <class ArrowType>
typename std::enable_if<is_arrow_bool<ArrowType>::value, Status>::type NonNullArray(
    size_t size, std::shared_ptr<Array>* out) {
  std::vector<uint8_t> values;
  ::arrow::test::randint<uint8_t>(size, 0, 1, &values);
  ::arrow::BooleanBuilder builder;
  RETURN_NOT_OK(builder.Append(values.data(), values.size()));
  return builder.Finish(out);
}

// This helper function only supports (size/2) nulls.
template <typename ArrowType>
typename std::enable_if<is_arrow_float<ArrowType>::value, Status>::type NullableArray(
    size_t size, size_t num_nulls, uint32_t seed, std::shared_ptr<Array>* out) {
  std::vector<typename ArrowType::c_type> values;
  ::arrow::test::random_real<typename ArrowType::c_type>(size, seed, -1e10, 1e10,
                                                         &values);
  std::vector<uint8_t> valid_bytes(size, 1);

  for (size_t i = 0; i < num_nulls; i++) {
    valid_bytes[i * 2] = 0;
  }

  ::arrow::NumericBuilder<ArrowType> builder;
  RETURN_NOT_OK(builder.Append(values.data(), values.size(), valid_bytes.data()));
  return builder.Finish(out);
}

// This helper function only supports (size/2) nulls.
template <typename ArrowType>
typename std::enable_if<
    is_arrow_int<ArrowType>::value && !is_arrow_date<ArrowType>::value, Status>::type
NullableArray(size_t size, size_t num_nulls, uint32_t seed, std::shared_ptr<Array>* out) {
  std::vector<typename ArrowType::c_type> values;

  // Seed is random in Arrow right now
  (void)seed;
  ::arrow::test::randint<typename ArrowType::c_type>(size, 0, 64, &values);
  std::vector<uint8_t> valid_bytes(size, 1);

  for (size_t i = 0; i < num_nulls; i++) {
    valid_bytes[i * 2] = 0;
  }

  // Passing data type so this will work with TimestampType too
  ::arrow::NumericBuilder<ArrowType> builder(std::make_shared<ArrowType>(),
                                             ::arrow::default_memory_pool());
  RETURN_NOT_OK(builder.Append(values.data(), values.size(), valid_bytes.data()));
  return builder.Finish(out);
}

template <typename ArrowType>
typename std::enable_if<is_arrow_date<ArrowType>::value, Status>::type NullableArray(
    size_t size, size_t num_nulls, uint32_t seed, std::shared_ptr<Array>* out) {
  std::vector<typename ArrowType::c_type> values;

  // Seed is random in Arrow right now
  (void)seed;
  ::arrow::test::randint<typename ArrowType::c_type>(size, 0, 64, &values);
  for (size_t i = 0; i < size; i++) {
    values[i] *= 86400000;
  }
  std::vector<uint8_t> valid_bytes(size, 1);

  for (size_t i = 0; i < num_nulls; i++) {
    valid_bytes[i * 2] = 0;
  }

  // Passing data type so this will work with TimestampType too
  ::arrow::NumericBuilder<ArrowType> builder(std::make_shared<ArrowType>(),
                                             ::arrow::default_memory_pool());
  builder.Append(values.data(), values.size(), valid_bytes.data());
  return builder.Finish(out);
}

// This helper function only supports (size/2) nulls yet.
template <typename ArrowType>
typename std::enable_if<
    is_arrow_string<ArrowType>::value || is_arrow_binary<ArrowType>::value, Status>::type
NullableArray(size_t size, size_t num_nulls, uint32_t seed,
              std::shared_ptr<::arrow::Array>* out) {
  std::vector<uint8_t> valid_bytes(size, 1);

  for (size_t i = 0; i < num_nulls; i++) {
    valid_bytes[i * 2] = 0;
  }

  using BuilderType = typename ::arrow::TypeTraits<ArrowType>::BuilderType;
  BuilderType builder;

  const int kBufferSize = 10;
  uint8_t buffer[kBufferSize];
  for (size_t i = 0; i < size; i++) {
    if (!valid_bytes[i]) {
      RETURN_NOT_OK(builder.AppendNull());
    } else {
      ::arrow::test::random_bytes(kBufferSize, seed + static_cast<uint32_t>(i), buffer);
      RETURN_NOT_OK(builder.Append(buffer, kBufferSize));
    }
  }
  return builder.Finish(out);
}

// This helper function only supports (size/2) nulls yet,
// same as NullableArray<String|Binary>(..)
template <typename ArrowType>
typename std::enable_if<is_arrow_fixed_size_binary<ArrowType>::value, Status>::type
NullableArray(size_t size, size_t num_nulls, uint32_t seed,
              std::shared_ptr<::arrow::Array>* out) {
  std::vector<uint8_t> valid_bytes(size, 1);

  for (size_t i = 0; i < num_nulls; i++) {
    valid_bytes[i * 2] = 0;
  }

  using BuilderType = typename ::arrow::TypeTraits<ArrowType>::BuilderType;
  const int byte_width = 10;
  BuilderType builder(::arrow::fixed_size_binary(byte_width));

  const int kBufferSize = byte_width;
  uint8_t buffer[kBufferSize];
  for (size_t i = 0; i < size; i++) {
    if (!valid_bytes[i]) {
      RETURN_NOT_OK(builder.AppendNull());
    } else {
      ::arrow::test::random_bytes(kBufferSize, seed + static_cast<uint32_t>(i), buffer);
      RETURN_NOT_OK(builder.Append(buffer));
    }
  }
  return builder.Finish(out);
}

template <typename ArrowType>
typename std::enable_if<is_arrow_decimal<ArrowType>::value, Status>::type NullableArray(
    size_t size, size_t num_nulls, uint32_t seed, std::shared_ptr<::arrow::Array>* out) {
  std::vector<uint8_t> valid_bytes(size, 1);

  for (size_t i = 0; i < num_nulls; i++) {
    valid_bytes[i * 2] = 0;
  }

  constexpr int32_t kDecimalPrecision = 24;
  constexpr int32_t kDecimalScale = 7;
  const auto type = ::arrow::decimal(kDecimalPrecision, kDecimalScale);

  using BuilderType = typename ::arrow::TypeTraits<ArrowType>::BuilderType;
  BuilderType builder(type);

  std::mt19937 gen(seed);
  UniformDecimalDistribution decimal_dist(kDecimalPrecision);

  for (size_t i = 0; i < size; i++) {
    if (!valid_bytes[i]) {
      RETURN_NOT_OK(builder.AppendNull());
    } else {
      RETURN_NOT_OK(builder.Append(decimal_dist(gen)));
    }
  }
  return builder.Finish(out);
}

// This helper function only supports (size/2) nulls yet.
template <class ArrowType>
typename std::enable_if<is_arrow_bool<ArrowType>::value, Status>::type NullableArray(
    size_t size, size_t num_nulls, uint32_t seed, std::shared_ptr<Array>* out) {
  std::vector<uint8_t> values;

  // Seed is random in Arrow right now
  (void)seed;

  ::arrow::test::randint<uint8_t>(size, 0, 1, &values);
  std::vector<uint8_t> valid_bytes(size, 1);

  for (size_t i = 0; i < num_nulls; i++) {
    valid_bytes[i * 2] = 0;
  }

  ::arrow::BooleanBuilder builder;
  RETURN_NOT_OK(builder.Append(values.data(), values.size(), valid_bytes.data()));
  return builder.Finish(out);
}

/// Wrap an Array into a ListArray by splitting it up into size lists.
///
/// This helper function only supports (size/2) nulls.
Status MakeListArray(const std::shared_ptr<Array>& values, int64_t size,
                     int64_t null_count, bool nullable_values,
                     std::shared_ptr<::arrow::ListArray>* out) {
  // We always include an empty list
  int64_t non_null_entries = size - null_count - 1;
  int64_t length_per_entry = values->length() / non_null_entries;

  auto offsets = std::make_shared<::arrow::PoolBuffer>(::arrow::default_memory_pool());
  RETURN_NOT_OK(offsets->Resize((size + 1) * sizeof(int32_t)));
  int32_t* offsets_ptr = reinterpret_cast<int32_t*>(offsets->mutable_data());

  auto null_bitmap =
      std::make_shared<::arrow::PoolBuffer>(::arrow::default_memory_pool());
  int64_t bitmap_size = ::arrow::BitUtil::CeilByte(size) / 8;
  RETURN_NOT_OK(null_bitmap->Resize(bitmap_size));
  uint8_t* null_bitmap_ptr = null_bitmap->mutable_data();
  memset(null_bitmap_ptr, 0, bitmap_size);

  int32_t current_offset = 0;
  for (int64_t i = 0; i < size; i++) {
    offsets_ptr[i] = current_offset;
    if (!(((i % 2) == 0) && ((i / 2) < null_count))) {
      // Non-null list (list with index 1 is always empty).
      ::arrow::BitUtil::SetBit(null_bitmap_ptr, i);
      if (i != 1) {
        current_offset += static_cast<int32_t>(length_per_entry);
      }
    }
  }
  offsets_ptr[size] = static_cast<int32_t>(values->length());

  auto value_field = ::arrow::field("item", values->type(), nullable_values);
  *out = std::make_shared<::arrow::ListArray>(::arrow::list(value_field), size, offsets,
                                              values, null_bitmap, null_count);

  return Status::OK();
}

static std::shared_ptr<::arrow::Column> MakeColumn(const std::string& name,
                                                   const std::shared_ptr<Array>& array,
                                                   bool nullable) {
  auto field = ::arrow::field(name, array->type(), nullable);
  return std::make_shared<::arrow::Column>(field, array);
}

static std::shared_ptr<::arrow::Column> MakeColumn(
    const std::string& name, const std::vector<std::shared_ptr<Array>>& arrays,
    bool nullable) {
  auto field = ::arrow::field(name, arrays[0]->type(), nullable);
  return std::make_shared<::arrow::Column>(field, arrays);
}

std::shared_ptr<::arrow::Table> MakeSimpleTable(const std::shared_ptr<Array>& values,
                                                bool nullable) {
  std::shared_ptr<::arrow::Column> column = MakeColumn("col", values, nullable);
  std::vector<std::shared_ptr<::arrow::Column>> columns({column});
  std::vector<std::shared_ptr<::arrow::Field>> fields({column->field()});
  auto schema = std::make_shared<::arrow::Schema>(fields);
  return std::make_shared<::arrow::Table>(schema, columns);
}

template <typename T>
void ExpectArray(T* expected, Array* result) {
  auto p_array = static_cast<::arrow::PrimitiveArray*>(result);
  for (int i = 0; i < result->length(); i++) {
    EXPECT_EQ(expected[i], reinterpret_cast<const T*>(p_array->values()->data())[i]);
  }
}

template <typename ArrowType>
void ExpectArrayT(void* expected, Array* result) {
  ::arrow::PrimitiveArray* p_array = static_cast<::arrow::PrimitiveArray*>(result);
  for (int64_t i = 0; i < result->length(); i++) {
    EXPECT_EQ(reinterpret_cast<typename ArrowType::c_type*>(expected)[i],
              reinterpret_cast<const typename ArrowType::c_type*>(
                  p_array->values()->data())[i]);
  }
}

template <>
void ExpectArrayT<::arrow::BooleanType>(void* expected, Array* result) {
  ::arrow::BooleanBuilder builder;
  EXPECT_OK(builder.Append(reinterpret_cast<uint8_t*>(expected), result->length()));

  std::shared_ptr<Array> expected_array;
  EXPECT_OK(builder.Finish(&expected_array));
  EXPECT_TRUE(result->Equals(*expected_array));
}

}  // namespace arrow

}  // namespace parquet
