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

#include <algorithm>
#include <cstring>

#include "parquet/column/statistics.h"
#include "parquet/encodings/plain-encoding.h"
#include "parquet/exception.h"
#include "parquet/util/buffer.h"
#include "parquet/util/comparison.h"
#include "parquet/util/output.h"

namespace parquet {

template <typename DType>
TypedRowGroupStatistics<DType>::TypedRowGroupStatistics(
    const ColumnDescriptor* schema, MemoryAllocator* allocator)
    : allocator_(allocator), min_buffer_(0, allocator_), max_buffer_(0, allocator_) {
  SetDescr(schema);
  Reset();
}

template <typename DType>
TypedRowGroupStatistics<DType>::TypedRowGroupStatistics(const typename DType::c_type& min,
    const typename DType::c_type& max, int64_t num_values, int64_t null_count,
    int64_t distinct_count)
    : allocator_(default_allocator()),
      min_buffer_(0, allocator_),
      max_buffer_(0, allocator_) {
  IncrementNumValues(num_values);
  IncrementNullCount(null_count);
  IncrementDistinctCount(distinct_count);

  Copy(min, &min_, min_buffer_);
  Copy(max, &max_, max_buffer_);
  has_min_max_ = true;
}

template <typename DType>
TypedRowGroupStatistics<DType>::TypedRowGroupStatistics(const ColumnDescriptor* schema,
    const std::string& encoded_min, const std::string& encoded_max, int64_t num_values,
    int64_t null_count, int64_t distinct_count, MemoryAllocator* allocator)
    : allocator_(allocator), min_buffer_(0, allocator_), max_buffer_(0, allocator_) {
  IncrementNumValues(num_values);
  IncrementNullCount(null_count);
  IncrementDistinctCount(distinct_count);

  SetDescr(schema);

  if (!encoded_min.empty()) { PlainDecode(encoded_min, &min_); }
  if (!encoded_max.empty()) { PlainDecode(encoded_max, &max_); }
  has_min_max_ = !encoded_min.empty() && !encoded_max.empty();
}

template <typename DType>
bool TypedRowGroupStatistics<DType>::HasMinMax() const {
  return has_min_max_;
}

template <typename DType>
void TypedRowGroupStatistics<DType>::Reset() {
  ResetCounts();
  has_min_max_ = false;
}

template <typename DType>
void TypedRowGroupStatistics<DType>::Update(
    const T* values, int64_t num_not_null, int64_t num_null) {
  DCHECK(num_not_null >= 0);
  DCHECK(num_null >= 0);

  IncrementNullCount(num_null);
  IncrementNumValues(num_not_null);
  // TODO: support distinct count?
  if (num_not_null == 0) return;

  Compare<T> compare(descr_);
  auto batch_minmax = std::minmax_element(values, values + num_not_null, compare);
  if (!has_min_max_) {
    has_min_max_ = true;
    Copy(*batch_minmax.first, &min_, min_buffer_);
    Copy(*batch_minmax.second, &max_, max_buffer_);
  } else {
    Copy(std::min(min_, *batch_minmax.first, compare), &min_, min_buffer_);
    Copy(std::max(max_, *batch_minmax.second, compare), &max_, max_buffer_);
  }
}

template <typename DType>
void TypedRowGroupStatistics<DType>::Merge(const TypedRowGroupStatistics<DType>& other) {
  this->MergeCounts(other);

  if (!other.HasMinMax()) return;

  if (!has_min_max_) {
    Copy(other.min_, &this->min_, min_buffer_);
    Copy(other.max_, &this->max_, max_buffer_);
    has_min_max_ = true;
    return;
  }

  Compare<T> compare(descr_);
  Copy(std::min(this->min_, other.min_, compare), &this->min_, min_buffer_);
  Copy(std::max(this->max_, other.max_, compare), &this->max_, max_buffer_);
}

template <typename DType>
std::string TypedRowGroupStatistics<DType>::EncodeMin() {
  std::string s;
  if (HasMinMax()) this->PlainEncode(min_, &s);
  return s;
}

template <typename DType>
std::string TypedRowGroupStatistics<DType>::EncodeMax() {
  std::string s;
  if (HasMinMax()) this->PlainEncode(max_, &s);
  return s;
}

template <typename DType>
EncodedStatistics TypedRowGroupStatistics<DType>::Encode() {
  EncodedStatistics s;
  if (HasMinMax()) {
    s.set_min(this->EncodeMin());
    s.set_max(this->EncodeMax());
  }
  s.set_null_count(this->null_count());
  return s;
}

template <typename DType>
void TypedRowGroupStatistics<DType>::PlainEncode(const T& src, std::string* dst) {
  PlainEncoder<DType> encoder(descr(), allocator_);
  encoder.Put(&src, 1);
  auto buffer = encoder.FlushValues();
  auto ptr = reinterpret_cast<const char*>(buffer->data());
  dst->assign(ptr, buffer->size());
}

template <typename DType>
void TypedRowGroupStatistics<DType>::PlainDecode(const std::string& src, T* dst) {
  PlainDecoder<DType> decoder(descr());
  decoder.SetData(1, reinterpret_cast<const uint8_t*>(src.c_str()), src.size());
  decoder.Decode(dst, 1);
}

template class TypedRowGroupStatistics<BooleanType>;
template class TypedRowGroupStatistics<Int32Type>;
template class TypedRowGroupStatistics<Int64Type>;
template class TypedRowGroupStatistics<Int96Type>;
template class TypedRowGroupStatistics<FloatType>;
template class TypedRowGroupStatistics<DoubleType>;
template class TypedRowGroupStatistics<ByteArrayType>;
template class TypedRowGroupStatistics<FLBAType>;

}  // namespace parquet
