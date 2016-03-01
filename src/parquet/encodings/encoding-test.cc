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
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "parquet/schema/descriptor.h"
#include "parquet/encodings/dictionary-encoding.h"
#include "parquet/encodings/plain-encoding.h"
#include "parquet/types.h"
#include "parquet/schema/types.h"
#include "parquet/util/bit-util.h"
#include "parquet/util/buffer.h"
#include "parquet/util/dict-encoding.h"
#include "parquet/util/output.h"
#include "parquet/util/test-common.h"

using std::string;
using std::vector;

namespace parquet_cpp {

namespace test {

TEST(VectorBooleanTest, TestEncodeDecode) {
  // PARQUET-454
  int nvalues = 10000;
  int nbytes = BitUtil::Ceil(nvalues, 8);

  // seed the prng so failure is deterministic
  vector<bool> draws = flip_coins_seed(nvalues, 0.5, 0);

  PlainEncoder<Type::BOOLEAN> encoder(nullptr);
  PlainDecoder<Type::BOOLEAN> decoder(nullptr);

  InMemoryOutputStream dst;
  encoder.Encode(draws, nvalues, &dst);

  std::shared_ptr<Buffer> encode_buffer = dst.GetBuffer();
  ASSERT_EQ(nbytes, encode_buffer->size());

  vector<uint8_t> decode_buffer(nbytes);
  const uint8_t* decode_data = &decode_buffer[0];

  decoder.SetData(nvalues, encode_buffer->data(), encode_buffer->size());
  int values_decoded = decoder.Decode(&decode_buffer[0], nvalues);
  ASSERT_EQ(nvalues, values_decoded);

  for (int i = 0; i < nvalues; ++i) {
    ASSERT_EQ(draws[i], BitUtil::GetArrayBit(decode_data, i)) << i;
  }
}

// ----------------------------------------------------------------------
// test data generation

template <typename T>
void GenerateData(int num_values, T* out, vector<uint8_t>* heap) {
  // seed the prng so failure is deterministic
  random_numbers(num_values, 0, std::numeric_limits<T>::min(),
      std::numeric_limits<T>::max(), out);
}

template <>
void GenerateData<bool>(int num_values, bool* out, vector<uint8_t>* heap) {
  // seed the prng so failure is deterministic
  random_bools(num_values, 0.5, 0, out);
}

template <>
void GenerateData<Int96>(int num_values, Int96* out, vector<uint8_t>* heap) {
  // seed the prng so failure is deterministic
  random_Int96_numbers(num_values, 0, std::numeric_limits<int32_t>::min(),
      std::numeric_limits<int32_t>::max(), out);
}

template <>
void GenerateData<ByteArray>(int num_values, ByteArray* out, vector<uint8_t>* heap) {
  // seed the prng so failure is deterministic
  int max_byte_array_len = 12;
  int num_bytes = max_byte_array_len + sizeof(uint32_t);
  heap->resize(num_values * max_byte_array_len);
  random_byte_array(num_values, 0, heap->data(), out, 2, max_byte_array_len);
}

static int flba_length = 8;

template <>
void GenerateData<FLBA>(int num_values, FLBA* out, vector<uint8_t>* heap) {
  // seed the prng so failure is deterministic
  heap->resize(num_values * flba_length);
  random_fixed_byte_array(num_values, 0, heap->data(), flba_length, out);
}

template <typename T>
void VerifyResults(T* result, T* expected, int num_values) {
  for (int i = 0; i < num_values; ++i) {
    ASSERT_EQ(expected[i], result[i]) << i;
  }
}

template <>
void VerifyResults<FLBA>(FLBA* result, FLBA* expected, int num_values) {
  for (int i = 0; i < num_values; ++i) {
    ASSERT_EQ(0, memcmp(expected[i].ptr, result[i].ptr, flba_length)) << i;
  }
}

// ----------------------------------------------------------------------
// Create some column descriptors

template <typename T>
std::shared_ptr<ColumnDescriptor> ExampleDescr() {
  return nullptr;
}

template <>
std::shared_ptr<ColumnDescriptor> ExampleDescr<FLBA>() {
  auto node = schema::PrimitiveNode::Make("name", Repetition::OPTIONAL,
      Type::FIXED_LEN_BYTE_ARRAY, LogicalType::DECIMAL, flba_length, 10, 2);
  return std::make_shared<ColumnDescriptor>(node, 0, 0);
}

// ----------------------------------------------------------------------
// Plain encoding tests

template <typename Type>
class TestEncodingBase : public ::testing::Test {
 public:
  typedef typename Type::c_type T;
  static constexpr int TYPE = Type::type_num;

  void SetUp() {
    descr_ = ExampleDescr<T>();
    if (descr_) {
      type_length_ = descr_->type_length();
    }
  }

  void TearDown() {
    pool_.FreeAll();
  }

  void InitData(int nvalues, int repeats) {
    num_values_ = nvalues * repeats;
    input_bytes_.resize(num_values_ * sizeof(T));
    output_bytes_.resize(num_values_ * sizeof(T));
    draws_ = reinterpret_cast<T*>(input_bytes_.data());
    decode_buf_ = reinterpret_cast<T*>(output_bytes_.data());
    GenerateData<T>(nvalues, draws_, &data_buffer_);

    // add some repeated values
    for (int j = 1; j < repeats; ++j) {
      for (int i = 0; i < nvalues; ++i) {
        draws_[nvalues * j + i] = draws_[i];
      }
    }
  }

  virtual void CheckRoundtrip() = 0;

  void Execute(int nvalues, int repeats) {
    InitData(nvalues, repeats);
    CheckRoundtrip();
  }

 protected:
  MemPool pool_;

  int num_values_;
  int type_length_;
  T* draws_;
  T* decode_buf_;
  vector<uint8_t> input_bytes_;
  vector<uint8_t> output_bytes_;
  vector<uint8_t> data_buffer_;

  std::shared_ptr<Buffer> encode_buffer_;
  std::shared_ptr<ColumnDescriptor> descr_;
};

// Member variables are not visible to templated subclasses. Possibly figure
// out an alternative to this class layering at some point
#define USING_BASE_MEMBERS()                    \
  using TestEncodingBase<Type>::pool_;          \
  using TestEncodingBase<Type>::descr_;         \
  using TestEncodingBase<Type>::num_values_;    \
  using TestEncodingBase<Type>::draws_;         \
  using TestEncodingBase<Type>::data_buffer_;   \
  using TestEncodingBase<Type>::type_length_;   \
  using TestEncodingBase<Type>::encode_buffer_; \
  using TestEncodingBase<Type>::decode_buf_;


template <typename Type>
class TestPlainEncoding : public TestEncodingBase<Type> {
 public:
  typedef typename Type::c_type T;
  static constexpr int TYPE = Type::type_num;

  virtual void CheckRoundtrip() {
    PlainEncoder<TYPE> encoder(descr_.get());
    PlainDecoder<TYPE> decoder(descr_.get());
    InMemoryOutputStream dst;
    encoder.Encode(draws_, num_values_, &dst);

    encode_buffer_ = dst.GetBuffer();

    decoder.SetData(num_values_, encode_buffer_->data(),
        encode_buffer_->size());
    int values_decoded = decoder.Decode(decode_buf_, num_values_);
    ASSERT_EQ(num_values_, values_decoded);
    VerifyResults<T>(decode_buf_, draws_, num_values_);
  }

 protected:
  USING_BASE_MEMBERS();
};

TYPED_TEST_CASE(TestPlainEncoding, ParquetTypes);

TYPED_TEST(TestPlainEncoding, BasicRoundTrip) {
  this->Execute(10000, 1);
}

// ----------------------------------------------------------------------
// Dictionary encoding tests

typedef ::testing::Types<Int32Type, Int64Type, Int96Type, FloatType, DoubleType,
                         ByteArrayType, FLBAType> DictEncodedTypes;

template <typename Type>
class TestDictionaryEncoding : public TestEncodingBase<Type> {
 public:
  typedef typename Type::c_type T;
  static constexpr int TYPE = Type::type_num;

  void CheckRoundtrip() {
    DictEncoder<T> encoder(&pool_, type_length_);

    dict_buffer_ = std::make_shared<OwnedMutableBuffer>();
    auto indices = std::make_shared<OwnedMutableBuffer>();

    ASSERT_NO_THROW(
        {
          for (int i = 0; i < num_values_; ++i) {
            encoder.Put(draws_[i]);
          }
        });
    dict_buffer_->Resize(encoder.dict_encoded_size());
    encoder.WriteDict(dict_buffer_->mutable_data());

    indices->Resize(encoder.EstimatedDataEncodedSize());
    int actual_bytes = encoder.WriteIndices(indices->mutable_data(),
        indices->size());
    indices->Resize(actual_bytes);

    PlainDecoder<TYPE> dict_decoder(descr_.get());
    dict_decoder.SetData(encoder.num_entries(), dict_buffer_->data(),
        dict_buffer_->size());

    DictionaryDecoder<TYPE> decoder(descr_.get());
    decoder.SetDict(&dict_decoder);

    decoder.SetData(num_values_, indices->data(), indices->size());
    int values_decoded = decoder.Decode(decode_buf_, num_values_);
    ASSERT_EQ(num_values_, values_decoded);

    // TODO(wesm): The DictionaryDecoder must stay alive because the decoded
    // values' data is owned by a buffer inside the DictionaryEncoder. We
    // should revisit when data lifetime is reviewed more generally.
    VerifyResults<T>(decode_buf_, draws_, num_values_);
  }

 protected:
  USING_BASE_MEMBERS();
  std::shared_ptr<OwnedMutableBuffer> dict_buffer_;
};

TYPED_TEST_CASE(TestDictionaryEncoding, DictEncodedTypes);

TYPED_TEST(TestDictionaryEncoding, BasicRoundTrip) {
  this->Execute(2500, 2);
}

TEST(TestDictionaryEncoding, CannotDictDecodeBoolean) {
  PlainDecoder<Type::BOOLEAN> dict_decoder(nullptr);
  DictionaryDecoder<Type::BOOLEAN> decoder(nullptr);

  ASSERT_THROW(decoder.SetDict(&dict_decoder), ParquetException);
}

} // namespace test

} // namespace parquet_cpp
