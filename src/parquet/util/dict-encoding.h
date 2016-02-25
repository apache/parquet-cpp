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

// Initially imported from Apache Impala on 2016-02-22, and has been modified
// since for parquet-cpp

#ifndef PARQUET_UTIL_DICT_ENCODING_H
#define PARQUET_UTIL_DICT_ENCODING_H

#include <algorithm>
#include <cstdint>
#include <limits>
#include <vector>

#include "parquet/types.h"
#include "parquet/encodings/plain-encoding.h"
#include "parquet/util/hash-util.h"
#include "parquet/util/mem-pool.h"
#include "parquet/util/rle-encoding.h"

namespace parquet_cpp {

// Initially 1024 elements
static constexpr int INITIAL_HASH_TABLE_SIZE = 1 << 10;
static constexpr int32_t HASH_SLOT_EMPTY = std::numeric_limits<int32_t>::max();

// The maximum load factor for the hash table before resizing.
static constexpr double MAX_HASH_LOAD = 0.7;

/// See the dictionary encoding section of https://github.com/Parquet/parquet-format.
/// The encoding supports streaming encoding. Values are encoded as they are added while
/// the dictionary is being constructed. At any time, the buffered values can be
/// written out with the current dictionary size. More values can then be added to
/// the encoder, including new dictionary entries.
class DictEncoderBase {
 public:
  virtual ~DictEncoderBase() {
    DCHECK(buffered_indices_.empty());
  }

  /// Writes out the encoded dictionary to buffer. buffer must be preallocated to
  /// dict_encoded_size() bytes.
  virtual void WriteDict(uint8_t* buffer) = 0;

  /// The number of entries in the dictionary.
  virtual int num_entries() const = 0;

  /// Clears all the indices (but leaves the dictionary).
  void ClearIndices() { buffered_indices_.clear(); }

  /// Returns a conservative estimate of the number of bytes needed to encode the buffered
  /// indices. Used to size the buffer passed to WriteIndices().
  int EstimatedDataEncodedSize() {
    return 1 + RleEncoder::MaxBufferSize(bit_width(), buffered_indices_.size());
  }

  /// The minimum bit width required to encode the currently buffered indices.
  int bit_width() const {
    if (UNLIKELY(num_entries() == 0)) return 0;
    if (UNLIKELY(num_entries() == 1)) return 1;
    return BitUtil::Log2(num_entries());
  }

  /// Writes out any buffered indices to buffer preceded by the bit width of this data.
  /// Returns the number of bytes written.
  /// If the supplied buffer is not big enough, returns -1.
  /// buffer must be preallocated with buffer_len bytes. Use EstimatedDataEncodedSize()
  /// to size buffer.
  int WriteIndices(uint8_t* buffer, int buffer_len);

  int hash_table_size() { return hash_table_size_; }
  int dict_encoded_size() { return dict_encoded_size_; }

 protected:
  explicit DictEncoderBase(MemPool* pool) :
      hash_table_size_(INITIAL_HASH_TABLE_SIZE),
      mod_bitmask_(hash_table_size_ - 1),
      hash_slots_(hash_table_size_, HASH_SLOT_EMPTY),
      pool_(pool),
      dict_encoded_size_(0) {}

  typedef int32_t hash_slot_t;

  /// Size of the table. Must be a power of 2.
  int hash_table_size_;

  // Store hash_table_size_ - 1, so that j & mod_bitmask_ is equivalent to j %
  // hash_table_size_, but uses far fewer CPU cycles
  int mod_bitmask_;

  // We use a fixed-size hash table with linear probing
  //
  // These values correspond to the uniques_ array
  std::vector<hash_slot_t> hash_slots_;

  // For ByteArray / FixedLenByteArray data. Not owned
  MemPool* pool_;

  /// Indices that have not yet be written out by WriteIndices().
  std::vector<int> buffered_indices_;

  /// The number of bytes needed to encode the dictionary.
  int dict_encoded_size_;
};

template <typename T>
class DictEncoder : public DictEncoderBase {
 public:
  explicit DictEncoder(MemPool* pool = nullptr, int type_length = -1) :
      DictEncoderBase(pool),
      type_length_(type_length) { }

  // TODO(wesm): think about how to address the construction semantics in
  // encodings/dictionary-encoding.h
  void SetMemPool(MemPool* pool) {
    pool_ = pool;
  }

  /// Encode value. Note that this does not actually write any data, just
  /// buffers the value's index to be written later.
  void Put(const T& value);

  virtual void WriteDict(uint8_t* buffer);

  virtual int num_entries() const { return uniques_.size(); }

 private:
  // The unique observed values
  std::vector<T> uniques_;

  bool SlotDifferent(const T& v, hash_slot_t slot);
  void DoubleTableSize();

  /// Size of each encoded dictionary value. -1 for variable-length types.
  int type_length_;

  /// Hash function for mapping a value to a bucket.
  inline uint32_t Hash(const T& value) const;

  /// Adds value to the hash table and updates dict_encoded_size_
  void AddDictKey(const T& value);
};

template<typename T>
inline uint32_t DictEncoder<T>::Hash(const T& value) const {
  return HashUtil::Hash(&value, sizeof(value), 0);
}

template<>
inline uint32_t DictEncoder<ByteArray>::Hash(const ByteArray& value) const {
  return HashUtil::Hash(value.ptr, value.len, 0);
}

template<>
inline uint32_t DictEncoder<FixedLenByteArray>::Hash(
    const FixedLenByteArray& value) const {
  return HashUtil::Hash(value.ptr, type_length_, 0);
}

template <typename T>
inline bool DictEncoder<T>::SlotDifferent(const T& v, hash_slot_t slot) {
  return v != uniques_[slot];
}

template <>
inline bool DictEncoder<FixedLenByteArray>::SlotDifferent(
    const FixedLenByteArray& v, hash_slot_t slot) {
  return 0 != memcmp(v.ptr, uniques_[slot].ptr, type_length_);
}

template <typename T>
inline void DictEncoder<T>::Put(const T& v) {
  uint32_t j = Hash(v) & mod_bitmask_;
  hash_slot_t index = hash_slots_[j];

  // Find an empty slot
  while (HASH_SLOT_EMPTY != index && SlotDifferent(v, index)) {
    // Linear probing
    ++j;
    if (j == hash_table_size_) j = 0;
    index = hash_slots_[j];
  }

  int bytes_added = 0;
  if (index == HASH_SLOT_EMPTY) {
    // Not in the hash table, so we insert it now
    index = uniques_.size();
    hash_slots_[j] = index;
    AddDictKey(v);

    if (UNLIKELY(uniques_.size() >
            static_cast<size_t>(hash_table_size_ * MAX_HASH_LOAD))) {
      DoubleTableSize();
    }
  }

  buffered_indices_.push_back(index);
}

template <typename T>
inline void DictEncoder<T>::DoubleTableSize() {
  int new_size = hash_table_size_ * 2;
  std::vector<hash_slot_t> new_hash_slots(new_size, HASH_SLOT_EMPTY);
  hash_slot_t index, slot;
  uint32_t j;
  for (int i = 0; i < hash_table_size_; ++i) {
    index = hash_slots_[i];

    if (index == HASH_SLOT_EMPTY) {
      continue;
    }

    // Compute the hash value mod the new table size to start looking for an
    // empty slot
    const T& v = uniques_[index];

    // Find an empty slot in the new hash table
    j = Hash(v) & (new_size - 1);
    slot = new_hash_slots[j];
    while (HASH_SLOT_EMPTY != slot && SlotDifferent(v, slot)) {
      ++j;
      if (j == new_size) j = 0;
      slot = new_hash_slots[j];
    }

    // Copy the old slot index to the new hash table
    new_hash_slots[j] = index;
  }

  hash_table_size_ = new_size;
  mod_bitmask_ = new_size - 1;
  new_hash_slots.swap(hash_slots_);
}

template<typename T>
inline void DictEncoder<T>::AddDictKey(const T& v) {
  uniques_.push_back(v);
  dict_encoded_size_ += sizeof(T);
}

template<>
inline void DictEncoder<ByteArray>::AddDictKey(const ByteArray& v) {
  uint8_t* heap = pool_->Allocate(v.len);
  if (UNLIKELY(heap == nullptr)) {
    throw ParquetException("out of memory");
  }
  memcpy(heap, v.ptr, v.len);

  uniques_.push_back(ByteArray(v.len, heap));
  dict_encoded_size_ += v.len + sizeof(uint32_t);
}

template<>
inline void DictEncoder<FixedLenByteArray>::AddDictKey(const FixedLenByteArray& v) {
  uint8_t* heap = pool_->Allocate(type_length_);
  if (UNLIKELY(heap == nullptr)) {
    throw ParquetException("out of memory");
  }
  memcpy(heap, v.ptr, type_length_);

  uniques_.push_back(FixedLenByteArray(heap));
  dict_encoded_size_ += type_length_;
}

template <typename T>
inline void DictEncoder<T>::WriteDict(uint8_t* buffer) {
  // For primitive types, only a memcpy
  memcpy(buffer, &uniques_[0], sizeof(T) * uniques_.size());
}

// ByteArray and FLBA already have the dictionary encoded in their data heaps
template <>
inline void DictEncoder<ByteArray>::WriteDict(uint8_t* buffer) {
  for (const ByteArray& v : uniques_) {
    memcpy(buffer, reinterpret_cast<const void*>(&v.len), sizeof(uint32_t));
    buffer += sizeof(uint32_t);
    memcpy(buffer, v.ptr, v.len);
    buffer += v.len;
  }
}

template <>
inline void DictEncoder<FixedLenByteArray>::WriteDict(uint8_t* buffer) {
  for (const FixedLenByteArray& v : uniques_) {
    memcpy(buffer, v.ptr, type_length_);
    buffer += type_length_;
  }
}

inline int DictEncoderBase::WriteIndices(uint8_t* buffer, int buffer_len) {
  // Write bit width in first byte
  *buffer = bit_width();
  ++buffer;
  --buffer_len;

  RleEncoder encoder(buffer, buffer_len, bit_width());
  for (int index : buffered_indices_) {
    if (!encoder.Put(index)) return -1;
  }
  encoder.Flush();
  return 1 + encoder.len();
}

// ----------------------------------------------------------------------
// Dictionary decoding

/// Decoder class for dictionary encoded data. This class does not allocate any
/// buffers. The input buffers (dictionary buffer and RLE buffer) must be maintained
/// by the caller and valid as long as this object is.
class DictDecoderBase {
 public:
  /// The rle encoded indices into the dictionary.
  void SetData(uint8_t* buffer, int buffer_len) {
    DCHECK_GT(buffer_len, 0);
    uint8_t bit_width = *buffer;
    DCHECK_GE(bit_width, 0);
    ++buffer;
    --buffer_len;
    data_decoder_.Reset(buffer, buffer_len, bit_width);
  }

  virtual ~DictDecoderBase() {}

  virtual int num_entries() const = 0;

 protected:
  RleDecoder data_decoder_;
};

template<typename T>
class DictDecoder : public DictDecoderBase {
 public:
  /// The input buffer containing the dictionary.  'dict_len' is the byte length
  /// of dict_buffer.
  /// For string data, the decoder returns StringValues with data directly from
  /// dict_buffer (i.e. no copies).
  /// fixed_len_size is the size that must be passed to decode fixed-length
  /// dictionary values (values stored using FIXED_LEN_BYTE_ARRAY).
  DictDecoder(uint8_t* dict_buffer, int dict_len, int num_values, int fixed_len_size);

  /// Construct empty dictionary.
  DictDecoder() {}

  /// Reset decoder to fresh state.
  void Reset(uint8_t* dict_buffer, int dict_len, int num_values, int fixed_len_size);

  virtual int num_entries() const { return dict_.size(); }

  /// Returns the next value.  Returns false if the data is invalid.
  /// For StringValues, this does not make a copy of the data.  Instead,
  /// the string data is from the dictionary buffer passed into the c'tor.
  bool GetValue(T* value);

 private:
  std::vector<T> dict_;
};

template<typename T>
inline bool DictDecoder<T>::GetValue(T* value) {
  int index = -1; // Initialize to avoid compiler warning.
  bool result = data_decoder_.Get(&index);
  // Use & to avoid branches.
  if (LIKELY(result & (index >= 0) & (index < dict_.size()))) {
    *value = dict_[index];
    return true;
  }
  return false;
}

template<typename T>
inline DictDecoder<T>::DictDecoder(uint8_t* dict_buffer, int dict_len, int num_values,
    int fixed_len_size) {
  Reset(dict_buffer, dict_len, num_values, fixed_len_size);
}

template <typename T>
inline void DictDecoder<T>::Reset(uint8_t* dict_buffer, int dict_len, int num_values,
    int fixed_len_size) {
  dict_.resize(dict_len);
  DecodePlain<T>(dict_buffer, dict_len, num_values, fixed_len_size, &dict_[0]);
}

} // namespace parquet_cpp

#endif // PARQUET_UTIL_DICT_ENCODING_H
