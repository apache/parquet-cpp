// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <parquet/parquet.h>
#include <iostream>
#include <stdio.h>

#include "example_util.h"
#include "compression/codec.h"
#include "encodings/encodings.h"
#include "util/stopwatch.h"

using namespace impala;
using namespace parquet;
using namespace parquet_cpp;
using namespace std;

/**
 * Test bed for encodings and some utilities to measure their throughput.
 * TODO: this file needs some major cleanup.
 */

class DeltaBitPackEncoder {
 public:
  DeltaBitPackEncoder(int mini_block_size = 8) {
    mini_block_size_ = mini_block_size;
  }

  void Add(int64_t v) {
    values_.push_back(v);
  }

  uint8_t* Encode(int* encoded_len) {
    uint8_t* result = new uint8_t[10 * 1024 * 1024];
    int num_mini_blocks = BitUtil::Ceil(num_values() - 1, mini_block_size_);
    uint8_t* mini_block_widths = NULL;

    BitWriter writer(result, 10 * 1024 * 1024);

    // Writer the size of each block. We only use 1 block currently.
    writer.PutVlqInt(num_mini_blocks * mini_block_size_);

    // Write the number of mini blocks.
    writer.PutVlqInt(num_mini_blocks);

    // Write the number of values.
    writer.PutVlqInt(num_values() - 1);

    // Write the first value.
    writer.PutZigZagVlqInt(values_[0]);

    // Compute the values as deltas and the min delta.
    int64_t min_delta = numeric_limits<int64_t>::max();
    for (int i = values_.size() - 1; i > 0; --i) {
      values_[i] -= values_[i - 1];
      min_delta = min(min_delta, values_[i]);
    }

    // Write out the min delta.
    writer.PutZigZagVlqInt(min_delta);

    // We need to save num_mini_blocks bytes to store the bit widths of the mini blocks.
    mini_block_widths = writer.GetNextBytePtr(num_mini_blocks);

    int idx = 1;
    for (int i = 0; i < num_mini_blocks; ++i) {
      int n = min(mini_block_size_, num_values() - idx);

      // Compute the max delta in this mini block.
      int64_t max_delta = numeric_limits<int64_t>::min();
      for (int j = 0; j < n; ++j) {
        max_delta = max(values_[idx + j], max_delta);
      }

      // The bit width for this block is the number of bits needed to store
      // (max_delta - min_delta).
      int bit_width = BitUtil::NumRequiredBits(max_delta - min_delta);
      mini_block_widths[i] = bit_width;

      // Encode this mini blocking using min_delta and bit_width
      for (int j = 0; j < n; ++j) {
        writer.PutValue(values_[idx + j] - min_delta, bit_width);
      }

      // Pad out the last block.
      for (int j = n; j < mini_block_size_; ++j) {
        writer.PutValue(0, bit_width);
      }
      idx += n;
    }

    writer.Flush();
    *encoded_len = writer.bytes_written();
    return result;
  }

  int num_values() const { return values_.size(); }

 private:
  int mini_block_size_;
  vector<int64_t> values_;
};

class DeltaLengthByteArrayEncoder {
 public:
  DeltaLengthByteArrayEncoder(int mini_block_size = 8) :
    len_encoder_(mini_block_size),
    buffer_(new uint8_t[10 * 1024 * 1024]),
    offset_(0),
    plain_encoded_len_(0) {
  }

  void Add(const string& s) {
    Add(reinterpret_cast<const uint8_t*>(s.data()), s.size());
  }

  void Add(const uint8_t* ptr, int len) {
    plain_encoded_len_ += len + sizeof(int);
    len_encoder_.Add(len);
    memcpy(buffer_ + offset_, ptr, len);
    offset_ += len;
  }

  uint8_t* Encode(int* encoded_len) {
    uint8_t* encoded_lengths = len_encoder_.Encode(encoded_len);
    memmove(buffer_ + *encoded_len + sizeof(int), buffer_, offset_);
    memcpy(buffer_, encoded_len, sizeof(int));
    memcpy(buffer_ + sizeof(int), encoded_lengths, *encoded_len);
    *encoded_len += offset_ + sizeof(int);
    return buffer_;
  }

  int num_values() const { return len_encoder_.num_values(); }
  int plain_encoded_len() const { return plain_encoded_len_; }

 private:
  DeltaBitPackEncoder len_encoder_;
  uint8_t* buffer_;
  int offset_;
  int plain_encoded_len_;
};

class DeltaByteArrayEncoder {
 public:
  DeltaByteArrayEncoder() : plain_encoded_len_(0) {}

  void Add(const string& s) {
    plain_encoded_len_ += s.size() + sizeof(int);
    int min_len = min(s.size(), last_value_.size());
    int prefix_len = 0;
    for (int i = 0; i < min_len; ++i) {
      if (s[i] == last_value_[i]) {
        ++prefix_len;
      } else {
        break;
      }
    }
    prefix_len_encoder_.Add(prefix_len);
    suffix_encoder_.Add(reinterpret_cast<const uint8_t*>(s.data()) + prefix_len,
        s.size() - prefix_len);
    last_value_ = s;
  }

  uint8_t* Encode(int* encoded_len) {
    int prefix_buffer_len;
    uint8_t* prefix_buffer = prefix_len_encoder_.Encode(&prefix_buffer_len);
    int suffix_buffer_len;
    uint8_t* suffix_buffer = suffix_encoder_.Encode(&suffix_buffer_len);

    uint8_t* buffer = new uint8_t[10 * 1024 * 1024];
    memcpy(buffer, &prefix_buffer_len, sizeof(int));
    memcpy(buffer + sizeof(int), prefix_buffer, prefix_buffer_len);
    memcpy(buffer + sizeof(int) + prefix_buffer_len, suffix_buffer, suffix_buffer_len);
    *encoded_len = sizeof(int) + prefix_buffer_len + suffix_buffer_len;
    return buffer;
  }

  int num_values() const { return prefix_len_encoder_.num_values(); }
  int plain_encoded_len() const { return plain_encoded_len_; }

 private:
  DeltaBitPackEncoder prefix_len_encoder_;
  DeltaLengthByteArrayEncoder suffix_encoder_;
  string last_value_;
  int plain_encoded_len_;
};


uint64_t TestPlainIntEncoding(const uint8_t* data, int num_values, int batch_size) {
  uint64_t result = 0;
  PlainDecoder decoder(Type::INT64);
  decoder.SetData(num_values, data, num_values * sizeof(int64_t));
  int64_t values[batch_size];
  for (int i = 0; i < num_values;) {
    int n = decoder.GetInt64(values, batch_size);
    for (int j = 0; j < n; ++j) {
      result += values[j];
    }
    i += n;
  }
  return result;
}

uint64_t TestBinaryPackedEncoding(const char* name, const vector<int64_t>& values,
    int benchmark_iters = -1, int benchmark_batch_size = 1) {
  int mini_block_size;
  if (values.size() < 8) {
    mini_block_size = 8;
  } else if (values.size() < 16) {
    mini_block_size = 16;
  } else {
    mini_block_size = 32;
  }
  DeltaBitPackDecoder decoder(Type::INT64);
  DeltaBitPackEncoder encoder(mini_block_size);
  for (int i = 0; i < values.size(); ++i) {
    encoder.Add(values[i]);
  }

  int raw_len = encoder.num_values() * sizeof(int);
  int len;
  uint8_t* buffer = encoder.Encode(&len);

  if (benchmark_iters == -1) {
    printf("%s\n", name);
    printf("  Raw len: %d\n", raw_len);
    printf("  Encoded len: %d (%0.2f%%)\n", len, len * 100 / (float)raw_len);
    decoder.SetData(encoder.num_values(), buffer, len);
    for (int i = 0; i < encoder.num_values(); ++i) {
      int64_t x = 0;
      decoder.GetInt64(&x, 1);
      if (values[i] != x) {
        cerr << "Bad: " << i << endl;
        cerr << "  " << x << " != " << values[i] << endl;
        break;
      }
    }
    return 0;
  } else {
    printf("%s\n", name);
    printf("  Raw len: %d\n", raw_len);
    printf("  Encoded len: %d (%0.2f%%)\n", len, len * 100 / (float)raw_len);

    uint64_t result = 0;
    int64_t buf[benchmark_batch_size];
    StopWatch sw;
    sw.Start();\
    for (int k = 0; k < benchmark_iters; ++k) {
      decoder.SetData(encoder.num_values(), buffer, len);
      for (int i = 0; i < values.size();) {
        int n = decoder.GetInt64(buf, benchmark_batch_size);
        for (int j = 0; j < n; ++j) {
          result += buf[j];
        }
        i += n;
      }
    }
    uint64_t elapsed = sw.Stop();
    double num_ints = values.size() * benchmark_iters * 1000.;
    printf("%s rate (batch size = %2d): %0.3fM per second.\n",
        name, benchmark_batch_size, num_ints / elapsed);
    return result;
  }
}

#define TEST(NAME, FN, DATA, BATCH_SIZE)\
  sw.Start();\
  for (int i = 0; i < NUM_ITERS; ++i) {\
    FN(reinterpret_cast<uint8_t*>(&DATA[0]), NUM_VALUES, BATCH_SIZE);\
  }\
  elapsed = sw.Stop();\
  printf("%s rate (batch size = %2d): %0.3fM per second.\n",\
      NAME, BATCH_SIZE, mult / elapsed);

void TestPlainIntCompressed(Codec* codec, const vector<int64_t>& data, int num_iters, int batch_size) {
  const uint8_t* raw_data = reinterpret_cast<const uint8_t*>(&data[0]);
  int uncompressed_len = data.size() * sizeof(int64_t);
  uint8_t* decompressed_data = new uint8_t[uncompressed_len];

  int max_compressed_size = codec->MaxCompressedLen(uncompressed_len, raw_data);
  uint8_t* compressed_data = new uint8_t[max_compressed_size];
  int compressed_len = codec->Compress(uncompressed_len, raw_data,
      max_compressed_size, compressed_data);

  printf("\n%s:\n  Uncompressed len: %d\n  Compressed len:   %d\n",
      codec->name(), uncompressed_len, compressed_len);

  double mult = num_iters * data.size() * 1000.;
  StopWatch sw;
  sw.Start();
  uint64_t r = 0;
  for (int i = 0; i < num_iters; ++i) {
    codec->Decompress(compressed_len, compressed_data,
        uncompressed_len, decompressed_data);
    r += TestPlainIntEncoding(decompressed_data, data.size(), batch_size);
  }
  int64_t elapsed = sw.Stop();\
  printf("Compressed(%s) plain int rate (batch size = %2d): %0.3fM per second.\n",
      codec->name(), batch_size, mult / elapsed);

  delete[] compressed_data;
  delete[] decompressed_data;
}

void TestBinaryPacking() {
  vector<int64_t> values;
  values.clear();
  for (int i = 0; i < 100; ++i) values.push_back(0);
  TestBinaryPackedEncoding("Zeros", values);

  values.clear();
  for (int i = 1; i <= 5; ++i) values.push_back(i);
  TestBinaryPackedEncoding("Example 1", values);

  values.clear();
  values.push_back(7);
  values.push_back(5);
  values.push_back(3);
  values.push_back(1);
  values.push_back(2);
  values.push_back(3);
  values.push_back(4);
  values.push_back(5);
  TestBinaryPackedEncoding("Example 2", values);

  // Test rand ints between 0 and 10K
  values.clear();
  for (int i = 0; i < 500000; ++i) {
    values.push_back(rand() % (10000));
  }
  TestBinaryPackedEncoding("Rand [0, 10000)", values);

  // Test rand ints between 0 and 100
  values.clear();
  for (int i = 0; i < 500000; ++i) {
    values.push_back(rand() % 100);
  }
  TestBinaryPackedEncoding("Rand [0, 100)", values);
}

void TestDeltaLengthByteArray() {
  DeltaLengthByteArrayDecoder decoder;
  DeltaLengthByteArrayEncoder encoder;

  vector<string> values;
  values.push_back("Hello");
  values.push_back("World");
  values.push_back("Foobar");
  values.push_back("ABCDEF");

  for (int i = 0; i < values.size(); ++i) {
    encoder.Add(values[i]);
  }

  int len = 0;
  uint8_t* buffer = encoder.Encode(&len);
  printf("DeltaLengthByteArray\n  Raw len: %d\n  Encoded len: %d\n",
      encoder.plain_encoded_len(), len);
  decoder.SetData(encoder.num_values(), buffer, len);
  for (int i = 0; i < encoder.num_values(); ++i) {
    ByteArray v;
    decoder.GetByteArray(&v, 1);
    string r = string((char*)v.ptr, v.len);
    if (r != values[i]) {
      cout << "Bad " << r << " != " << values[i] << endl;
    }
  }
}

void TestDeltaByteArray() {
  DeltaByteArrayDecoder decoder;
  DeltaByteArrayEncoder encoder;

  vector<string> values;

  // Wikipedia example
  values.push_back("myxa");
  values.push_back("myxophyta");
  values.push_back("myxopod");
  values.push_back("nab");
  values.push_back("nabbed");
  values.push_back("nabbing");
  values.push_back("nabit");
  values.push_back("nabk");
  values.push_back("nabob");
  values.push_back("nacarat");
  values.push_back("nacelle");

  for (int i = 0; i < values.size(); ++i) {
    encoder.Add(values[i]);
  }

  int len = 0;
  uint8_t* buffer = encoder.Encode(&len);
  printf("DeltaLengthByteArray\n  Raw len: %d\n  Encoded len: %d\n",
      encoder.plain_encoded_len(), len);
  decoder.SetData(encoder.num_values(), buffer, len);
  for (int i = 0; i < encoder.num_values(); ++i) {
    ByteArray v;
    decoder.GetByteArray(&v, 1);
    string r = string((char*)v.ptr, v.len);
    if (r != values[i]) {
      cout << "Bad " << r << " != " << values[i] << endl;
    }
  }
}

int main(int argc, char** argv) {
  TestBinaryPacking();
  TestDeltaLengthByteArray();
  TestDeltaByteArray();

  StopWatch sw;
  uint64_t elapsed = 0;

  const int NUM_VALUES = 1024 * 1024;
  const int NUM_ITERS = 10;
  const double mult = NUM_VALUES * NUM_ITERS * 1000.;

  vector<int64_t> plain_int_data;
  plain_int_data.resize(NUM_VALUES);

  TEST("Plain decoder", TestPlainIntEncoding, plain_int_data, 1);
  TEST("Plain decoder", TestPlainIntEncoding, plain_int_data, 16);
  TEST("Plain decoder", TestPlainIntEncoding, plain_int_data, 32);
  TEST("Plain decoder", TestPlainIntEncoding, plain_int_data, 64);

  // Test rand ints between 0 and 10K
  vector<int64_t> values;
  for (int i = 0; i < 1000000; ++i) {
    values.push_back(rand() % 10000);
  }
  TestBinaryPackedEncoding("Rand 0-10K", values, 100, 1);
  TestBinaryPackedEncoding("Rand 0-10K", values, 100, 16);
  TestBinaryPackedEncoding("Rand 0-10K", values, 100, 32);
  TestBinaryPackedEncoding("Rand 0-10K", values, 100, 64);

  SnappyCodec snappy_codec;
  Lz4Codec lz4_codec;

  TestPlainIntCompressed(&snappy_codec, values, 100, 1);
  TestPlainIntCompressed(&snappy_codec, values, 100, 16);
  TestPlainIntCompressed(&snappy_codec, values, 100, 32);
  TestPlainIntCompressed(&snappy_codec, values, 100, 64);

  TestPlainIntCompressed(&lz4_codec, values, 100, 1);
  TestPlainIntCompressed(&lz4_codec, values, 100, 16);
  TestPlainIntCompressed(&lz4_codec, values, 100, 32);
  TestPlainIntCompressed(&lz4_codec, values, 100, 64);

  return 0;
}
