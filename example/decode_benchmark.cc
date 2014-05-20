#include <parquet/parquet.h>
#include <iostream>
#include <stdio.h>

#include "example_util.h"
#include "encodings.h"

using namespace impala;
using namespace parquet;
using namespace parquet_cpp;
using namespace std;

class DeltaEncoding {
 public:
  DeltaEncoding(int block_size) {
    buffer_.resize(BitUtil::RoundUp(block_size, BLOCK_SIZE));
    index_ = 0;
  }

  void AddInt32(int32_t v) {
    buffer_[index_++] = v;
  }

  int unencoded_size() const { return index_ * sizeof(int32_t); }
  int num_values() const { return index_; }

  uint8_t* Encode(int* encoded_len) {
    uint8_t* result = new uint8_t[1024 * 1024];

    int num_values_padded = index_;
    int num_blocks = BitUtil::Ceil(num_values_padded, BLOCK_SIZE);

    vector<int> delta_buffer;
    delta_buffer.resize(num_values_padded);
    for (int i = 1; i < num_values_padded; ++i) {
      delta_buffer[i] = buffer_[i] - buffer_[i - 1];
    }

    BitWriter writer(result, 1024 * 1024);
    int idx = 0;
    for (int b = 0; b < num_blocks; ++b) {
      int values_this_block = num_values_padded - idx;
      if (values_this_block > BLOCK_SIZE) values_this_block = BLOCK_SIZE;

      int mini_blocks_per_block = BitUtil::Ceil(values_this_block, MINI_BLOCK_SIZE);
      cout << "Values this block: " << values_this_block << endl;
      cout << "Num mini blocks: " << mini_blocks_per_block << endl;

      writer.PutVlqInt(BLOCK_SIZE);
      writer.PutVlqInt(mini_blocks_per_block);
      writer.PutVlqInt(values_this_block);
      writer.PutZigZagVlqInt(buffer_[idx]);
      int end_idx = idx + values_this_block;
      ++idx;

      while (idx < end_idx) {
        int mini_block_size = num_values_padded - idx;
        if (mini_block_size > MINI_BLOCK_SIZE) mini_block_size = MINI_BLOCK_SIZE;

        int min_delta = INT_MAX;
        for (int i = 0; i < mini_block_size; ++i) {
          min_delta = ::min(min_delta, delta_buffer[idx + i]);
        }
        int for_delta = 0;
        for (int i = 0; i < mini_block_size; ++i) {
          delta_buffer[idx + i] = delta_buffer[idx + i] - min_delta;
          for_delta = max(for_delta, delta_buffer[idx + i]);
        }
        int bit_width = BitUtil::Log2(for_delta);

/*
        cout << "deltas in mini block " << mini_block_size << endl;
        cout << "min delta: " << min_delta << endl;
        cout << "for delta: " << for_delta << endl;
        cout << "bit width: " << bit_width << endl;
*/

        writer.PutZigZagVlqInt(min_delta);
        writer.PutAligned<uint8_t>(bit_width, 8);

        for (int i = 0; i < mini_block_size; ++i) {
          writer.PutValue(delta_buffer[idx + i], bit_width);
        }
        idx += mini_block_size;
      }
    }

    writer.Flush();
    *encoded_len = writer.bytes_written();
    return result;
  }

 private:
  vector<int32_t> buffer_;
  int index_;

  static const int BLOCK_SIZE = 128;
  static const int MINI_BLOCK_SIZE = 32;
};

class StopWatch {
 public:
  StopWatch() {
  }

  void Start() {
    clock_gettime(CLOCK_MONOTONIC, &start_);
  }

  // Returns time in nanoseconds.
  uint64_t Stop() {
    timespec end;
    clock_gettime(CLOCK_MONOTONIC, &end);
    return (end.tv_sec - start_.tv_sec) * 1000L * 1000L * 1000L +
           (end.tv_nsec - start_.tv_nsec);
  }

 private:
  timespec start_;
};

uint64_t TestPlainIntEncoding(const uint8_t* data, int num_values, int batch_size) {
  uint64_t result = 0;
  PlainDecoder decoder(NULL);
  decoder.SetData(num_values, data, num_values * sizeof(int32_t));
  int32_t values[batch_size];
  for (int i = 0; i < num_values;) {
    int n = decoder.GetInt32(values, batch_size);
    for (int j = 0; j < n; ++j) {
      result += values[j];
    }
    i += n;
  }
  return result;
}

#define TEST(NAME, FN, DATA, BATCH_SIZE)\
  sw.Start();\
  for (int i = 0; i < NUM_ITERS; ++i) {\
    FN(reinterpret_cast<uint8_t*>(&DATA[0]), NUM_VALUES, BATCH_SIZE);\
  }\
  elapsed = sw.Stop();\
  printf("%s rate (batch size = %2d): %0.2f per second.\n",\
      NAME, BATCH_SIZE, mult / elapsed);

int main(int argc, char** argv) {
  StopWatch sw;
  uint64_t elapsed = 0;

  const int NUM_VALUES = 1024 * 1024;
  const int NUM_ITERS = 10;
  const double mult = NUM_VALUES * 1000. * 1000. * 1000. * NUM_ITERS;

  vector<int32_t> plain_int_data;
  plain_int_data.resize(NUM_VALUES);

  //TEST("Plain decoder", TestPlainIntEncoding, plain_int_data, 1);
  //TEST("Plain decoder", TestPlainIntEncoding, plain_int_data, 16);
  //TEST("Plain decoder", TestPlainIntEncoding, plain_int_data, 32);
  //TEST("Plain decoder", TestPlainIntEncoding, plain_int_data, 64);


  DeltaEncoding encoder(128);
  //for (int i = 1; i <= 5; ++i) encoder.AddInt32(i);
  encoder.AddInt32(7);
  encoder.AddInt32(5);
  encoder.AddInt32(3);
  encoder.AddInt32(1);
  encoder.AddInt32(2);
  encoder.AddInt32(3);
  encoder.AddInt32(4);
  encoder.AddInt32(5);
  int len;
  uint8_t* buffer = encoder.Encode(&len);
  cout << "Raw len: " << encoder.unencoded_size() << endl;
  cout << "Encoded len: " << len << endl;

  cout << "Decoding: " << endl;
  DeltaBinaryPackedDecoder decoder(NULL);
  decoder.SetData(encoder.num_values(), buffer, len);
  for (int i = 0; i < encoder.num_values(); ++i) {
    int32_t x;
    decoder.GetInt32(&x, 1);
    cout << x << endl;
  }

  return 0;
}
