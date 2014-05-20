#include <parquet/parquet.h>
#include <iostream>
#include <stdio.h>

#include "example_util.h"
#include "encodings.h"

using namespace parquet;
using namespace parquet_cpp;
using namespace std;

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

  TEST("Plain decoder", TestPlainIntEncoding, plain_int_data, 1);
  TEST("Plain decoder", TestPlainIntEncoding, plain_int_data, 16);
  TEST("Plain decoder", TestPlainIntEncoding, plain_int_data, 32);
  TEST("Plain decoder", TestPlainIntEncoding, plain_int_data, 64);

  return 0;
}
