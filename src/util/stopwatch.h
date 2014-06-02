#ifndef PARQUET_UTIL_STOPWATCH_H
#define PARQUET_UTIL_STOPWATCH_H

#include <iostream>
#include <stdio.h>

namespace parquet_cpp {

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

}

#endif

