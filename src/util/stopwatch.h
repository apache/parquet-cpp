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

