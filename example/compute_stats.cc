#include <parquet/parquet.h>
#include <iostream>
#include <stdio.h>

#include "example_util.h"

using namespace parquet;
using namespace parquet_cpp;
using namespace std;

int main(int argc, char** argv) {
  if (argc != 2) {
    cerr << "Usage: compute_stats <file>" << endl;
    return -1;
  }
  FileMetaData metadata;
  if (!GetFileMetadata(argv[1], &metadata)) return -1;

  ColumnReader reader(NULL, NULL);
  printf("%d\n", reader.HasNext());
  return 0;
}
