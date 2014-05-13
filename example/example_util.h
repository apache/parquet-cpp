#ifndef PARQUET_EXAMPLE_UTIL_H
#define PARQUET_EXAMPLE_UTIL_H

#include <string>
#include <parquet/parquet.h>

bool GetFileMetadata(const std::string& path, parquet::FileMetaData* metadata);

#endif
