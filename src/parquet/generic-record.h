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

#ifndef PARQUET_GENERIC_RECORD_H
#define PARQUET_GENERIC_RECORD_H

#include "parquet/parquet.h"

#include <map>
#include <string>
#include <vector>
#include <boost/shared_ptr.hpp>
#include <boost/lexical_cast.hpp>

namespace parquet_cpp {

// These classes provide a generic record abstraction that fits the parquet
// data model. It is expected though, that most applications will work directly
// with their record abstraction (e.g. thrift, avro or protos) without using
// this class.
// The focus of this implementation is simplicity and not performance.

// Generic interface for the base types.
class GenericDatum {
 public:
  enum DatumType {
    PRIMITIVE,
    LIST,
    MAP,
    STRUCT
  };

  virtual bool GetBool() const = 0;
  virtual int32_t GetInt32() const = 0;
  virtual int64_t GetInt64() const = 0;
  virtual float GetFloat() const = 0;
  virtual double GetDouble() const = 0;
  virtual ByteArray GetByteArray() const = 0;

  virtual std::string ToString() const = 0;

  virtual DatumType datum_type() const = 0;

 protected:
  GenericDatum() {}

 private:
  GenericDatum(const GenericDatum&);
  GenericDatum& operator=(const GenericDatum&);
};

// Templated implementation for GenericDatum, one is stamped out for each
// type.
template <typename T>
class PrimitiveDatum : public GenericDatum {
 public:
  static boost::shared_ptr<PrimitiveDatum<T> > Create(T v) {
    boost::shared_ptr<PrimitiveDatum<T> > d(new PrimitiveDatum<T>(v));
    return d;
  }

  virtual std::string ToString() const {
    return boost::lexical_cast<std::string>(v_);
  }

  virtual DatumType datum_type() const { return PRIMITIVE; }

  // Template specialization is used so only the one for type T is defined.
  // e.g., PrimitiveDatum<int> will special GetInt32.
  virtual bool GetBool() const { throw ParquetException("Unsupported type."); }
  virtual int32_t GetInt32() const { throw ParquetException("Unsupported type."); }
  virtual int64_t GetInt64() const { throw ParquetException("Unsupported type."); }
  virtual float GetFloat() const { throw ParquetException("Unsupported type."); }
  virtual double GetDouble() const { throw ParquetException("Unsupported type."); }
  virtual ByteArray GetByteArray() const { throw ParquetException("Unsupported type."); }

  T get() const { return v_; }
  void set(T v) { v_ = v; }

 private:
  PrimitiveDatum(T v) : v_(v) { }

  PrimitiveDatum(const PrimitiveDatum&);
  PrimitiveDatum& operator=(const PrimitiveDatum&);

  T v_;
};

typedef PrimitiveDatum<bool> BoolDatum;
typedef PrimitiveDatum<int32_t> Int32Datum;
typedef PrimitiveDatum<int64_t> Int64Datum;
typedef PrimitiveDatum<float> FloatDatum;
typedef PrimitiveDatum<double> DoubleDatum;
typedef PrimitiveDatum<ByteArray> ByteArrayDatum;

// Template specializations.
template<> inline bool BoolDatum::GetBool() const { return v_; }
template<> inline int32_t Int32Datum::GetInt32() const { return v_; }
template<> inline int64_t Int64Datum::GetInt64() const { return v_; }
template<> inline float FloatDatum::GetFloat() const { return v_; }
template<> inline double DoubleDatum::GetDouble() const { return v_; }
template<> inline ByteArray ByteArrayDatum::GetByteArray() const { return v_; }

template<>
inline std::string ByteArrayDatum::ToString() const {
  return std::string(reinterpret_cast<const char*>(v_.ptr), v_.len);
}

class GenericList {
 public:
   const GenericDatum* Get(int idx);

   virtual GenericDatum::DatumType datum_type() const { return GenericDatum::LIST; }

 private:
  GenericList(const GenericList&);
  GenericList& operator=(const GenericList&);
  std::vector<boost::shared_ptr<GenericDatum> > elements_;
};

class GenericMap {
 public:
  const GenericDatum* Get(GenericDatum* key);

  virtual GenericDatum::DatumType datum_type() const { return GenericDatum::MAP; }

 private:
  GenericMap(const GenericMap&);
  GenericMap& operator=(const GenericMap&);
  std::map<boost::shared_ptr<GenericDatum>, boost::shared_ptr<GenericDatum> > elements_;
};

class GenericStruct {
 public:
  virtual GenericDatum::DatumType datum_type() const { return GenericDatum::STRUCT; }

  static boost::shared_ptr<GenericStruct> Create() {
    return boost::shared_ptr<GenericStruct>(new GenericStruct());
  }

  const GenericDatum* Get(const std::string& name) const {
    std::map<std::string, boost::shared_ptr<GenericDatum> >::const_iterator it =
        fields_.find(name);
    if (it == fields_.end()) return NULL;
    return it->second.get();
  }

  void Put(const std::string& name, const boost::shared_ptr<GenericDatum>& d) {
    fields_[name] = d;
  }


  std::string ToString() const;

 private:
  GenericStruct() { }
  GenericStruct(const GenericStruct&);
  GenericStruct& operator=(const GenericStruct&);

  std::map<std::string, boost::shared_ptr<GenericDatum> > fields_;
};

class RecordReader {
 public:
  // Creates a record reader for a single row group. This object can then be used
  // to return batches of rows.
  RecordReader(const parquet::FileMetaData* metadata,
      int row_group_idx,
      const std::vector<const parquet::SchemaElement*>& projected_columns,
      const std::vector<const parquet::ColumnMetaData*>& projected_metadata,
      const std::vector<InputStream*>& streams);

  ~RecordReader();

  int64_t rows_left() const {
    return metadata_->row_groups[row_group_idx_].num_rows - rows_returned_;
  }

  // Returns the next batch of records.
  std::vector<boost::shared_ptr<GenericStruct> > GetNext();

 private:
  const parquet::FileMetaData* metadata_;
  const int row_group_idx_;
  std::vector<ColumnReader*> readers_;

  int64_t rows_returned_;
};

}

#endif

