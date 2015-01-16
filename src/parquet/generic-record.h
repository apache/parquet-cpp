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
#include "parquet/schema.h"

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

// Base class for all generic objects.
class GenericElement {
 public:
  enum DatumType {
    PRIMITIVE,
    LIST,
    MAP,
    STRUCT
  };

  virtual std::string ToString(const Schema::Element* schema,
      const std::string& prefix = "") const = 0;

  virtual DatumType datum_type() const = 0;
};

// Base class for all primitives.
class GenericDatum : public GenericElement {
 public:
  virtual DatumType datum_type() const { return PRIMITIVE; }

  virtual bool GetBool() const = 0;
  virtual int32_t GetInt32() const = 0;
  virtual int64_t GetInt64() const = 0;
  virtual float GetFloat() const = 0;
  virtual double GetDouble() const = 0;
  virtual ByteArray GetByteArray() const = 0;

 protected:
  GenericDatum() {}

 private:
  GenericDatum(const GenericDatum&);
  GenericDatum& operator=(const GenericDatum&);
};

// Templated implementation for GenericDatum, one is stamped out for each type.
template <typename T>
class PrimitiveDatum : public GenericDatum {
 public:
  static boost::shared_ptr<PrimitiveDatum<T> > Create(T v) {
    boost::shared_ptr<PrimitiveDatum<T> > d(new PrimitiveDatum<T>(v));
    return d;
  }

  virtual std::string ToString(const Schema::Element* schema,
      const std::string& prefix = "") const {
    std::stringstream ss;
    if (schema != NULL) {
      ss << prefix << schema->name() << ": " << v_;
    } else {
      ss << prefix << v_;
    }
    return ss.str();
  }

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
inline std::string ByteArrayDatum::ToString(
    const Schema::Element* schema, const std::string& prefix) const {
  std::string v(reinterpret_cast<const char*>(v_.ptr), v_.len);
  std::stringstream ss;
  if (schema != NULL) {
    ss << prefix << schema->name() << ":" << v;
  } else {
    ss << prefix << v;
  }
  return ss.str();
}

class GenericList : public GenericElement {
 public:
  static boost::shared_ptr<GenericList> Create() {
    return boost::shared_ptr<GenericList>(new GenericList());
  }

  virtual GenericDatum::DatumType datum_type() const { return GenericDatum::LIST; }
  virtual std::string ToString(const Schema::Element* schema,
      const std::string& prefix = "") const;

  const GenericDatum* Get(int idx) const;
  void Put(const boost::shared_ptr<GenericDatum>& e) {
    elements_.push_back(e);
  }

 private:
  GenericList() {}

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

class GenericStruct : public GenericElement {
 public:
  static boost::shared_ptr<GenericStruct> Create(int initial_size = 0) {
    return boost::shared_ptr<GenericStruct>(new GenericStruct(initial_size));
  }

  virtual std::string ToString(const Schema::Element* schema,
      const std::string& prefix = "") const;
  virtual GenericDatum::DatumType datum_type() const { return GenericDatum::STRUCT; }


  const GenericElement* Get(uint32_t idx) const {
    if (idx >= fields_.size()) return NULL;
    return fields_[idx].get();
  }

  GenericElement* Get(uint32_t idx) {
    if (idx >= fields_.size()) return NULL;
    return fields_[idx].get();
  }

  void Put(int idx, const boost::shared_ptr<GenericElement>& d) {
    if (fields_.size() <= idx) fields_.resize(idx + 1);
    fields_[idx] = d;
  }

 private:
  GenericStruct(int initial_size) {
    fields_.resize(initial_size);
  }

  GenericStruct(const GenericStruct&);
  GenericStruct& operator=(const GenericStruct&);

  std::vector<boost::shared_ptr<GenericElement> > fields_;
};

class RecordReader {
 public:
  // Creates a record reader for a single row group. This object can then be used
  // to return batches of rows.
  RecordReader(
      const Schema* schema,
      const parquet::FileMetaData* metadata,
      int row_group_idx,
      const std::vector<const Schema::Element*>& projected_columns,
      const std::vector<const parquet::ColumnMetaData*>& projected_metadata,
      const std::vector<InputStream*>& streams);

  ~RecordReader();

  int64_t rows_left() const {
    return metadata_->row_groups[row_group_idx_].num_rows - rows_returned_;
  }

  // Returns the next batch of records.
  std::vector<boost::shared_ptr<GenericStruct> > GetNext();

 private:
  struct ColumnState {
    ColumnReader* reader;
    boost::shared_ptr<GenericDatum> datum;
    int rep_level;
    int def_level;

    ColumnState() : reader(NULL) {}
    void ReadNext();
  };

  const Schema* schema_;
  const parquet::FileMetaData* metadata_;
  const int row_group_idx_;
  std::vector<ColumnState> readers_;

  int64_t rows_returned_;
};

}

#endif

