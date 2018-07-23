#include "decryption_key_retriever.h"

namespace parquet {

// integer key retriever
void IntegerKeyIdRetriever::put_key(uint32_t key_id, const std::string& key) {
  key_map_[key_id] = key;
}

std::string IntegerKeyIdRetriever::get_key(const std::string& key_metadata)
{
  uint32_t key_id;
  memcpy(reinterpret_cast<uint8_t*>(&key_id), key_metadata.c_str(), 4);

  return key_map_[key_id];
}

// string key retriever
void StringKeyIdRetriever::put_key(const std::string& key_id, const std::string& key)
{
  key_map_[key_id] = key;
}

std::string StringKeyIdRetriever::get_key(const std::string& key_id)
{
  return key_map_[key_id];
}

}  // namespace parquet
