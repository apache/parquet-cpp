// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <memory>
#include "parquet/util/crypto.h"
#include <openssl/aes.h>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <string>
#include <sstream>
#include <iostream>
#include "parquet/exception.h"

namespace parquet {

constexpr int gcm_tag_len = 16;
constexpr int gcm_iv_len = 12;
constexpr int ctr_iv_len = 16;
constexpr long max_bytes = 32;

struct EvpCipherCtxDeleter {
  void operator()(EVP_CIPHER_CTX *ctx) const {
    if (nullptr != ctx) { 
      EVP_CIPHER_CTX_free(ctx);
   }
  }
};

using EvpCipherCtxPtr = std::unique_ptr<EVP_CIPHER_CTX, EvpCipherCtxDeleter>;

int gcm_encrypt(const uint8_t *plaintext, int plaintext_len, 
                   uint8_t *key, int key_len, uint8_t *aad, int aad_len, 
                   uint8_t *ciphertext) 
{
  int len;
  int ciphertext_len;
  uint8_t tag[gcm_tag_len];
  uint8_t iv[gcm_iv_len];
  
  // Random IV
  RAND_load_file("/dev/urandom", max_bytes);
  RAND_bytes(iv, sizeof(iv));

  // Init cipher context
  EvpCipherCtxPtr ctx(EVP_CIPHER_CTX_new());
  if(nullptr == ctx.get()) {
    throw ParquetException("Couldn't init cipher context");
  }

  if (16 == key_len) {
    // Init AES-GCM with 128-bit key
    if(1 != EVP_EncryptInit_ex(ctx.get(), EVP_aes_128_gcm(), nullptr, nullptr, nullptr)) {
      throw ParquetException("Couldn't init AES_GCM_128 encryption");
    }
  }
  else if (24 == key_len) {
    // Init AES-GCM with 192-bit key
    if(1 != EVP_EncryptInit_ex(ctx.get(), EVP_aes_192_gcm(), nullptr, nullptr, nullptr)) {
      throw ParquetException("Couldn't init AES_GCM_192 encryption");
    }
  }
  else if (32 == key_len) {
    // Init AES-GCM with 256-bit key
    if(1 != EVP_EncryptInit_ex(ctx.get(), EVP_aes_256_gcm(), nullptr, nullptr, nullptr)) {
      throw ParquetException("Couldn't init AES_GCM_256 encryption");
    }
  }

  // Setting key and IV 
  if(1 != EVP_EncryptInit_ex(ctx.get(), nullptr, nullptr, key, iv)) {
    throw ParquetException("Couldn't set key and IV");
  }

  // Setting additional authenticated data
  if (nullptr != aad) {
    if(1 != EVP_EncryptUpdate(ctx.get(), nullptr, &len, aad, aad_len)) {
      throw ParquetException("Couldn't set AAD");
    }
  }

  // Encryption
  if(1 != EVP_EncryptUpdate(ctx.get(), ciphertext + gcm_iv_len, &len, plaintext, plaintext_len)) {
    throw ParquetException("Failed encryption update");
  }

  ciphertext_len = len;

  // Finalization
  if(1 != EVP_EncryptFinal_ex(ctx.get(), ciphertext + gcm_iv_len + len, &len)) {
    throw ParquetException("Failed encryption finalization");
  }
  
  ciphertext_len += len;

  // Getting the tag
  if(1 != EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_GET_TAG, gcm_tag_len, tag)) {
    throw ParquetException("Couldn't get AES-GCM tag");
  }
  
  // Copying the IV and tag to ciphertext
  std::copy(iv, iv + gcm_iv_len, ciphertext);
  std::copy(tag, tag + gcm_tag_len, ciphertext + gcm_iv_len + ciphertext_len);
  
  return gcm_iv_len + ciphertext_len + gcm_tag_len;
}

int ctr_encrypt(const uint8_t *plaintext, int plaintext_len, 
                   uint8_t *key, int key_len, uint8_t *ciphertext) 
{             
  int len;
  int ciphertext_len;
  uint8_t iv[ctr_iv_len];
  
  // Random IV
  RAND_load_file("/dev/urandom", max_bytes);
  RAND_bytes(iv, sizeof(iv));

  // Init cipher context
  EvpCipherCtxPtr ctx(EVP_CIPHER_CTX_new());
  if(nullptr == ctx.get()) {
    throw ParquetException("Couldn't init cipher context");
  }

  if (16 == key_len) {
    // Init AES-CTR with 128-bit key
    if(1 != EVP_EncryptInit_ex(ctx.get(), EVP_aes_128_ctr(), nullptr, nullptr, nullptr)) {
      throw ParquetException("Couldn't init AES_CTR_128 encryption");
    }
  }
  else if (24 == key_len) {
    // Init AES-CTR with 192-bit key
    if(1 != EVP_EncryptInit_ex(ctx.get(), EVP_aes_192_ctr(), nullptr, nullptr, nullptr)) {
      throw ParquetException("Couldn't init AES_CTR_192 encryption");
    }
  }
  else if (32 == key_len) {
    // Init AES-CTR with 256-bit key
    if(1 != EVP_EncryptInit_ex(ctx.get(), EVP_aes_256_ctr(), nullptr, nullptr, nullptr)) {
      throw ParquetException("Couldn't init AES_CTR_256 encryption");
    }
  }

  // Setting key and IV 
  if(1 != EVP_EncryptInit_ex(ctx.get(), nullptr, nullptr, key, iv)) {
    throw ParquetException("Couldn't set key and IV");
  }

  // Encryption
  if(1 != EVP_EncryptUpdate(ctx.get(), ciphertext + ctr_iv_len, &len, plaintext, plaintext_len)) {
    throw ParquetException("Failed encryption update");
  }

  ciphertext_len = len;

  // Finalization
  if(1 != EVP_EncryptFinal_ex(ctx.get(), ciphertext + ctr_iv_len + len, &len)) {
    throw ParquetException("Failed encryption finalization");
  }
  
  ciphertext_len += len;

  // Copying the IV ciphertext
  std::copy(iv, iv + ctr_iv_len, ciphertext);
    
  return ctr_iv_len + ciphertext_len;
}


int encrypt(Encryption::type alg_id, bool metadata, 
                   const uint8_t *plaintext, int plaintext_len, 
                   uint8_t *key, int key_len, uint8_t *aad, int aad_len, 
                   uint8_t *ciphertext) 
{
                   
  if (Encryption::AES_GCM_V1 != alg_id && Encryption::AES_GCM_CTR_V1 != alg_id) {
    std::stringstream ss;
    ss << "Crypto algorithm " << alg_id << " is not supported";
    throw parquet::ParquetException(ss.str());
  }
  
  if (16 != key_len  && 24 != key_len  && 32 != key_len) {
    std::stringstream ss;
    ss << "Wrong key length: " << key_len;
    throw parquet::ParquetException(ss.str());
  }
  
  if (metadata || (Encryption::AES_GCM_V1 == alg_id)) {
    return gcm_encrypt(plaintext, plaintext_len, key, key_len, aad, aad_len, ciphertext);
  }
  
  // Data (page) encryption with AES_GCM_CTR_V1
  return ctr_encrypt(plaintext, plaintext_len, key, key_len, ciphertext);
}

int encrypt(std::shared_ptr<EncryptionProperties> encryption_props, bool metadata, 
                   const uint8_t *plaintext, int plaintext_len,
                   uint8_t *ciphertext) 
{
  return encrypt(encryption_props->algorithm(), metadata, plaintext, plaintext_len, 
    encryption_props->key_bytes(), encryption_props->key_length(),
    encryption_props->aad_bytes(), encryption_props->aad_length(), ciphertext);
}

 
int gcm_decrypt(const uint8_t *ciphertext, int ciphertext_len,  
                   uint8_t *key, int key_len, uint8_t *aad, int aad_len, 
                   uint8_t *plaintext)
{
  int len;
  int plaintext_len;
  
  uint8_t tag[gcm_tag_len];
  uint8_t iv[gcm_iv_len];
  
  // Extracting IV and tag
  std::copy(ciphertext, ciphertext + gcm_iv_len, iv);
  std::copy(ciphertext + ciphertext_len - gcm_tag_len, ciphertext + ciphertext_len, tag);
  
  // Init cipher context
  EvpCipherCtxPtr ctx(EVP_CIPHER_CTX_new());
  if(nullptr == ctx.get()) {
    throw ParquetException("Couldn't init cipher context");
  }

  if (16 == key_len) {
    // Init AES-GCM with 128-bit key
    if(1 != EVP_DecryptInit_ex(ctx.get(), EVP_aes_128_gcm(), nullptr, nullptr, nullptr)) {
      throw ParquetException("Couldn't init AES_GCM_128 decryption");
    }
  }
  else if (24 == key_len) {
    // Init AES-GCM with 192-bit key
    if(1 != EVP_DecryptInit_ex(ctx.get(), EVP_aes_192_gcm(), nullptr, nullptr, nullptr)) {
      throw ParquetException("Couldn't init AES_GCM_192 decryption");
    }
  }
  else if (32 == key_len) {
    // Init AES-GCM with 256-bit key
    if(1 != EVP_DecryptInit_ex(ctx.get(), EVP_aes_256_gcm(), nullptr, nullptr, nullptr)) {
      throw ParquetException("Couldn't init AES_GCM_256 decryption");
    }
  }

  // Setting key and IV 
  if(1 != EVP_DecryptInit_ex(ctx.get(), nullptr, nullptr, key, iv)) {
    throw ParquetException("Couldn't set key and IV");
  }

  // Setting additional authenticated data
  if (nullptr != aad) {
    if(1 != EVP_DecryptUpdate(ctx.get(), nullptr, &len, aad, aad_len)) {
      throw ParquetException("Couldn't set AAD");
    }
  }
   
  // Decryption
  if(!EVP_DecryptUpdate(ctx.get(), plaintext, &len, ciphertext + gcm_iv_len, 
      ciphertext_len - gcm_iv_len - gcm_tag_len)) {
    throw ParquetException("Failed decryption update");
  }
  
  plaintext_len = len;

  // Checking the tag (authentication)
  if(!EVP_CIPHER_CTX_ctrl(ctx.get(), EVP_CTRL_GCM_SET_TAG, gcm_tag_len, tag)) {
    throw ParquetException("Failed authentication");
  }

  // Finalization
  if (1 != EVP_DecryptFinal_ex(ctx.get(), plaintext + len, &len)) {
    throw ParquetException("Failed decryption finalization");
  }

  plaintext_len += len;
  return plaintext_len;
}

int ctr_decrypt(const uint8_t *ciphertext, int ciphertext_len,  
                   uint8_t *key, int key_len, uint8_t *plaintext)
{
  int len;
  int plaintext_len;
  
  uint8_t iv[ctr_iv_len];
  
  // Extracting IV and tag
  std::copy(ciphertext, ciphertext + ctr_iv_len, iv);  
  
  // Init cipher context
  EvpCipherCtxPtr ctx(EVP_CIPHER_CTX_new());
  if(nullptr == ctx.get()) {
    throw ParquetException("Couldn't init cipher context");
  }

  if (16 == key_len) {
    // Init AES-CTR with 128-bit key
    if(1 != EVP_DecryptInit_ex(ctx.get(), EVP_aes_128_ctr(), nullptr, nullptr, nullptr)) {
      throw ParquetException("Couldn't init AES_CTR_128 decryption");
    }
  }
  else if (24 == key_len) {
    // Init AES-CTR with 192-bit key
    if(1 != EVP_DecryptInit_ex(ctx.get(), EVP_aes_192_ctr(), nullptr, nullptr, nullptr)) {
      throw ParquetException("Couldn't init AES_CTR_192 decryption");
    }
  }
  else if (32 == key_len) {
    // Init AES-CTR with 256-bit key
    if(1 != EVP_DecryptInit_ex(ctx.get(), EVP_aes_256_ctr(), nullptr, nullptr, nullptr)) {
      throw ParquetException("Couldn't init AES_CTR_256 decryption");
    }
  }

  // Setting key and IV 
  if(1 != EVP_DecryptInit_ex(ctx.get(), nullptr, nullptr, key, iv)) {
    throw ParquetException("Couldn't set key and IV");
  }
   
  // Decryption
  if(!EVP_DecryptUpdate(ctx.get(), plaintext, &len, ciphertext + ctr_iv_len, 
      ciphertext_len - ctr_iv_len)) {
    throw ParquetException("Failed decryption update");
  }
  
  plaintext_len = len;

  // Finalization
  if (1 != EVP_DecryptFinal_ex(ctx.get(), plaintext + len, &len)) {
    throw ParquetException("Failed decryption finalization");
  }

  plaintext_len += len;
  return plaintext_len;
}

int decrypt(Encryption::type alg_id, bool metadata, 
                   const uint8_t *ciphertext, int ciphertext_len,  
                   uint8_t *key, int key_len, uint8_t *aad, int aad_len, 
                   uint8_t *plaintext)
{

  if (Encryption::AES_GCM_V1 != alg_id && Encryption::AES_GCM_CTR_V1 != alg_id) {
    std::stringstream ss;
    ss << "Crypto algorithm " << alg_id << " is not supported";
    throw parquet::ParquetException(ss.str());
  }
  
  if (16 != key_len  && 24 != key_len  && 32 != key_len) {
    std::stringstream ss;
    ss << "Wrong key length: " << key_len;
    throw parquet::ParquetException(ss.str());
  }
  
  if (metadata || (Encryption::AES_GCM_V1 == alg_id)) {
    return gcm_decrypt(ciphertext, ciphertext_len, key, key_len, aad, aad_len, plaintext);
  }

  // Data (page) decryption with AES_GCM_CTR_V1
  return ctr_decrypt(ciphertext, ciphertext_len, key, key_len, plaintext);
}

int decrypt(std::shared_ptr<EncryptionProperties> encryption_props, bool metadata,
                   const uint8_t *ciphertext, int ciphertext_len,
                   uint8_t *plaintext)
{
  return decrypt(encryption_props->algorithm(), metadata, ciphertext, ciphertext_len, 
    encryption_props->key_bytes(), encryption_props->key_length(),
    encryption_props->aad_bytes(), encryption_props->aad_length(), plaintext);
}

}  // namespace parquet

