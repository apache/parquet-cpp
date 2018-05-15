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


#ifndef PARQUET_UTIL_CRYPTO_H
#define PARQUET_UTIL_CRYPTO_H

#include <openssl/aes.h>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <string>
#include <sstream>
#include <iostream>
#include "parquet/exception.h"
#include "parquet/types.h" 

namespace parquet {
 
inline int encrypt(Encryption::type alg_id, const uint8_t *plaintext, int plaintext_len, 
                   uint8_t *key, int key_len, uint8_t *aad, int aad_len, 
                   uint8_t *ciphertext) {
                   
  if (Encryption::PARQUET_AES_GCM_V1 != alg_id) {
    std::stringstream ss;
    ss << "Crypto algorithm " << alg_id << " currently unsupported\n";
    throw parquet::ParquetException(ss.str());
  }
  
  if (16 != key_len) {
    std::stringstream ss;
    ss << "Key length " << key_len << " currently unsupported\n";
    throw parquet::ParquetException(ss.str());
  }
    
  EVP_CIPHER_CTX *ctx;
  int tag_len = 16;
  int iv_len = 12;

  int len;
  int ciphertext_len;
  uint8_t tag[tag_len];
  uint8_t iv[iv_len];
  
  // Random IV
  RAND_load_file("/dev/urandom", 32);
  RAND_bytes(iv, sizeof(iv));

  // Init cipher context
  if(!(ctx = EVP_CIPHER_CTX_new())) {
    std::stringstream ss;
    ss << "Couldn't init cipher context\n";
    throw parquet::ParquetException(ss.str());
  }

  // Init AES-GCM with 128-bit key
  if(1 != EVP_EncryptInit_ex(ctx, EVP_aes_128_gcm(), NULL, NULL, NULL)) {
    std::stringstream ss;
    ss << "Couldn't init AES_GCM_128 encryption\n";
    throw parquet::ParquetException(ss.str());
  }

  // Setting custom IV length
  if (12 != iv_len) {
    if(1 != EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN, iv_len, NULL)) {
      std::stringstream ss;
      ss << "Couldn't set IV length: " << iv_len << "\n";
      throw parquet::ParquetException(ss.str());
    }
  }

  // Setting key and IV 
  if(1 != EVP_EncryptInit_ex(ctx, NULL, NULL, key, iv)) {
    std::stringstream ss;
    ss << "Couldn't set key and IV\n";
    throw parquet::ParquetException(ss.str());
  }

  // Setting additional authenticated data
  if (NULL != aad) {
    if(1 != EVP_EncryptUpdate(ctx, NULL, &len, aad, aad_len)) {
      std::stringstream ss;
      ss << "Couldn't set AAD\n";
      throw parquet::ParquetException(ss.str());
    }
  }

  // Encryption
  if(1 != EVP_EncryptUpdate(ctx, ciphertext + iv_len, &len, plaintext, plaintext_len)) {
    std::stringstream ss;
    ss << "Failed encryption update\n";
    throw parquet::ParquetException(ss.str());
  }

  ciphertext_len = len;

  // Finalization
  if(1 != EVP_EncryptFinal_ex(ctx, ciphertext + iv_len + len, &len)) {
    std::stringstream ss;
    ss << "Failed encryption finalization\n";
    throw parquet::ParquetException(ss.str());
  }
  
  ciphertext_len += len;

  // Getting the tag
  if(1 != EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_GET_TAG, tag_len, tag)) {
    std::stringstream ss;
    ss << "Couldn't get AES-GCM tag\n";
    throw parquet::ParquetException(ss.str());
  }

  // Cleaning up
  EVP_CIPHER_CTX_free(ctx);
  
  // Copying the IV and tag to ciphertext
  std::copy(iv, iv+iv_len, ciphertext);
  std::copy(tag, tag+tag_len, ciphertext + iv_len + ciphertext_len);
  
  return iv_len + ciphertext_len + tag_len;
}
 
inline int decrypt(Encryption::type alg_id, const uint8_t *ciphertext, int ciphertext_len,  
                   uint8_t *key, int key_len, uint8_t *aad, int aad_len, 
                   uint8_t *plaintext)
{

  if (Encryption::PARQUET_AES_GCM_V1 != alg_id) {
    std::stringstream ss;
    ss << "Crypto algorithm " << alg_id << " currently unsupported\n";
    throw parquet::ParquetException(ss.str());
  }
  
  if (16 != key_len) {
    std::stringstream ss;
    ss << "Key length " << key_len << " currently unsupported\n";
    throw parquet::ParquetException(ss.str());
  }

  EVP_CIPHER_CTX *ctx;
  int len;
  int plaintext_len;
  int ret;
  
  int tag_len = 16;
  int iv_len = 12;
  uint8_t tag[tag_len];
  uint8_t iv[iv_len];
  
  // Extracting IV and tag
  std::copy(ciphertext, ciphertext + iv_len, iv);
  std::copy(ciphertext + ciphertext_len - tag_len, ciphertext + ciphertext_len, tag);
  
  
  // Init cipher context
  if(!(ctx = EVP_CIPHER_CTX_new())) {
    std::stringstream ss;
    ss << "Couldn't init cipher context\n";
    throw parquet::ParquetException(ss.str());
  }

  // Init AES-GCM with 128-bit key
  if(!EVP_DecryptInit_ex(ctx, EVP_aes_128_gcm(), NULL, NULL, NULL)) {
    std::stringstream ss;
    ss << "Couldn't init AES_GCM_128 decryption\n";
    throw parquet::ParquetException(ss.str());
  }
  
  // Setting custom IV length
  if (12 != iv_len) {
    if(1 != EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN, iv_len, NULL)) {
      std::stringstream ss;
      ss << "Couldn't set IV length: " << iv_len << "\n";
      throw parquet::ParquetException(ss.str());
    }
  }

  // Setting key and IV 
  if(1 != EVP_DecryptInit_ex(ctx, NULL, NULL, key, iv)) {
    std::stringstream ss;
    ss << "Couldn't set key and IV\n";
    throw parquet::ParquetException(ss.str());
  }

  // Setting additional authenticated data
  if (NULL != aad) {
    if(1 != EVP_DecryptUpdate(ctx, NULL, &len, aad, aad_len)) {
      std::stringstream ss;
      ss << "Couldn't set AAD\n";
      throw parquet::ParquetException(ss.str());
    }
  }
   
  // Decryption
  if(!EVP_DecryptUpdate(ctx, plaintext, &len, ciphertext + iv_len, ciphertext_len - iv_len - tag_len)) {
    std::stringstream ss;
    ss << "Failed decryption update\n";
    throw parquet::ParquetException(ss.str());
  }
  
  plaintext_len = len;

  // Checking the tag (authentication)
  if(!EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG, tag_len, tag)) {
    std::stringstream ss;
    ss << "Failed authentication\n";
    throw parquet::ParquetException(ss.str());
  }

  // Finalization
  ret = EVP_DecryptFinal_ex(ctx, plaintext + len, &len);

  // Cleaning up
  EVP_CIPHER_CTX_free(ctx);

  if(ret > 0) {
    plaintext_len += len;
    return plaintext_len;
  }
  else {
    return -1;
  }
}

}  // namespace parquet

#endif //PARQUET_UTIL_CRYPTO_H
