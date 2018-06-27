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


#include <openssl/aes.h>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <string>
#include <sstream>
#include <iostream>
#include "parquet/exception.h"
#include "parquet/types.h" 

#include "parquet/util/crypto.h"

namespace parquet {

const int gcm_tag_len = 16;
const int gcm_iv_len = 12;
const int ctr_iv_len = 16;

void handleError(const char *message, EVP_CIPHER_CTX *ctx) {
  if (NULL != ctx) EVP_CIPHER_CTX_free(ctx);
  std::stringstream ss;
  ss << message;
  throw parquet::ParquetException(ss.str());
}

int gcm_encrypt(const uint8_t *plaintext, int plaintext_len, 
                   uint8_t *key, int key_len, uint8_t *aad, int aad_len, 
                   uint8_t *ciphertext) 
{
  EVP_CIPHER_CTX *ctx = NULL;

  int len;
  int ciphertext_len;
  uint8_t tag[gcm_tag_len];
  uint8_t iv[gcm_iv_len];
  
  // Random IV
  RAND_load_file("/dev/urandom", 32);
  RAND_bytes(iv, sizeof(iv));

  // Init cipher context
  if(!(ctx = EVP_CIPHER_CTX_new())) {
    handleError("Couldn't init cipher context", ctx);
  }

  // Init AES-GCM with 128-bit key
  if(1 != EVP_EncryptInit_ex(ctx, EVP_aes_128_gcm(), NULL, NULL, NULL)) {
    handleError("Couldn't init AES_GCM_128 encryption", ctx);
  }

  // Setting key and IV 
  if(1 != EVP_EncryptInit_ex(ctx, NULL, NULL, key, iv)) {
    handleError("Couldn't set key and IV", ctx);
  }

  // Setting additional authenticated data
  if (NULL != aad) {
    if(1 != EVP_EncryptUpdate(ctx, NULL, &len, aad, aad_len)) {
      handleError("Couldn't set AAD", ctx);
    }
  }

  // Encryption
  if(1 != EVP_EncryptUpdate(ctx, ciphertext + gcm_iv_len, &len, plaintext, plaintext_len)) {
    handleError("Failed encryption update", ctx);
  }

  ciphertext_len = len;

  // Finalization
  if(1 != EVP_EncryptFinal_ex(ctx, ciphertext + gcm_iv_len + len, &len)) {
    handleError("Failed encryption finalization", ctx);
  }
  
  ciphertext_len += len;

  // Getting the tag
  if(1 != EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_GET_TAG, gcm_tag_len, tag)) {
    handleError("Couldn't get AES-GCM tag", ctx);
  }

  // Cleaning up
  EVP_CIPHER_CTX_free(ctx);
  
  // Copying the IV and tag to ciphertext
  std::copy(iv, iv + gcm_iv_len, ciphertext);
  std::copy(tag, tag + gcm_tag_len, ciphertext + gcm_iv_len + ciphertext_len);
  
  return gcm_iv_len + ciphertext_len + gcm_tag_len;
}

int ctr_encrypt(const uint8_t *plaintext, int plaintext_len, 
                   uint8_t *key, int key_len, uint8_t *ciphertext) 
{             
  EVP_CIPHER_CTX *ctx;

  int len;
  int ciphertext_len;
  uint8_t iv[ctr_iv_len];
  
  // Random IV
  RAND_load_file("/dev/urandom", 32);
  RAND_bytes(iv, sizeof(iv));

  // Init cipher context
  if(!(ctx = EVP_CIPHER_CTX_new())) {
    handleError("Couldn't init cipher context", ctx);
  }

  // Init AES-CTR with 128-bit key
  if(1 != EVP_EncryptInit_ex(ctx, EVP_aes_128_ctr(), NULL, NULL, NULL)) {
    handleError("Couldn't init AES_CTR_128 encryption", ctx);
  }

  // Setting key and IV 
  if(1 != EVP_EncryptInit_ex(ctx, NULL, NULL, key, iv)) {
    handleError("Couldn't set key and IV", ctx);
  }

  // Encryption
  if(1 != EVP_EncryptUpdate(ctx, ciphertext + ctr_iv_len, &len, plaintext, plaintext_len)) {
    handleError("Failed encryption update", ctx);
  }

  ciphertext_len = len;

  // Finalization
  if(1 != EVP_EncryptFinal_ex(ctx, ciphertext + ctr_iv_len + len, &len)) {
    handleError("Failed encryption finalization", ctx);
  }
  
  ciphertext_len += len;

  // Cleaning up
  EVP_CIPHER_CTX_free(ctx);
  
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
    ss << "Crypto algorithm " << alg_id << " currently unsupported";
    throw parquet::ParquetException(ss.str());
  }
  
  if (16 != key_len) {
    std::stringstream ss;
    ss << "Key length " << key_len << " currently unsupported";
    throw parquet::ParquetException(ss.str());
  }
  
  if (metadata || (Encryption::AES_GCM_V1 == alg_id)) {
    return gcm_encrypt(plaintext, plaintext_len, key, key_len, aad, aad_len, ciphertext);
  }
  
  // Data (page) encryption with AES_GCM_CTR_V1
  return ctr_encrypt(plaintext, plaintext_len, key, key_len, ciphertext);
}
 
int gcm_decrypt(const uint8_t *ciphertext, int ciphertext_len,  
                   uint8_t *key, int key_len, uint8_t *aad, int aad_len, 
                   uint8_t *plaintext)
{
  EVP_CIPHER_CTX *ctx = NULL;
  int len;
  int plaintext_len;
  
  uint8_t tag[gcm_tag_len];
  uint8_t iv[gcm_iv_len];
  
  // Extracting IV and tag
  std::copy(ciphertext, ciphertext + gcm_iv_len, iv);
  std::copy(ciphertext + ciphertext_len - gcm_tag_len, ciphertext + ciphertext_len, tag);
  
  
  // Init cipher context
  if(!(ctx = EVP_CIPHER_CTX_new())) {
    handleError("Couldn't init cipher context", ctx);
  }

  // Init AES-GCM with 128-bit key
  if(!EVP_DecryptInit_ex(ctx, EVP_aes_128_gcm(), NULL, NULL, NULL)) {
    handleError("Couldn't init AES_GCM_128 decryption", ctx);
  }

  // Setting key and IV 
  if(1 != EVP_DecryptInit_ex(ctx, NULL, NULL, key, iv)) {
    handleError("Couldn't set key and IV", ctx);
  }

  // Setting additional authenticated data
  if (NULL != aad) {
    if(1 != EVP_DecryptUpdate(ctx, NULL, &len, aad, aad_len)) {
      handleError("Couldn't set AAD", ctx);
    }
  }
   
  // Decryption
  if(!EVP_DecryptUpdate(ctx, plaintext, &len, ciphertext + gcm_iv_len, 
      ciphertext_len - gcm_iv_len - gcm_tag_len)) {
    handleError("Failed decryption update", ctx);
  }
  
  plaintext_len = len;

  // Checking the tag (authentication)
  if(!EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG, gcm_tag_len, tag)) {
    handleError("Failed authentication", ctx);
  }

  // Finalization
  if (1 != EVP_DecryptFinal_ex(ctx, plaintext + len, &len)) {
    handleError("Failed decryption finalization", ctx);
  }

  // Cleaning up
  EVP_CIPHER_CTX_free(ctx);

  plaintext_len += len;
  return plaintext_len;
}

int ctr_decrypt(const uint8_t *ciphertext, int ciphertext_len,  
                   uint8_t *key, int key_len, uint8_t *plaintext)
{
  EVP_CIPHER_CTX *ctx = NULL;
  int len;
  int plaintext_len;
  
  uint8_t iv[ctr_iv_len];
  
  // Extracting IV and tag
  std::copy(ciphertext, ciphertext + ctr_iv_len, iv);  
  
  // Init cipher context
  if(!(ctx = EVP_CIPHER_CTX_new())) {
    handleError("Couldn't init cipher context", ctx);
  }

  // Init AES-CTR with 128-bit key
  if(!EVP_DecryptInit_ex(ctx, EVP_aes_128_ctr(), NULL, NULL, NULL)) {
    handleError("Couldn't init AES_CTR_128 decryption", ctx);
  }

  // Setting key and IV 
  if(1 != EVP_DecryptInit_ex(ctx, NULL, NULL, key, iv)) {
    handleError("Couldn't set key and IV", ctx);
  }
   
  // Decryption
  if(!EVP_DecryptUpdate(ctx, plaintext, &len, ciphertext + ctr_iv_len, 
      ciphertext_len - ctr_iv_len)) {
    handleError("Failed decryption update", ctx);
  }
  
  plaintext_len = len;

  // Finalization
  if (1 != EVP_DecryptFinal_ex(ctx, plaintext + len, &len)) {
    handleError("Failed decryption finalization", ctx);
  }

  // Cleaning up
  EVP_CIPHER_CTX_free(ctx);

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
    ss << "Crypto algorithm " << alg_id << " currently unsupported";
    throw parquet::ParquetException(ss.str());
  }
  
  if (16 != key_len) {
    std::stringstream ss;
    ss << "Key length " << key_len << " currently unsupported";
    throw parquet::ParquetException(ss.str());
  }
  
  if (metadata || (Encryption::AES_GCM_V1 == alg_id)) {
    return gcm_decrypt(ciphertext, ciphertext_len, key, key_len, aad, aad_len, plaintext);
  }

  // Data (page) decryption with AES_GCM_CTR_V1
  return ctr_decrypt(ciphertext, ciphertext_len, key, key_len, plaintext);
}

}  // namespace parquet

