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

#include "parquet/types.h"
#include "parquet/properties.h"

namespace parquet {

int encrypt(Encryption::type alg_id, bool metadata, 
                   const uint8_t *plaintext, int plaintext_len, 
                   uint8_t *key, int key_len, uint8_t *aad, int aad_len, 
                   uint8_t *ciphertext);
                   
int encrypt(std::shared_ptr<EncryptionProperties> encryption_props, bool metadata, 
                   const uint8_t *plaintext, int plaintext_len,
                   uint8_t *ciphertext);

int decrypt(Encryption::type alg_id, bool metadata,
                   const uint8_t *ciphertext, int ciphertext_len,  
                   uint8_t *key, int key_len, uint8_t *aad, int aad_len, 
                   uint8_t *plaintext);

int decrypt(std::shared_ptr<EncryptionProperties> encryption_props, bool metadata,
                   const uint8_t *ciphertext, int ciphertext_len,
                   uint8_t *plaintext);
}  // namespace parquet

#endif //PARQUET_UTIL_CRYPTO_H
