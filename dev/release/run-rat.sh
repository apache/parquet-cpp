#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# download apache rat
curl -s https://repo1.maven.org/maven2/org/apache/rat/apache-rat/0.12/apache-rat-0.12.jar > apache-rat-0.12.jar

RAT="java -jar apache-rat-0.12.jar -d "

# generate the rat report
$RAT $1 \
  -e ".*" \
  -e mman.h \
  -e "*_generated.h" \
  -e asan_symbolize.py \
  -e cpplint.py \
  -e pax_global_header \
  -e clang_format_exclusions.txt \
  > rat.txt
cat rat.txt
UNAPPROVED=`cat rat.txt  | grep "Unknown Licenses" | head -n 1 | cut -d " " -f 1`

if [ "0" -eq "${UNAPPROVED}" ]; then
  echo "No unnaproved licenses"
else
  echo "${UNAPPROVED} unapproved licences. Check rat report: rat.txt"
  exit 1
fi


