<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->


How to release
==============

1. Do a fresh `git clone` of the repository using the ASF git as origin.
2. Set the git author to your gpg signing key using `git config --local user.email "<mail>"`.
3. Create an RC using `./dev/release/release-candidate -r <rcNum> -v <version>` starting with `rcNum=0`.
   You could first do a dry-run by adding `-p` at the end.
4. Check that the uploaded RC is valid using `./dev/release/verify-release-candidate <version> <rcNum>`.
5. Send the generated mail to dev@parquet.apache.org.
6. Wait for the vote to end, either proceed to the next step or abort the RC and continue with the
   necessary fixes.
7. In the release SVN, copy the contents of the RC folder into the release folder.
8. Delete the previous `parquet-cpp` release from SVN.
9. Cherry-pick the commits in the `master-after-X` branch into master.
10. Send out the release mail to `dev@parquet.apache.org` and `announce@apache.org`.
