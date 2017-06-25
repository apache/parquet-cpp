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

#ifndef PARQUET_UTIL_VISIBILITY_H
#define PARQUET_UTIL_VISIBILITY_H

#if defined(_WIN32) || defined(__CYGWIN__)
#ifdef _MSC_VER
#pragma warning(push)
// Disable warning for STL types usage in DLL interface
// https://web.archive.org/web/20130317015847/http://connect.microsoft.com/VisualStudio/feedback/details/696593/vc-10-vs-2010-basic-string-exports
#pragma warning(disable : 4275 4251)
// Disable diamond inheritance warnings
#pragma warning(disable : 4250)
// Disable macro redefinition warnings
#pragma warning(disable : 4005)
// Disable extern before exported template warnings
#pragma warning(disable : 4910)
#endif
#define PARQUET_EXPORT __declspec(dllexport)
#define PARQUET_NO_EXPORT
#else  // Not Windows
#ifndef PARQUET_EXPORT
#define PARQUET_EXPORT __attribute__((visibility("default")))
#endif
#ifndef PARQUET_NO_EXPORT
#define PARQUET_NO_EXPORT __attribute__((visibility("hidden")))
#endif
#endif  // Non-Windows

// gcc and clang disagree about how to handle template visibility when you have
// explicit specializations https://llvm.org/bugs/show_bug.cgi?id=24815

#if defined(__clang__)
#define PARQUET_EXTERN_TEMPLATE extern template class PARQUET_EXPORT
#else
#define PARQUET_EXTERN_TEMPLATE extern template class
#endif

// This is a complicated topic, some reading on it:
// http://www.codesynthesis.com/~boris/blog/2010/01/18/dll-export-cxx-templates/
#ifdef _MSC_VER
#define PARQUET_TEMPLATE_EXPORT PARQUET_EXPORT
#else
#define PARQUET_TEMPLATE_EXPORT
#endif

#endif  // PARQUET_UTIL_VISIBILITY_H
