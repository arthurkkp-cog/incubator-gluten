/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include "velox/functions/Macros.h"
#include "velox/functions/lib/string/StringCore.h"
#include "velox/functions/lib/string/StringImpl.h"

namespace gluten {

/// locate function with 2 arguments (substring, string)
/// locate(substring, string) -> integer
///
/// Returns the 1-based position of the first occurrence of 'substring' in
/// 'string'. The search is from the beginning of 'string' to the end.
/// This is equivalent to calling locate(substring, string, 1).
///
/// Returns 1 if 'substring' is empty.
/// Returns 0 if 'substring' is not found in 'string'.
/// Returns NULL if 'substring' or 'string' is NULL.
template <typename T>
struct LocateFunctionTwoArgs {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void callAscii(
      out_type<int32_t>& result,
      const arg_type<facebook::velox::Varchar>& subString,
      const arg_type<facebook::velox::Varchar>& string) {
    if (subString.empty()) {
      result = 1;
    } else {
      const auto position = facebook::velox::functions::stringImpl::stringPosition<true /*isAscii*/>(
          std::string_view(string.data(), string.size()),
          std::string_view(subString.data(), subString.size()),
          1 /*instance*/);
      result = position;
    }
  }

  FOLLY_ALWAYS_INLINE bool callNullable(
      out_type<int32_t>& result,
      const arg_type<facebook::velox::Varchar>* subString,
      const arg_type<facebook::velox::Varchar>* string) {
    if (subString == nullptr || string == nullptr) {
      return false;
    }
    if (subString->empty()) {
      result = 1;
      return true;
    }

    const auto position = facebook::velox::functions::stringImpl::stringPosition<false /*isAscii*/>(
        std::string_view(string->data(), string->size()),
        std::string_view(subString->data(), subString->size()),
        1 /*instance*/);
    result = position;
    return true;
  }
};

} // namespace gluten
