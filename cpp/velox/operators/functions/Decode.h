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

#include <algorithm>
#include <cctype>
#include <cstring>
#include <string>

#include <folly/CPortability.h>
#include <unicode/ucnv.h>
#include <unicode/utypes.h>
#include "velox/functions/Macros.h"
#include "velox/type/Type.h"

namespace gluten {

template <typename T>
struct DecodeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<velox::Varchar>& result,
      const arg_type<velox::Varbinary>& input,
      const arg_type<velox::Varchar>& charset) {
    if (input.size() == 0) {
      result.resize(0);
      return true;
    }

    std::string charsetName(charset.data(), charset.size());

    if (isUtf8Compatible(charsetName)) {
      result.resize(input.size());
      if (input.size() > 0) {
        std::memcpy(result.data(), input.data(), input.size());
      }
      return true;
    }

    UErrorCode status = U_ZERO_ERROR;
    UConverter* conv = ucnv_open(charsetName.c_str(), &status);
    if (U_FAILURE(status)) {
      VELOX_USER_FAIL("Unsupported charset: {}", charsetName);
    }

    status = U_ZERO_ERROR;
    int32_t outputSize = ucnv_toAlgorithmic(
        UCNV_UTF8, conv, nullptr, 0, input.data(), input.size(), &status);

    if (status != U_BUFFER_OVERFLOW_ERROR && U_FAILURE(status)) {
      ucnv_close(conv);
      VELOX_USER_FAIL("Decode failed for charset: {}", charsetName);
    }

    status = U_ZERO_ERROR;
    result.resize(outputSize);
    ucnv_toAlgorithmic(
        UCNV_UTF8,
        conv,
        result.data(),
        outputSize,
        input.data(),
        input.size(),
        &status);
    ucnv_close(conv);

    if (U_FAILURE(status)) {
      VELOX_USER_FAIL("Decode failed for charset: {}", charsetName);
    }

    return true;
  }

 private:
  static bool isUtf8Compatible(const std::string& charset) {
    std::string upper;
    upper.reserve(charset.size());
    for (char c : charset) {
      upper += static_cast<char>(
          std::toupper(static_cast<unsigned char>(c)));
    }
    return upper == "UTF-8" || upper == "UTF8";
  }
};

} // namespace gluten
