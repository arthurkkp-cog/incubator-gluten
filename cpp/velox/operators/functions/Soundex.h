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

#include <cstring>
#include <velox/functions/Macros.h>

namespace gluten {

template <typename T>
struct SoundexFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<facebook::velox::Varchar>& result,
      const arg_type<facebook::velox::Varchar>& input) {
    static constexpr char kMapping[] = {
        '0', '1', '2', '3', '0', '1', '2', '0', '0', '2', '2', '4', '5', '5',
        '0', '1', '2', '6', '2', '3', '0', '1', '0', '2', '0', '2'};

    const auto size = input.size();
    if (size == 0) {
      result.resize(0);
      return;
    }

    const char* data = input.data();
    char firstChar = data[0];

    if (!isAsciiLetter(firstChar)) {
      result.resize(size);
      std::memcpy(result.data(), data, size);
      return;
    }

    char sx[4] = {'0', '0', '0', '0'};
    sx[0] = toUpperAscii(firstChar);

    int sxi = 1;
    int idx = 1;
    while (idx < size && sxi < 4) {
      char c = data[idx];
      if (isAsciiLetter(c)) {
        char code = kMapping[toUpperAscii(c) - 'A'];
        if (code != '0') {
          if (code != sx[sxi - 1]) {
            sx[sxi] = code;
            sxi++;
          }
        }
      }
      idx++;
    }

    result.resize(4);
    std::memcpy(result.data(), sx, 4);
  }

 private:
  static FOLLY_ALWAYS_INLINE bool isAsciiLetter(char c) {
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
  }

  static FOLLY_ALWAYS_INLINE char toUpperAscii(char c) {
    return (c >= 'a' && c <= 'z') ? (c - 32) : c;
  }
};

} // namespace gluten
