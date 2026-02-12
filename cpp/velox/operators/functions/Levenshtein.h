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

#include <folly/CPortability.h>
#include <algorithm>
#include <cstdint>
#include <vector>

#include "velox/functions/Macros.h"

namespace gluten {

template <typename T>
struct LevenshteinFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void
  call(int32_t& result, const arg_type<velox::Varchar>& left, const arg_type<velox::Varchar>& right) {
    result = computeDistance(left.data(), left.size(), right.data(), right.size());
  }

  FOLLY_ALWAYS_INLINE void call(
      int32_t& result,
      const arg_type<velox::Varchar>& left,
      const arg_type<velox::Varchar>& right,
      const int32_t& threshold) {
    if (threshold < 0) {
      result = -1;
      return;
    }
    int32_t distance = computeDistance(left.data(), left.size(), right.data(), right.size());
    result = (distance > threshold) ? -1 : distance;
  }

 private:
  static FOLLY_ALWAYS_INLINE int32_t utf8CharLength(const char* str, size_t remaining) {
    auto c = static_cast<unsigned char>(str[0]);
    if (c < 0x80)
      return 1;
    if ((c & 0xE0) == 0xC0)
      return remaining >= 2 ? 2 : 1;
    if ((c & 0xF0) == 0xE0)
      return remaining >= 3 ? 3 : 1;
    if ((c & 0xF8) == 0xF0)
      return remaining >= 4 ? 4 : 1;
    return 1;
  }

  static FOLLY_ALWAYS_INLINE int32_t utf8NumChars(const char* str, size_t size) {
    int32_t count = 0;
    size_t pos = 0;
    while (pos < size) {
      pos += utf8CharLength(str + pos, size - pos);
      ++count;
    }
    return count;
  }

  static FOLLY_ALWAYS_INLINE bool
  utf8CharsEqual(const char* left, int32_t leftCharLen, const char* right, int32_t rightCharLen) {
    if (leftCharLen != rightCharLen)
      return false;
    for (int32_t i = 0; i < leftCharLen; ++i) {
      if (left[i] != right[i])
        return false;
    }
    return true;
  }

  static int32_t computeDistance(const char* left, size_t leftSize, const char* right, size_t rightSize) {
    int32_t leftLen = utf8NumChars(left, leftSize);
    int32_t rightLen = utf8NumChars(right, rightSize);

    if (leftLen == 0)
      return rightLen;
    if (rightLen == 0)
      return leftLen;

    std::vector<int32_t> prev(rightLen + 1);
    std::vector<int32_t> curr(rightLen + 1);

    for (int32_t j = 0; j <= rightLen; ++j) {
      prev[j] = j;
    }

    size_t leftPos = 0;
    for (int32_t i = 1; i <= leftLen; ++i) {
      int32_t leftCharLen = utf8CharLength(left + leftPos, leftSize - leftPos);
      curr[0] = i;

      size_t rightPos = 0;
      for (int32_t j = 1; j <= rightLen; ++j) {
        int32_t rightCharLen = utf8CharLength(right + rightPos, rightSize - rightPos);
        int32_t cost = utf8CharsEqual(left + leftPos, leftCharLen, right + rightPos, rightCharLen) ? 0 : 1;
        curr[j] = std::min({prev[j] + 1, curr[j - 1] + 1, prev[j - 1] + cost});
        rightPos += rightCharLen;
      }

      std::swap(prev, curr);
      leftPos += leftCharLen;
    }

    return prev[rightLen];
  }
};

} // namespace gluten
