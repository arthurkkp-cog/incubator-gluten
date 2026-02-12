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

#include <cmath>
#include <cstdint>
#include <sstream>
#include <string>
#include <type_traits>

#include <folly/CPortability.h>
#include "velox/functions/Macros.h"
#include "velox/type/StringView.h"

namespace gluten {

template <typename T>
struct FormatNumberFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<facebook::velox::Varchar>& result,
      const double x,
      const int32_t d) {
    if (std::isnan(x)) {
      result.append("NaN");
      return true;
    }
    if (std::isinf(x)) {
      if (x > 0) {
        result.append("Infinity");
      } else {
        result.append("-Infinity");
      }
      return true;
    }

    int32_t decimals = d < 0 ? 0 : d;

    std::stringstream ss;
    ss << std::fixed;
    ss.precision(decimals);
    ss << x;
    std::string numStr = ss.str();

    bool negative = false;
    std::string intPart;
    std::string fracPart;

    size_t dotPos = numStr.find('.');
    if (dotPos != std::string::npos) {
      intPart = numStr.substr(0, dotPos);
      fracPart = numStr.substr(dotPos + 1);
    } else {
      intPart = numStr;
    }

    if (!intPart.empty() && intPart[0] == '-') {
      negative = true;
      intPart = intPart.substr(1);
    }

    if (intPart == "0" && negative) {
      bool allZeros = true;
      for (char c : fracPart) {
        if (c != '0') {
          allZeros = false;
          break;
        }
      }
      if (allZeros) {
        negative = false;
      }
    }

    std::string formatted;
    int len = static_cast<int>(intPart.size());
    for (int i = 0; i < len; ++i) {
      if (i > 0 && (len - i) % 3 == 0) {
        formatted += ',';
      }
      formatted += intPart[i];
    }

    if (negative) {
      result.append("-");
    }
    result.append(formatted);
    if (!fracPart.empty()) {
      result.append(".");
      result.append(fracPart);
    }

    return true;
  }
};

} // namespace gluten
