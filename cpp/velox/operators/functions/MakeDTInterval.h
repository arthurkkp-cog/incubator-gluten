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
#include <stdint.h>
#include "velox/functions/Macros.h"
#include "velox/type/Type.h"

namespace gluten {

template <typename T>
struct MakeDTIntervalFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  static constexpr int64_t kMicrosPerSecond = 1000000L;
  static constexpr int64_t kMicrosPerMinute = kMicrosPerSecond * 60;
  static constexpr int64_t kMicrosPerHour = kMicrosPerMinute * 60;
  static constexpr int64_t kMicrosPerDay = kMicrosPerHour * 24;

  FOLLY_ALWAYS_INLINE void call(out_type<facebook::velox::IntervalDayTime>& result) {
    result = 0;
  }

  FOLLY_ALWAYS_INLINE void call(out_type<facebook::velox::IntervalDayTime>& result, const int32_t days) {
    result = static_cast<int64_t>(days) * kMicrosPerDay;
  }

  FOLLY_ALWAYS_INLINE void
  call(out_type<facebook::velox::IntervalDayTime>& result, const int32_t days, const int32_t hours) {
    result = static_cast<int64_t>(days) * kMicrosPerDay + static_cast<int64_t>(hours) * kMicrosPerHour;
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<facebook::velox::IntervalDayTime>& result,
      const int32_t days,
      const int32_t hours,
      const int32_t mins) {
    result = static_cast<int64_t>(days) * kMicrosPerDay + static_cast<int64_t>(hours) * kMicrosPerHour +
        static_cast<int64_t>(mins) * kMicrosPerMinute;
  }

  template <typename TSecond>
  FOLLY_ALWAYS_INLINE void call(
      out_type<facebook::velox::IntervalDayTime>& result,
      const int32_t days,
      const int32_t hours,
      const int32_t mins,
      const TSecond& secs) {
    int64_t secsMicros;
    if constexpr (std::is_integral_v<TSecond>) {
      secsMicros = static_cast<int64_t>(secs) * kMicrosPerSecond;
    } else {
      secsMicros = static_cast<int64_t>(static_cast<double>(secs) * kMicrosPerSecond);
    }
    result = static_cast<int64_t>(days) * kMicrosPerDay + static_cast<int64_t>(hours) * kMicrosPerHour +
        static_cast<int64_t>(mins) * kMicrosPerMinute + secsMicros;
  }
};

} // namespace gluten
