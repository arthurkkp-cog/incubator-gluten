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
#include "velox/functions/Udf.h"
#include "velox/type/Type.h"

namespace gluten {

template <typename T>
struct MakeIntervalFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(
      out_type<Row<int32_t, int32_t, int64_t>>& result,
      const arg_type<int32_t>& years,
      const arg_type<int32_t>& months,
      const arg_type<int32_t>& weeks,
      const arg_type<int32_t>& days,
      const arg_type<int32_t>& hours,
      const arg_type<int32_t>& mins,
      const arg_type<double>& secs) {
    int32_t totalMonths = years * 12 + months;
    int32_t totalDays = weeks * 7 + days;
    int64_t totalMicros = static_cast<int64_t>(hours) * 3600000000LL +
        static_cast<int64_t>(mins) * 60000000LL +
        static_cast<int64_t>(secs * 1000000.0);

    result.get_writer_at<0>() = totalMonths;
    result.get_writer_at<1>() = totalDays;
    result.get_writer_at<2>() = totalMicros;
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Row<int32_t, int32_t, int64_t>>& result,
      const arg_type<int32_t>& years,
      const arg_type<int32_t>& months,
      const arg_type<int32_t>& weeks,
      const arg_type<int32_t>& days,
      const arg_type<int32_t>& hours,
      const arg_type<int32_t>& mins) {
    call(result, years, months, weeks, days, hours, mins, 0.0);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Row<int32_t, int32_t, int64_t>>& result,
      const arg_type<int32_t>& years,
      const arg_type<int32_t>& months,
      const arg_type<int32_t>& weeks,
      const arg_type<int32_t>& days,
      const arg_type<int32_t>& hours) {
    call(result, years, months, weeks, days, hours, 0, 0.0);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Row<int32_t, int32_t, int64_t>>& result,
      const arg_type<int32_t>& years,
      const arg_type<int32_t>& months,
      const arg_type<int32_t>& weeks,
      const arg_type<int32_t>& days) {
    call(result, years, months, weeks, days, 0, 0, 0.0);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Row<int32_t, int32_t, int64_t>>& result,
      const arg_type<int32_t>& years,
      const arg_type<int32_t>& months,
      const arg_type<int32_t>& weeks) {
    call(result, years, months, weeks, 0, 0, 0, 0.0);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Row<int32_t, int32_t, int64_t>>& result,
      const arg_type<int32_t>& years,
      const arg_type<int32_t>& months) {
    call(result, years, months, 0, 0, 0, 0, 0.0);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Row<int32_t, int32_t, int64_t>>& result,
      const arg_type<int32_t>& years) {
    call(result, years, 0, 0, 0, 0, 0, 0.0);
  }

  FOLLY_ALWAYS_INLINE void call(out_type<Row<int32_t, int32_t, int64_t>>& result) {
    call(result, 0, 0, 0, 0, 0, 0, 0.0);
  }
};

} // namespace gluten
