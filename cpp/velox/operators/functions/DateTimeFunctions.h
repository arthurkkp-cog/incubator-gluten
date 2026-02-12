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
#include "velox/type/Timestamp.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace gluten {

using namespace facebook::velox;

template <typename T>
struct FromUtcTimestampFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& /*config*/,
      const arg_type<Timestamp>* /*input*/,
      const arg_type<Varchar>* timezone) {
    if (timezone) {
      timeZone_ = tz::locateZone(std::string_view(*timezone), false);
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Timestamp>& result,
      const arg_type<Timestamp>& timestamp,
      const arg_type<Varchar>& timezone) {
    result = timestamp;
    const auto* toTimeZone = timeZone_ != nullptr
        ? timeZone_
        : tz::locateZone(std::string_view(timezone), false);
    VELOX_USER_CHECK_NOT_NULL(toTimeZone, "Unknown time zone: '{}'", timezone);
    result.toTimezone(*toTimeZone);
  }

 private:
  const tz::TimeZone* timeZone_{nullptr};
};

} // namespace gluten
