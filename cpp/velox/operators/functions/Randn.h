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

#include <random>

#include <folly/CPortability.h>
#include "velox/functions/Macros.h"

namespace gluten {

template <typename T>
struct RandnFunction {
  static constexpr bool is_deterministic = false;

  template <typename TInput>
  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<facebook::velox::TypePtr>& /*inputTypes*/,
      const facebook::velox::core::QueryConfig& config,
      const TInput* seedInput) {
    const auto partitionId = config.sparkPartitionId();
    int64_t seed = seedInput ? (int64_t)*seedInput : 0;
    generator_.seed(seed + partitionId);
  }

  FOLLY_ALWAYS_INLINE void call(double& result) {
    std::normal_distribution<double> dist(0.0, 1.0);
    result = dist(defaultGenerator_);
  }

  template <typename TInput>
  FOLLY_ALWAYS_INLINE void callNullable(double& result, TInput /*seedInput*/) {
    std::normal_distribution<double> dist(0.0, 1.0);
    result = dist(generator_);
  }

 private:
  std::mt19937 generator_;
  std::mt19937 defaultGenerator_{std::random_device{}()};
};

} // namespace gluten
