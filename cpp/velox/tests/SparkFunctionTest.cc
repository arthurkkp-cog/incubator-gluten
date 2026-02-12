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

#include <vector>

#include "operators/functions/RegistrationAllFunctions.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

using namespace facebook::velox::functions::sparksql::test;
using namespace facebook::velox;

class SparkFunctionTest : public SparkFunctionBaseTest {
 public:
  SparkFunctionTest() {
    gluten::registerAllFunctions();
  }

 protected:
  template <typename T>
  void runRoundTest(const std::vector<std::tuple<T, T>>& data) {
    auto result = evaluate<SimpleVector<T>>("round(c0)", makeRowVector({makeFlatVector<T, 0>(data)}));
    for (int32_t i = 0; i < data.size(); ++i) {
      ASSERT_EQ(result->valueAt(i), std::get<1>(data[i]));
    }
  }

  template <typename T>
  void runRoundWithDecimalTest(const std::vector<std::tuple<T, int32_t, T>>& data) {
    auto result = evaluate<SimpleVector<T>>(
        "round(c0, c1)", makeRowVector({makeFlatVector<T, 0>(data), makeFlatVector<int32_t, 1>(data)}));
    for (int32_t i = 0; i < data.size(); ++i) {
      ASSERT_EQ(result->valueAt(i), std::get<2>(data[i]));
    }
  }

  template <typename T>
  std::vector<std::tuple<T, T>> testRoundFloatData() {
    return {
        {1.0, 1.0},
        {1.9, 2.0},
        {1.3, 1.0},
        {0.0, 0.0},
        {0.9999, 1.0},
        {-0.9999, -1.0},
        {1.0 / 9999999, 0},
        {123123123.0 / 9999999, 12.0}};
  }

  template <typename T>
  std::vector<std::tuple<T, T>> testRoundIntegralData() {
    return {{1, 1}, {0, 0}, {-1, -1}};
  }

  template <typename T>
  std::vector<std::tuple<T, int32_t, T>> testRoundWithDecFloatAndDoubleData() {
    return {{1.122112, 0, 1},       {1.129, 1, 1.1},        {1.129, 2, 1.13},         {1.0 / 3, 0, 0.0},
            {1.0 / 3, 1, 0.3},      {1.0 / 3, 2, 0.33},     {1.0 / 3, 6, 0.333333},   {-1.122112, 0, -1},
            {-1.129, 1, -1.1},      {-1.129, 2, -1.13},     {-1.129, 2, -1.13},       {-1.0 / 3, 0, 0.0},
            {-1.0 / 3, 1, -0.3},    {-1.0 / 3, 2, -0.33},   {-1.0 / 3, 6, -0.333333}, {1.0, -1, 0.0},
            {0.0, -2, 0.0},         {-1.0, -3, 0.0},        {11111.0, -1, 11110.0},   {11111.0, -2, 11100.0},
            {11111.0, -3, 11000.0}, {11111.0, -4, 10000.0}, {0.575, 2, 0.58},         {0.574, 2, 0.57},
            {-0.575, 2, -0.58},     {-0.574, 2, -0.57}};
  }

  template <typename T>
  std::vector<std::tuple<T, int32_t, T>> testRoundWithDecIntegralData() {
    return {
        {1, 0, 1},
        {0, 0, 0},
        {-1, 0, -1},
        {1, 1, 1},
        {0, 1, 0},
        {-1, 1, -1},
        {1, 10, 1},
        {0, 10, 0},
        {-1, 10, -1},
        {1, -1, 0},
        {0, -2, 0},
        {-1, -3, 0}};
  }
};

TEST_F(SparkFunctionTest, round) {
  runRoundTest<float>(testRoundFloatData<float>());
  runRoundTest<double>(testRoundFloatData<double>());
  runRoundTest<int64_t>(testRoundIntegralData<int64_t>());
  runRoundTest<int32_t>(testRoundIntegralData<int32_t>());
  runRoundTest<int16_t>(testRoundIntegralData<int16_t>());
  runRoundTest<int8_t>(testRoundIntegralData<int8_t>());
}

TEST_F(SparkFunctionTest, cot) {
  const auto cotDouble = [&](std::optional<double> a) {
    return evaluateOnce<double>("cot(c0)", a);
  };
  ASSERT_EQ(cotDouble(0.0), std::numeric_limits<double>::infinity());
  ASSERT_DOUBLE_EQ(cotDouble(1.0).value(), 1.0 / std::tan(1.0));
  ASSERT_DOUBLE_EQ(cotDouble(-1.0).value(), 1.0 / std::tan(-1.0));
  ASSERT_DOUBLE_EQ(cotDouble(M_PI / 4).value(), 1.0 / std::tan(M_PI / 4));
  ASSERT_DOUBLE_EQ(cotDouble(M_PI / 2).value(), 1.0 / std::tan(M_PI / 2));
  ASSERT_TRUE(std::isnan(cotDouble(std::numeric_limits<double>::quiet_NaN()).value()));
  ASSERT_TRUE(std::isnan(cotDouble(std::numeric_limits<double>::infinity()).value()));
  ASSERT_TRUE(std::isnan(cotDouble(-std::numeric_limits<double>::infinity()).value()));
  ASSERT_EQ(cotDouble(std::nullopt), std::nullopt);

  const auto cotFloat = [&](std::optional<float> a) {
    return evaluateOnce<double, float>("cot(c0)", a);
  };
  ASSERT_DOUBLE_EQ(cotFloat(1.0f).value(), 1.0 / std::tan(1.0f));
  ASSERT_DOUBLE_EQ(cotFloat(-1.0f).value(), 1.0 / std::tan(-1.0f));
  ASSERT_EQ(cotFloat(std::nullopt), std::nullopt);

  const auto cotInt = [&](std::optional<int32_t> a) {
    return evaluateOnce<double, int32_t>("cot(c0)", a);
  };
  ASSERT_EQ(cotInt(0), std::numeric_limits<double>::infinity());
  ASSERT_DOUBLE_EQ(cotInt(1).value(), 1.0 / std::tan(1.0));
  ASSERT_DOUBLE_EQ(cotInt(-1).value(), 1.0 / std::tan(-1.0));
  ASSERT_EQ(cotInt(std::nullopt), std::nullopt);
}

TEST_F(SparkFunctionTest, roundWithDecimal) {
  runRoundWithDecimalTest<float>(testRoundWithDecFloatAndDoubleData<float>());
  runRoundWithDecimalTest<double>(testRoundWithDecFloatAndDoubleData<double>());
  runRoundWithDecimalTest<int64_t>(testRoundWithDecIntegralData<int64_t>());
  runRoundWithDecimalTest<int32_t>(testRoundWithDecIntegralData<int32_t>());
  runRoundWithDecimalTest<int16_t>(testRoundWithDecIntegralData<int16_t>());
  runRoundWithDecimalTest<int8_t>(testRoundWithDecIntegralData<int8_t>());
}
