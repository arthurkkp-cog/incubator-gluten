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
  void runBRoundTest(const std::vector<std::tuple<T, T>>& data) {
    auto result = evaluate<SimpleVector<T>>("bround(c0)", makeRowVector({makeFlatVector<T, 0>(data)}));
    for (int32_t i = 0; i < data.size(); ++i) {
      ASSERT_EQ(result->valueAt(i), std::get<1>(data[i]));
    }
  }

  template <typename T>
  void runBRoundWithDecimalTest(const std::vector<std::tuple<T, int32_t, T>>& data) {
    auto result = evaluate<SimpleVector<T>>(
        "bround(c0, c1)", makeRowVector({makeFlatVector<T, 0>(data), makeFlatVector<int32_t, 1>(data)}));
    for (int32_t i = 0; i < data.size(); ++i) {
      ASSERT_EQ(result->valueAt(i), std::get<2>(data[i]));
    }
  }

  template <typename T>
  std::vector<std::tuple<T, T>> testBRoundFloatData() {
    return {
        {0.5, 0.0},
        {1.5, 2.0},
        {2.5, 2.0},
        {3.5, 4.0},
        {4.5, 4.0},
        {-0.5, 0.0},
        {-1.5, -2.0},
        {-2.5, -2.0},
        {1.0, 1.0},
        {1.3, 1.0},
        {1.9, 2.0},
        {0.0, 0.0}};
  }

  template <typename T>
  std::vector<std::tuple<T, T>> testBRoundIntegralData() {
    return {{1, 1}, {0, 0}, {-1, -1}, {2, 2}};
  }

  template <typename T>
  std::vector<std::tuple<T, int32_t, T>> testBRoundWithDecFloatAndDoubleData() {
    return {
        {2.5, 0, 2.0},
        {3.5, 0, 4.0},
        {2.25, 1, 2.2},
        {2.35, 1, 2.4},
        {2.45, 1, 2.4},
        {-2.5, 0, -2.0},
        {-3.5, 0, -4.0},
        {1.0 / 3, 2, 0.33},
        {1.0 / 3, 6, 0.333333},
        {1.0, -1, 0.0},
        {0.0, -2, 0.0},
        {11111.0, -1, 11110.0},
        {11111.0, -2, 11100.0},
        {15.0, -1, 20.0},
        {25.0, -1, 20.0},
        {35.0, -1, 40.0}};
  }

  template <typename T>
  std::vector<std::tuple<T, int32_t, T>> testBRoundWithDecIntegralData() {
    return {
        {1, 0, 1},
        {0, 0, 0},
        {-1, 0, -1},
        {1, 1, 1},
        {0, 1, 0},
        {-1, 1, -1},
        {1, -1, 0},
        {0, -2, 0},
        {-1, -3, 0},
        {15, -1, 20},
        {25, -1, 20},
        {35, -1, 40}};
  }

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

TEST_F(SparkFunctionTest, roundWithDecimal) {
  runRoundWithDecimalTest<float>(testRoundWithDecFloatAndDoubleData<float>());
  runRoundWithDecimalTest<double>(testRoundWithDecFloatAndDoubleData<double>());
  runRoundWithDecimalTest<int64_t>(testRoundWithDecIntegralData<int64_t>());
  runRoundWithDecimalTest<int32_t>(testRoundWithDecIntegralData<int32_t>());
  runRoundWithDecimalTest<int16_t>(testRoundWithDecIntegralData<int16_t>());
  runRoundWithDecimalTest<int8_t>(testRoundWithDecIntegralData<int8_t>());
}

TEST_F(SparkFunctionTest, bround) {
  runBRoundTest<float>(testBRoundFloatData<float>());
  runBRoundTest<double>(testBRoundFloatData<double>());
  runBRoundTest<int64_t>(testBRoundIntegralData<int64_t>());
  runBRoundTest<int32_t>(testBRoundIntegralData<int32_t>());
  runBRoundTest<int16_t>(testBRoundIntegralData<int16_t>());
  runBRoundTest<int8_t>(testBRoundIntegralData<int8_t>());
}

TEST_F(SparkFunctionTest, broundWithDecimal) {
  runBRoundWithDecimalTest<float>(testBRoundWithDecFloatAndDoubleData<float>());
  runBRoundWithDecimalTest<double>(testBRoundWithDecFloatAndDoubleData<double>());
  runBRoundWithDecimalTest<int64_t>(testBRoundWithDecIntegralData<int64_t>());
  runBRoundWithDecimalTest<int32_t>(testBRoundWithDecIntegralData<int32_t>());
  runBRoundWithDecimalTest<int16_t>(testBRoundWithDecIntegralData<int16_t>());
  runBRoundWithDecimalTest<int8_t>(testBRoundWithDecIntegralData<int8_t>());
}
