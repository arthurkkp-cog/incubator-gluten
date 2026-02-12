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

TEST_F(SparkFunctionTest, roundWithDecimal) {
  runRoundWithDecimalTest<float>(testRoundWithDecFloatAndDoubleData<float>());
  runRoundWithDecimalTest<double>(testRoundWithDecFloatAndDoubleData<double>());
  runRoundWithDecimalTest<int64_t>(testRoundWithDecIntegralData<int64_t>());
  runRoundWithDecimalTest<int32_t>(testRoundWithDecIntegralData<int32_t>());
  runRoundWithDecimalTest<int16_t>(testRoundWithDecIntegralData<int16_t>());
  runRoundWithDecimalTest<int8_t>(testRoundWithDecIntegralData<int8_t>());
}

TEST_F(SparkFunctionTest, formatStringBasic) {
  auto formatVector = makeFlatVector<StringView>({"Hello %s", "Value: %d", "Pi is %.2f", "100%%"});
  auto stringArg = makeFlatVector<StringView>({"World", "unused", "unused", "unused"});
  auto intArg = makeFlatVector<int32_t>({0, 42, 0, 0});
  auto doubleArg = makeFlatVector<double>({0.0, 0.0, 3.14159, 0.0});

  auto result =
      evaluate<SimpleVector<StringView>>("format_string(c0, c1)", makeRowVector({formatVector, stringArg}));
  ASSERT_EQ(result->valueAt(0).getString(), "Hello World");

  auto result2 = evaluate<SimpleVector<StringView>>("format_string(c0, c1)", makeRowVector({formatVector, intArg}));
  ASSERT_EQ(result2->valueAt(1).getString(), "Value: 42");

  auto result3 =
      evaluate<SimpleVector<StringView>>("format_string(c0, c1)", makeRowVector({formatVector, doubleArg}));
  ASSERT_EQ(result3->valueAt(2).getString(), "Pi is 3.14");

  auto fmtPercent = makeFlatVector<StringView>({"100%%"});
  auto dummyArg = makeFlatVector<int32_t>({0});
  auto result4 =
      evaluate<SimpleVector<StringView>>("format_string(c0)", makeRowVector({fmtPercent}));
  ASSERT_EQ(result4->valueAt(0).getString(), "100%");
}

TEST_F(SparkFunctionTest, formatStringMultipleArgs) {
  auto formatVector = makeFlatVector<StringView>({"%s has %d items"});
  auto strArg = makeFlatVector<StringView>({"Alice"});
  auto intArg = makeFlatVector<int32_t>({5});

  auto result = evaluate<SimpleVector<StringView>>(
      "format_string(c0, c1, c2)", makeRowVector({formatVector, strArg, intArg}));
  ASSERT_EQ(result->valueAt(0).getString(), "Alice has 5 items");
}

TEST_F(SparkFunctionTest, formatStringIntegerTypes) {
  auto fmtVec = makeFlatVector<StringView>({"%d", "%d", "%d", "%d"});

  auto tinyArg = makeFlatVector<int8_t>({42, 0, 0, 0});
  auto result1 = evaluate<SimpleVector<StringView>>("format_string(c0, c1)", makeRowVector({fmtVec, tinyArg}));
  ASSERT_EQ(result1->valueAt(0).getString(), "42");

  auto smallArg = makeFlatVector<int16_t>({0, 1000, 0, 0});
  auto result2 = evaluate<SimpleVector<StringView>>("format_string(c0, c1)", makeRowVector({fmtVec, smallArg}));
  ASSERT_EQ(result2->valueAt(1).getString(), "1000");

  auto intArg = makeFlatVector<int32_t>({0, 0, -999, 0});
  auto result3 = evaluate<SimpleVector<StringView>>("format_string(c0, c1)", makeRowVector({fmtVec, intArg}));
  ASSERT_EQ(result3->valueAt(2).getString(), "-999");

  auto bigArg = makeFlatVector<int64_t>({0, 0, 0, 1234567890123LL});
  auto result4 = evaluate<SimpleVector<StringView>>("format_string(c0, c1)", makeRowVector({fmtVec, bigArg}));
  ASSERT_EQ(result4->valueAt(3).getString(), "1234567890123");
}

TEST_F(SparkFunctionTest, formatStringFloatTypes) {
  auto fmtVec = makeFlatVector<StringView>({"%.4f"});
  auto doubleArg = makeFlatVector<double>({2.71828});
  auto result1 = evaluate<SimpleVector<StringView>>("format_string(c0, c1)", makeRowVector({fmtVec, doubleArg}));
  ASSERT_EQ(result1->valueAt(0).getString(), "2.7183");

  auto fmtVec2 = makeFlatVector<StringView>({"%e"});
  auto floatArg = makeFlatVector<float>({123456.0f});
  auto result2 = evaluate<SimpleVector<StringView>>("format_string(c0, c1)", makeRowVector({fmtVec2, floatArg}));
  auto res2str = result2->valueAt(0).getString();
  ASSERT_TRUE(res2str.find("e+") != std::string::npos || res2str.find("E+") != std::string::npos);
}

TEST_F(SparkFunctionTest, formatStringHexOctal) {
  auto fmtHex = makeFlatVector<StringView>({"%x"});
  auto intArg = makeFlatVector<int32_t>({255});
  auto result1 = evaluate<SimpleVector<StringView>>("format_string(c0, c1)", makeRowVector({fmtHex, intArg}));
  ASSERT_EQ(result1->valueAt(0).getString(), "ff");

  auto fmtOct = makeFlatVector<StringView>({"%o"});
  auto result2 = evaluate<SimpleVector<StringView>>("format_string(c0, c1)", makeRowVector({fmtOct, intArg}));
  ASSERT_EQ(result2->valueAt(0).getString(), "377");
}

TEST_F(SparkFunctionTest, formatStringWidthPadding) {
  auto fmtVec = makeFlatVector<StringView>({"%04d"});
  auto intArg = makeFlatVector<int32_t>({42});
  auto result = evaluate<SimpleVector<StringView>>("format_string(c0, c1)", makeRowVector({fmtVec, intArg}));
  ASSERT_EQ(result->valueAt(0).getString(), "0042");

  auto fmtVec2 = makeFlatVector<StringView>({"%10s"});
  auto strArg = makeFlatVector<StringView>({"hello"});
  auto result2 = evaluate<SimpleVector<StringView>>("format_string(c0, c1)", makeRowVector({fmtVec2, strArg}));
  ASSERT_EQ(result2->valueAt(0).getString(), "     hello");
}

TEST_F(SparkFunctionTest, formatStringNoArgs) {
  auto fmtVec = makeFlatVector<StringView>({"Hello World"});
  auto result = evaluate<SimpleVector<StringView>>("format_string(c0)", makeRowVector({fmtVec}));
  ASSERT_EQ(result->valueAt(0).getString(), "Hello World");
}

TEST_F(SparkFunctionTest, formatStringNullFormat) {
  auto fmtVec = makeNullableFlatVector<StringView>({std::nullopt});
  auto strArg = makeFlatVector<StringView>({"test"});
  auto result =
      evaluate<SimpleVector<StringView>>("format_string(c0, c1)", makeRowVector({fmtVec, strArg}));
  ASSERT_TRUE(result->isNullAt(0));
}

TEST_F(SparkFunctionTest, formatStringNullArg) {
  auto fmtVec = makeFlatVector<StringView>({"Value: %s"});
  auto strArg = makeNullableFlatVector<StringView>({std::nullopt});
  auto result =
      evaluate<SimpleVector<StringView>>("format_string(c0, c1)", makeRowVector({fmtVec, strArg}));
  ASSERT_EQ(result->valueAt(0).getString(), "Value: null");
}
