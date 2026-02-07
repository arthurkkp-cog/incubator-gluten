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
#include <cctype>
#include <sstream>
#include <string>
#include <vector>
#include "velox/functions/Udf.h"
#include "velox/type/StringView.h"

namespace gluten {

namespace {

inline bool isInteger(const std::string& s) {
  if (s.empty()) {
    return false;
  }
  size_t start = 0;
  if (s[0] == '-' || s[0] == '+') {
    if (s.length() == 1) {
      return false;
    }
    start = 1;
  }
  for (size_t i = start; i < s.length(); ++i) {
    if (!std::isdigit(static_cast<unsigned char>(s[i]))) {
      return false;
    }
  }
  return true;
}

inline bool isDouble(const std::string& s) {
  if (s.empty()) {
    return false;
  }
  char* end = nullptr;
  std::strtod(s.c_str(), &end);
  return end != s.c_str() && *end == '\0';
}

inline bool isBoolean(const std::string& s) {
  std::string lower = s;
  std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char c) { return std::tolower(c); });
  return lower == "true" || lower == "false";
}

inline std::string inferType(const std::string& value) {
  if (value.empty()) {
    return "STRING";
  }
  if (isBoolean(value)) {
    return "BOOLEAN";
  }
  if (isInteger(value)) {
    return "INT";
  }
  if (isDouble(value)) {
    return "DOUBLE";
  }
  return "STRING";
}

inline std::string trim(const std::string& s) {
  size_t start = s.find_first_not_of(" \t\n\r");
  if (start == std::string::npos) {
    return "";
  }
  size_t end = s.find_last_not_of(" \t\n\r");
  return s.substr(start, end - start + 1);
}

inline std::vector<std::string> parseCsvLine(const std::string& line, char delimiter) {
  std::vector<std::string> fields;
  std::string field;
  bool inQuotes = false;

  for (size_t i = 0; i < line.length(); ++i) {
    char c = line[i];
    if (c == '"') {
      if (inQuotes && i + 1 < line.length() && line[i + 1] == '"') {
        field += '"';
        ++i;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (c == delimiter && !inQuotes) {
      fields.push_back(trim(field));
      field.clear();
    } else {
      field += c;
    }
  }
  fields.push_back(trim(field));
  return fields;
}

} // namespace

template <typename T>
struct SchemaOfCsvFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(out_type<facebook::velox::Varchar>& result, const arg_type<facebook::velox::Varchar>& csv) {
    callImpl(result, csv, ',');
  }

  FOLLY_ALWAYS_INLINE void
  call(out_type<facebook::velox::Varchar>& result, const arg_type<facebook::velox::Varchar>& csv, const arg_type<facebook::velox::Varchar>& delimiter) {
    char delim = ',';
    if (delimiter.size() > 0) {
      delim = delimiter.data()[0];
    }
    callImpl(result, csv, delim);
  }

 private:
  FOLLY_ALWAYS_INLINE void callImpl(out_type<facebook::velox::Varchar>& result, const arg_type<facebook::velox::Varchar>& csv, char delimiter) {
    std::string csvStr(csv.data(), csv.size());
    std::vector<std::string> fields = parseCsvLine(csvStr, delimiter);

    std::ostringstream schema;
    schema << "STRUCT<";
    for (size_t i = 0; i < fields.size(); ++i) {
      if (i > 0) {
        schema << ", ";
      }
      schema << "_c" << i << ": " << inferType(fields[i]);
    }
    schema << ">";

    std::string schemaStr = schema.str();
    result.resize(schemaStr.size());
    std::memcpy(result.data(), schemaStr.data(), schemaStr.size());
  }
};

} // namespace gluten
