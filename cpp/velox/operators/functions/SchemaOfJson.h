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

#include <map>
#include <sstream>
#include <string>
#include <vector>
#include "velox/functions/Macros.h"
#include "velox/functions/prestosql/json/SIMDJsonUtil.h"

namespace gluten {

/// schema_of_json(jsonString) -> string
///
/// Returns the schema of a JSON string in DDL format.
/// For example:
///   schema_of_json('[{"col":0}]') returns 'ARRAY<STRUCT<col: BIGINT>>'
///   schema_of_json('{"a":1,"b":"hello"}') returns 'STRUCT<a: BIGINT, b: STRING>'
template <typename T>
struct SchemaOfJsonFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE bool call(
      out_type<facebook::velox::Varchar>& result,
      const arg_type<facebook::velox::Varchar>& json) {
    simdjson::ondemand::document jsonDoc;
    simdjson::padded_string paddedJson(json.data(), json.size());

    // Parse the JSON string.
    if (simdjsonParse(paddedJson).get(jsonDoc)) {
      return false;
    }

    // Infer the schema from the JSON document.
    std::string schema;
    if (!inferSchema(jsonDoc, schema)) {
      return false;
    }

    result.append(schema);
    return true;
  }

 private:
  // Infers the schema from a JSON value and returns it as a DDL string.
  bool inferSchema(simdjson::ondemand::value value, std::string& schema) {
    simdjson::ondemand::json_type type;
    if (value.type().get(type)) {
      return false;
    }

    switch (type) {
      case simdjson::ondemand::json_type::null:
        schema = "STRING";
        return true;

      case simdjson::ondemand::json_type::boolean:
        schema = "BOOLEAN";
        return true;

      case simdjson::ondemand::json_type::number: {
        simdjson::ondemand::number_type numType;
        if (value.get_number_type().get(numType)) {
          return false;
        }
        switch (numType) {
          case simdjson::ondemand::number_type::floating_point_number:
            schema = "DOUBLE";
            return true;
          case simdjson::ondemand::number_type::signed_integer:
          case simdjson::ondemand::number_type::unsigned_integer:
          case simdjson::ondemand::number_type::big_integer:
            schema = "BIGINT";
            return true;
        }
        return false;
      }

      case simdjson::ondemand::json_type::string:
        schema = "STRING";
        return true;

      case simdjson::ondemand::json_type::array: {
        simdjson::ondemand::array arr;
        if (value.get_array().get(arr)) {
          return false;
        }

        // Get the first element to infer the element type.
        std::string elementSchema = "STRING";
        bool hasElement = false;
        for (auto element : arr) {
          if (element.error()) {
            return false;
          }
          hasElement = true;
          if (!inferSchema(element.value(), elementSchema)) {
            return false;
          }
          break; // Only use the first element for type inference.
        }

        schema = "ARRAY<" + elementSchema + ">";
        return true;
      }

      case simdjson::ondemand::json_type::object: {
        simdjson::ondemand::object obj;
        if (value.get_object().get(obj)) {
          return false;
        }

        std::vector<std::pair<std::string, std::string>> fields;
        for (auto field : obj) {
          if (field.error()) {
            return false;
          }

          std::string_view key = field.unescaped_key();
          std::string fieldSchema;
          if (!inferSchema(field.value(), fieldSchema)) {
            return false;
          }
          fields.emplace_back(std::string(key), fieldSchema);
        }

        std::ostringstream oss;
        oss << "STRUCT<";
        for (size_t i = 0; i < fields.size(); ++i) {
          if (i > 0) {
            oss << ", ";
          }
          oss << fields[i].first << ": " << fields[i].second;
        }
        oss << ">";
        schema = oss.str();
        return true;
      }
    }
    return false;
  }

  // Infers the schema from a JSON document.
  bool inferSchema(simdjson::ondemand::document& doc, std::string& schema) {
    simdjson::ondemand::json_type type;
    if (doc.type().get(type)) {
      return false;
    }

    switch (type) {
      case simdjson::ondemand::json_type::null:
        schema = "STRING";
        return true;

      case simdjson::ondemand::json_type::boolean:
        schema = "BOOLEAN";
        return true;

      case simdjson::ondemand::json_type::number: {
        simdjson::ondemand::number_type numType;
        if (doc.get_number_type().get(numType)) {
          return false;
        }
        switch (numType) {
          case simdjson::ondemand::number_type::floating_point_number:
            schema = "DOUBLE";
            return true;
          case simdjson::ondemand::number_type::signed_integer:
          case simdjson::ondemand::number_type::unsigned_integer:
          case simdjson::ondemand::number_type::big_integer:
            schema = "BIGINT";
            return true;
        }
        return false;
      }

      case simdjson::ondemand::json_type::string:
        schema = "STRING";
        return true;

      case simdjson::ondemand::json_type::array: {
        simdjson::ondemand::array arr;
        if (doc.get_array().get(arr)) {
          return false;
        }

        // Get the first element to infer the element type.
        std::string elementSchema = "STRING";
        for (auto element : arr) {
          if (element.error()) {
            return false;
          }
          if (!inferSchema(element.value(), elementSchema)) {
            return false;
          }
          break; // Only use the first element for type inference.
        }

        schema = "ARRAY<" + elementSchema + ">";
        return true;
      }

      case simdjson::ondemand::json_type::object: {
        simdjson::ondemand::object obj;
        if (doc.get_object().get(obj)) {
          return false;
        }

        std::vector<std::pair<std::string, std::string>> fields;
        for (auto field : obj) {
          if (field.error()) {
            return false;
          }

          std::string_view key = field.unescaped_key();
          std::string fieldSchema;
          if (!inferSchema(field.value(), fieldSchema)) {
            return false;
          }
          fields.emplace_back(std::string(key), fieldSchema);
        }

        std::ostringstream oss;
        oss << "STRUCT<";
        for (size_t i = 0; i < fields.size(); ++i) {
          if (i > 0) {
            oss << ", ";
          }
          oss << fields[i].first << ": " << fields[i].second;
        }
        oss << ">";
        schema = oss.str();
        return true;
      }
    }
    return false;
  }
};

} // namespace gluten
