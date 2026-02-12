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

#include "operators/functions/FormatString.h"

#include <cstdio>
#include <string>
#include "velox/expression/DecodedArgs.h"
#include "velox/expression/VectorFunction.h"
#include "velox/type/Type.h"

namespace gluten {

using namespace facebook::velox;

namespace {

std::string formatArg(
    const DecodedVector& decoded,
    vector_size_t row,
    const TypePtr& type,
    const std::string& specifier) {
  char buf[256];
  switch (type->kind()) {
    case TypeKind::BOOLEAN: {
      auto val = decoded.valueAt<bool>(row);
      if (specifier.find('s') != std::string::npos) {
        return val ? "true" : "false";
      }
      std::snprintf(buf, sizeof(buf), specifier.c_str(), val ? 1 : 0);
      return buf;
    }
    case TypeKind::TINYINT: {
      auto val = decoded.valueAt<int8_t>(row);
      if (specifier.find('s') != std::string::npos) {
        return std::to_string(val);
      }
      std::snprintf(buf, sizeof(buf), specifier.c_str(), static_cast<int>(val));
      return buf;
    }
    case TypeKind::SMALLINT: {
      auto val = decoded.valueAt<int16_t>(row);
      if (specifier.find('s') != std::string::npos) {
        return std::to_string(val);
      }
      std::snprintf(buf, sizeof(buf), specifier.c_str(), static_cast<int>(val));
      return buf;
    }
    case TypeKind::INTEGER: {
      auto val = decoded.valueAt<int32_t>(row);
      if (specifier.find('s') != std::string::npos) {
        return std::to_string(val);
      }
      std::snprintf(buf, sizeof(buf), specifier.c_str(), val);
      return buf;
    }
    case TypeKind::BIGINT: {
      auto val = decoded.valueAt<int64_t>(row);
      if (specifier.find('s') != std::string::npos) {
        return std::to_string(val);
      }
      auto spec = specifier;
      auto pos = spec.find_last_of("dioxX");
      if (pos != std::string::npos) {
        spec.insert(pos, "l");
      }
      std::snprintf(buf, sizeof(buf), spec.c_str(), val);
      return buf;
    }
    case TypeKind::REAL: {
      auto val = decoded.valueAt<float>(row);
      if (specifier.find('s') != std::string::npos) {
        std::snprintf(buf, sizeof(buf), "%f", static_cast<double>(val));
        return buf;
      }
      std::snprintf(buf, sizeof(buf), specifier.c_str(), static_cast<double>(val));
      return buf;
    }
    case TypeKind::DOUBLE: {
      auto val = decoded.valueAt<double>(row);
      if (specifier.find('s') != std::string::npos) {
        std::snprintf(buf, sizeof(buf), "%f", val);
        return buf;
      }
      std::snprintf(buf, sizeof(buf), specifier.c_str(), val);
      return buf;
    }
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY: {
      auto val = decoded.valueAt<StringView>(row);
      if (specifier == "%s") {
        return std::string(val.data(), val.size());
      }
      std::string strVal(val.data(), val.size());
      std::snprintf(buf, sizeof(buf), specifier.c_str(), strVal.c_str());
      return buf;
    }
    default:
      VELOX_UNSUPPORTED("Unsupported type for format_string: {}", type->toString());
  }
}

class FormatStringFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    context.ensureWritable(rows, VARCHAR(), result);
    auto* flatResult = result->asFlatVector<StringView>();

    exec::DecodedArgs decodedArgs(rows, args, context);

    rows.applyToSelected([&](vector_size_t row) {
      for (int i = 0; i < args.size(); ++i) {
        if (decodedArgs.at(i)->isNullAt(row)) {
          flatResult->setNull(row, true);
          return;
        }
      }

      auto format = decodedArgs.at(0)->valueAt<StringView>(row);
      std::string formatStr(format.data(), format.size());
      std::string output;
      output.reserve(formatStr.size() * 2);

      size_t argIndex = 1;
      size_t i = 0;
      while (i < formatStr.size()) {
        if (formatStr[i] != '%') {
          output.push_back(formatStr[i]);
          ++i;
          continue;
        }

        if (i + 1 < formatStr.size() && formatStr[i + 1] == '%') {
          output.push_back('%');
          i += 2;
          continue;
        }

        size_t specStart = i;
        ++i;

        while (i < formatStr.size() && (formatStr[i] == '-' || formatStr[i] == '+' || formatStr[i] == ' ' ||
                                         formatStr[i] == '0' || formatStr[i] == '#')) {
          ++i;
        }

        while (i < formatStr.size() && std::isdigit(formatStr[i])) {
          ++i;
        }

        if (i < formatStr.size() && formatStr[i] == '.') {
          ++i;
          while (i < formatStr.size() && std::isdigit(formatStr[i])) {
            ++i;
          }
        }

        if (i < formatStr.size()) {
          ++i;
        }

        std::string specifier = formatStr.substr(specStart, i - specStart);

        if (argIndex < args.size()) {
          output += formatArg(*decodedArgs.at(argIndex), row, args[argIndex]->type(), specifier);
          ++argIndex;
        }
      }

      flatResult->set(row, StringView(output));
    });
  }
};

} // namespace

std::vector<std::shared_ptr<exec::FunctionSignature>> formatStringSignatures() {
  return {exec::FunctionSignatureBuilder()
              .returnType("varchar")
              .argumentType("varchar")
              .variableArity("any")
              .build()};
}

std::shared_ptr<exec::VectorFunction> makeFormatString(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  return std::make_shared<FormatStringFunction>();
}

} // namespace gluten
