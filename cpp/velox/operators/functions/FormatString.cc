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

#include <cinttypes>
#include <cstdio>
#include <string>

#include "velox/expression/ConstantExpr.h"
#include "velox/expression/VectorFunction.h"

namespace gluten {

namespace {

int64_t getIntValue(facebook::velox::DecodedVector* decoded, facebook::velox::vector_size_t row) {
  auto kind = decoded->base()->typeKind();
  switch (kind) {
    case facebook::velox::TypeKind::TINYINT:
      return decoded->valueAt<int8_t>(row);
    case facebook::velox::TypeKind::SMALLINT:
      return decoded->valueAt<int16_t>(row);
    case facebook::velox::TypeKind::INTEGER:
      return decoded->valueAt<int32_t>(row);
    case facebook::velox::TypeKind::BIGINT:
      return decoded->valueAt<int64_t>(row);
    case facebook::velox::TypeKind::REAL:
      return static_cast<int64_t>(decoded->valueAt<float>(row));
    case facebook::velox::TypeKind::DOUBLE:
      return static_cast<int64_t>(decoded->valueAt<double>(row));
    default:
      VELOX_UNSUPPORTED("Unsupported type for integer format: {}", facebook::velox::mapTypeKindToName(kind));
  }
}

double getDoubleValue(facebook::velox::DecodedVector* decoded, facebook::velox::vector_size_t row) {
  auto kind = decoded->base()->typeKind();
  switch (kind) {
    case facebook::velox::TypeKind::TINYINT:
      return decoded->valueAt<int8_t>(row);
    case facebook::velox::TypeKind::SMALLINT:
      return decoded->valueAt<int16_t>(row);
    case facebook::velox::TypeKind::INTEGER:
      return decoded->valueAt<int32_t>(row);
    case facebook::velox::TypeKind::BIGINT:
      return static_cast<double>(decoded->valueAt<int64_t>(row));
    case facebook::velox::TypeKind::REAL:
      return decoded->valueAt<float>(row);
    case facebook::velox::TypeKind::DOUBLE:
      return decoded->valueAt<double>(row);
    default:
      VELOX_UNSUPPORTED("Unsupported type for float format: {}", facebook::velox::mapTypeKindToName(kind));
  }
}

std::string argToString(facebook::velox::DecodedVector* decoded, facebook::velox::vector_size_t row) {
  auto kind = decoded->base()->typeKind();
  switch (kind) {
    case facebook::velox::TypeKind::VARCHAR: {
      auto sv = decoded->valueAt<facebook::velox::StringView>(row);
      return std::string(sv.data(), sv.size());
    }
    case facebook::velox::TypeKind::BOOLEAN:
      return decoded->valueAt<bool>(row) ? "true" : "false";
    case facebook::velox::TypeKind::TINYINT:
      return std::to_string(decoded->valueAt<int8_t>(row));
    case facebook::velox::TypeKind::SMALLINT:
      return std::to_string(decoded->valueAt<int16_t>(row));
    case facebook::velox::TypeKind::INTEGER:
      return std::to_string(decoded->valueAt<int32_t>(row));
    case facebook::velox::TypeKind::BIGINT:
      return std::to_string(decoded->valueAt<int64_t>(row));
    case facebook::velox::TypeKind::REAL: {
      char buf[64];
      snprintf(buf, sizeof(buf), "%g", static_cast<double>(decoded->valueAt<float>(row)));
      return buf;
    }
    case facebook::velox::TypeKind::DOUBLE: {
      char buf[64];
      snprintf(buf, sizeof(buf), "%g", decoded->valueAt<double>(row));
      return buf;
    }
    default:
      return decoded->base()->toString(decoded->index(row));
  }
}

std::string formatRow(
    const std::string& fmt,
    const std::vector<facebook::velox::exec::LocalDecodedVector>& decodedArgs,
    facebook::velox::vector_size_t row) {
  std::string result;
  int argIndex = 0;
  const int numArgs = static_cast<int>(decodedArgs.size());

  for (size_t i = 0; i < fmt.size(); ++i) {
    if (fmt[i] != '%') {
      result += fmt[i];
      continue;
    }

    ++i;
    if (i >= fmt.size()) {
      result += '%';
      break;
    }

    if (fmt[i] == '%') {
      result += '%';
      continue;
    }

    std::string spec = "%";

    while (i < fmt.size() && (fmt[i] == '-' || fmt[i] == '+' || fmt[i] == '0' || fmt[i] == ' ' || fmt[i] == '#')) {
      spec += fmt[i++];
    }

    while (i < fmt.size() && isdigit(fmt[i])) {
      spec += fmt[i++];
    }

    if (i < fmt.size() && fmt[i] == '.') {
      spec += fmt[i++];
      while (i < fmt.size() && isdigit(fmt[i])) {
        spec += fmt[i++];
      }
    }

    if (i >= fmt.size()) {
      result += spec;
      break;
    }

    char conversion = fmt[i];

    if (argIndex >= numArgs) {
      result += spec + conversion;
      continue;
    }

    auto* decoded = decodedArgs[argIndex].get();
    ++argIndex;

    if (decoded->isNullAt(row)) {
      result += "null";
      continue;
    }

    char buf[4096];
    switch (conversion) {
      case 's': {
        std::string val = argToString(decoded, row);
        spec += "s";
        snprintf(buf, sizeof(buf), spec.c_str(), val.c_str());
        result += buf;
        break;
      }
      case 'd': {
        int64_t val = getIntValue(decoded, row);
        spec += PRId64;
        snprintf(buf, sizeof(buf), spec.c_str(), val);
        result += buf;
        break;
      }
      case 'o': {
        int64_t val = getIntValue(decoded, row);
        spec += PRIo64;
        snprintf(buf, sizeof(buf), spec.c_str(), val);
        result += buf;
        break;
      }
      case 'x': {
        int64_t val = getIntValue(decoded, row);
        spec += PRIx64;
        snprintf(buf, sizeof(buf), spec.c_str(), val);
        result += buf;
        break;
      }
      case 'X': {
        int64_t val = getIntValue(decoded, row);
        spec += PRIX64;
        snprintf(buf, sizeof(buf), spec.c_str(), val);
        result += buf;
        break;
      }
      case 'f':
      case 'e':
      case 'E':
      case 'g':
      case 'G': {
        double val = getDoubleValue(decoded, row);
        spec += conversion;
        snprintf(buf, sizeof(buf), spec.c_str(), val);
        result += buf;
        break;
      }
      default: {
        result += spec + conversion;
        break;
      }
    }
  }

  return result;
}

class FormatStringFunction : public facebook::velox::exec::VectorFunction {
 public:
  void apply(
      const facebook::velox::SelectivityVector& rows,
      std::vector<facebook::velox::VectorPtr>& args,
      const facebook::velox::TypePtr& outputType,
      facebook::velox::exec::EvalCtx& context,
      facebook::velox::VectorPtr& result) const override {
    using namespace facebook::velox;

    context.ensureWritable(rows, VARCHAR(), result);
    auto* flatResult = result->asFlatVector<StringView>();

    exec::LocalDecodedVector decodedFormat(context, *args[0], rows);

    std::vector<exec::LocalDecodedVector> decodedArgs;
    decodedArgs.reserve(args.size() - 1);
    for (size_t i = 1; i < args.size(); ++i) {
      decodedArgs.emplace_back(context, *args[i], rows);
    }

    rows.applyToSelected([&](vector_size_t row) {
      if (decodedFormat->isNullAt(row)) {
        result->setNull(row, true);
        return;
      }

      auto formatStr = decodedFormat->valueAt<StringView>(row);
      std::string fmt(formatStr.data(), formatStr.size());

      std::string formatted = formatRow(fmt, decodedArgs, row);

      flatResult->set(row, StringView(formatted));
    });
  }
};

} // namespace

facebook::velox::TypePtr FormatStringCallToSpecialForm::resolveType(
    const std::vector<facebook::velox::TypePtr>& /*argTypes*/) {
  return facebook::velox::VARCHAR();
}

facebook::velox::exec::ExprPtr FormatStringCallToSpecialForm::constructSpecialForm(
    const facebook::velox::TypePtr& type,
    std::vector<facebook::velox::exec::ExprPtr>&& args,
    bool trackCpuUsage,
    const facebook::velox::core::QueryConfig& config) {
  VELOX_USER_CHECK_GE(args.size(), 1, "format_string requires at least one argument, but got {}.", args.size());
  VELOX_USER_CHECK(args[0]->type()->isVarchar(), "The first argument of format_string must be a varchar.");

  auto formatStringFunction = std::make_shared<FormatStringFunction>();
  return std::make_shared<facebook::velox::exec::Expr>(
      type,
      std::move(args),
      std::move(formatStringFunction),
      facebook::velox::exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build(),
      kFormatString,
      trackCpuUsage);
}

} // namespace gluten
