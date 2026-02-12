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
#include <cstring>
#include <string_view>

namespace gluten {

namespace {

struct ParsedUrl {
  std::string_view protocol;
  std::string_view host;
  std::string_view path;
  std::string_view query;
  std::string_view ref;
  std::string_view authority;
  std::string_view userInfo;
  bool hasProtocol = false;
  bool hasAuthority = false;
  bool hasHost = false;
  bool hasPath = false;
  bool hasQuery = false;
  bool hasRef = false;
  bool hasUserInfo = false;
  bool isOpaque = false;
};

inline void parseAuthority(ParsedUrl& result) {
  const char* start = result.authority.data();
  const char* end = start + result.authority.size();

  const char* atPos = nullptr;
  for (const char* c = start; c < end; ++c) {
    if (*c == '@') {
      atPos = c;
      break;
    }
  }

  const char* hostStart = start;
  if (atPos) {
    result.userInfo = std::string_view(start, atPos - start);
    result.hasUserInfo = true;
    hostStart = atPos + 1;
  }

  if (hostStart < end && *hostStart == '[') {
    const char* bracketEnd = nullptr;
    for (const char* c = hostStart + 1; c < end; ++c) {
      if (*c == ']') {
        bracketEnd = c;
        break;
      }
    }
    if (bracketEnd) {
      result.host = std::string_view(hostStart, bracketEnd + 1 - hostStart);
      result.hasHost = true;
    }
  } else {
    const char* portColon = nullptr;
    for (const char* c = hostStart; c < end; ++c) {
      if (*c == ':') {
        portColon = c;
      }
    }
    if (portColon) {
      result.host = std::string_view(hostStart, portColon - hostStart);
    } else {
      result.host = std::string_view(hostStart, end - hostStart);
    }
    result.hasHost = !result.host.empty();
  }
}

inline bool parseUrl(const char* url, size_t len, ParsedUrl& result) {
  if (len == 0) {
    return false;
  }

  const char* p = url;
  const char* end = url + len;

  const char* schemeEnd = nullptr;
  for (const char* c = p; c < end; ++c) {
    if (*c == ':') {
      schemeEnd = c;
      break;
    }
    if (*c == '/' || *c == '?' || *c == '#') {
      break;
    }
  }

  if (schemeEnd && schemeEnd > p) {
    bool validScheme = ((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z'));
    if (validScheme) {
      for (const char* c = p + 1; c < schemeEnd; ++c) {
        char ch = *c;
        if (!((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '+' ||
              ch == '-' || ch == '.')) {
          validScheme = false;
          break;
        }
      }
    }
    if (validScheme) {
      result.protocol = std::string_view(p, schemeEnd - p);
      result.hasProtocol = true;
      p = schemeEnd + 1;
    }
  }

  if (result.hasProtocol && (p >= end || *p != '/')) {
    result.isOpaque = true;
    for (const char* c = p; c < end; ++c) {
      if (*c == '#') {
        result.ref = std::string_view(c + 1, end - c - 1);
        result.hasRef = true;
        break;
      }
    }
    return true;
  }

  if (p + 1 < end && p[0] == '/' && p[1] == '/') {
    p += 2;
    const char* authEnd = p;
    while (authEnd < end && *authEnd != '/' && *authEnd != '?' && *authEnd != '#') {
      ++authEnd;
    }
    result.authority = std::string_view(p, authEnd - p);
    result.hasAuthority = true;
    parseAuthority(result);
    p = authEnd;
  }

  const char* pathEnd = p;
  while (pathEnd < end && *pathEnd != '?' && *pathEnd != '#') {
    ++pathEnd;
  }
  result.path = std::string_view(p, pathEnd - p);
  result.hasPath = true;
  p = pathEnd;

  if (p < end && *p == '?') {
    ++p;
    const char* queryEnd = p;
    while (queryEnd < end && *queryEnd != '#') {
      ++queryEnd;
    }
    result.query = std::string_view(p, queryEnd - p);
    result.hasQuery = true;
    p = queryEnd;
  }

  if (p < end && *p == '#') {
    ++p;
    result.ref = std::string_view(p, end - p);
    result.hasRef = true;
  }

  return true;
}

inline bool extractQueryParam(const std::string_view& query, const std::string_view& key, std::string_view& value) {
  const char* p = query.data();
  const char* end = p + query.size();

  while (p < end) {
    const char* ampPos = end;
    for (const char* c = p; c < end; ++c) {
      if (*c == '&') {
        ampPos = c;
        break;
      }
    }

    const char* eqPos = nullptr;
    for (const char* c = p; c < ampPos; ++c) {
      if (*c == '=') {
        eqPos = c;
        break;
      }
    }

    if (eqPos) {
      std::string_view paramKey(p, eqPos - p);
      if (paramKey == key) {
        value = std::string_view(eqPos + 1, ampPos - eqPos - 1);
        return true;
      }
    }

    p = ampPos + 1;
  }

  return false;
}

} // namespace

template <typename T>
struct ParseUrlFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& url,
      const arg_type<Varchar>& partToExtract) {
    ParsedUrl parts;
    if (!parseUrl(url.data(), url.size(), parts)) {
      return false;
    }

    std::string_view partStr(partToExtract.data(), partToExtract.size());
    return extractPart(result, parts, partStr);
  }

 private:
  FOLLY_ALWAYS_INLINE bool extractPart(
      out_type<Varchar>& result,
      const ParsedUrl& parts,
      const std::string_view& partStr) const {
    if (partStr == "HOST") {
      if (parts.isOpaque || !parts.hasHost) {
        return false;
      }
      result.append(parts.host.data(), parts.host.size());
      return true;
    }
    if (partStr == "PATH") {
      if (parts.isOpaque) {
        return false;
      }
      result.append(parts.path.data(), parts.path.size());
      return true;
    }
    if (partStr == "QUERY") {
      if (parts.isOpaque || !parts.hasQuery) {
        return false;
      }
      result.append(parts.query.data(), parts.query.size());
      return true;
    }
    if (partStr == "REF") {
      if (!parts.hasRef) {
        return false;
      }
      result.append(parts.ref.data(), parts.ref.size());
      return true;
    }
    if (partStr == "PROTOCOL") {
      if (!parts.hasProtocol) {
        return false;
      }
      result.append(parts.protocol.data(), parts.protocol.size());
      return true;
    }
    if (partStr == "FILE") {
      if (parts.isOpaque || !parts.hasPath) {
        return false;
      }
      result.append(parts.path.data(), parts.path.size());
      if (parts.hasQuery) {
        result.append("?", 1);
        result.append(parts.query.data(), parts.query.size());
      }
      return true;
    }
    if (partStr == "AUTHORITY") {
      if (parts.isOpaque || !parts.hasAuthority) {
        return false;
      }
      result.append(parts.authority.data(), parts.authority.size());
      return true;
    }
    if (partStr == "USERINFO") {
      if (parts.isOpaque || !parts.hasUserInfo) {
        return false;
      }
      result.append(parts.userInfo.data(), parts.userInfo.size());
      return true;
    }
    return false;
  }
};

template <typename T>
struct ParseUrlWithKeyFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& url,
      const arg_type<Varchar>& partToExtract,
      const arg_type<Varchar>& key) {
    std::string_view partStr(partToExtract.data(), partToExtract.size());

    if (partStr != "QUERY") {
      return false;
    }

    ParsedUrl parts;
    if (!parseUrl(url.data(), url.size(), parts)) {
      return false;
    }

    if (parts.isOpaque || !parts.hasQuery) {
      return false;
    }

    std::string_view keyStr(key.data(), key.size());
    std::string_view value;
    if (extractQueryParam(parts.query, keyStr, value)) {
      result.append(value.data(), value.size());
      return true;
    }
    return false;
  }
};

} // namespace gluten
