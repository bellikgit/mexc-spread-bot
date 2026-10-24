#pragma once

#include "spred/platform.hpp"

namespace spred {

std::string b64(const unsigned char* data, size_t len);
long long epochMs();
std::string hmacSha256Hex(const std::string& key, const std::string& data);
std::string fmtDec(double v);
std::string base36(long long v);

} // namespace spred
