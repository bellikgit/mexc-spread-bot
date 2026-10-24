#pragma once

#include "spred/platform.hpp"

namespace spred {

bool jsonFindNum(const std::string& s, const std::string& key, double& out);
bool jsonFindStr(const std::string& s, const std::string& key,
                        std::string& out);
std::vector<std::string> jsonSplitObjects(const std::string& s);
std::string jsonExtractArray(const std::string& s, const char* key);

} // namespace spred
