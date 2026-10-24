#pragma once

#include "spred/platform.hpp"

namespace spred {

std::string envOr(const char* name, const std::string& dflt);
double envD(const char* name, double dflt);
int envI(const char* name, int dflt);
double envDsym(const char* name, const std::string& sym, double dflt);
std::vector<std::string> splitSymbols(const std::string& s);

} // namespace spred
