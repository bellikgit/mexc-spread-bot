#pragma once

#include "spred/platform.hpp"

namespace spred {

extern bool g_replay;
extern double g_simNowMs;
extern FILE* g_logFile;
extern volatile std::sig_atomic_t g_stop;
double nowMsSteady();
double nowSecs();
std::string nowWallClock(bool withMs = false);
void evAdd(std::vector<std::string>& v, const std::string& s);
void lprintf(const char* fmt, ...);
void lflush();
void onSigint(int);

} // namespace spred
