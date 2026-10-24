#pragma once

#include "spred/core/runtime.hpp"

namespace spred {

struct FairQ { double bid = 0, ask = 0, tMs = 0, bq = 0, aq = 0; long n = 0; };

extern int g_fairMode;
extern std::mutex g_fairMu;
extern std::map<std::string, FairQ> g_fair;
extern std::vector<std::string> g_fairLog;
extern std::atomic<long> g_fairMsgs, g_fairReconn;
extern bool g_fairUsdcMap;
void fairLog(const std::string& m);
std::string fairRef(const std::string& sym);
bool fairGet(const std::string& sym0, FairQ& q);
void bybitFairThread(std::vector<std::string> syms, std::string host, int port, std::string path);

} // namespace spred
