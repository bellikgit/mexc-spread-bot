#pragma once

#include "spred/trading/live_config.hpp"

namespace spred {

struct SessionOptions {
    LiveCfg& lcfg;
    bool logDeals;
    double fairBasisSec, fairWarmSec;
    double& rttMs;
    double t0;
    const std::string& replayPath;
    int& sessions;
    long& msgCount;
    long& binFrames;
    long& parseFails;
    double& connAt;
    bool& lost;
};

} // namespace spred
