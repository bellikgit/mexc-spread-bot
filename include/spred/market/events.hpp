#pragma once

#include "spred/protocol/mexc.hpp"

namespace spred {

// [v2.12 ФИКС 7] event-time очередь
struct PendEv {
    double exchMs = 0;
    double recvMs = 0;
    int kind = 0;                 // 0=depth, 1=ticker, 2=deal
    std::vector<Level> bids, asks;
    double bidP = 0, bidQ = 0, askP = 0, askQ = 0;
    MexcPush::Deal deal;
};

} // namespace spred
