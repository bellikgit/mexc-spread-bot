#pragma once

#include "spred/protocol/mexc.hpp"

namespace spred {

struct FlowTracker {
    struct Ev { double t, signedQty; };
    std::deque<Ev> ev;
    double windowSec = 60.0;
    double runDecaySec = 20.0;
    double lastRunT = -1e9;
    int runLen = 0;
    int minDeals = 5;

    void prune(double now) {
        while (!ev.empty() && now - ev.front().t > windowSec) ev.pop_front();
    }

    void onDeal(const MexcPush::Deal& d, double now,
                bool haveExchClock, double exchOffMs) {
        double s = (d.side == 1) ? d.qty : (d.side == 2) ? -d.qty : 0.0;
        if (s == 0.0) return;
        double t = now;
        if (haveExchClock && d.timeMs > 0)
            t = ((double)d.timeMs + exchOffMs) / 1000.0;
        if (t > now) t = now;   // защита от рассинхрона часов
        ev.push_back({t, s});
        int sg = (s > 0) ? 1 : -1;
        if (t - lastRunT > runDecaySec) runLen = 0;
        runLen = (runLen != 0 && ((runLen > 0) == (sg > 0))) ? runLen + sg : sg;
        lastRunT = t;
        prune(now);
    }

    double tfi(double now) {
        prune(now);
        if ((int)ev.size() < minDeals) return 0.0;
        double b = 0, s = 0;
        for (const auto& e : ev) (e.signedQty > 0 ? b : s) += std::fabs(e.signedQty);
        return (b + s) > 0 ? (b - s) / (b + s) : 0.0;
    }

    int run(double now) const {
        return (now - lastRunT > runDecaySec) ? 0 : runLen;
    }
};

} // namespace spred
