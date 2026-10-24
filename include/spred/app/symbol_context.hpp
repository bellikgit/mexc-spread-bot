#pragma once

#include "spred/exchange/symbol_info.hpp"
#include "spred/trading/paper.hpp"
#include "spred/trading/live_executor.hpp"
#include "spred/market/events.hpp"

namespace spred {

struct PendEsc { bool active = false; bool isBuy = false;
                 bool flat = false;   // [F57] закрыть всю позицию (HALT)
                 double qty = 0, fireAt = 0, expAvg = 0; };


struct SymCtx {
    std::string symbol;
    SymbolInfo si;
    double tick = 0.01;
    PaperTrader tr;                 // shadow-движок v2.16 (не тронут)
    BookState book;
    BookState xbook;                // [F52] live: книга БЕЗ наших реальных ордеров
    struct OwnLvl { int side; double px, qty, until; };
    std::deque<OwnLvl> ownRecent;   // [F52] недавно снятые/исполненные наши ордера
    std::string ownCid[2];
    double ownPx[2] = {0, 0}, ownRem[2] = {0, 0};
    std::deque<double> fillTimes;   // [F53] времена реальных филлов (breaker)
    long fillEvSeenBr[2] = {0, 0};
    double breakerUntil = 0;
    int breakerCnt = 0;
    FlowTracker flow;
    std::deque<PendEv> evq;         // [v2.12 Ф7] event-time очередь (per-sym)
    double evqMaxExchMs = 0;
    double lastAppliedExchMs = 0;   // [v2.16 P0-1] монотонность (per-sym)
    long lateEvents = 0;
    double gapMid0 = 0;             // [v2.12 Ф8] мид до обрыва
    bool gapMid0Valid = false;
    std::vector<std::string> events;
    LiveExecutor live;              // свой RestClient (keep-alive HTTPS на пару)
    double startUsdt = 10000;       // локальный лимит задействования quote
    double quoteShare = 1.0;        // доля свободных USDT кошелька [v3.1]
    std::string killFileSym;        // per-symbol kill-switch [v3.1]
    std::vector<double> priceSamples;
    int depthSeen = 0;
    double retryInitAt = 0;
    bool depthReady = false, warnedNoDepth = false, warnedBothPaused = false;
    double lastStratT = 0, lastFlatten = 0;
    int haltFlatTries = 0; double haltFlatAt = 0;          // [F57]
    std::deque<std::pair<double, double>> trendHist;       // [F58] (t, mid) раз в 5 с
    double lastTrendSample = 0, trendBlockBuyS = 0, trendBlockSellS = 0, lastTrendT = 0;
    int trendState = 0;                                    // -1 блок bid, +1 блок ask
    PendEsc pendEsc;
    long depthFrames = 0, tickerFrames = 0, dealFrames = 0;
    std::deque<std::array<double, 3>> bboHist;   // [F45] (exchMs, bestBid, bestAsk)
    // [F55] MM_FAIR=bybit
    double fairD = 15, fairSkew = 0, fairRq = 3, fairStaleMs = 1500, fairMaxDev = 100;
    double basis = 0, basisT = 0, basisT0 = 0, lastFair = 0, lastYAge = 0, devSum = 0;
    bool basisInit = false;
    long fairTicks = 0, fairStale = 0, fairDevCnt = 0, fairWarm = 0, fairOk = 0;
    // [v3.3] F60 maker-escape
    int escMk = 0; double escMkBps = 0, escMkSec = 180, escMkHard = 1.5;
    bool escMkOn = false; double escMkSince = 0, escMkTime = 0;
    long escMkStarts = 0, escMkDone = 0, escMkMarket = 0, escMarketCnt = 0;
    // F62 сигналы, F63 Avellaneda–Stoikov
    double fairMicro = 0, fairImbBps = 0, fairSigMax = 0;
    double asGamma = 0, asTau = 60, asVolSec = 300, asMax = 0;
    double asVar = 0, asLastF = 0, asLastT = 0; bool asInit = false;
    double sigSum = 0, asSum = 0, asAbsMax = 0; long sigN = 0;
    SymCtx(const std::string& host, bool nv) : live(host, nv) {}
};

} // namespace spred
