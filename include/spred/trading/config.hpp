#pragma once

#include "spred/platform.hpp"

namespace spred {

struct Config {
    double orderSizeQuote = 100;
    double maxPosQuote = 500;
    double makerFeeBps = 0.0;
    double takerFeeBps = 5.0;
    double latencyS = 0.15;
    double maxSpreadBps = 250;
    double requoteFrac = 0.05;
    double minRequoteSec = 0;
    double sweepCooldownS = 15;
    int skewImb = 1;
    int skewTfi = 1;
    double imbSkewFrac = 0.10;
    double tfiSkewFrac = 0.10;
    double invSkewFrac = 0.25;
    double tfiWindowSec = 60.0;
    double tfiThresh = 0.40;
    int runStop = 3;
    double rateRps = 8;
    double maxLoss = 100;
    double staleSecs = 30;
    double escapeSecs = 30;
    int lossBasis = 1;              // [F56] 1 = стоп по mm-P&L, 0 = полный P&L
    int haltFlat = 1;               // [F57] закрыть позицию при HALT
    double trendBps = 0;            // [F58] 0 = выкл
    double trendSec = 900;          // [F58]
    bool allowShort = false;
    double minInitFrac = 0.0;
    double cancelAheadFrac = 0.5;   // [v2.11 FIX 4]
    // [v2.12]
    double reorderMs = 250;
    int gapFillMode = 0;            // 0=песс, 1=50%, 2=опт
    // [v2.13] latency как распределение (множители к базе)
    int latDist = 0;                // 0 = константа (как v2.12), 1 = распределение
    double latP90Mult = 1.6;
    double latP99Mult = 2.5;
    double latMinMult = 0.3;
    // [v2.13.1] карантин «сделок из будущего»
    double quarantineMs = 2000;
    // [v2.14] ФИКС 17/18/19 — все OFF по умолчанию (= поведение v2.13.1)
    int initMode = 0;               // 0=classic (taker pre-buy), 1=ramp (bid-only)
    double escapeCooldownS = 0;     // пауза стороны после escape (0=off)
    double cutoffBps = 0;           // порог пробития уровня для cutoff (0=off)
    double cutoffSec = 60;          // длительность cutoff-паузы
    int printFilter = 1;            // [F45] 0=off 1=no THROUGH из внутр. принтов 2=игнор
    double printFilterMs = 1500;    // [F45] окно истории BBO до принта (биржевое время)
    double printFillP = 0;          // [F49] вероятность, что внутренний принт всё же даёт филл
    // [v2.15 ФИКС 20/21/22] — все OFF по умолчанию (= поведение v2.14)
    int queueMode = 0;              // Ф20: 0=frac(как v2.14) 1=pess 2=opt
    int escapeSkipLevels = 0;       // Ф22: 0..3 — escape-walk с уровня N+1
    int selfInBook = 0;             // Ф21: 1 = сигнальный имбаланс вкл. наш ордер
    double qtyDriftUsdt = 0;        // [v3.1.1] 0 = порог 1000 ед. (как v3.1); >0 = порог в USDT
};

} // namespace spred
