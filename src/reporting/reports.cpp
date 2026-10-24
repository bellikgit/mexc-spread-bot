#include "spred/app/session_steps.hpp"
#include "spred/core/config.hpp"
#include "spred/feeds/bybit_fair.hpp"
#include "spred/parsing/json.hpp"

namespace spred {

// [v2.13] percentile по вектору (копия сортируется)
static double moPct(std::vector<double> v, double p) {
    if (v.empty()) return 0.0;
    std::sort(v.begin(), v.end());
    double idx = p * (double)(v.size() - 1);
    size_t lo = (size_t)idx;
    size_t hi = std::min(lo + 1, v.size() - 1);
    double fr = idx - (double)lo;
    return v[lo] * (1.0 - fr) + v[hi] * fr;
}


void reportSymbol(SymCtx& C, double now, SessionOptions& options) {
    auto& lcfg = options.lcfg;
    auto& rttMs = options.rttMs;

    auto& tr = C.tr; auto& book = C.book; auto& flow = C.flow;
    auto& live = C.live;
    // [v2.13.1] сигнал о расхождении часов (send vs deal)
    {
        double skw = tr.clockSkewMs();
        if (std::fabs(skw) > 2000.0 && !tr.skewAlerted) {
            tr.skewAlerted = true;
            char b[160];
            std::snprintf(b, sizeof b,
                          "[SKEW   ] send-vs-deal clock offset %+.0f ms "
                          "(гейт в deal-домене, сортировка компенсируется)", skw);
            evAdd(C.events, b);
        } else if (std::fabs(skw) < 1000.0 && tr.skewAlerted) {
            tr.skewAlerted = false;
            char b[128];
            std::snprintf(b, sizeof b, "[SKEW   ] recovered: %+.0f ms", skw);
            evAdd(C.events, b);
        }
    }
    tr.rttMs = rttMs;
    double mid = book.ok() ? book.mid() : tr.lastMid;
    if (mid > 0) {
        double invPnl = tr.base0 * (mid - tr.mid0);
        double mmPnl = tr.totalPnl(mid) + tr.setupCost - invPnl;
        double qbuy = (tr.buy.st == QState::LIVE) ? tr.buy.queueAhead : -1.0;
        double qsell = (tr.sell.st == QState::LIVE) ? tr.sell.queueAhead : -1.0;
        lprintf(
            "[%s] mid %.10g | spr %.2f bps | imb %.2f | tfi %+.2f | run %+d | "
            "pos %+.1f | mm %+.2f inv %+.2f | eq %.2f | P&L %+.4f real %+.4f | "
            "fills %d (b%d/s%d p%d sw%d late%d) | vol %.0f | q %.1f/%.1f | "
            "thr %d rpl %d | mo %.0f/%.0f/%.0f | pxmo %.0f/%.0f/%.0f | "
            "txmo %.0f/%.0f/%.0f bps | %s\n",
            nowWallClock().c_str(), mid, book.spreadBps(), book.imbalance(),
            flow.tfi(now), flow.run(now),
            tr.posValue(mid), mmPnl, invPnl,
            tr.equity(mid), tr.totalPnl(mid), tr.realized,
            tr.fills, tr.buyFills, tr.sellFills, tr.partialFills,
            tr.forcedFills, tr.lateFills, tr.volumeTraded,
            qbuy, qsell, tr.throttled, tr.replaces,
            tr.markout(0), tr.markout(1), tr.markout(2),
            tr.proxyMarkout(0), tr.proxyMarkout(1), tr.proxyMarkout(2),
            tr.takerMarkout(0), tr.takerMarkout(1), tr.takerMarkout(2),
            C.symbol.c_str());
        // [v3.0] вторая строка — РЕАЛЬНЫЙ учёт (истина с биржи)
        if (lcfg.live) {
            live.ledger.updateMarkouts(now, mid);
            std::string bOrd = live.s[0].open
                ? fmtDec(live.s[0].px) : std::string("-");
            std::string sOrd = live.s[1].open
                ? fmtDec(live.s[1].px) : std::string("-");
            lprintf(
                "      [LIVE %s] pos %+.10g (%+.1f USDT) | P&L %+.4f "
                "real %+.4f | fills %d (b%d/s%d g%d/%d) | vol %.0f | "
                "ord B %s / S %s | free %.2f USDT, %.10g %s | drift %.3g\n",
                C.symbol.c_str(),
                live.ledger.baseInt, live.ledger.posValue(mid),
                live.ledger.sessionPnl(mid), live.ledger.realized,
                live.ledger.fills, live.ledger.buyFills,
                live.ledger.sellFills, live.ledger.ghostFillsWs,
                live.ledger.ghostFillsRecon, live.ledger.volume,
                bOrd.c_str(), sOrd.c_str(),
                live.ledger.quoteFree, live.ledger.baseFree,
                live.baseAsset.c_str(), live.ledger.driftMaxBase);
        }
    }
    if (!tr.invInitialized)
        lprintf("      [STATUS %s] WAIT-INIT (retry in %.0f s)\n",
                C.symbol.c_str(), std::max(0.0, C.retryInitAt - now));
    else if (tr.halted)
        lprintf("      [STATUS %s] HALTED: loss limit hit\n", C.symbol.c_str());
    flushEvents(C, options);
}

void printSummary(SymCtx& C, FILE* fp, bool sessionHeader, SessionOptions& options) {
    auto& lcfg = options.lcfg;
    const double t0 = options.t0;
    const auto& replayPath = options.replayPath;
    const int sessions = options.sessions;
    const long msgCount = options.msgCount, binFrames = options.binFrames, parseFails = options.parseFails;

    auto& tr = C.tr; auto& cfg = C.tr.cfg; auto& book = C.book;
    auto& flow = C.flow; auto& live = C.live;
    double mid = book.ok() ? book.mid() : tr.lastMid;
    tr.updateMarkouts(nowSecs(), mid);
    double invPnl = tr.base0 * (mid - tr.mid0);
    double mmPnl = tr.totalPnl(mid) + tr.setupCost - invPnl;
    double bb = book.ok() ? book.bestBid() : mid;
    double liqPnl = tr.totalPnl(mid) - std::max(0.0, tr.base) * (mid - bb);
    std::fprintf(fp,
        "\n================================================================\n"
        "  %s\n"
        "================================================================\n"
        "  symbol          : %s\n"
        "  duration        : %.0f s   (ws sessions: %d)\n"
        "  ws messages     : %ld (bin %ld, depth %ld, ticker %ld, deals %ld, parseFails %ld, lateEv %ld)\n"
        "  tick used       : %.10g   rtt: %.0f ms\n"
        "  last mid        : %.10g\n"
        "  ------------------------------\n"
        "  total P&L       : %+.4f USDT (%+.3f%%)\n"
        "    setup cost    : %+.4f  (полспреда + fee при INIT; 0 в RAMP)\n"
        "    inv drift     : %+.4f  (дрейф стартового инвентаря; 0 в RAMP)\n"
        "    MM P&L        : %+.4f  (заработок СТРАТЕГИИ)\n"
        "    liq P&L       : %+.4f  (с учётом ликвидации по bid)\n"
        "  realized (avg-cost): %+.4f    fees: %.4f\n"
        "  fills           : %d (b%d/s%d)  partial %d  toxic %d  LATE %d (gap %d)\n"
        "  cancels/replaces: %d / %d    throttled: %d\n"
        "  traded volume   : %.0f USDT   P&L/объём (mm): %+.2f bps\n"
        "  sell undersized : %.0f s\n"
        "  ------------------------------ markout ------------------------------\n"
        "  mo    5/30/60c: %+.1f / %+.1f / %+.1f bps\n"
        "  pxmo  5/30/60c: %+.1f / %+.1f / %+.1f bps\n"
        "  txmo  5/30/60c: %+.1f / %+.1f / %+.1f bps\n"
        "  ---------------------------------------------------------------------\n"
        "  final TFI/run   : %+.2f / %+d\n"
        "  INIT status     : %s\n"
        "================================================================\n",
        sessionHeader ? "SESSION SUMMARY (spred v3.1)"
                      : "PAIR SUMMARY (spred v3.1)",
        C.symbol.c_str(),
        nowSecs() - t0, sessions, msgCount, binFrames, C.depthFrames,
        C.tickerFrames, C.dealFrames, parseFails, C.lateEvents,
        C.tick, tr.rttMs, mid,
        tr.totalPnl(mid), tr.totalPnl(mid) / tr.initialQuote * 100,
        tr.setupCost, invPnl, mmPnl, liqPnl,
        tr.realized, tr.feesPaid,
        tr.fills, tr.buyFills, tr.sellFills, tr.partialFills,
        tr.toxicFills, tr.lateFills, tr.gapFills,
        tr.cancels, tr.replaces, tr.throttled,
        tr.volumeTraded,
        tr.volumeTraded > 0 ? mmPnl / tr.volumeTraded * 1e4 : 0.0,
        tr.sellSmallSecs,
        tr.markout(0), tr.markout(1), tr.markout(2),
        tr.proxyMarkout(0), tr.proxyMarkout(1), tr.proxyMarkout(2),
        tr.takerMarkout(0), tr.takerMarkout(1), tr.takerMarkout(2),
        flow.tfi(nowSecs()), flow.run(nowSecs()),
        tr.invInitialized
            ? (cfg.initMode == 1 ? "RAMP (passive inventory build)"
                                 : "inventory bought")
            : "NEVER INITIALIZED");
    // [v2.12 Ф10] + [v2.13] распределения в тех же строках
    std::fprintf(fp, "  ------------------------- fills by reason -------------------------\n");
    for (int r = 0; r < FR_N; ++r) {
        if (tr.reasonCnt(r) == 0) continue;
        const std::vector<double>& rv = tr.moR30[r];
        std::fprintf(fp, "  %-8s n=%-4d  mo %+.1f/%+.1f/%+.1f  med %+.1f  p05 %+.1f  "
                "p95 %+.1f  $%+.3f (30c)\n",
                FR_NAMES[r], tr.reasonCnt(r),
                tr.reasonMo(r, 0), tr.reasonMo(r, 1), tr.reasonMo(r, 2),
                moPct(rv, 0.5), moPct(rv, 0.05), moPct(rv, 0.95),
                tr.reasonUsd(r, 1));
    }
    std::fprintf(fp, "  ---------------------- mo 30c distribution -----------------------\n");
    const std::vector<double>& a30 = tr.moH[1];
    if (!a30.empty())
        std::fprintf(fp, "  ALL    n=%-5ld p05 %+.1f | p25 %+.1f | med %+.1f | p75 %+.1f | "
                "p95 %+.1f | worst %+.1f | best %+.1f bps\n",
                (long)a30.size(),
                moPct(a30, 0.05), moPct(a30, 0.25), moPct(a30, 0.5),
                moPct(a30, 0.75), moPct(a30, 0.95),
                moPct(a30, 0.0), moPct(a30, 1.0));
    if (!tr.moBuy30.empty())
        std::fprintf(fp, "  BUY    n=%-5ld med %+.1f  p05 %+.1f  p95 %+.1f bps\n",
                (long)tr.moBuy30.size(), moPct(tr.moBuy30, 0.5),
                moPct(tr.moBuy30, 0.05), moPct(tr.moBuy30, 0.95));
    if (!tr.moSell30.empty())
        std::fprintf(fp, "  SELL   n=%-5ld med %+.1f  p05 %+.1f  p95 %+.1f bps\n",
                (long)tr.moSell30.size(), moPct(tr.moSell30, 0.5),
                moPct(tr.moSell30, 0.05), moPct(tr.moSell30, 0.95));
    if (tr.escCnt > 0)
        std::fprintf(fp, "  escape slippage : n=%d mean %+.1f bps worst %+.1f bps "
                "(realized vs expected VWAP)\n",
                tr.escCnt, tr.escSlipSum / tr.escCnt, tr.escSlipWorst);
    std::fprintf(fp, "  fees            : maker %.4f / taker %.4f / total %.4f USDT\n",
            tr.makerFees, tr.takerFees, tr.feesPaid);
    std::fprintf(fp, "  gap             : certain %d / possible %d / filled %d "
            "(MM_GAP_FILL_MODE=%d)\n",
            tr.gapCertain, tr.gapPossible, tr.gapFills, cfg.gapFillMode);
    std::fprintf(fp, "  clock           : skew send-vs-deal max %.0f ms, last %.0f ms; "
            "quarantined deals %d (MM_QUAR_MS=%.0f)\n",
            tr.skewMaxMs, tr.clockSkewMs(), tr.skewQuar, cfg.quarantineMs);
    std::fprintf(fp, "  risk-add        : init %s | esc-cooldown %.0f s | cutoff "
            "%.0f bps / %.0f s (events %d)\n",
            cfg.initMode == 1 ? "RAMP" : "classic",
            cfg.escapeCooldownS, cfg.cutoffBps, cfg.cutoffSec, tr.cutCnt);
    std::fprintf(fp, "  v3.2.1 F56-58   : loss-basis %s | halt-flat %d (попыток %d) | "
            "trend %.0f bps / %.0f s: bid OFF %.0f s, ask OFF %.0f s\n",
            cfg.lossBasis == 1 ? "mm" : "total", cfg.haltFlat, C.haltFlatTries,
            cfg.trendBps, cfg.trendSec, C.trendBlockBuyS, C.trendBlockSellS);
    std::fprintf(fp, "  print-filter    : mode %d (окно %.0f ms) | принтов %ld, "
            "внутри спреда %ld (%.0f%%), срезано филлов %ld\n",
            cfg.printFilter, cfg.printFilterMs, tr.prAll, tr.prInside,
            tr.prAll > 0 ? 100.0 * tr.prInside / tr.prAll : 0.0, tr.prCutFills);
    std::fprintf(fp, "  v3.3 F60-63     : esc-maker %d (старт %ld, мейкером %ld, MARKET %ld, "
            "ср. %.0f s) | ESCAPE market %ld | imb %.1f bps micro %.2f ср.сдвиг %+.2f | "
            "AS g %.3g tau %.0f: ср.|сдвиг| %.2f макс %.2f bps, sigma %.2f bps/sqrt(s)\n",
            C.escMk, C.escMkStarts, C.escMkDone, C.escMkMarket,
            (C.escMkDone + C.escMkMarket) > 0 ? C.escMkTime / (C.escMkDone + C.escMkMarket) : 0.0,
            C.escMarketCnt, C.fairImbBps, C.fairMicro,
            C.sigN > 0 && C.fairImbBps != 0 ? C.sigSum / C.sigN : 0.0,
            C.asGamma, C.asTau, C.sigN > 0 ? C.asSum / C.sigN : 0.0, C.asAbsMax,
            std::sqrt(std::max(0.0, C.asVar)));
    if (g_fairMode)   // [F55]
        std::fprintf(fp, "  fair (F55)      : bybit | d %.1f bps skew %.1f rq %.1f | basis %+.1f bps "
                "(ср. откл. %.1f) | тиков %ld: ok %ld, stale %ld, dev %ld, прогрев %ld | "
                "bybit msgs %ld reconn %ld\n",
                C.fairD, C.fairSkew, C.fairRq, C.basis * 1e4,
                C.fairTicks > C.fairStale ? C.devSum / (double)(C.fairTicks - C.fairStale) : 0.0,
                C.fairTicks, C.fairOk, C.fairStale, C.fairDevCnt, C.fairWarm,
                g_fairMsgs.load(), g_fairReconn.load());
    if (cfg.printFillP > 0)   // [F49] строка только при p>0 (формат =0/=1 как v3.1.9)
        std::fprintf(fp, "  print-fill-p    : p %.3f | пропущено внутренних филлов %ld "
                "(срезано %ld)\n", cfg.printFillP, tr.prPassFills, tr.prCutFills);
    std::fprintf(fp, "  sim-add         : queue %d | escSkip %d | self %d | replay %s\n",
            cfg.queueMode, cfg.escapeSkipLevels, cfg.selfInBook,
            g_replay ? replayPath.c_str() : "off");
    std::fprintf(fp, "  honesty         : mo — верхняя граница (нет конкурентов за\n"
            "                    очередь); fill-модель — контрфакт FIFO\n"
            "================================================================\n");
    // ================= [v3.0] LIVE-блок: РЕАЛЬНЫЙ учёт
    if (lcfg.live) {
        live.ledger.updateMarkouts(nowSecs(), mid);
        std::string feeOth = live.ledger.feesOther > 0
            ? " + " + fmtDec(live.ledger.feesOther) + " " + live.ledger.feesOtherCur
            : "";
        const LiveSide& sb = live.s[0];
        const LiveSide& ss = live.s[1];
        std::fprintf(fp,
            "  ===================== LIVE (реальная торговля) ====================\n"
            "  LIVE session P&L  : %+.4f USDT  (quoteNet %+.4f + pos %.10g x mid)\n"
            "  LIVE realized     : %+.4f (avg-cost)   fees: %.4f USDT + %.10g %s%s\n"
            "  LIVE позиция      : %+.10g %s (%+.2f USDT), base0 %.10g\n"
            "  LIVE кошелёк      : %.4f %s free (locked %.4f) | %.10g %s free "
            "(locked %.10g)\n"
            "  LIVE fills        : %d (b%d/s%d)   vol %.2f USDT   P&L/объём %+.2f bps\n"
            "  LIVE markout 5/30/60c   : %+.1f / %+.1f / %+.1f bps (vol-w)\n"
            "  LIVE esc-markout 5/30/60: %+.1f / %+.1f / %+.1f bps\n"
            "  LIVE ghost        : ws %d, recon %d, drift max %.6g %s\n"
            "  LIVE fill_cal     : shadow fills %d vs real %d; shadow P&L %+.4f vs "
            "real %+.4f (расхождение = цена модели)\n"
            "  LIVE REST         : places %d/%d (fail %d/%d), cancels %d/%d "
            "(fail %d/%d), resolves %d, reconEv %d, orphanCancels %d, "
            "errCount %d, timeSyncs %d\n"
            "  LIVE REST lat ms  : place avg %.0f max %.0f | cancel avg %.0f "
            "max %.0f\n"
            "  LIVE privWS msgs  : orders %d, deals %d, account %d\n"
            "================================================================\n",
            live.ledger.sessionPnl(mid), live.ledger.quoteNet, live.ledger.baseInt,
            live.ledger.realized, live.ledger.feesQuote, live.ledger.feesBase,
            live.baseAsset.c_str(), feeOth.c_str(),
            live.ledger.baseInt, live.baseAsset.c_str(),
            live.ledger.posValue(mid), live.ledger.base0,
            live.ledger.quoteFree, live.quoteAsset.c_str(),
            live.ledger.quoteLocked,
            live.ledger.baseFree, live.baseAsset.c_str(), live.ledger.baseLocked,
            live.ledger.fills, live.ledger.buyFills, live.ledger.sellFills,
            live.ledger.volume,
            live.ledger.volume > 0
                ? live.ledger.sessionPnl(mid) / live.ledger.volume * 1e4 : 0.0,
            live.ledger.markout(0), live.ledger.markout(1), live.ledger.markout(2),
            live.ledger.markoutE(0), live.ledger.markoutE(1), live.ledger.markoutE(2),
            live.ledger.ghostFillsWs, live.ledger.ghostFillsRecon,
            live.ledger.driftMaxBase, live.baseAsset.c_str(),
            tr.fills, live.ledger.fills,
            tr.totalPnl(mid), live.ledger.sessionPnl(mid),
            sb.places, ss.places, sb.placeFails, ss.placeFails,
            sb.cancels, ss.cancels, sb.cancelFails, ss.cancelFails,
            live.resolves, live.reconEvents, live.orphanCancels,
            live.rc.errCount, live.rc.timeSyncs,
            sb.places + ss.places > 0
                ? (sb.placeLatSum + ss.placeLatSum) / (sb.places + ss.places) : 0.0,
            std::max(sb.placeLatMax, ss.placeLatMax),
            sb.cancels + ss.cancels > 0
                ? (sb.cancelLatSum + ss.cancelLatSum) / (sb.cancels + ss.cancels) : 0.0,
            std::max(sb.cancelLatMax, ss.cancelLatMax),
            live.wsPrivOrders, live.wsPrivDeals, live.wsPrivAcct);
        std::fprintf(fp, "  LIVE sync (F50)   : %s | REAL_SYNC %d, REAL_SWEEP %d "
                "(cooldown/cutoff от реальных филлов)\n"
                "================================================================\n",
                lcfg.realSync ? "on" : "off (MM_LIVE_SYNC=0)",
                tr.realSyncCnt, tr.realSweepCnt);
        std::fprintf(fp, "  LIVE fees (F54)   : myTrades запросов %d (ошибок %d), сделок с "
                "комиссией %d, из них TAKER %d | privWS deals %d | breaker %d\n",
                live.feeLookups, live.feeFail, live.feeTrades, live.feeTaker,
                live.wsPrivDeals, C.breakerCnt);
    }
    std::fflush(fp);
}

} // namespace spred
