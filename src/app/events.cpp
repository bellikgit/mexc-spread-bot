#include "spred/app/session_steps.hpp"
#include "spred/core/config.hpp"
#include "spred/feeds/bybit_fair.hpp"
#include "spred/parsing/json.hpp"

namespace spred {

void flushEvents(SymCtx& C, SessionOptions& options) {
    (void)options;

    for (auto& e : C.events) lprintf("      [%s] %s\n", C.symbol.c_str(), e.c_str());
    C.events.clear();
}

void applyEvent(SymCtx& C, const PendEv& e, SessionOptions& options) {
    auto& lcfg = options.lcfg;
    const bool logDeals = options.logDeals;

    auto& tr = C.tr; auto& cfg = C.tr.cfg; auto& book = C.book;
    auto& flow = C.flow; auto& tick = C.tick; auto& events = C.events;
    // [v2.16 P0-1] монотонность: позднее событие НЕ откатывает книгу
    if (e.exchMs < C.lastAppliedExchMs) { ++C.lateEvents; return; }
    if (e.exchMs > C.lastAppliedExchMs) C.lastAppliedExchMs = e.exchMs;
    double now = nowSecs();
    if (e.kind == 0) {
        book.bids = e.bids;
        book.asks = e.asks;
        std::sort(book.bids.begin(), book.bids.end(),
                  [](const Level& a, const Level& b) { return a.price > b.price; });
        std::sort(book.asks.begin(), book.asks.end(),
                  [](const Level& a, const Level& b) { return a.price < b.price; });
        book.lastRecv = now;
        if (!book.bids.empty() && !book.asks.empty()) C.depthReady = true;
        tr.onDepth(tr.buy, book, tick);
        tr.onDepth(tr.sell, book, tick);
        if (book.ok()) C.bboHist.push_back({e.exchMs, book.bestBid(), book.bestAsk()});  // [F45]
        if (C.depthSeen < 30) {
            ++C.depthSeen;
            for (const auto& l : book.bids) C.priceSamples.push_back(l.price);
            for (const auto& l : book.asks) C.priceSamples.push_back(l.price);
        }
    } else if (e.kind == 1) {
        if (!book.ok()) {
            book.bids.assign(1, Level{e.bidP, e.bidQ});
            book.asks.assign(1, Level{e.askP, e.askQ});
        } else {
            double ep = tick * 0.49;
            if (e.bidP > 0) {
                book.bids.erase(
                    std::remove_if(book.bids.begin(), book.bids.end(),
                        [&](const Level& l) { return l.price >= e.bidP - ep; }),
                    book.bids.end());
                book.bids.insert(book.bids.begin(), Level{e.bidP, e.bidQ});
            }
            if (e.askP > 0) {
                book.asks.erase(
                    std::remove_if(book.asks.begin(), book.asks.end(),
                        [&](const Level& l) { return l.price <= e.askP + ep; }),
                    book.asks.end());
                book.asks.insert(book.asks.begin(), Level{e.askP, e.askQ});
            }
        }
        book.lastRecv = now;
        if (book.ok()) C.bboHist.push_back({e.exchMs, book.bestBid(), book.bestAsk()});  // [F45]
    } else {
        const MexcPush::Deal& d = e.deal;
        // [v2.13.1] карантин «сделок из будущего» (страховка гейта Ф16)
        bool quar = false;
        if (tr.dealOffInit && d.timeMs > 0) {
            double dlag = nowMsSteady() - tr.exchOffDealMs - (double)d.timeMs;
            quar = (dlag < -cfg.quarantineMs);
        }
        if (quar) ++tr.skewQuar;
        flow.onDeal(d, now, tr.dealOffInit, tr.exchOffDealMs);
        if (logDeals && book.ok()) {
            double lag = (d.timeMs > 0 && tr.dealOffInit)
                             ? (nowMsSteady() - tr.exchOffDealMs - (double)d.timeMs)
                             : -1.0;
            char ourB[32], ourS[32];
            if (tr.buy.st == QState::LIVE || tr.buy.st == QState::WAIT_CANCEL)
                std::snprintf(ourB, sizeof ourB, "%.10g", tr.buy.cur.price);
            else std::snprintf(ourB, sizeof ourB, "-");
            if (tr.sell.st == QState::LIVE || tr.sell.st == QState::WAIT_CANCEL)
                std::snprintf(ourS, sizeof ourS, "%.10g", tr.sell.cur.price);
            else std::snprintf(ourS, sizeof ourS, "-");
            lprintf("      [DEAL]%s %-10s %s %.10g @ %.10g | bbo %.10g/%.10g | "
                    "our %s/%s | lag %.0f ms\n",
                    quar ? "!" : " ", C.symbol.c_str(),
                    d.side == 1 ? "BUY " : d.side == 2 ? "SELL" : "?  ",
                    d.qty, d.price, book.bestBid(), book.bestAsk(),
                    ourB, ourS, lag);
        }
        // [F45] принт строго внутри видимого спреда за окно до принта?
        {
            double wMs = cfg.printFilterMs;
            while (C.bboHist.size() > 2 && C.bboHist.front()[0] < e.exchMs - wMs - 5000)
                C.bboHist.pop_front();
            double hiBid = book.ok() ? book.bestBid() : 0;
            double loAsk = book.ok() ? book.bestAsk() : 0;
            for (const auto& h : C.bboHist) {
                if (h[0] < e.exchMs - wMs || h[0] > e.exchMs) continue;
                if (h[1] > hiBid) hiBid = h[1];
                if (loAsk <= 0 || (h[2] > 0 && h[2] < loAsk)) loAsk = h[2];
            }
            double ep = tick * 0.49;
            tr.curPrintInside = hiBid > 0 && loAsk > 0 &&
                                d.price > hiBid + ep && d.price < loAsk - ep;
            ++tr.prAll;
            if (tr.curPrintInside) ++tr.prInside;
        }
        if (lcfg.live && lcfg.realSync) {   // [F50] для REAL_SWEEP
            tr.recentPr.push_back({now, d.price, (double)d.side});
            while (!tr.recentPr.empty() && now - tr.recentPr.front()[0] > 6.0)
                tr.recentPr.pop_front();
        }
        if (!quar) {
            tr.onGhost(tr.ghostBuy, d, book, now, events);
            tr.onGhost(tr.ghostSell, d, book, now, events);
            tr.onPrint(tr.buy, Order::BUY, d, book, tick, now, events);
            tr.onPrint(tr.sell, Order::SELL, d, book, tick, now, events);
        }
    }
}

void flushEvq(SymCtx& C, SessionOptions& options) {

    if (C.evq.empty()) return;
    std::stable_sort(C.evq.begin(), C.evq.end(),
                     [](const PendEv& a, const PendEv& b) {
                         return a.exchMs < b.exchMs;
                     });
    double nowFlushMs = nowMsSteady();
    while (!C.evq.empty()) {
        const PendEv& e = C.evq.front();
        bool aged = (nowFlushMs - e.recvMs) >= C.tr.cfg.reorderMs;
        bool watermark = e.exchMs <= C.evqMaxExchMs - C.tr.cfg.reorderMs;
        if (!aged && !watermark) break;
        applyEvent(C, e, options);
        C.evq.pop_front();
    }
}

} // namespace spred
