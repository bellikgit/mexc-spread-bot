#include "spred/app/session_steps.hpp"
#include "spred/core/config.hpp"
#include "spred/feeds/bybit_fair.hpp"
#include "spred/parsing/json.hpp"

namespace spred {

void tickSymbol(SymCtx& C, double now, SessionOptions& options) {
    auto& lcfg = options.lcfg;
    const double fairBasisSec = options.fairBasisSec, fairWarmSec = options.fairWarmSec;
    auto& connAt = options.connAt;
    auto& lost = options.lost;

    auto& tr = C.tr; auto& cfg = C.tr.cfg;
    auto& tick = C.tick; auto& si = C.si; auto& flow = C.flow;
    auto& events = C.events; auto& live = C.live;
    // [F52] live: наши реальные ордера стоят в публичной книге. Котировки
    // считались от bestBid/bestAsk, которыми был НАШ же ордер -> каждый
    // реквот на шаг ближе к середине (погоня за собой, сжатие спреда до
    // встречной стороны). Теперь стратегия в live видит книгу без наших
    // ордеров (живые + снятые/исполненные за последние MM_SELF_EXCL_SEC).
    if (lcfg.live && lcfg.selfExcl) {
        for (int i = 0; i < 2; ++i) {
            const LiveSide& ls = live.s[i];
            std::string cid = ls.open ? ls.cid : std::string();
            if (cid != C.ownCid[i]) {
                if (!C.ownCid[i].empty() && C.ownRem[i] > 0)
                    C.ownRecent.push_back({i, C.ownPx[i], C.ownRem[i],
                                           now + lcfg.selfExclSec});
                C.ownCid[i] = cid;
            }
            if (ls.open) {
                C.ownPx[i] = ls.px;
                C.ownRem[i] = std::max(0.0, ls.qty - ls.dealtQty);
            } else {
                C.ownRem[i] = 0;
            }
        }
        while (!C.ownRecent.empty() && C.ownRecent.front().until < now)
            C.ownRecent.pop_front();
        C.xbook = C.book;
        double eps = C.tick * 0.49;
        auto sub = [&](int side, double px, double q) {
            subOwnLevel(C.xbook, side, px, q, eps);
        };
        for (int i = 0; i < 2; ++i)
            if (live.s[i].open && C.ownRem[i] > 0) sub(i, C.ownPx[i], C.ownRem[i]);
        for (const auto& o : C.ownRecent) sub(o.side, o.px, o.qty);
    }
    const BookState& book = (lcfg.live && lcfg.selfExcl) ? C.xbook : C.book;

    // [v2.12 ФИКС 8] gap: CERTAIN / POSSIBLE
    if (tr.gapPending && book.ok()) {
        tr.gapPending = false;
        double midN = book.mid();
        for (auto& g : tr.gapCheck) {
            bool isBuy = (g.side == (int)Order::BUY);
            bool crossedNow = isBuy ? (midN <= g.price) : (midN >= g.price);
            bool crossedBefore = C.gapMid0Valid &&
                (isBuy ? (C.gapMid0 <= g.price) : (C.gapMid0 >= g.price));
            double q = 0;
            if (crossedNow || crossedBefore) {
                ++tr.gapCertain;
                q = g.qty;
            } else {
                ++tr.gapPossible;
                if (cfg.gapFillMode == 2) q = g.qty;
                else if (cfg.gapFillMode == 1) q = tr.roundQty(g.qty * 0.5);
            }
            if (q > 0) {
                tr.applyTrade(isBuy ? q : -q, g.price);
                ++tr.fills;
                if (isBuy) ++tr.buyFills; else ++tr.sellFills;
                ++tr.forcedFills; ++tr.lateFills; ++tr.gapFills;
                ++tr.rCnt[FR_GAP];
                double proxyPx = isBuy ? book.bestBid() : book.bestAsk();
                if (proxyPx <= 0) proxyPx = g.price;
                double takerPx = isBuy ? book.bestAsk() : book.bestBid();
                if (takerPx <= 0) takerPx = g.price;
                // [v2.16 P0-4] GAP — контрфакт после реконнекта: якорь = now
                tr.mo.push_back({now, isBuy ? 1 : -1, g.price, proxyPx,
                                 takerPx, q, 0, FR_GAP});
                char buf[224];
                std::snprintf(buf, sizeof buf,
                              "[FILL %s GAP%s ] %.10g @ %.10g (%s)",
                              isBuy ? "BUY " : "SELL",
                              (crossedNow || crossedBefore) ? "  " : "? ",
                              q, g.price,
                              (crossedNow || crossedBefore) ? "certain"
                                                            : "possible");
                evAdd(events, buf);
            }
        }
        tr.gapCheck.clear();
    }

    // INIT
    if (!tr.invInitialized && !cfg.allowShort && !C.depthReady && book.ok() &&
        !C.warnedNoDepth) {
        C.warnedNoDepth = true;
        evAdd(events, std::string("[INIT] waiting for depth snapshot "
                                  "(book seeded from bookTicker only)"));
    }
    if (!tr.invInitialized && book.ok() && (C.depthReady || cfg.allowShort) &&
        now >= C.retryInitAt) {
        double sprLive = book.bestAsk() - book.bestBid();
        bool tickBad = !(tick > 0) || tick > sprLive + 1e-12;
        if (tickBad && C.priceSamples.size() >= 60) {
            std::sort(C.priceSamples.begin(), C.priceSamples.end());
            double best = 0;
            for (size_t i = 1; i < C.priceSamples.size(); ++i) {
                double d = C.priceSamples[i] - C.priceSamples[i - 1];
                if (d > 0 && (best == 0 || d < best)) best = d;
            }
            if (best > 0 && best <= sprLive + 1e-12) {
                tick = best;
                tickBad = false;
                char tb[96];
                std::snprintf(tb, sizeof tb,
                              "[INIT] tick derived from live book: %.3g", tick);
                evAdd(events, tb);
            }
            C.priceSamples.clear();
            C.depthSeen = 0;
        }
        if (tickBad) {
            C.retryInitAt = now + 30.0;
            char buf[224];
            std::snprintf(buf, sizeof buf,
                          "[INIT] DEFER 30s: tick %.10g vs live spread %.10g",
                          tick, sprLive);
            evAdd(events, buf);
        } else if (!cfg.allowShort) {
            // [v2.14 ФИКС 17] RAMP: вход пассивно — только bid, sell из
            // филлов. setup cost = 0, весь P&L — mm-компонента.
            if (cfg.initMode == 1) {
                tr.invInitialized = true;
                tr.base = 0;
                tr.base0 = 0;
                tr.mid0 = book.mid();
                tr.avgEntry = 0;
                tr.setupCost = 0;
                evAdd(events, std::string(
                    "[INIT] RAMP mode: bid-only до первого филла "
                    "(setup cost 0, инвентарь пассивно)"));
            } else {
            double ask = book.bestAsk();
            double mid0 = book.mid();
            double budget = cfg.maxPosQuote;
            double lotCost = si.qtyStep * ask;
            double minBuy = std::max(si.minNotional, lotCost);
            double minInit = cfg.minInitFrac * cfg.orderSizeQuote;
            double minViable = std::max(minBuy, minInit);
            double fq = 0, avg = 0;
            bool budgetTooSmall = minViable > budget;
            if (!budgetTooSmall) {
                fq = std::floor(walkBudgetBuy(book, budget) / si.qtyStep + 1e-9)
                     * si.qtyStep;
                double filled = 0;
                avg = fq > 0 ? walkBook(book, true, fq, filled) : 0.0;
            }
            if (budgetTooSmall) {
                C.retryInitAt = now + 60.0;
                char buf[352];
                std::snprintf(buf, sizeof buf,
                              "[INIT] DEFER 60s: budget %.2f USDT < min viable buy "
                              "%.4f USDT (lot %.10g @ %.10g = %.4f, minNotional %.4g, "
                              "minInit %.4f) — raise MM_MAX_POS%s",
                              budget, minViable, si.qtyStep, ask, lotCost,
                              si.minNotional, minInit,
                              minInit > minBuy ? " or lower MM_MIN_INIT_FRAC" : "");
                evAdd(events, buf);
            } else if (fq <= 0 || avg <= 0 || fq * avg < minViable) {
                C.retryInitAt = now + 60.0;
                char buf[320];
                std::snprintf(buf, sizeof buf,
                              "[INIT] DEFER 60s: ask side %.4f USDT < min viable "
                              "%.4f USDT (min buy %.4f, minInit %.4f, budget %.2f)",
                              fq * avg, minViable, minBuy, minInit, budget);
                evAdd(events, buf);
            } else {
                double cost = fq * avg;
                double fee = cost * cfg.takerFeeBps / 1e4;
                if (cost + fee > tr.quote) {
                    C.retryInitAt = now + 60.0;
                    evAdd(events, std::string(
                        "[INIT] DEFER 60s: cost+fee exceeds quote balance"));
                } else {
                    tr.invInitialized = true;
                    tr.base = fq;
                    tr.quote -= cost + fee;
                    tr.feesPaid += fee;
                    tr.takerFees += fee;
                    tr.base0 = fq;
                    tr.mid0 = mid0;
                    tr.avgEntry = mid0;
                    tr.setupCost = cost + fee - fq * mid0;
                    char buf[288];
                    std::snprintf(buf, sizeof buf,
                                  "[INIT] spot inventory: bought %.10g %s @ avg %.10g "
                                  "= %.2f USDT (fee %.4f, setup cost %.4f)",
                                  fq, C.symbol.c_str(), avg, cost, fee, tr.setupCost);
                    evAdd(events, buf);
                    if (si.qtyStep * avg > cfg.orderSizeQuote) {
                        char w[224];
                        std::snprintf(w, sizeof w,
                            "[INIT] WARNING: 1 qtyStep = %.2f USDT > MM_ORDER_USDT "
                            "(%.0f) — quotes suppressed",
                            si.qtyStep * avg, cfg.orderSizeQuote);
                        evAdd(events, w);
                    }
                    if (tr.base * mid0 < cfg.orderSizeQuote) {
                        char w[256];
                        std::snprintf(w, sizeof w,
                            "[INIT] NOTE: inventory %.10g < order size "
                            "(%.0f USDT) — sell-side sized to available base",
                            tr.base, cfg.orderSizeQuote);
                        evAdd(events, w);
                    }
                }
            }
            }
        } else {
            tr.invInitialized = true;
        }
    }

    if (book.ok()) {
        tr.advance(tr.buy, book, tick, now);
        tr.advance(tr.sell, book, tick, now);
    }

    // ---- strategy tick
    if (book.ok() && tr.invInitialized) {
        double mid = book.mid();
        tr.lastMid = mid;

        if (!cfg.allowShort && C.lastStratT > 0 && now > C.lastStratT &&
            now - C.lastStratT < 5.0 && tr.base * mid < cfg.orderSizeQuote)
            tr.sellSmallSecs += now - C.lastStratT;
        C.lastStratT = now;

        // [v2.11] исполнение отложенного escape (только paper-ветка;
        // в live излишек РЕАЛЬНОЙ позиции сносится MARKET-ордером ниже)
        if (!lcfg.live && C.pendEsc.active && now >= C.pendEsc.fireAt) {
            C.pendEsc.active = false;
            double excess = std::fabs(tr.posValue(mid)) -
                            (C.pendEsc.flat ? 0.0 : cfg.maxPosQuote * 0.75);   // [F57]
            C.pendEsc.flat = false;
            if (excess > 0) {
                double over = std::min(
                    C.pendEsc.qty,
                    std::floor((excess / mid) / si.qtyStep + 1e-9) * si.qtyStep);
                bool isBuy = C.pendEsc.isBuy;
                if (isBuy) {
                    double maxAfford = tr.quote * 0.98 / mid;
                    if (over > maxAfford)
                        over = std::floor(maxAfford / si.qtyStep + 1e-9) * si.qtyStep;
                } else {
                    if (over > tr.base)
                        over = std::floor(tr.base / si.qtyStep + 1e-9) * si.qtyStep;
                }
                if (over > 0) {
                    // [v2.15 ФИКС 22] MM_ESCAPE_SKIP_LEVELS: стресс-escape
                    double fq = 0;
                    double avg = walkBookSkip(book, isBuy, over, fq,
                                              cfg.escapeSkipLevels);
                    if (fq > 0) {
                        tr.applyTrade(isBuy ? fq : -fq, avg);
                        double fee = fq * avg * (cfg.takerFeeBps - cfg.makerFeeBps) / 1e4;
                        if (fee > 0) { tr.quote -= fee; tr.feesPaid += fee;
                                       tr.takerFees += fee; }
                        ++tr.rCnt[FR_ESCAPE];
                        // [v2.16 P0-4] escape — решение симулятора: якорь = now
                        tr.mo.push_back({now, isBuy ? 1 : -1, avg, avg, avg,
                                         fq, 0, FR_ESCAPE});
                        double slipBps = 0.0;
                        if (C.pendEsc.expAvg > 0) {
                            double slip = isBuy ? (C.pendEsc.expAvg - avg)
                                                : (avg - C.pendEsc.expAvg);
                            slipBps = slip / C.pendEsc.expAvg * 1e4;
                            ++tr.escCnt;
                            tr.escSlipSum += slipBps;
                            if (tr.escCnt == 1 || slipBps < tr.escSlipWorst)
                                tr.escSlipWorst = slipBps;
                        }
                        char buf[224];
                        std::snprintf(buf, sizeof buf,
                                      "[ESCAPE ] taker %s %.10g @ avg %.10g "
                                      "(через %.0f ms, slip %+.1f bps)",
                                      isBuy ? "BUY " : "SELL", fq, avg,
                                      cfg.latencyS * 1000, slipBps);
                        evAdd(events, buf);
                        // [v2.14 ФИКС 18] cooldown после escape
                        if (cfg.escapeCooldownS > 0) {
                            if (isBuy) tr.escPauseSellUntil = now + cfg.escapeCooldownS;
                            else       tr.escPauseBuyUntil  = now + cfg.escapeCooldownS;
                        }
                    }
                }
            } else {
                evAdd(events, std::string(
                    "[ESCAPE ] skipped: позиция вернулась в band за время полёта"));
            }
        }

        double dataAge = book.lastRecv > 0 ? (now - book.lastRecv)
                                           : (now - connAt);
        if (dataAge > cfg.staleSecs) {
            if (tr.buy.st != QState::NONE || tr.sell.st != QState::NONE) {
                evAdd(events, std::string("[risk] no data > ") +
                       std::to_string((int)dataAge) + "s -> pulling quotes");
            }
            tr.requestCancel(tr.buy, now, events);
            tr.requestCancel(tr.sell, now, events);
            if (dataAge > cfg.staleSecs * 2.0) {
                // [v3.1] коннект общий — стейл любой пары реконнектит всех
                if (!g_replay) {
                    lprintf("[net] %s no data > %.0f s -> reconnecting\n",
                            C.symbol.c_str(), dataAge);
                    lost = true;
                }
            }
        } else if (lcfg.live && now < C.breakerUntil) {
            // [F53] breaker: слишком частые реальные филлы -> пара на паузе
            tr.requestCancel(tr.buy, now, events);
            tr.requestCancel(tr.sell, now, events);
        } else if (!tr.halted &&
                   (lcfg.live ? live.ledger.sessionPnl(mid)
                              : (cfg.lossBasis == 1                       // [F56]
                                 ? tr.totalPnl(mid) + tr.setupCost - tr.base0 * (mid - tr.mid0)
                                 : tr.totalPnl(mid))) < -cfg.maxLoss) {
            tr.halted = true;
            char buf[192];
            std::snprintf(buf, sizeof buf,
                          "[risk] loss %.2f exceeded limit %.2f -> HALT%s",
                          lcfg.live ? live.ledger.sessionPnl(mid)
                                    : (cfg.lossBasis == 1
                                       ? tr.totalPnl(mid) + tr.setupCost - tr.base0 * (mid - tr.mid0)
                                       : tr.totalPnl(mid)),
                          cfg.maxLoss, lcfg.live ? " (REAL)" : "");
            evAdd(events, buf);
            tr.requestCancel(tr.buy, now, events);
            tr.requestCancel(tr.sell, now, events);
            if (lcfg.live) live.cancelAll(events);   // риск-офф немедленно
        } else if (tr.halted) {
            tr.requestCancel(tr.buy, now, events);
            tr.requestCancel(tr.sell, now, events);
            // [F57] закрыть позицию стратегии (первая попытка сразу после HALT)
            double posH = lcfg.live ? live.ledger.posValue(mid) : tr.posValue(mid);
            if (cfg.haltFlat && C.haltFlatTries < 5 &&
                std::fabs(posH) >= std::max(si.minNotional, 1.0) &&
                now - C.haltFlatAt >= (C.haltFlatTries == 0 ? 0.5 : std::max(5.0, cfg.escapeSecs)) &&
                (lcfg.live ? live.ledger.acctInit : !C.pendEsc.active)) {
                ++C.haltFlatTries; C.haltFlatAt = now;
                bool isBuy = posH < 0;
                double qty = std::floor((std::fabs(posH) / mid) / si.qtyStep + 1e-9) * si.qtyStep;
                char buf[192];
                if (lcfg.live) {
                    if (isBuy) {
                        double maxAfford = std::max(0.0, live.ledger.quoteFree * C.quoteShare
                                                         - lcfg.usdtReserve) / mid;
                        if (qty > maxAfford)
                            qty = std::floor(maxAfford / si.qtyStep + 1e-9) * si.qtyStep;
                        if (qty > 0 && qty * mid >= si.minNotional) {
                            double q = qty * mid * (1.0 + cfg.takerFeeBps / 1e4);
                            std::snprintf(buf, sizeof buf, "[HALT-FLAT] LIVE #%d BUY market %.6g USDT",
                                          C.haltFlatTries, q);
                            evAdd(events, buf);
                            live.escapeMarket(true, 0, q, now, events);
                        }
                    } else {
                        if (qty > live.ledger.baseFree)
                            qty = std::floor(live.ledger.baseFree / si.qtyStep + 1e-9) * si.qtyStep;
                        if (qty > 0 && qty * mid >= si.minNotional) {
                            std::snprintf(buf, sizeof buf, "[HALT-FLAT] LIVE #%d SELL market %.10g",
                                          C.haltFlatTries, qty);
                            evAdd(events, buf);
                            live.escapeMarket(false, qty, 0, now, events);
                        }
                    }
                } else {
                    if (isBuy) {
                        double maxAfford = tr.quote * 0.98 / mid;
                        if (qty > maxAfford)
                            qty = std::floor(maxAfford / si.qtyStep + 1e-9) * si.qtyStep;
                    } else if (qty > tr.base) {
                        qty = std::floor(tr.base / si.qtyStep + 1e-9) * si.qtyStep;
                    }
                    if (qty > 0) {
                        double fqE = 0, avgE = walkBook(book, isBuy, qty, fqE);
                        C.pendEsc.active = true; C.pendEsc.flat = true;
                        C.pendEsc.isBuy = isBuy; C.pendEsc.qty = qty;
                        C.pendEsc.fireAt = now + cfg.latencyS;
                        C.pendEsc.expAvg = (fqE > 0) ? avgE : 0.0;
                        std::snprintf(buf, sizeof buf, "[HALT-FLAT] #%d %s %.10g (pos %+.2f USDT)",
                                      C.haltFlatTries, isBuy ? "BUY " : "SELL", qty, posH);
                        evAdd(events, buf);
                    }
                }
            }
        } else {
            double spr = book.bestAsk() - book.bestBid();
            double sprTicks = spr / tick;
            double imb = book.imbalance();
            // [v2.15 ФИКС 21] MM_SELF_IN_BOOK (только сигналы)
            if (cfg.selfInBook) {
                double bb2 = 0, ba2 = 0;
                for (size_t i = 0; i < book.bids.size() && i < 10; ++i) bb2 += book.bids[i].qty;
                for (size_t i = 0; i < book.asks.size() && i < 10; ++i) ba2 += book.asks[i].qty;
                if (tr.buy.st == QState::LIVE || tr.buy.st == QState::WAIT_CANCEL)
                    bb2 += tr.buy.cur.qty - tr.buy.cur.filled;
                if (tr.sell.st == QState::LIVE || tr.sell.st == QState::WAIT_CANCEL)
                    ba2 += tr.sell.cur.qty - tr.sell.cur.filled;
                if (bb2 + ba2 > 0) imb = bb2 / (bb2 + ba2);
            }
            double posV = lcfg.live ? live.ledger.posValue(mid)
                                    : tr.posValue(mid);   // гейты — РЕАЛЬНЫЕ
            double tfi = flow.tfi(now);
            int run = flow.run(now);

            if (cfg.runStop > 0 && run <= -cfg.runStop) tr.pauseBuy = true;
            if (cfg.runStop > 0 && run >= +cfg.runStop) tr.pauseSell = true;
            if (tr.pauseBuy && run > -cfg.runStop && tfi > -cfg.tfiThresh)
                tr.pauseBuy = false;
            if (tr.pauseSell && run < +cfg.runStop && tfi < +cfg.tfiThresh)
                tr.pauseSell = false;

            tr.updateMarkouts(now, mid);

            // [F60] maker-escape: состояние
            bool escMarketOK = true;
            if (C.escMk) {
                double ap = std::fabs(posV);
                if (!C.escMkOn && ap > cfg.maxPosQuote) {
                    C.escMkOn = true; C.escMkSince = now; ++C.escMkStarts;
                    char eb[160];
                    std::snprintf(eb, sizeof eb, "[ESC-MAKER] старт: pos %+.2f > %.0f -> %s по fair%+.1f bps",
                                  posV, cfg.maxPosQuote, posV > 0 ? "SELL" : "BUY ", C.escMkBps);
                    evAdd(events, eb);
                } else if (C.escMkOn && ap <= cfg.maxPosQuote * 0.75) {
                    C.escMkOn = false; ++C.escMkDone; C.escMkTime += now - C.escMkSince;
                    char eb[128];
                    std::snprintf(eb, sizeof eb, "[ESC-MAKER] готово за %.0f s: pos %+.2f",
                                  now - C.escMkSince, posV);
                    evAdd(events, eb);
                }
                escMarketOK = C.escMkOn && (now - C.escMkSince >= C.escMkSec ||
                                            ap > C.escMkHard * cfg.maxPosQuote);
            }
            // ESCAPE — решение; [v3.0] live: реальный MARKET-ордер сразу
            if (cfg.escapeSecs > 0 && std::fabs(posV) > cfg.maxPosQuote && escMarketOK &&
                now - C.lastFlatten >= cfg.escapeSecs &&
                (lcfg.live ? live.ledger.acctInit : !C.pendEsc.active)) {
                C.lastFlatten = now;
                ++C.escMarketCnt;
                if (C.escMk) {   // [F60] мейкер не успел -> MARKET, новое окно
                    ++C.escMkMarket; C.escMkTime += now - C.escMkSince; C.escMkSince = now;
                    evAdd(events, "[ESC-MAKER] не вышли мейкером -> MARKET");
                }
                double excess = std::fabs(posV) - cfg.maxPosQuote * 0.75;
                double over = std::floor((excess / mid) / si.qtyStep + 1e-9) * si.qtyStep;
                bool isBuy = posV < 0;
                if (lcfg.live) {
                    if (isBuy) {
                        double maxAfford =
                            std::max(0.0, live.ledger.quoteFree * C.quoteShare
                                          - lcfg.usdtReserve) / mid;
                        if (over > maxAfford)
                            over = std::floor(maxAfford / si.qtyStep + 1e-9) * si.qtyStep;
                        if (over > 0 && over * mid >= si.minNotional) {
                            // [fix 3.1-F6] +taker сверху: иначе bought
                            // base < over — хвост позиции после escape
                            double escQ = over * mid * (1.0 + cfg.takerFeeBps / 1e4);
                            evAdd(events, std::string("[ESCAPE ] LIVE решение: BUY market ") +
                                  fmtDec(escQ) + " USDT");
                            live.escapeMarket(true, 0, escQ, now, events);
                        }
                    } else {
                        if (over > live.ledger.baseFree)
                            over = std::floor(live.ledger.baseFree / si.qtyStep + 1e-9) * si.qtyStep;
                        if (over > 0 && over * mid >= si.minNotional) {
                            evAdd(events, std::string("[ESCAPE ] LIVE решение: SELL market ") +
                                  fmtDec(over));
                            live.escapeMarket(false, over, 0, now, events);
                        }
                    }
                    if (cfg.escapeCooldownS > 0) {
                        if (isBuy) tr.escPauseSellUntil = now + cfg.escapeCooldownS;
                        else       tr.escPauseBuyUntil  = now + cfg.escapeCooldownS;
                    }
                } else {
                if (isBuy) {
                    double maxAfford = tr.quote * 0.98 / mid;
                    if (over > maxAfford)
                        over = std::floor(maxAfford / si.qtyStep + 1e-9) * si.qtyStep;
                } else {
                    if (over > tr.base)
                        over = std::floor(tr.base / si.qtyStep + 1e-9) * si.qtyStep;
                }
                if (over > 0) {
                    double fqE = 0, avgE = walkBook(book, isBuy, over, fqE);
                    C.pendEsc.active = true;
                    C.pendEsc.isBuy = isBuy;
                    C.pendEsc.qty = over;
                    C.pendEsc.fireAt = now + cfg.latencyS;
                    C.pendEsc.expAvg = (fqE > 0) ? avgE : 0.0;
                    char buf[192];
                    std::snprintf(buf, sizeof buf,
                                  "[ESCAPE ] решение: %s %.10g (исполнение через %.0f ms)",
                                  isBuy ? "BUY " : "SELL", over, cfg.latencyS * 1000);
                    evAdd(events, buf);
                }
                }   // end else (paper-ветка escape)
            }

            double bidPx, askPx;
            bool fairBlock = false;     // [F55]
            double fairPx = 0;
            if (g_fairMode) {
                ++C.fairTicks;
                FairQ fq;
                double tms = nowMsSteady();
                bool have = fairGet(C.symbol, fq);
                double age = have ? tms - fq.tMs : 1e9;
                C.lastYAge = age;
                if (!have || age > C.fairStaleMs || fq.bid <= 0 || fq.ask <= fq.bid) {
                    fairBlock = true; ++C.fairStale;
                } else {
                    double ymid = 0.5 * (fq.bid + fq.ask);
                    double inst = std::log(mid / ymid);
                    if (!C.basisInit) {
                        C.basisInit = true; C.basis = inst; C.basisT = now; C.basisT0 = now;
                    } else {
                        double dt = std::max(0.0, now - C.basisT);
                        double al = 1.0 - std::exp(-dt / std::max(1.0, fairBasisSec));
                        C.basis += al * (inst - C.basis);
                        C.basisT = now;
                    }
                    double dev = std::fabs(inst - C.basis) * 1e4;
                    C.devSum += dev;
                    fairPx = ymid * std::exp(C.basis);
                    if (C.fairMicro != 0 && fq.bq > 0 && fq.aq > 0) {   // [F62] микроцена Bybit
                        double micro = (fq.bid * fq.aq + fq.ask * fq.bq) / (fq.bq + fq.aq);
                        fairPx = (ymid + C.fairMicro * (micro - ymid)) * std::exp(C.basis);
                    }
                    C.lastFair = fairPx;
                    if (C.asGamma > 0) {   // [F63] EWMA дисперсии fair, bps^2/c
                        if (!C.asInit) { C.asInit = true; C.asLastF = fairPx; C.asLastT = now; }
                        else if (now - C.asLastT >= 1.0) {
                            double dt = now - C.asLastT;
                            double r = std::log(fairPx / C.asLastF) * 1e4;
                            double v = std::min(r * r / dt, 1e4);
                            double al = 1.0 - std::exp(-dt / std::max(1.0, C.asVolSec));
                            C.asVar = C.asVar <= 0 ? v : C.asVar + al * (v - C.asVar);
                            C.asLastF = fairPx; C.asLastT = now;
                        }
                    }
                    if (now - C.basisT0 < fairWarmSec) { fairBlock = true; ++C.fairWarm; }
                    else if (dev > C.fairMaxDev) { fairBlock = true; ++C.fairDevCnt; }
                    else ++C.fairOk;
                }
            }
            if (g_fairMode) {
                double f0 = fairPx > 0 ? fairPx : mid;
                double sk = -(posV / cfg.maxPosQuote) * C.fairSkew;
                if (C.fairImbBps != 0 && fairPx > 0) {   // [F62] дисбаланс MEXC
                    double sg = (imb - 0.5) * 2.0 * C.fairImbBps;
                    sg = std::max(-C.fairSigMax, std::min(C.fairSigMax, sg));
                    sk += sg; C.sigSum += sg; ++C.sigN;
                }
                if (C.asGamma > 0 && fairPx > 0) {     // [F63] Avellaneda–Stoikov
                    double qf = std::max(-1.5, std::min(1.5, posV / cfg.maxPosQuote));
                    double as = -qf * C.asGamma * C.asVar * C.asTau;
                    as = std::max(-C.asMax, std::min(C.asMax, as));
                    sk += as; C.asSum += std::fabs(as);
                    C.asAbsMax = std::max(C.asAbsMax, std::fabs(as));
                    if (C.fairImbBps == 0) ++C.sigN;
                }
                bidPx = f0 * (1.0 + (-C.fairD + sk) / 1e4);
                askPx = f0 * (1.0 + (C.fairD + sk) / 1e4);
                if (C.escMkOn) {   // [F60] сокращающая сторона — к fair
                    if (posV > 0) askPx = f0 * (1.0 + C.escMkBps / 1e4);
                    else          bidPx = f0 * (1.0 - C.escMkBps / 1e4);
                }
            } else if (sprTicks >= 4) {
                bidPx = book.bestBid() + spr * 0.25;
                askPx = book.bestAsk() - spr * 0.25;
            } else {
                bidPx = book.bestBid();
                askPx = book.bestAsk();
            }
            if (cfg.skewImb && !g_fairMode) {
                double s = (imb - 0.5) * 2.0 * cfg.imbSkewFrac * spr;
                bidPx += s; askPx += s;
            }
            if (cfg.skewTfi && !g_fairMode && std::fabs(tfi) > cfg.tfiThresh) {
                double s = tfi * cfg.tfiSkewFrac * spr;
                bidPx += s; askPx += s;
            }
            if (!g_fairMode) {
                double s = -(posV / cfg.maxPosQuote) * cfg.invSkewFrac * spr;
                bidPx += s; askPx += s;
            }
            if (g_fairMode) {   // [F55] не лучше fair±d: bid вниз, ask вверх
                bidPx = std::floor(bidPx / tick + 1e-9) * tick;
                askPx = std::ceil(askPx / tick - 1e-9) * tick;
            } else {
            bidPx = std::floor(bidPx / tick + 0.5) * tick;
            askPx = std::floor(askPx / tick + 0.5) * tick;
            }
            bidPx = std::min(bidPx, book.bestAsk() - tick);
            askPx = std::max(askPx, book.bestBid() + tick);

            bool spreadOK = book.spreadBps() <= cfg.maxSpreadBps;
            bool sane = bidPx > 0 && bidPx < askPx;
            double notional = std::max(cfg.orderSizeQuote, si.minNotional);

            auto makeQty = [&](double px, double& qty) -> bool {
                if (px <= 0) return false;
                qty = std::floor((notional / px) / si.qtyStep + 1e-9) * si.qtyStep;
                if (qty <= 0) return false;
                if (qty * px < si.minNotional) qty += si.qtyStep;
                return qty * px >= si.minNotional;
            };

            bool sweepCoolB = cfg.sweepCooldownS <= 0 ||
                              now - tr.buy.lastSweepAt >= cfg.sweepCooldownS;
            bool sweepCoolS = cfg.sweepCooldownS <= 0 ||
                              now - tr.sell.lastSweepAt >= cfg.sweepCooldownS;

            double rqThresh = std::max(1.5 * tick, cfg.requoteFrac * spr);
            if (g_fairMode && fairPx > 0)
                rqThresh = std::max(1.5 * tick, fairPx * C.fairRq / 1e4);

            double bq = 0, sq = 0;
            bool mkB = makeQty(bidPx, bq);

            bool mkS = makeQty(askPx, sq);
            if (C.escMkOn && mid > 0) {   // [F60] объём до излишка
                double exq = std::floor(((std::fabs(posV) - cfg.maxPosQuote * 0.75) / mid)
                                        / si.qtyStep + 1e-9) * si.qtyStep;
                if (posV > 0 && mkS && exq > sq) sq = exq;
                if (posV < 0 && mkB && exq > bq) bq = exq;
            }
            // [v3.0] sell клампится РЕАЛЬНЫМ базовым инвентарём
            // [fix F6] live: продаём только инвентарь СЕССИИ (baseInt от нуля), а не
            // весь кошелёк — иначе бот распродаёт ранее купленные монеты и уводит
            // baseInt в минус. MM_SELL_WALLET_BASE=1 возвращает поведение v3.1.
            double availBase = tr.base;
            if (lcfg.live)
                availBase = lcfg.sellWallet
                    ? live.ledger.totalBase()
                    : std::min(std::max(0.0, live.ledger.baseInt),
                               live.ledger.totalBase());
            if (!cfg.allowShort && availBase < sq - 1e-12) {
                sq = std::floor(availBase / si.qtyStep + 1e-9) * si.qtyStep;
                mkS = (sq > 0 && sq * askPx >= si.minNotional);
            }

            // [v3.0/v3.1] live: локальная аллокация (MM_START_USDT) и
            // ДОЛЯ свободных USDT кошелька (MM_QUOTE_SHARE, дефолт 1/N)
            bool liveReady = !lcfg.live || live.ledger.acctInit;
            double buyBudget = tr.quote;
            if (lcfg.live) {
                // [fix F25] quoteFree уже без НАШЕГО залоченного BUY: без
                // возврата его в бюджет стоящий ордер «не помещался» и
                // снимался, баланс освобождался, ордер ставился снова.
                const LiveSide& lb = live.s[0];
                double ownLocked = lb.open
                    ? std::max(0.0, lb.qty - lb.dealtQty) * lb.px : 0.0;
                double wallet = live.ledger.quoteFree + live.ledger.quoteLocked;
                buyBudget = std::min({C.startUsdt + live.ledger.quoteNet,
                                      wallet * C.quoteShare - lcfg.usdtReserve,
                                      live.ledger.quoteFree + ownLocked
                                      - lcfg.usdtReserve});
            }

            // [v2.14] ФИКС 18/19: esc-/cut-паузы в гейтах котирования
            if (fairBlock) { spreadOK = false; sane = false; }   // [F55] нет fair -> снять
            // [F58] фильтр тренда: не наращивать позицию против движения
            bool trendNoBuy = false, trendNoSell = false;
            if (cfg.trendBps > 0 && mid > 0) {
                if (now - C.lastTrendSample >= 5.0) {
                    C.lastTrendSample = now;
                    C.trendHist.push_back({now, mid});
                    while (C.trendHist.size() > 2 &&
                           now - C.trendHist[1].first >= cfg.trendSec)
                        C.trendHist.pop_front();
                }
                int st = 0;
                if (!C.trendHist.empty() &&
                    now - C.trendHist.front().first >= cfg.trendSec * 0.9) {
                    double trd = (mid / C.trendHist.front().second - 1.0) * 1e4;
                    if (trd < -cfg.trendBps && posV >= -cfg.orderSizeQuote * 0.5) st = -1;
                    if (trd > +cfg.trendBps && posV <= +cfg.orderSizeQuote * 0.5) st = +1;
                    if (st != C.trendState) {
                        char tb[160];
                        std::snprintf(tb, sizeof tb, "[TREND] %+.0f bps / %.0f s -> %s",
                                      trd, cfg.trendSec,
                                      st < 0 ? "bid OFF" : st > 0 ? "ask OFF" : "оба ON");
                        evAdd(events, tb);
                        C.trendState = st;
                    }
                }
                double dt = (C.lastTrendT > 0 && now > C.lastTrendT && now - C.lastTrendT < 5.0)
                            ? now - C.lastTrendT : 0.0;
                if (st < 0) { trendNoBuy = true;  C.trendBlockBuyS += dt; }
                if (st > 0) { trendNoSell = true; C.trendBlockSellS += dt; }
                C.lastTrendT = now;
            }
            bool wantBuy = spreadOK && sane && mkB && sweepCoolB && liveReady
                           && !trendNoBuy
                           && !(C.escMkOn && posV > 0)
                           && !tr.pauseBuy
                           && now >= tr.cutPauseBuyUntil
                           && now >= tr.escPauseBuyUntil
                           && posV < cfg.maxPosQuote
                           && bq * bidPx <= buyBudget;
            bool wantSell = spreadOK && sane && mkS && sweepCoolS && liveReady
                            && !trendNoSell
                            && !(C.escMkOn && posV < 0)
                            && !tr.pauseSell
                            && now >= tr.cutPauseSellUntil
                            && now >= tr.escPauseSellUntil
                            && posV > -cfg.maxPosQuote
                            && (cfg.allowShort || sq > 0);

            if (!C.warnedBothPaused && !wantBuy && !wantSell && spreadOK && sane) {
                C.warnedBothPaused = true;
                char buf[384];
                std::snprintf(buf, sizeof buf,
                    "[WARN] both sides paused: buy(%s%s%s%s%s) sell(%s%s%s%s%s) "
                    "run=%+d tfi=%+.2f pos=%+.0f",
                    tr.pauseBuy ? "MOM" : "",
                    !sweepCoolB ? " COOL" : "",
                    !mkB ? " QTY" : "",
                    now < tr.escPauseBuyUntil ? " ESC" : "",
                    now < tr.cutPauseBuyUntil ? " CUT" : "",
                    tr.pauseSell ? "MOM" : "",
                    !sweepCoolS ? " COOL" : "",
                    !mkS ? " QTY" : "",
                    now < tr.escPauseSellUntil ? " ESC" : "",
                    now < tr.cutPauseSellUntil ? " CUT" : "",
                    run, tfi, posV);
                evAdd(events, buf);
            }

            if (wantBuy)
                tr.requestQuote(tr.buy, Order::BUY, bidPx, bq, rqThresh, now, events);
            else
                tr.requestCancel(tr.buy, now, events);
            if (wantSell)
                tr.requestQuote(tr.sell, Order::SELL, askPx, sq, rqThresh, now, events);
            else
                tr.requestCancel(tr.sell, now, events);
        }
    }

    // ============ [v3.0 L3] зеркало решений на биржу ============
    // Желаемое состояние = состояние shadow-машины ПОСЛЕ решения.
    // Каждый тик (включая stale/halt): shadow NONE -> реальные снимаем.
    // [F50] реальные филлы -> shadow: sweep-cooldown/cutoff от РЕАЛЬНОГО
    // исполнения (раньше только от paper-филлов) и REAL_SYNC: реальный
    // ордер исполнен целиком, а shadow ещё стоит -> shadow-ордер закрыт,
    // иначе зеркало выставило бы его повторно (двойной набор позиции).
    if (lcfg.live) live.pollFees(now, events);   // [F54]
    // [F53] breaker: > MM_LIVE_MAX_FILLS_MIN реальных филлов за 60 c ->
    // снять ордера пары и пауза MM_LIVE_BREAKER_SEC (защита от «сжигания»)
    if (lcfg.live && lcfg.maxFillsMin > 0) {
        for (int i = 0; i < 2; ++i) {
            long d = live.realFillEv[i] - C.fillEvSeenBr[i];
            for (long k = 0; k < d && k < 1000; ++k) C.fillTimes.push_back(now);
            C.fillEvSeenBr[i] = live.realFillEv[i];
        }
        while (!C.fillTimes.empty() && now - C.fillTimes.front() > 60.0)
            C.fillTimes.pop_front();
        if ((int)C.fillTimes.size() > lcfg.maxFillsMin && now >= C.breakerUntil) {
            C.breakerUntil = now + lcfg.breakerSec;
            ++C.breakerCnt;
            char bb[200];
            std::snprintf(bb, sizeof bb,
                "[LIVE BREAKER] %zu реальных филлов за 60 c (лимит %d) -> "
                "ордера сняты, пауза %.0f s", C.fillTimes.size(),
                lcfg.maxFillsMin, lcfg.breakerSec);
            evAdd(events, bb);
            lprintf("[%s] %s\n", C.symbol.c_str(), bb);
            C.fillTimes.clear();
            tr.requestCancel(tr.buy, now, events);
            tr.requestCancel(tr.sell, now, events);
            live.cancelAll(events);
        }
    }
    if (lcfg.live && lcfg.realSync) {
        for (int i = 0; i < 2; ++i) {
            SideBook& sbx = (i == 0) ? tr.buy : tr.sell;
            // проверка через 0.5 c после филла: публичный принт мог
            // прийти позже приватной сделки
            if (live.realFillEv[i] != live.realFillSeen[i] &&
                now - live.realFillAt[i] >= 0.5) {
                live.realFillSeen[i] = live.realFillEv[i];
                // пробитие = публичный принт агрессора по нашей стороне
                // (sell-агрессор для BUY) ЗА ценой нашего ордера в
                // последние 3 c — уровень прошли насквозь, как toxic/
                // cutoff в paper (там — по принту против цены ордера)
                double P = live.realFillPx[i];
                double worst = 0;
                for (const auto& r : tr.recentPr) {
                    if (r[0] < live.realFillAt[i] - 3.0) continue;
                    if (i == 0 && (int)r[2] == 2 && (worst <= 0 || r[1] < worst)) worst = r[1];
                    if (i == 1 && (int)r[2] == 1 && r[1] > worst) worst = r[1];
                }
                if (P > 0 && worst > 0) {
                    double eps = tick * 0.49;
                    bool thru = (i == 0) ? (worst < P - eps) : (worst > P + eps);
                    double depthBps = std::fabs(P - worst) / P * 1e4;
                    double best = worst;
                    if (thru) {
                        sbx.lastSweepAt = now;
                        ++tr.realSweepCnt;
                        bool cut = cfg.cutoffBps > 0 && depthBps > cfg.cutoffBps;
                        if (cut) {
                            ++tr.cutCnt;
                            double until = now + cfg.cutoffSec;
                            if (i == 0) tr.cutPauseBuyUntil = std::max(tr.cutPauseBuyUntil, until);
                            else        tr.cutPauseSellUntil = std::max(tr.cutPauseSellUntil, until);
                        }
                        char rb[224];
                        std::snprintf(rb, sizeof rb,
                            "[REAL_SWEEP] %s: реальный филл @ %.10g, уровень пробит "
                            "(принт %.10g, %.0f бп) -> cooldown %.0f s%s",
                            i == 0 ? "BUY " : "SELL", P, best, depthBps,
                            cfg.sweepCooldownS, cut ? " + cutoff" : "");
                        evAdd(events, rb);
                    }
                }
            }
            if (live.realFull[i]) {
                live.realFull[i] = false;
                // только если shadow всё ещё держит ТОТ ЖЕ ордер, что
                // зеркалил реальный (shadow мог уже сам исполниться и
                // выставить новый — его не трогаем)
                int fid = live.fullMirId[i];
                if (fid != 0 && sbx.hasNext && sbx.nxt.id == fid) {
                    // реальный зеркалил отложенный реквот: он уже исполнен
                    sbx.hasNext = false;
                    ++tr.realSyncCnt;
                    char rb[160];
                    std::snprintf(rb, sizeof rb,
                        "[REAL_SYNC] %s #%d: реальный ордер исполнен, отложенный "
                        "shadow-реквот снят", i == 0 ? "BUY " : "SELL", fid);
                    evAdd(events, rb);
                } else if (fid != 0 && sbx.cur.id == fid) {
                    tr.realSync(sbx, now, events);
                }
            }
        }
    }
    if (lcfg.live && tr.invInitialized) {
        double sprNow = book.ok() ? (book.bestAsk() - book.bestBid()) : 0;
        double rqLive = std::max(1.5 * tick, cfg.requoteFrac * sprNow);
        for (int sideIdx = 0; sideIdx < 2; ++sideIdx) {
            const SideBook& sb = (sideIdx == 0) ? tr.buy : tr.sell;
            bool want = !(sb.st == QState::NONE ||
                          (sb.st == QState::WAIT_CANCEL && !sb.hasNext));
            double dpx = 0, dqty = 0;
            if (want) {
                dpx = sb.hasNext ? sb.nxt.price : sb.cur.price;
                dqty = sb.hasNext ? (sb.nxt.qty - sb.nxt.filled)
                                  : (sb.cur.qty - sb.cur.filled);
                // [fix 3.1-F1] желаемое -= РЕАЛЬНОЕ исполнение биржи:
                // shadow.filled и ls.dealtQty расходятся на частичных
                // филлах; без коррекции place создал бы дубль-ордер.
                if (live.s[sideIdx].open && live.s[sideIdx].dealtQty > 1e-15)
                    dqty -= live.s[sideIdx].dealtQty;
                if (dqty * dpx < si.minNotional) want = false;
            }
            live.ensureSide(sideIdx, want, dpx, dqty, rqLive, now, events);
            // [F50] запомнить, какой shadow-ордер зеркалит новый реальный
            if (live.s[sideIdx].open && live.s[sideIdx].cid != live.mirCid[sideIdx]) {
                live.mirCid[sideIdx] = live.s[sideIdx].cid;
                live.mirId[sideIdx] = sb.hasNext ? sb.nxt.id : sb.cur.id;
            }
        }
    }
}

} // namespace spred
