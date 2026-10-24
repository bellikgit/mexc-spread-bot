#include "spred/trading/paper.hpp"

namespace spred {

double PaperTrader::prUrand() { return (double)(prng() >> 8) * (1.0 / 16777216.0); }

bool PaperTrader::realSync(SideBook& sb, double now, std::vector<std::string>& ev) {
    if (!(sb.st == QState::LIVE || sb.st == QState::WAIT_PLACE)) return false;
    if (sb.cur.qty - sb.cur.filled <= 1e-15) return false;
    ++realSyncCnt;
    char buf[200];
    std::snprintf(buf, sizeof buf,
                  "[REAL_SYNC] %s #%d: реальный ордер исполнен, shadow-ордер "
                  "(%.10g @ %.10g, filled %.10g) закрыт без paper-филла",
                  sb.cur.side == Order::BUY ? "BUY " : "SELL", sb.cur.id,
                  sb.cur.qty, sb.cur.price, sb.cur.filled);
    evAdd(ev, buf);
    if (sb.hasNext) {
        sb.cur = sb.nxt;
        sb.hasNext = false;
        sb.st = QState::WAIT_PLACE;
        sb.curPlaceLatMs = orderLatMs * sampleLatMult();
        sb.pendingUntil = now + sb.curPlaceLatMs / 1000.0;
        sb.placeSentMs = nowMsSteady();
        sb.lastRequoteAt = now;
        sb.queueInited = false;
    } else {
        sb.cur = Order{};
        sb.st = QState::NONE;
    }
    return true;
}

double PaperTrader::equity(double mid) const { return quote + base * mid; }

double PaperTrader::totalPnl(double mid) const { return equity(mid) - initialQuote; }

double PaperTrader::posValue(double mid) const { return (base - base0) * mid; }

double PaperTrader::roundQty(double q) const { return std::floor(q / qtyStep + 1e-9) * qtyStep; }

void PaperTrader::calibDealClock(double sample) {
    if (!dealOffInit) {
        exchOffDealMs = sample;
        dealOffInit = true;
    } else {
        double d = sample - exchOffDealMs;
        if (d <= -30000.0 || d >= 5000.0) return;
        double step = (d < 0.0 ? 0.30 : 0.05) * d;
        if (step < -1000.0) step = -1000.0;
        exchOffDealMs += step;
    }
    if (exchOffInit) {
        double s = std::fabs(exchOffMs - exchOffDealMs);
        if (s > skewMaxMs) skewMaxMs = s;
    }
}

double PaperTrader::clockSkewMs() const {
    return (exchOffInit && dealOffInit) ? (exchOffMs - exchOffDealMs) : 0.0;
}

double PaperTrader::sampleLatMult() {
    if (cfg.latDist <= 0) return 1.0;
    std::uniform_real_distribution<double> u(0.0, 1.0);
    double x = u(rng);
    double m;
    if (x < 0.5)
        m = cfg.latMinMult + (1.0 - cfg.latMinMult) * (x / 0.5);
    else if (x < 0.9)
        m = 1.0 + (cfg.latP90Mult - 1.0) * ((x - 0.5) / 0.4);
    else if (x < 0.99)
        m = cfg.latP90Mult + (cfg.latP99Mult - cfg.latP90Mult) * ((x - 0.9) / 0.09);
    else
        m = cfg.latP99Mult * (1.0 + 0.5 * ((x - 0.99) / 0.01));
    if (m < cfg.latMinMult) m = cfg.latMinMult;
    return m;
}

void PaperTrader::applyTrade(double signedQty, double px) {
    double notional = signedQty * px;
    double fee = std::fabs(notional) * cfg.makerFeeBps / 1e4;
    feesPaid += fee;
    makerFees += fee;   // [v2.13] (escape добавляет taker-дельту отдельно)
    quote -= notional + fee;
    volumeTraded += std::fabs(notional);
    const double EPS = 1e-12;
    bool wasLong = base > EPS, wasShort = base < -EPS;
    double nb = base + signedQty;
    if (!wasLong && !wasShort) {
        avgEntry = px;
    } else if (wasLong) {
        if (signedQty < 0) {
            double close = std::min(-signedQty, base);
            realized += close * (px - avgEntry);
            if (nb < -EPS) avgEntry = px;
            else if (std::fabs(nb) <= EPS) avgEntry = 0;
        } else {
            avgEntry = (avgEntry * base + px * signedQty) / nb;
        }
    } else {
        if (signedQty > 0) {
            double close = std::min(signedQty, -base);
            realized += close * (avgEntry - px);
            if (nb > EPS) avgEntry = px;
            else if (std::fabs(nb) <= EPS) avgEntry = 0;
        } else {
            avgEntry = (avgEntry * (-base) + px * (-signedQty)) / (-nb);
        }
    }
    base = nb;
}

double PaperTrader::levelQty(const BookState& b, Order::Side side, double px, double tick) {
    double eps = tick * 0.49, sum = 0;
    const std::vector<Level>& v = (side == Order::BUY) ? b.bids : b.asks;
    for (const auto& l : v)
        if (std::fabs(l.price - px) <= eps) sum += l.qty;
    return sum;
}

void PaperTrader::makeGhost(const Order& o, double placeSentMs, double placeLatMs,
                   double cancelSentMs, double cancelLat) {
    // [v2.13.1] окно живости ордера на бирже — в deal-домене: границы
    // окна сравниваются с d.timeMs в onGhost.
    if (!dealOffInit) return;
    Ghost& g = (o.side == Order::BUY) ? ghostBuy : ghostSell;
    g.active = true;
    g.side = (int)o.side;
    g.price = o.price;
    g.qty = o.qty - o.filled;
    g.startExchMs = placeSentMs + placeLatMs - exchOffDealMs;   // [P0-3]
    g.untilExchMs = cancelSentMs + cancelLat - exchOffDealMs;
}

void PaperTrader::requestQuote(SideBook& sb, Order::Side side, double px, double qty,
                      double rqThresh, double now, std::vector<std::string>& ev) {
    if (sb.st == QState::NONE) {
        if (!rate.take(RK_NEW)) { ++throttled; return; }
        sb.cur = Order{side, px, qty, 0, orderSeq++};
        sb.hasNext = false;
        sb.st = QState::WAIT_PLACE;
        // [v2.13] каждому ордеру — своя латентность
        sb.curPlaceLatMs = orderLatMs * sampleLatMult();
        sb.pendingUntil = now + sb.curPlaceLatMs / 1000.0;
        sb.placeSentMs = nowMsSteady();
        sb.lastRequoteAt = now;
        // [v2.11 FIX 3] новый ордер — очередь с чистого листа
        sb.queueAhead = 0;
        sb.lastLevelQty = 0;
        sb.tradedAtLevel = 0;
        sb.queueInited = false;
        char buf[192];
        std::snprintf(buf, sizeof buf,
                      "[PLACE %s] #%d %.10g @ %.10g (live in %.0f ms)",
                      side == Order::BUY ? "BUY " : "SELL", sb.cur.id, qty, px,
                      sb.curPlaceLatMs);
        evAdd(ev, buf);
    } else if (sb.st == QState::WAIT_PLACE) {
        if (std::fabs(sb.cur.price - px) >= rqThresh) {
            if (!rate.take(RK_NEW)) { ++throttled; return; }
            sb.cur.price = px;
            sb.cur.qty = qty;
            sb.lastRequoteAt = now;
        }
    } else {
        // LIVE or WAIT_CANCEL: replace if price moved enough OR qty drifted
        double target = sb.hasNext ? sb.nxt.price : sb.cur.price;
        double targetQty = sb.hasNext ? sb.nxt.qty : sb.cur.qty;
        bool pxMoved = std::fabs(target - px) >= rqThresh;
        double denom = std::max(targetQty, qty);
        // [fix F6] порог дрейфа qty: по умолчанию 1000 ед. (как v3.1, историю
        // ночей не ломаем); MM_QTY_DRIFT_USDT>0 — в USDT, как в LiveExecutor
        double minDrift = (cfg.qtyDriftUsdt > 0 && px > 0) ? cfg.qtyDriftUsdt / px
                                                            : 1000.0;
        bool qtyDrift = denom > 0 &&
                        std::fabs(targetQty - qty) / denom >= 0.50 &&
                        std::fabs(targetQty - qty) >= minDrift;
        if (!pxMoved && !qtyDrift) return;
        if (cfg.minRequoteSec > 0 && now - sb.lastRequoteAt < cfg.minRequoteSec) return;
        if (!rate.take(sb.st == QState::LIVE ? RK_REPLACE : RK_NEW)) { ++throttled; return; }
        ++replaces;
        sb.nxt = Order{side, px, qty, 0, orderSeq++};
        sb.hasNext = true;
        sb.lastRequoteAt = now;
        if (sb.st == QState::LIVE) {
            sb.st = QState::WAIT_CANCEL;
            // [v2.13] каждой отмене — своя латентность
            sb.curCancelLatMs = cancelLatMs * sampleLatMult();
            sb.pendingUntil = now + sb.curCancelLatMs / 1000.0;
            sb.cancelSentMs = nowMsSteady();
            ++cancels;
            char buf[256];
            std::snprintf(buf, sizeof buf,
                          "[REQUOTE   ] cancel #%d @ %.10g%s -> #%d @ %.10g x%.10g",
                          sb.cur.id, sb.cur.price,
                          qtyDrift && !pxMoved ? " (qty-sync)" : "",
                          sb.nxt.id, px, qty);
            evAdd(ev, buf);
        }
    }
}

void PaperTrader::requestCancel(SideBook& sb, double now, std::vector<std::string>& ev) {
    if (sb.st == QState::NONE) return;
    if (sb.st == QState::WAIT_CANCEL) { sb.hasNext = false; return; }
    if (!rate.take(RK_CANCEL)) { ++throttled; return; }
    ++cancels;
    if (sb.st == QState::WAIT_PLACE) {
        sb.st = QState::NONE;
        sb.hasNext = false;
        return;
    }
    sb.st = QState::WAIT_CANCEL;
    // [v2.13] каждой отмене — своя латентность
    sb.curCancelLatMs = cancelLatMs * sampleLatMult();
    sb.pendingUntil = now + sb.curCancelLatMs / 1000.0;
    sb.cancelSentMs = nowMsSteady();
    char buf[128];
    std::snprintf(buf, sizeof buf, "[CANCEL    ] #%d @ %.10g", sb.cur.id, sb.cur.price);
    evAdd(ev, buf);
    sb.hasNext = false;
}

void PaperTrader::advance(SideBook& sb, const BookState& book, double tick, double now) {
    if (sb.st == QState::WAIT_PLACE && now >= sb.pendingUntil) {
        sb.st = QState::LIVE;
        if (!sb.queueInited) {
            sb.queueAhead = levelQty(book, sb.cur.side, sb.cur.price, tick);
            sb.lastLevelQty = sb.queueAhead;
            sb.tradedAtLevel = 0;
            sb.queueInited = true;
        }
    } else if (sb.st == QState::WAIT_CANCEL && now >= sb.pendingUntil) {
        if (sb.cur.qty - sb.cur.filled > 1e-15)
            makeGhost(sb.cur, sb.placeSentMs, sb.curPlaceLatMs,     // [v2.16 P0-3]
                      sb.cancelSentMs, sb.curCancelLatMs);
        sb.cur = Order{};
        if (sb.hasNext) {
            sb.cur = sb.nxt;
            sb.hasNext = false;
            sb.st = QState::WAIT_PLACE;
            sb.curPlaceLatMs = orderLatMs * sampleLatMult();   // [v2.13]
            sb.pendingUntil = now + sb.curPlaceLatMs / 1000.0;
            sb.placeSentMs = nowMsSteady();
            sb.queueInited = false;
        } else {
            sb.st = QState::NONE;
        }
    }
}

void PaperTrader::onDepth(SideBook& sb, const BookState& book, double tick) {
    if (sb.st != QState::LIVE && sb.st != QState::WAIT_CANCEL) return;
    if (sb.cur.price <= 0) return;
    double eps = tick * 0.49;
    const std::vector<Level>& v = (sb.cur.side == Order::BUY) ? book.bids : book.asks;
    bool outOfWindow =
        v.empty() ||
        (sb.cur.side == Order::BUY
             ? (v.back().price > sb.cur.price + eps)
             : (v.back().price < sb.cur.price - eps));
    double lq = levelQty(book, sb.cur.side, sb.cur.price, tick);
    if (outOfWindow) {
        sb.lastLevelQty = lq;
        sb.tradedAtLevel = 0;
        return;
    }
    double cancelled = sb.lastLevelQty - sb.tradedAtLevel - lq;
    // [v2.15 ФИКС 20] MM_QUEUE_MODE: bounding вместо точки. Матрицы
    // TAG/ZIL (n9): при requote ~10с очередь эпизодическая, frac не
    // меняет поток филлов — ось закрыта, флаг остаётся страховкой
    // для пар с редкими requot'ами.
    if (cancelled > 0) {
        double frac = (cfg.queueMode == 1) ? 0.0
                    : (cfg.queueMode == 2) ? 1.0
                                           : cfg.cancelAheadFrac;
        sb.queueAhead -= frac * cancelled;
    }
    if (sb.queueAhead < 0) sb.queueAhead = 0;
    sb.lastLevelQty = lq;
    sb.tradedAtLevel = 0;
}

void PaperTrader::updateMarkouts(double now, double mid) {
    if (mid <= 0) return;
    for (auto& f : mo)
        while (f.done < 3 && now - f.t >= MO_H[f.done]) {
            double bps = f.sgn * (mid - f.px) / f.px * 1e4;
            if (f.reason >= 0 && f.reason < FR_N) {
                rmoSum[f.reason][f.done] += bps * f.qty * f.px;
                rmoVol[f.reason][f.done] += f.qty * f.px;
                if (f.done == 1) moR30[f.reason].push_back(bps);
            }
            if (f.reason == FR_ESCAPE) { ++f.done; continue; }
            moSum[f.done] += bps * f.qty * f.px;
            moVol[f.done] += f.qty * f.px;
            double pbps = f.sgn * (mid - f.proxyPx) / f.proxyPx * 1e4;
            pmoSum[f.done] += pbps * f.qty * f.proxyPx;
            pmoVol[f.done] += f.qty * f.proxyPx;   // [fix F5]
            double tbps = f.sgn * (mid - f.takerPx) / f.takerPx * 1e4;
            tmoSum[f.done] += tbps * f.qty * f.takerPx;
            tmoVol[f.done] += f.qty * f.takerPx;   // [fix F5]
            moH[f.done].push_back(bps);
            if (f.done == 1)
                (f.sgn > 0 ? moBuy30 : moSell30).push_back(bps);
            ++f.done;
        }
    while (!mo.empty() && mo.front().done >= 3) mo.pop_front();
}

double PaperTrader::markout(int i) const { return moVol[i] > 0 ? moSum[i] / moVol[i] : 0.0; }

double PaperTrader::proxyMarkout(int i) const { return pmoVol[i] > 0 ? pmoSum[i] / pmoVol[i] : 0.0; }

double PaperTrader::takerMarkout(int i) const { return tmoVol[i] > 0 ? tmoSum[i] / tmoVol[i] : 0.0; }

double PaperTrader::reasonMo(int r, int i) const { return rmoVol[r][i] > 0 ? rmoSum[r][i] / rmoVol[r][i] : 0.0; }

double PaperTrader::reasonUsd(int r, int i) const { return rmoSum[r][i] / 1e4; }

int PaperTrader::reasonCnt(int r) const { return rCnt[r]; }

void PaperTrader::doFill(SideBook& sb, Order::Side side, double px, double qty, bool toxic,
                double now, double fillAnchorSec, const BookState& book,
                std::vector<std::string>& ev, int reason) {
    qty = std::min(qty, sb.cur.qty - sb.cur.filled);
    if (side == Order::SELL && !cfg.allowShort) {
        if (qty > base) qty = std::floor(base / qtyStep + 1e-9) * qtyStep;
    }
    if (qty <= 1e-15) return;
    applyTrade(side == Order::BUY ? qty : -qty, px);
    ++fills;
    if (side == Order::BUY) ++buyFills; else ++sellFills;
    if (toxic) { ++forcedFills; ++toxicFills; sb.lastSweepAt = now; }
    if (reason >= 0 && reason < FR_N) ++rCnt[reason];
    sb.cur.filled += qty;
    bool partial = sb.cur.filled < sb.cur.qty - 1e-15;
    if (partial) ++partialFills;
    double proxyPx = (side == Order::BUY) ? book.bestBid() : book.bestAsk();
    if (proxyPx <= 0) proxyPx = px;
    double takerPx = (side == Order::BUY) ? book.bestAsk() : book.bestBid();
    if (takerPx <= 0) takerPx = px;
    mo.push_back({fillAnchorSec, side == Order::BUY ? 1 : -1, px, proxyPx,
                  takerPx, qty, 0, reason});   // [v2.16 P0-4]
    char buf[256];
    std::snprintf(buf, sizeof buf,
                  "[FILL %s%s] #%d %.10g @ %.10g%s (pos %+.10g, realized %+.4f)",
                  side == Order::BUY ? "BUY " : "SELL",
                  toxic ? " SWEEP" : "      ",
                  sb.cur.id, qty, px, partial ? " PARTIAL" : "", base, realized);
    evAdd(ev, buf);
    if (!partial) {
        if (sb.hasNext) {
            sb.cur = sb.nxt;
            sb.hasNext = false;
            sb.st = QState::WAIT_PLACE;
            sb.curPlaceLatMs = orderLatMs * sampleLatMult();   // [v2.13]
            sb.pendingUntil = now + sb.curPlaceLatMs / 1000.0;
            sb.placeSentMs = nowMsSteady();
            sb.lastRequoteAt = now;
            sb.queueInited = false;
        } else {
            sb.cur = Order{};
            sb.st = QState::NONE;
        }
    }
}

void PaperTrader::onPrint(SideBook& sb, Order::Side side, const MexcPush::Deal& d,
                 const BookState& book, double tick, double now,
                 std::vector<std::string>& ev) {
    if (sb.st == QState::NONE) return;
    double P = sb.cur.price, eps = tick * 0.49;
    double remaining = sb.cur.qty - sb.cur.filled;
    if (remaining <= 1e-15) return;
    bool isBuy = (side == Order::BUY);
    if (isBuy ? (d.side != 2) : (d.side != 1)) return;

    // [v2.13.1 ФИКС 16] гейт ЦЕЛИКОМ в deal-домене: d.timeMs сравнивается
    // с приходом ордера, пересчитанным через deal-офсет. Раньше
    // arriveExch считался через send-офсет (exchOffMs): при расхождении
    // часов шлюза и матчинга на S гейт уезжал на S и пропускал сделки,
    // случившиеся ДО ордера (лаг до -26319 мс в логе 16:48-17:06).
    bool haveExchTime = dealOffInit && d.timeMs > 0;
    // [v2.16 P0-4] якорь markout-окон: биржевое время сделки (deal-домен,
    // сек; exchOffDealMs маппит d.timeMs в steady). Нет метки — now.
    double fillAnchorSec = haveExchTime
        ? ((double)d.timeMs + exchOffDealMs) / 1000.0
        : now;
    if (haveExchTime) {
        double dealExchMs = (double)d.timeMs;
        // [v2.13] гейт по латентности КОНКРЕТНОГО ордера
        double arriveExch = sb.placeSentMs + sb.curPlaceLatMs - exchOffDealMs;
        if (sb.st == QState::WAIT_PLACE) {
            if (dealExchMs < arriveExch) return;
            // сделка прошла гейт => ордер уже активен на бирже
            sb.st = QState::LIVE;
            if (!sb.queueInited) {
                sb.queueAhead = levelQty(book, sb.cur.side, sb.cur.price, tick);
                sb.lastLevelQty = sb.queueAhead;
                sb.tradedAtLevel = 0;
                sb.queueInited = true;
            }
        } else if (sb.st == QState::WAIT_CANCEL) {
            double cancelArrive = sb.cancelSentMs + sb.curCancelLatMs - exchOffDealMs;
            if (dealExchMs > cancelArrive) return;
        } else {
            if (dealExchMs < arriveExch) return;
        }
    } else if (sb.st == QState::WAIT_PLACE) {
        return;
    }

    bool through = isBuy ? (d.price < P - eps) : (d.price > P + eps);
    // [F45] внутренний принт не пробивает наш уровень (консервативно)
    // [F47] внутренний принт (строго внутри видимого спреда) не даёт
    // НИКАКОГО филла: ни THROUGH/TOXIC, ни «в уровень» (цена == наш
    // ордер: внутри спреда мы одни, queueAhead = 0, и любой синтетический
    // принт ровно по нашей цене засчитывался FR_QUEUE). Очередь таким
    // принтом тоже не уменьшается. Счётчик — только реально срезанные
    // филлы (раньше считался каждый внутренний принт на каждой стороне).
    if (curPrintInside && cfg.printFilter >= 1) {
        bool beyond = isBuy ? (d.price > P + eps) : (d.price < P - eps);
        bool wouldFill = through || (!beyond && sb.queueAhead - d.qty <= 1e-15);
        // [F49] принт, касающийся нашего уровня, с вероятностью p
        // обрабатывается как при =0 (очередь + филл); p=0 -> как v3.1.9
        bool pass = cfg.printFillP > 0 && !beyond &&
                    (cfg.printFillP >= 1 || prUrand() < cfg.printFillP);
        if (!pass) {
            if (wouldFill) ++prCutFills;
            return;
        }
        if (wouldFill) ++prPassFills;
    }
    if (through) {
        // Полный филл на toxic — корректный контрфакт price-time priority
        bool toxic = isBuy ? (d.price < book.bestBid() - eps)
                           : (d.price > book.bestAsk() + eps);
        // [v2.14 ФИКС 19] CUTOFF: глубина пробития НАШЕГО уровня —
        // не зависит от порядка событий (книга к этому моменту уже
        // обновлена принтом, из-за чего toxic-флаг почти не срабатывал:
        // sw0 всю сессию, включая нож -2% в 22:15:56). Филл остаётся —
        // мы реально стоим в книге; пауза запрещает перекотироваться
        // в ту же сторону до истечения MM_CUTOFF_SEC.
        if (cfg.cutoffBps > 0) {
            double depthBps = (P > 0) ? std::fabs(P - d.price) / P * 1e4 : 0.0;
            if (depthBps > cfg.cutoffBps || toxic) {
                ++cutCnt;
                double until = now + cfg.cutoffSec;
                if (isBuy) cutPauseBuyUntil = until;
                else       cutPauseSellUntil = until;
                char cb[192];
                std::snprintf(cb, sizeof cb,
                              "[CUTOFF  ] %s: пробитие уровня на %.0f бп "
                              "(порог %.0f) -> пауза %.0f s",
                              isBuy ? "BUY " : "SELL", depthBps,
                              cfg.cutoffBps, cfg.cutoffSec);
                evAdd(ev, cb);
            }
        }
        double fq = toxic ? roundQty(remaining)
                          : roundQty(std::min(remaining, d.qty));
        if (fq > 1e-15)
            doFill(sb, side, P, fq, toxic, now, fillAnchorSec, book, ev,
                   toxic ? FR_TOXIC : FR_THROUGH);   // [v2.16 P0-4]
        return;
    }
    if (isBuy ? (d.price > P + eps) : (d.price < P - eps)) return;
    sb.tradedAtLevel += d.qty;
    sb.queueAhead -= d.qty;
    if (sb.queueAhead <= 1e-15) {
        double reached = -sb.queueAhead;
        sb.queueAhead = 0;
        double fq = roundQty(std::min(remaining, reached));
        if (fq > 1e-15)
            doFill(sb, side, P, fq, false, now, fillAnchorSec, book, ev,
                   FR_QUEUE);   // [v2.16 P0-4]
    }
}

void PaperTrader::onGhost(Ghost& g, const MexcPush::Deal& d, const BookState& book,
                 double now, std::vector<std::string>& ev) {
    (void)now;   // [fix F40]
    if (!g.active) return;
    // [v2.13.1] сравнение d.timeMs с окном живости — оба в deal-домене
    if (!(dealOffInit && d.timeMs > 0)) { g.active = false; return; }
    double dealExchMs = (double)d.timeMs;
    if (dealExchMs > g.untilExchMs) { g.active = false; return; }
    // [v2.16 P0-3] нижняя граница: сделка старше активации ордера
    // физически не могла нас заполнить
    if (dealExchMs < g.startExchMs) return;
    bool isBuy = (g.side == (int)Order::BUY);
    if (isBuy ? (d.side != 2) : (d.side != 1)) return;
    bool through = isBuy ? (d.price < g.price) : (d.price > g.price);
    if (!through) return;
    if (curPrintInside && cfg.printFilter >= 1) {   // [F45]
        bool pass = cfg.printFillP > 0 &&
                    (cfg.printFillP >= 1 || prUrand() < cfg.printFillP);   // [F49]
        if (!pass) { ++prCutFills; return; }
        ++prPassFills;
    }
    double fq = roundQty(std::min(g.qty, d.qty));
    if (fq <= 1e-15) return;
    applyTrade(isBuy ? fq : -fq, g.price);
    g.qty -= fq;
    if (g.qty <= 1e-15) g.active = false;
    ++fills;
    if (isBuy) ++buyFills; else ++sellFills;
    ++forcedFills; ++lateFills;
    ++rCnt[FR_GHOST];
    double proxyPx = isBuy ? book.bestBid() : book.bestAsk();
    if (proxyPx <= 0) proxyPx = g.price;
    double takerPx = isBuy ? book.bestAsk() : book.bestBid();
    if (takerPx <= 0) takerPx = g.price;
    double fillAnchorSec = ((double)d.timeMs + exchOffDealMs) / 1000.0;  // [P0-4]
    mo.push_back({fillAnchorSec, isBuy ? 1 : -1, g.price, proxyPx,
                  takerPx, fq, 0, FR_GHOST});
    char buf[256];
    std::snprintf(buf, sizeof buf,
                  "[FILL %s LATE  ] ghost %.10g @ %.10g (pos %+.10g, realized %+.4f)",
                  isBuy ? "BUY " : "SELL", fq, g.price, base, realized);
    evAdd(ev, buf);
}

} // namespace spred
