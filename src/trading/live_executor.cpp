#include "spred/trading/live_executor.hpp"
#include "spred/core/config.hpp"

namespace spred {

static long g_cidSeq = 1;

void LiveExecutor::addOwnOid(const std::string& oid) {
    if (oid.empty() || !ownOids.insert(oid).second) return;
    ownOidsOrder.push_back(oid);
    if (ownOidsOrder.size() > 4096) { ownOids.erase(ownOidsOrder.front()); ownOidsOrder.pop_front(); }
}

void LiveExecutor::archiveClosed(const LiveSide& ls, double now) {
    double credit = ls.dealtQty - ls.dealSeenQty;
    if (credit > 1e-12 && (!ls.cid.empty() || !ls.oid.empty())) {
        closedOrds.push_back({ls.cid, ls.oid, credit, now});
        if (closedOrds.size() > 64) closedOrds.erase(closedOrds.begin());
    }
}

void LiveExecutor::pruneClosed(double now) {
    closedOrds.erase(
        std::remove_if(closedOrds.begin(), closedOrds.end(),
                       [&](const ClosedOrd& c) {
                           return c.credit <= 1e-12 || now - c.at > 60.0;
                       }),
        closedOrds.end());
}

LiveExecutor::ClosedOrd* LiveExecutor::findClosed(const std::string& cid, const std::string& oid) {
    for (auto& c : closedOrds)
        if ((!cid.empty() && c.cid == cid) || (!oid.empty() && c.oid == oid))
            return &c;
    return nullptr;
}

void LiveExecutor::noteDealt(const LiveSide& ls, bool wasOpen) {
    int idx = (int)(&ls - s);
    if (idx < 0 || idx > 1) return;
    ++realFillEv[idx];
    realFillPx[idx] = ls.px;
    realFillAt[idx] = nowSecs();
    if (wasOpen && ls.qty > 0 &&
        ls.dealtQty >= ls.qty - std::max(1e-12, ls.qty * 1e-9)) {
        realFull[idx] = true;
        fullMirId[idx] = mirId[idx];
    }
}

bool LiveExecutor::markFeeTid(const std::string& tid) {   // true = комиссия ещё не учтена
    if (tid.empty()) return true;
    if (!feeTid.insert(tid).second) return false;
    feeTidOrder.push_back(tid);
    if (feeTidOrder.size() > 4096) { feeTid.erase(feeTidOrder.front()); feeTidOrder.pop_front(); }
    return true;
}

void LiveExecutor::queueFee(const std::string& oid, double at) {
    if (!feeLookup || oid.empty()) return;
    for (auto& f : feeQ) if (f.oid == oid) { f.at = std::max(f.at, at); return; }
    if (feeQ.size() < 256) feeQ.push_back({oid, at, 0});
}

void LiveExecutor::pollFees(double now, std::vector<std::string>& ev) {
    for (size_t i = 0; i < feeQ.size(); ++i) {
        if (now < feeQ[i].at) continue;
        FeeQ f = feeQ[i];
        if (!liveRate->take()) return;          // бюджет REST — позже
        feeQ.erase(feeQ.begin() + (long)i);
        RestResult r = rc.myTrades(f.oid);
        ++feeLookups;
        if (!r.ok) {
            ++feeFail;
            ++f.tries;
            if (f.tries < 3) { f.at = now + 5.0; feeQ.push_back(f); }
            char b[200];
            std::snprintf(b, sizeof b, "[LIVE FEE] myTrades %s: code %ld %s%s",
                          f.oid.c_str(), r.errCode, r.errMsg.c_str(),
                          f.tries < 3 ? " (повтор)" : " (сдаюсь)");
            evAdd(ev, b);
            return;
        }
        for (auto& ob : jsonSplitObjects(r.body)) {
            std::string tid, cur;
            if (!jsonFindStr(ob, "id", tid) || tid.empty()) continue;
            if (!markFeeTid(tid)) continue;     // уже учтена (WS-сделка/прошлый запрос)
            double fee = 0, qty = 0, px = 0;
            jsonFindNum(ob, "commission", fee);
            jsonFindStr(ob, "commissionAsset", cur);
            jsonFindNum(ob, "qty", qty);
            jsonFindNum(ob, "price", px);
            bool maker = ob.find("\"isMaker\":true") != std::string::npos ||
                         ob.find("\"isMaker\": true") != std::string::npos;
            if (!maker) ++feeTaker;
            ledger.applyFee(fee, cur, baseAsset, quoteAsset);
            ++feeTrades;
            char b[240];
            std::snprintf(b, sizeof b,
                          "[LIVE FEE] сделка %.24s: %.10g @ %.10g, комиссия %.10g %s, %s",
                          tid.c_str(), qty, px, fee, cur.c_str(),
                          maker ? "maker" : "TAKER");
            evAdd(ev, b);
        }
        return;                                  // не больше 1 запроса за вызов
    }
}

LiveExecutor::LiveExecutor(const std::string& host, bool noVerify) : rc(host, noVerify) { ownRate.init(10); }

std::string LiveExecutor::newCid() {
    std::string c = "sp" + base36((long long)getpid()) + "x" + base36(g_cidSeq++);
    if (c.size() > 30) c.resize(30);
    return c;
}

bool LiveExecutor::botCid(const std::string& c) {
    if (c.size() < 5 || c.compare(0, 2, "sp") != 0) return false;
    size_t x = c.find('x', 3);
    if (x == std::string::npos || x + 1 >= c.size()) return false;
    for (size_t i = 2; i < c.size(); ++i) {
        char ch = c[i];
        if (!((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'z'))) return false;
    }
    return true;
}

bool LiveExecutor::ourCid(const std::string& c) const {
    std::string pfx = "sp" + base36((long long)getpid()) + "x";
    return c.rfind(pfx, 0) == 0;
}

void LiveExecutor::onPrivDeal(const MexcPush::PrivDeal& d, double anchorSec,
                    std::vector<std::string>& ev) {
    ++wsPrivDeals;
    if (!(d.qty > 0)) return;
    if (!d.tradeId.empty()) {   // [fix F9]
        std::string key = d.tradeId + "|" + d.clientOrderId;
        if (!seenDeals.insert(key).second) { ++dupDeals; return; }
        seenDealsOrder.push_back(key);
        if (seenDealsOrder.size() > 4096) {
            seenDeals.erase(seenDealsOrder.front());
            seenDealsOrder.pop_front();
        }
    }
    const double EPS = 1e-12;
    bool isBuy = (d.tradeType == 1);
    pruneClosed(nowSecs());
    int idx = -1;
    for (int i = 0; i < 2; ++i)
        if ((!d.clientOrderId.empty() && s[i].cid == d.clientOrderId) ||
            (!d.orderId.empty() && s[i].oid == d.orderId)) { idx = i; break; }
    ClosedOrd* co = (idx < 0) ? findClosed(d.clientOrderId, d.orderId) : nullptr;
    // [fix F27] сделка не нашей сессии (ручная торговля, другой бот, ордер
    // прошлого запуска) -> в ledger не проводим: искажала бы позицию/maxLoss
    if (idx < 0 && !co && !ourCid(d.clientOrderId) && !escCids.count(d.clientOrderId) &&
        !(!d.orderId.empty() && ownOids.count(d.orderId))) {
        if (++foreignDeals <= 20 || foreignDeals % 100 == 0) {
            char fb[224];
            std::snprintf(fb, sizeof fb,
                          "[LIVE DEAL] чужая сделка (cid %.24s oid %.20s) %s %.10g @ %.10g "
                          "— не учитываю (#%d)", d.clientOrderId.c_str(),
                          d.orderId.c_str(), d.tradeType == 1 ? "BUY" : "SELL",
                          d.qty, d.price, foreignDeals);
            evAdd(ev, fb);
        }
        return;
    }

    // кредит: уже проведено дельтой cumQty/resolve, сама сделка ещё не пришла
    double credit = 0;
    if (idx >= 0) credit = std::max(0.0, s[idx].dealtQty - s[idx].dealSeenQty);
    else if (co)  credit = co->credit;

    double covered = std::min(d.qty, credit);   // уже в ledger -> только fee
    double fresh = d.qty - covered;             // реально новый филл
    double frac = fresh / d.qty;
    // сделка по снятой/неизвестной стороне, не покрытая кредитом = GHOST
    bool ghost = ((idx < 0) || !s[idx].open) && fresh > EPS;
    if (ghost) ++ledger.ghostFillsWs;

    int isEsc = escCids.count(d.clientOrderId) ? 1 : 0;
    // [F54] комиссия этой сделки уже взята из myTrades -> не дублировать
    double dFee = markFeeTid(d.tradeId) ? d.feeAmount : 0.0;
    if (fresh > EPS) {
        ledger.applyRealFill(isBuy, d.price, fresh, dFee * frac,
                             d.feeCurrency, baseAsset, quoteAsset, anchorSec,
                             isEsc);
        if (idx >= 0)
            s[idx].dealtAmt += (d.amount > 0 ? d.amount : d.qty * d.price) * frac;
    }
    if (covered > EPS)
        ledger.applyFee(dFee * (covered / d.qty), d.feeCurrency,
                        baseAsset, quoteAsset);
    if (idx >= 0) {
        s[idx].dealSeenQty += d.qty;
        s[idx].dealtQty += fresh;
        if (fresh > EPS) noteDealt(s[idx], s[idx].open);   // [F50]
    } else if (co) {
        co->credit = std::max(0.0, co->credit - covered);
    } else if (fresh > EPS) {   // [fix F21] сирота: запомнить проведённое
        const std::string& k = !d.clientOrderId.empty() ? d.clientOrderId : d.orderId;
        if (!k.empty()) {
            unknownDealt[k] += fresh;
            if (unknownDealt.size() > 512) unknownDealt.erase(unknownDealt.begin());
        }
    }

    char buf[320];
    if (fresh > EPS) {
        std::snprintf(buf, sizeof buf,
                      "[LIVE FILL %s%s] %.10g @ %.10g (fee %.10g %s, maker %d%s%s) "
                      "pos %+.10g, quoteNet %+.4f, realized %+.4f",
                      isBuy ? "BUY " : "SELL", ghost ? " GHOST" : "     ",
                      fresh, d.price, d.feeAmount * frac, d.feeCurrency.c_str(),
                      (int)d.isMaker, isEsc ? " ESC" : "",
                      covered > EPS ? " +cov" : "",
                      ledger.baseInt, ledger.quoteNet, ledger.realized);
    } else {
        std::snprintf(buf, sizeof buf,
                      "[LIVE DEAL ok] %s %.10g @ %.10g уже учтена по cumQty "
                      "(fee %.10g %s проведена)",
                      isBuy ? "BUY " : "SELL", d.qty, d.price, d.feeAmount,
                      d.feeCurrency.c_str());
    }
    evAdd(ev, buf);
}

void LiveExecutor::onPrivOrder(const MexcPush::PrivOrder& o, double now,
                     std::vector<std::string>& ev) {
    ++wsPrivOrders;
    int idx = -1;
    for (int i = 0; i < 2; ++i)
        if ((!o.clientId.empty() && s[i].cid == o.clientId) ||
            (!o.id.empty() && s[i].oid == o.id)) { idx = i; break; }
    if (idx < 0) return;            // чужой/старый — recon разберётся
    LiveSide& ls = s[idx];
    if (!o.id.empty() && ls.oid.empty()) ls.oid = o.id;
    bool terminal = (o.status == 2 || o.status == 4 || o.status == 5);
    if (o.cumQty > ls.dealtQty + 1e-12) {
        double dq = o.cumQty - ls.dealtQty;
        double dAmt = o.cumAmt - ls.dealtAmt;
        double px = dq > 0 ? dAmt / dq : (o.avgPrice > 0 ? o.avgPrice : o.price);
        if (px > 0 && dq > 0) {
            ++ledger.ghostFillsWs;
            ledger.applyRealFill(o.tradeType == 1, px, dq, 0, "",
                                 baseAsset, quoteAsset, now, 0);
            ls.dealtQty = o.cumQty;
            ls.dealtAmt = o.cumAmt;
            noteDealt(ls, ls.open);   // [F50]
            queueFee(ls.oid.empty() ? o.id : ls.oid, now + 2.0);   // [F54]
            char buf[224];
            std::snprintf(buf, sizeof buf,
                          "[LIVE GHOST-WS] %s дельта-филл %.10g @ ~%.10g "
                          "(cumQty биржи опередила deals-ленту)",
                          o.tradeType == 1 ? "BUY " : "SELL", dq, px);
            evAdd(ev, buf);
        }
    }
    if (terminal) {
        char buf[224];
        std::snprintf(buf, sizeof buf,
                      "[LIVE ORD  ] %s %s status %d (filled %.10g/%.10g)",
                      o.tradeType == 1 ? "BUY " : "SELL", ls.cid.c_str(),
                      o.status, o.cumQty, ls.qty);
        evAdd(ev, buf);
        // [fix F4] кредит (cumQty-дельта без подтверждающей сделки) уходит в
        // closedOrds: хвостовые deals гасят его, а не считаются заново.
        // Раньше хвост учитывался только для статусов FILLED, а
        // CANCELED/PARTIALLY_CANCELED с филлами считались дважды.
        archiveClosed(ls, now);
        ls.open = false;
        ls.cid.clear();
        ls.oid.clear();
        ls.dealtQty = 0;
        ls.dealtAmt = 0;
        ls.dealSeenQty = 0;
    }
}

void LiveExecutor::onPrivAccount(const MexcPush::PrivAccount& a) {
    ++wsPrivAcct;
    if (a.coin == baseAsset) {
        ledger.baseFree = a.balance;
        ledger.baseLocked = a.frozen;
    } else if (a.coin == quoteAsset) {
        ledger.quoteFree = a.balance;
        ledger.quoteLocked = a.frozen;
    }
}

bool LiveExecutor::transportUnknown(LiveSide& ls, const char* what, double now,
                          std::vector<std::string>& ev) {
    // транспортный сбой: состояние запроса неизвестно -> resolve по cid
    ++ls.fails;
    ls.nextRetryAt = now + std::min(30.0, std::pow(2.0, ls.fails));
    char buf[224];
    std::snprintf(buf, sizeof buf,
                  "[LIVE REST ] %s: транспортный сбой (%s) -> resolve по cid, "
                  "retry через %.0f с", ls.cid.c_str(), what, ls.nextRetryAt - now);
    evAdd(ev, buf);
    return false;
}

bool LiveExecutor::orderNotFound(const RestResult& r) {
    if (r.errCode == -2011 || r.errCode == -2013) return true;
    const std::string& m = r.errMsg;
    return m.find("Unknown order") != std::string::npos ||
           m.find("does not exist") != std::string::npos ||
           m.find("not exist") != std::string::npos;
}

void LiveExecutor::resolve(LiveSide& ls, bool isBuySide, double now,
                 std::vector<std::string>& ev, bool countFail) {
    ++resolves;
    RestResult r = rc.getOrder(ls.oid, ls.cid);
    if (!r.transportOk) {
        // [fix 3.1-F4] при вызове после transportUnknown в place
        // fails уже инкрементирован — не удваиваем backoff
        if (countFail) transportUnknown(ls, "resolve", now, ev);
        else ls.nextRetryAt = now + std::min(30.0, std::pow(2.0, ls.fails));
        return;
    }
    if (!r.ok) {
        // [fix F14] закрытым считаем ТОЛЬКО явный «ордера нет» (-2011
        // Unknown order sent; -2013 Binance-совместимый). Rate-limit, 5xx,
        // 700003 и прочее = состояние неизвестно: ордер, возможно, жив ->
        // open=true (cancel/cancelAll его попробуют снять), backoff.
        if (orderNotFound(r)) {
            ls.open = false;
            ls.fails = 0;
            char buf[192];
            std::snprintf(buf, sizeof buf,
                          "[LIVE RESOLVE] %s: ордер не найден (code %ld %.60s) -> закрыт",
                          ls.cid.c_str(), r.errCode, r.errMsg.c_str());
            evAdd(ev, buf);
            return;
        }
        if (r.errCode == 700003) rc.timeSync();
        if (countFail) ++ls.fails;
        ls.nextRetryAt = now + std::min(30.0, std::pow(2.0, std::max(1, ls.fails)));
        if (!ls.cid.empty() || !ls.oid.empty()) ls.open = true;
        char buf[224];
        std::snprintf(buf, sizeof buf,
                      "[LIVE RESOLVE] %s: ошибка запроса (http %d code %ld %.60s) "
                      "-> статус неизвестен, считаю живым, retry через %.0f с",
                      ls.cid.c_str(), r.httpCode, r.errCode, r.errMsg.c_str(),
                      ls.nextRetryAt - now);
        evAdd(ev, buf);
        return;
    }
    std::string status;
    jsonFindStr(r.body, "status", status);
    double execQty = 0, cumQuote = 0;
    jsonFindNum(r.body, "executedQty", execQty);
    jsonFindNum(r.body, "cummulativeQuoteQty", cumQuote);
    if (execQty > ls.dealtQty + 1e-12) {
        double dq = execQty - ls.dealtQty;
        double px = dq > 0 ? (cumQuote - ls.dealtAmt) / dq : 0;
        if (px <= 0) px = ls.px;
        ++ledger.ghostFillsRecon;
        ledger.applyRealFill(isBuySide, px, dq, 0, "", baseAsset, quoteAsset,
                             now, 0);
        ls.dealtQty = execQty;
        ls.dealtAmt = cumQuote;
        noteDealt(ls, ls.open);   // [F50]
        queueFee(ls.oid, now + 2.0);   // [F54]
        char buf[224];
        std::snprintf(buf, sizeof buf,
                      "[LIVE GHOST-REST] %s дельта-филл %.10g @ ~%.10g "
                      "(resolve: executedQty опередил deals)",
                      isBuySide ? "BUY " : "SELL", dq, px);
        evAdd(ev, buf);
    }
    bool terminal = (status == "FILLED" || status == "CANCELED" ||
                     status == "PARTIALLY_CANCELED" || status == "EXPIRED" ||
                     status == "REJECTED");
    if (terminal) {
        ls.open = false;
    } else {
        ls.open = true;     // NEW/PARTIALLY_FILLED — жив
        if (ls.oid.empty()) {
            std::string oid;
            if (jsonFindStr(r.body, "orderId", oid)) ls.oid = oid;
        }
        addOwnOid(ls.oid);   // [fix F27]
    }
    ls.fails = 0;
    char buf[192];
    std::snprintf(buf, sizeof buf,
                  "[LIVE RESOLVE] %s -> %s (exec %.10g)",
                  ls.cid.c_str(), status.c_str(), execQty);
    evAdd(ev, buf);
}

void LiveExecutor::place(LiveSide& ls, bool isBuy, double px, double qty, double now,
               std::vector<std::string>& ev) {
    if (!liveRate->take(RK_NEW)) return;
    archiveClosed(ls, now);   // [fix F4] кредит прежнего ордера не теряем
    ls.cid = newCid();
    ls.oid.clear();
    ls.dealtQty = 0;
    ls.dealtAmt = 0;
    ls.dealSeenQty = 0;
    RestResult r = rc.placeLimit(isBuy ? "BUY" : "SELL", px, qty, ls.cid);
    // [fix F18] 700003 (часы уехали): ресинк и один немедленный повтор;
    // биржа запрос отклонила, тот же cid повторно использовать можно
    if (r.transportOk && !r.ok && r.errCode == 700003 && rc.timeSync())
        r = rc.placeLimit(isBuy ? "BUY" : "SELL", px, qty, ls.cid);
    // [fix F31] 5xx от шлюза: биржа могла ордер принять -> как транспортный сбой
    if (r.transportOk && !r.ok && r.httpCode >= 500) r.transportOk = false;
    if (!r.transportOk) { ls.px = px; ls.qty = qty; ls.openedAt = now;   // [F14]
                          transportUnknown(ls, "place", now, ev);
                          resolve(ls, isBuy, now, ev, false); return; }
    if (r.ok) {
        ls.open = true;
        ls.px = px;
        ls.qty = qty;
        ls.openedAt = now;
        ls.fails = 0;
        ++ls.places;
        ls.placeLatSum += r.rttMs;
        if (r.rttMs > ls.placeLatMax) ls.placeLatMax = r.rttMs;
        std::string oid;
        if (jsonFindStr(r.body, "orderId", oid)) ls.oid = oid;
        addOwnOid(ls.oid);   // [fix F27]
        char buf[256];
        std::snprintf(buf, sizeof buf,
                      "[LIVE PLACE %s] %s %.10g @ %.10g (oid %.16s, %.0f ms)",
                      isBuy ? "BUY " : "SELL", ls.cid.c_str(), qty, px,
                      ls.oid.c_str(), r.rttMs);
        evAdd(ev, buf);
        return;
    }
    ++ls.placeFails;
    ++ls.fails;
    // 700003 и после повтора -> ещё раз ресинк, дальше обычный backoff
    if (r.errCode == 700003) { rc.timeSync(); }
    ls.nextRetryAt = now + std::min(30.0, std::pow(2.0, ls.fails));
    char buf[320];
    std::snprintf(buf, sizeof buf,
                  "[LIVE PLACE-FAIL %s] %s code %ld %.120s (retry через %.0f с)",
                  isBuy ? "BUY " : "SELL", ls.cid.c_str(), r.errCode, r.errMsg.c_str(),
                  ls.nextRetryAt - now);
    evAdd(ev, buf);
}

void LiveExecutor::cancel(LiveSide& ls, bool isBuy, double now,
                std::vector<std::string>& ev) {
    if (!liveRate->take(RK_CANCEL)) return;
    if (ls.cid.empty() && ls.oid.empty()) { ls.open = false; return; }
    RestResult r = rc.cancelOrder(ls.oid, ls.cid);
    if (!r.transportOk) { transportUnknown(ls, "cancel", now, ev);
                          resolve(ls, isBuy, now, ev); return; }
    ++ls.cancels;
    ls.cancelLatSum += r.rttMs;
    if (r.rttMs > ls.cancelLatMax) ls.cancelLatMax = r.rttMs;
    if (r.ok) {
        ls.open = false;
        ls.fails = 0;
        char buf[224];
        std::snprintf(buf, sizeof buf, "[LIVE CANCEL %s] %s (%.0f ms)",
                      isBuy ? "BUY " : "SELL", ls.cid.c_str(), r.rttMs);
        evAdd(ev, buf);
        // [fix F20] ответ cancel несёт executedQty: частичный филл, не
        // пришедший по приватному WS, иначе терялся
        double execQty = 0, cumQuote = 0;
        if (jsonFindNum(r.body, "executedQty", execQty) &&
            execQty > ls.dealtQty + 1e-12) {
            jsonFindNum(r.body, "cummulativeQuoteQty", cumQuote);
            double dq = execQty - ls.dealtQty;
            double px = (cumQuote - ls.dealtAmt) / dq;
            if (!(px > 0)) px = ls.px;
            ++ledger.ghostFillsRecon;
            ledger.applyRealFill(isBuy, px, dq, 0, "", baseAsset, quoteAsset,
                                 now, 0);
            ls.dealtQty = execQty;
            if (cumQuote > 0) ls.dealtAmt = cumQuote;
            noteDealt(ls, ls.open);   // [F50]
            queueFee(ls.oid, now + 2.0);   // [F54]
            std::snprintf(buf, sizeof buf,
                          "[LIVE GHOST-REST] %s дельта-филл %.10g @ ~%.10g "
                          "(cancel: executedQty опередил deals)",
                          isBuy ? "BUY " : "SELL", dq, px);
            evAdd(ev, buf);
        }
        return;
    }
    ++ls.cancelFails;
    // неизвестный ордер при отмене = его уже нет (исполнился/снят) ->
    // resolve добирает executedQty (GHOST-REST при расхождении)
    resolve(ls, isBuy, now, ev, false);
    if (!ls.open) return;
    ++ls.fails;
    ls.nextRetryAt = now + std::min(30.0, std::pow(2.0, ls.fails));
}

void LiveExecutor::ensureSide(int idx, bool want, double px, double qty, double rqThresh,
                    double now, std::vector<std::string>& ev) {
    LiveSide& ls = s[idx];
    bool isBuy = (idx == 0);
    if (now < ls.nextRetryAt) return;
    if (!want) {
        if (ls.open) cancel(ls, isBuy, now, ev);
        return;
    }
    if (!ls.open) { place(ls, isBuy, px, qty, now, ev); return; }
    bool pxMoved = std::fabs(ls.px - px) >= rqThresh;
    // [fix F32] qty — желаемый ОСТАТОК; сравниваем с остатком живого ордера
    double liveRem = std::max(0.0, ls.qty - ls.dealtQty);
    double denom = std::max(liveRem, qty);
    bool qtyDrift = denom > 0 &&
                    std::fabs(liveRem - qty) / denom >= 0.50 &&
                    std::fabs(liveRem - qty) * px >= 1.0;   // ≥1 USDT разницы
    if (!pxMoved && !qtyDrift) return;
    // реквот: cancel; place произойдёт на следующем тике (open=false
    // после успешного cancel) — та же последовательность, что в paper
    cancel(ls, isBuy, now, ev);
}

void LiveExecutor::cancelAll(std::vector<std::string>& ev) {
    for (int i = 0; i < 2; ++i)
        if (s[i].open) {
            // [fix F36] ждём токен (лимит биржи 5/с), не обнуляем бюджет
            liveRate->waitToken(1.5, RK_CANCEL);
            cancel(s[i], i == 0, nowSecs(), ev);
        }
}

bool LiveExecutor::escapeMarket(bool isBuy, double qty, double quoteQty, double now,
                      std::vector<std::string>& ev) {
    // [fix F35] self-trade: встречный СОБСТВЕННЫЙ лимит снять до MARKET
    LiveSide& opp = s[isBuy ? 1 : 0];
    if (opp.open) {
        liveRate->waitToken(1.0, RK_CANCEL);
        cancel(opp, !isBuy, now, ev);
        if (opp.open) {
            evAdd(ev, std::string("[LIVE ESCAPE] ") + (isBuy ? "BUY " : "SELL") +
                      " отложен: встречный ордер бота не снят (self-trade)");
            return false;
        }
    }
    liveRate->waitToken(1.0, RK_NEW);   // [fix F36] escape тоже в лимите 5/с
    liveRate->take(RK_NEW);
    std::string cid = newCid();
    escCids.insert(cid);
    RestResult r = rc.marketOrder(isBuy ? "BUY" : "SELL", qty, quoteQty, cid);
    // [fix F34] 5xx: биржа могла исполнить -> статус неизвестен, сверить
    if (r.transportOk && !r.ok && r.httpCode >= 500) r.transportOk = false;
    if (!r.transportOk || r.ok) {   // [fix F23] сверим исполнение позже
        std::string eoid;
        if (r.ok) jsonFindStr(r.body, "orderId", eoid);
        addOwnOid(eoid);   // [fix F27]
        escPending.push_back({cid, eoid, isBuy, now});
        if (escPending.size() > 32) escPending.erase(escPending.begin());
    }
    if (!r.transportOk) {
        char buf[192];
        std::snprintf(buf, sizeof buf,
                      "[LIVE ESCAPE] %s: транспортный сбой — проверить "
                      "позицию вручную/через recon!", isBuy ? "BUY " : "SELL");
        evAdd(ev, buf);
        return false;
    }
    if (!r.ok) {
        char buf[288];
        std::snprintf(buf, sizeof buf,
                      "[LIVE ESCAPE-FAIL] %s code %ld %.120s",
                      isBuy ? "BUY " : "SELL", r.errCode, r.errMsg.c_str());
        evAdd(ev, buf);
        return false;
    }
    double execQty = 0, cumQuote = 0;
    jsonFindNum(r.body, "executedQty", execQty);
    jsonFindNum(r.body, "cummulativeQuoteQty", cumQuote);
    char buf[288];
    std::snprintf(buf, sizeof buf,
                  "[LIVE ESCAPE] %s market qty %.10g -> exec %.10g за %.10g USDT "
                  "(%.0f ms), филлы подтвердятся по deals-ленте",
                  isBuy ? "BUY " : "SELL", qty, execQty, cumQuote, r.rttMs);
    evAdd(ev, buf);
    return true;
}

void LiveExecutor::reconcileEscapes(double now, std::vector<std::string>& ev) {
    for (size_t i = 0; i < escPending.size();) {
        EscPend& e = escPending[i];
        if (now - e.at < 1.0) { ++i; continue; }   // дать бирже исполнить
        RestResult g = rc.getOrder(e.oid, e.cid);
        if (!g.transportOk || (!g.ok && !orderNotFound(g))) { ++i; continue; }
        if (!g.ok) {   // не найден: не выставился (или ещё не виден)
            if (now - e.at > 30.0) escPending.erase(escPending.begin() + long(i));
            else ++i;
            continue;
        }
        std::string status;
        jsonFindStr(g.body, "status", status);
        bool terminal = (status == "FILLED" || status == "CANCELED" ||
                         status == "PARTIALLY_CANCELED" || status == "EXPIRED" ||
                         status == "REJECTED");
        if (!terminal && now - e.at < 60.0) { ++i; continue; }
        double execQty = 0, cumQuote = 0;
        jsonFindNum(g.body, "executedQty", execQty);
        jsonFindNum(g.body, "cummulativeQuoteQty", cumQuote);
        std::string oid = e.oid;
        if (oid.empty()) jsonFindStr(g.body, "orderId", oid);
        double seen = 0;
        auto it = unknownDealt.find(e.cid);
        if (it == unknownDealt.end() && !oid.empty()) it = unknownDealt.find(oid);
        if (it != unknownDealt.end()) seen = it->second;
        double dq = execQty - seen;
        if (dq > 1e-12) {
            double px = execQty > 0 ? cumQuote / execQty : 0;
            if (px > 0) {
                ++ledger.ghostFillsRecon;
                ledger.applyRealFill(e.isBuy, px, dq, 0, "", baseAsset, quoteAsset,
                                     now, 1);
                queueFee(oid, now + 1.0);   // [F54] escape — тейкер, комиссия есть
                closedOrds.push_back({e.cid, oid, dq, now});
                if (closedOrds.size() > 64) closedOrds.erase(closedOrds.begin());
                char buf[224];
                std::snprintf(buf, sizeof buf,
                              "[LIVE GHOST-REST] %s escape %.20s: дельта-филл "
                              "%.10g @ ~%.10g (deals-лента не подтвердила)",
                              e.isBuy ? "BUY " : "SELL", e.cid.c_str(), dq, px);
                evAdd(ev, buf);
            }
        }
        escPending.erase(escPending.begin() + long(i));
    }
}

bool LiveExecutor::adoptOrder(const std::string& ob, const std::string& oid,
                    const std::string& cid, bool thisSession, double now,
                    std::vector<std::string>& ev) {
    std::string side;
    if (!jsonFindStr(ob, "side", side) || (side != "BUY" && side != "SELL")) return false;
    int idx = side == "BUY" ? 0 : 1;
    LiveSide& ls = s[idx];
    if (ls.open) return false;
    double px = 0, oq = 0, eq = 0, cq = 0;
    jsonFindNum(ob, "price", px);
    jsonFindNum(ob, "origQty", oq);
    jsonFindNum(ob, "executedQty", eq);
    jsonFindNum(ob, "cummulativeQuoteQty", cq);
    if (!(px > 0) || !(oq > 0)) return false;
    archiveClosed(ls, now);
    double seen = 0;
    if (thisSession) {
        auto it = unknownDealt.find(cid);
        if (it == unknownDealt.end()) it = unknownDealt.find(oid);
        if (it != unknownDealt.end()) { seen = it->second; unknownDealt.erase(it); }
        double dq = eq - seen;
        if (dq > 1e-12) {
            ++ledger.ghostFillsRecon;
            ledger.applyRealFill(idx == 0, eq > 0 && cq > 0 ? cq / eq : px, dq, 0, "",
                                 baseAsset, quoteAsset, now, 0);
            queueFee(oid, now + 1.0);   // [F54]
        }
    } else {
        seen = eq;
    }
    ls.open = true;
    ls.cid = cid;
    ls.oid = oid;
    ls.px = px;
    ls.qty = oq;
    ls.dealtQty = eq;
    ls.dealtAmt = cq;
    ls.dealSeenQty = seen;
    ls.openedAt = now;
    ls.fails = 0;
    ls.nextRetryAt = 0;
    addOwnOid(oid);
    char buf[256];
    std::snprintf(buf, sizeof buf,
                  "[LIVE RECON] усыновлён %s %.20s (cid %.24s) %.10g @ %.10g, "
                  "исполнено %.10g%s", side.c_str(), oid.c_str(), cid.c_str(), oq, px,
                  eq, thisSession ? "" : " (прошлый запуск)");
    evAdd(ev, buf);
    return true;
}

void LiveExecutor::reconcile(double now, std::vector<std::string>& ev) {
    reconcileEscapes(now, ev);
    RestResult r = rc.openOrders();
    if (!r.transportOk || !r.ok) return;
    auto objs = jsonSplitObjects(r.body);
    std::set<std::string> exchOids;
    for (auto& ob : objs) {
        std::string oid, cid;
        if (!jsonFindStr(ob, "orderId", oid)) continue;
        exchOids.insert(oid);
        jsonFindStr(ob, "clientOrderId", cid);
        bool known = (s[0].open && s[0].oid == oid) ||
                     (s[1].open && s[1].oid == oid) ||
                     (s[0].open && !s[0].cid.empty() && s[0].cid == cid) ||
                     (s[1].open && !s[1].cid.empty() && s[1].cid == cid);
        if (known) {
            // [fix F30] частичный филл, не пришедший по WS: executedQty
            // из openOrders сверх учтённого -> дельта в ledger
            for (int i = 0; i < 2; ++i) {
                LiveSide& ls = s[i];
                if (!ls.open || !(ls.oid == oid || (!ls.cid.empty() && ls.cid == cid)))
                    continue;
                double eq = 0, cq = 0;
                if (!jsonFindNum(ob, "executedQty", eq) || eq <= ls.dealtQty + 1e-12)
                    break;
                jsonFindNum(ob, "cummulativeQuoteQty", cq);
                double dq = eq - ls.dealtQty;
                double px = (cq - ls.dealtAmt) / dq;
                if (!(px > 0)) px = ls.px;
                ++ledger.ghostFillsRecon;
                ledger.applyRealFill(i == 0, px, dq, 0, "", baseAsset, quoteAsset, now, 0);
                ls.dealtQty = eq;
                if (cq > 0) ls.dealtAmt = cq;
                noteDealt(ls, ls.open);   // [F50]
                queueFee(ls.oid.empty() ? oid : ls.oid, now + 2.0);   // [F54]
                char buf[224];
                std::snprintf(buf, sizeof buf,
                              "[LIVE GHOST-REST] %s %s дельта-филл %.10g @ ~%.10g "
                              "(recon: executedQty живого ордера опередил deals)",
                              i == 0 ? "BUY " : "SELL", ls.cid.c_str(), dq, px);
                evAdd(ev, buf);
                break;
            }
            continue;
        }
        // [fix F14] ордер биржи совпал с cid стороны, которая локально
        // закрыта (ошибочный resolve / поздняя видимость после place) ->
        // сторона снова open, дальше ею управляют ensureSide/cancelAll.
        // Раньше такой ордер считался «известным» и не снимался никогда.
        bool revived = false;
        for (int i = 0; i < 2 && !revived; ++i) {
            LiveSide& ls = s[i];
            if (ls.open || ls.cid.empty() || ls.cid != cid) continue;
            ls.open = true;
            ls.oid = oid;
            addOwnOid(oid);   // [fix F27]
            double v = 0;
            if (jsonFindNum(ob, "price", v) && v > 0) ls.px = v;
            if (jsonFindNum(ob, "origQty", v) && v > 0) ls.qty = v;
            ls.nextRetryAt = 0;
            ++reconEvents;
            revived = true;
            char buf[224];
            std::snprintf(buf, sizeof buf,
                          "[LIVE RECON] %s: локально закрыт, но жив на бирже "
                          "(oid %.20s) -> снова open", cid.c_str(), oid.c_str());
            evAdd(ev, buf);
        }
        if (revived) continue;
        // [fix F38] ордер бота прошлого запуска: adopt=1 -> усыновить
        if (!ourCid(cid) && botCid(cid) && lcfg && lcfg->reconAdopt) {
            ++reconEvents;
            if (!adoptOrder(ob, oid, cid, false, now, ev)) {
                RestResult cr = rc.cancelOrder(oid, "");
                char buf[224];
                std::snprintf(buf, sizeof buf,
                              "[LIVE RECON] ордер прошлого запуска %.20s: слот занят "
                              "-> снят (%s)", oid.c_str(), cr.ok ? "ok" : "fail");
                evAdd(ev, buf);
            }
            continue;
        }
        if (!ourCid(cid)) {
            if (knownForeignOids.insert(oid).second) {
                char buf[224];
                std::snprintf(buf, sizeof buf,
                              "[LIVE RECON] чужой ордер на бирже: %.20s "
                              "(cid %.24s) — НЕ трогаем", oid.c_str(), cid.c_str());
                evAdd(ev, buf);
            }
            continue;
        }
        // наш префикс, но неизвестен (перезапуск?) — политика:
        // adopt=0 -> снять (чистый лист), adopt=1 -> оставить и усыновить
        ++reconEvents;
        if (lcfg && lcfg->reconAdopt && adoptOrder(ob, oid, cid, true, now, ev)) {
            // [fix F38] усыновлён в свободный слот своей стороны
        } else {
            RestResult cr = rc.cancelOrder(oid, "");
            ++orphanCancels;
            char buf[224];
            std::snprintf(buf, sizeof buf,
                          "[LIVE RECON] снят сирота %.20s (%s)", oid.c_str(),
                          cr.ok ? "ok" : "fail");
            evAdd(ev, buf);
            // [fix F21] сирота этой сессии (pid в cid наш): её филлы — наши.
            // Проводим executedQty сверх уже пришедшего по WS; остаток
            // кладём кредитом, чтобы поздняя WS-сделка не учлась дважды.
            std::string srcBody = cr.ok ? cr.body : ob;
            if (!cr.ok) {   // уже исполнилась/снялась -> точный executedQty
                RestResult gr = rc.getOrder(oid, "");
                if (gr.ok) srcBody = gr.body;
            }
            double execQty = 0, cumQuote = 0;
            std::string side;
            jsonFindNum(srcBody, "executedQty", execQty);
            jsonFindNum(srcBody, "cummulativeQuoteQty", cumQuote);
            if (!jsonFindStr(srcBody, "side", side)) jsonFindStr(ob, "side", side);
            double seen = 0;
            auto it = unknownDealt.find(cid);
            if (it == unknownDealt.end()) it = unknownDealt.find(oid);
            if (it != unknownDealt.end()) seen = it->second;
            double dq = execQty - seen;
            if (dq > 1e-12 && (side == "BUY" || side == "SELL")) {
                double px = execQty > 0 ? cumQuote / execQty : 0;
                if (!(px > 0)) jsonFindNum(ob, "price", px);
                if (px > 0) {
                    ++ledger.ghostFillsRecon;
                    ledger.applyRealFill(side == "BUY", px, dq, 0, "", baseAsset,
                                         quoteAsset, now, 0);
                    queueFee(oid, now + 1.0);   // [F54]
                    closedOrds.push_back({cid, oid, dq, now});
                    if (closedOrds.size() > 64) closedOrds.erase(closedOrds.begin());
                    std::snprintf(buf, sizeof buf,
                                  "[LIVE GHOST-REST] %s сирота %.20s: дельта-филл "
                                  "%.10g @ ~%.10g", side.c_str(), oid.c_str(), dq, px);
                    evAdd(ev, buf);
                }
            }
        }
    }
    // наши open, которых нет на бирже -> resolve (филл в обход ленты?)
    for (int i = 0; i < 2; ++i) {
        LiveSide& ls = s[i];
        if (!ls.open || ls.oid.empty()) continue;
        if (exchOids.count(ls.oid)) continue;
        ++reconEvents;
        char buf[192];
        std::snprintf(buf, sizeof buf,
                      "[LIVE RECON] %s open локально, но нет на бирже -> resolve",
                      ls.cid.c_str());
        evAdd(ev, buf);
        resolve(ls, i == 0, now, ev);
    }
}

void LiveExecutor::pollBalance(std::vector<std::string>& ev) {
    RestResult r = rc.account();
    if (!r.transportOk || !r.ok) return;
    bool wasInit = ledger.acctInit;
    // [v3.0 fix] ответ = {"balances":[{...}],...} — массив на ГЛУБИНЕ 1,
    // jsonSplitObjects по всему телу его не видит. Сначала вырезаем
    // вложенный массив с баланс-скобками, потом разбираем объекты.
    std::string arr = jsonExtractArray(r.body, "balances");
    if (arr.empty()) arr = r.body;   // fallback: вдруг отдали плоский массив
    for (auto& ob : jsonSplitObjects(arr)) {
        std::string asset;
        if (!jsonFindStr(ob, "asset", asset)) continue;
        double fr = 0, lk = 0;
        jsonFindNum(ob, "free", fr);
        jsonFindNum(ob, "locked", lk);
        if (asset == baseAsset) {
            ledger.baseFree = fr;
            ledger.baseLocked = lk;
            ledger.acctInit = true;
        } else if (asset == quoteAsset) {
            ledger.quoteFree = fr;
            ledger.quoteLocked = lk;
            // [fix 3.1-F5] RAMP-старт с пустым base: ассет может
            // отсутствовать в первом ответе (нулевые не отдаются);
            // acctInit по quote — иначе base0 перезапишется позже
            // пост-филлевым значением и sessionPnl потеряет позицию
            if (!ledger.acctInit) ledger.acctInit = true;
        }
    }
    if (ledger.acctInit && !wasInit) {
        ledger.base0 = ledger.totalBase();
        char buf[288];
        std::snprintf(buf, sizeof buf,
                      "[LIVE BAL] старт: %.10g %s (free %.10g) | %.4f %s free "
                      "(base0 зафиксирован, сессионный P&L — от нуля)",
                      ledger.totalBase(), baseAsset.c_str(), ledger.baseFree,
                      ledger.quoteFree, quoteAsset.c_str());
        evAdd(ev, buf);
    }
    // дрифт внутреннего учёта vs биржа (сигнал пропущенных филлов)
    if (ledger.acctInit) {
        double d = std::fabs(ledger.baseInt - (ledger.totalBase() - ledger.base0));
        if (d > ledger.driftMaxBase) ledger.driftMaxBase = d;
    }
}

} // namespace spred
