#include "spred/trading/live_ledger.hpp"

namespace spred {

double LiveLedger::totalBase() const { return baseFree + baseLocked; }

double LiveLedger::totalQuote() const { return quoteFree + quoteLocked; }

double LiveLedger::sessionPnl(double mid) const { return quoteNet + baseInt * mid; }

double LiveLedger::posValue(double mid) const { return baseInt * mid; }

void LiveLedger::applyFee(double fee, const std::string& feeCur,
                  const std::string& baseAsset, const std::string& quoteAsset) {
    if (!(fee > 0)) return;
    if (feeCur == quoteAsset) { quoteNet -= fee; feesQuote += fee; }
    else if (feeCur == baseAsset) { baseInt -= fee; feesBase += fee; }
    else { feesOther += fee; feesOtherCur = feeCur; }
}

void LiveLedger::applyRealFill(bool isBuy, double px, double qty, double fee,
                       const std::string& feeCur, const std::string& baseAsset,
                       const std::string& quoteAsset, double anchorSec,
                       int isEscape) {
    double notional = qty * px;
    if (isBuy) { quoteNet -= notional; baseInt += qty; }
    else       { quoteNet += notional; baseInt -= qty; }
    applyFee(fee, feeCur, baseAsset, quoteAsset);
    volume += notional;
    ++fills;
    if (isBuy) ++buyFills; else ++sellFills;
    // avg-cost realized (та же семантика, что PaperTrader.applyTrade)
    const double EPS = 1e-12;
    double signedQty = isBuy ? qty : -qty;
    double pos = baseInt - signedQty;    // позиция ДО филла (с учётом fee-в-base нетто — приближение)
    bool wasLong = pos > EPS, wasShort = pos < -EPS;
    if (!wasLong && !wasShort) {
        avgEntry = px;
    } else if (wasLong) {
        if (signedQty < 0) {
            double close = std::min(-signedQty, pos);
            realized += close * (px - avgEntry);
            if (baseInt < -EPS) avgEntry = px;
            else if (std::fabs(baseInt) <= EPS) avgEntry = 0;
        } else {
            avgEntry = (avgEntry * pos + px * signedQty) / baseInt;
        }
    } else {
        if (signedQty > 0) {
            double close = std::min(signedQty, -pos);
            realized += close * (avgEntry - px);
            if (baseInt > EPS) avgEntry = px;
            else if (std::fabs(baseInt) <= EPS) avgEntry = 0;
        } else {
            avgEntry = (avgEntry * (-pos) + px * (-signedQty)) / (-baseInt);
        }
    }
    mo.push_back({anchorSec, isBuy ? 1 : -1, px, qty, 0, isEscape});
}

void LiveLedger::updateMarkouts(double now, double mid) {
    if (mid <= 0) return;
    for (auto& f : mo)
        while (f.done < 3 && now - f.t >= MO_H[f.done]) {
            double bps = f.sgn * (mid - f.px) / f.px * 1e4;
            if (f.esc) { moSumE[f.done] += bps * f.qty * f.px;
                         moVolE[f.done] += f.qty * f.px; }
            else       { moSum[f.done] += bps * f.qty * f.px;
                         moVol[f.done] += f.qty * f.px; }
            ++f.done;
        }
    while (!mo.empty() && mo.front().done >= 3) mo.pop_front();
}

double LiveLedger::markout(int i) const { return moVol[i] > 0 ? moSum[i] / moVol[i] : 0.0; }

double LiveLedger::markoutE(int i) const { return moVolE[i] > 0 ? moSumE[i] / moVolE[i] : 0.0; }

} // namespace spred
