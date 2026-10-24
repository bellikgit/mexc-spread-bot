#pragma once

#include "spred/core/runtime.hpp"

namespace spred {

struct LiveLedger {
    // exchange truth (поллинг + private.account)
    double baseFree = 0, baseLocked = 0, quoteFree = 0, quoteLocked = 0;
    bool acctInit = false;
    // internal session (с нуля, по филлам)
    double baseInt = 0, quoteNet = 0;
    double realized = 0, avgEntry = 0;
    double feesQuote = 0, feesBase = 0;      // в валюте сбора
    double feesOther = 0; std::string feesOtherCur;
    int fills = 0, buyFills = 0, sellFills = 0;
    int ghostFillsWs = 0, ghostFillsRecon = 0;
    double volume = 0;
    double base0 = 0;                        // стартовый инвентарь кошелька
    double driftMaxBase = 0;                 // max |internal - exchange|
    // реальные маркауты (те же горизонты, что у shadow)
    struct LFill { double t; int sgn; double px, qty; int done; int esc; };
    std::deque<LFill> mo;
    double moSum[3] = {0, 0, 0}, moVol[3] = {0, 0, 0};
    double moSumE[3] = {0, 0, 0}, moVolE[3] = {0, 0, 0};   // escape-филлы
    static constexpr double MO_H[3] = {5.0, 30.0, 60.0};

    double totalBase() const;
    double totalQuote() const;
    double sessionPnl(double mid) const;
    double posValue(double mid) const;  // как paper: от base0

    // [fix F4] комиссия отдельно: нужна, когда сама сделка уже проведена
    // дельтой cumQty (кредит), а fee приходит только с deal
    void applyFee(double fee, const std::string& feeCur,
                  const std::string& baseAsset, const std::string& quoteAsset);

    void applyRealFill(bool isBuy, double px, double qty, double fee,
                       const std::string& feeCur, const std::string& baseAsset,
                       const std::string& quoteAsset, double anchorSec,
                       int isEscape);

    void updateMarkouts(double now, double mid);
    double markout(int i) const;
    double markoutE(int i) const;
};
constexpr double LiveLedger::MO_H[3];

} // namespace spred
