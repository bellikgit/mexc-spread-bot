#pragma once

#include "spred/core/runtime.hpp"

namespace spred {

extern std::string g_apiHost;
struct SymbolInfo {
    bool ok = false;
    bool complete = false;   // [fix F15] есть И tick, И qtyStep (обязательно для live)
    bool makerOk = true;     // [fix F26] LIMIT_MAKER разрешён (или orderTypes не указан)
    double tick = 0.01;
    double qtyStep = 1e-8;
    double minNotional = 1.0;
    std::string baseAsset;    // [v3.0] для live: что покупаем/продаём
    std::string quoteAsset;   // [v3.0] валюта котировки (USDT)
};

// [v2.7 A] MEXC v3: quotePrecision — ЧИСЛО ЗНАКОВ (4 -> тик 1e-4);
// baseSizePrecision — УЖЕ ШАГ ("0.00001"; 0 -> шаг 1);

SymbolInfo fetchSymbolInfo(const std::string& symbol);

} // namespace spred
