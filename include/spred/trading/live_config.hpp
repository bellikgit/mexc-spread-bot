#pragma once

#include "spred/core/runtime.hpp"

namespace spred {

struct LiveSide {
    bool open = false;             // по нашим данным ордер стоит на бирже
    std::string cid, oid;
    double px = 0, qty = 0;        // параметры ВЫСТАВЛЕННОГО ордера
    double dealtQty = 0;           // [F4] qty, УЖЕ проведённая в ledger (любой источник)
    double dealtAmt = 0;
    double openedAt = 0;
    double dealSeenQty = 0;        // [F4] сумма qty, подтверждённая приватными deals
    int fails = 0;
    double nextRetryAt = 0;
    // статистика
    int places = 0, cancels = 0, placeFails = 0, cancelFails = 0;
    double placeLatSum = 0, placeLatMax = 0, cancelLatSum = 0, cancelLatMax = 0;
};

struct LiveCfg {
    bool live = false;
    bool confirm = false;
    bool tlsNoVerify = false;
    bool cleanStart = true;
    int cleanMode = 1;             // [fix F39] 1 = только ордера бота, 2 = все
    bool keepOnExit = false;
    bool reconAdopt = false;
    bool allowReplayLive = false;
    bool sellWallet = false;       // [fix F6] MM_SELL_WALLET_BASE: sell из ВСЕГО кошелька (v3.1)
    bool realSync = true;          // [F50] MM_LIVE_SYNC: реальные филлы -> shadow (cooldown/sync)
    bool selfExcl = true;          // [F52] MM_SELF_EXCL: стратегия видит книгу без наших ордеров
    double selfExclSec = 2.0;      // [F52] MM_SELF_EXCL_SEC: сколько помнить снятый ордер
    int maxFillsMin = 6;           // [F53] MM_LIVE_MAX_FILLS_MIN: breaker (0 = выкл)
    bool feeLookup = true;         // [F54] MM_FEE_LOOKUP: комиссии через myTrades
    double breakerSec = 600;       // [F53] MM_LIVE_BREAKER_SEC: пауза пары
    double reconSec = 15, balanceSec = 30;
    double usdtReserve = 1.0;      // не трогаем последний USDT кошелька
    double startUsdt = 10000;      // локальный лимит задействования quote
    std::string apiHost = "api.mexc.com";
    std::string killFile;
};

} // namespace spred
