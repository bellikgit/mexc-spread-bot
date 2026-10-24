#pragma once

#include "spred/platform.hpp"

namespace spred {

struct Order {
    enum Side { BUY, SELL } side = BUY;
    double price = 0, qty = 0, filled = 0;
    int id = 0;
};

enum class QState { NONE, WAIT_PLACE, LIVE, WAIT_CANCEL };

// [v2.12 ФИКС 10] классификация филлов. [v2.13] + ESCAPE (только per-reason)
enum FillReason { FR_QUEUE = 0, FR_THROUGH, FR_TOXIC, FR_GHOST, FR_GAP, FR_ESCAPE, FR_N };
inline const char* FR_NAMES[FR_N] = {"QUEUE", "THROUGH", "TOXIC", "GHOST", "GAP", "ESCAPE"};

struct SideBook {
    Order cur;
    Order nxt;
    bool hasNext = false;
    QState st = QState::NONE;
    double pendingUntil = 0;
    double placeSentMs = 0;
    double cancelSentMs = 0;
    double queueAhead = 0;
    double lastLevelQty = 0;
    double tradedAtLevel = 0;
    bool queueInited = false;      // [v2.11 FIX 3]
    double curPlaceLatMs = 150.0;  // [v2.13] латентность ЭТОГО ордера
    double curCancelLatMs = 150.0; // [v2.13] латентность ЭТОЙ отмены
    double lastRequoteAt = 0;
    double lastSweepAt = 0;
};

} // namespace spred
