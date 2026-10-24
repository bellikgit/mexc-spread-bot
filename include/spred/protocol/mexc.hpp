#pragma once

#include "spred/market/book.hpp"

namespace spred {

struct MexcPush {
    std::string channel, symbol;
    int64_t sendTimeMs = 0;
    bool hasTicker = false;
    double bidP = 0, bidQ = 0, askP = 0, askQ = 0;
    bool hasLimitDepth = false;
    std::vector<Level> bids, asks;
    struct Deal {
        double price = 0, qty = 0;
        int side = 0;
        int64_t timeMs = 0;
    };
    std::vector<Deal> deals;
    bool hasDeals = false;

    // [v3.0 L2] приватные каналы (wrapper-поля 304/306/307, proto из
    // mexcdevelop/websocket-proto). Заполнены только на приватном коннекте.
    struct PrivOrder {
        std::string id, clientId;
        double price = 0, qty = 0, amount = 0, avgPrice = 0;
        int orderType = 0;        // 1=LIMIT, 2=MARKET
        int tradeType = 0;        // 1=BUY, 2=SELL
        bool isMaker = false;
        double remainQty = 0, lastDealQty = 0, cumQty = 0, cumAmt = 0;
        int status = 0;           // 1=NEW 2=FILLED 3=PARTIALLY_FILLED
                                  // 4=CANCELED 5=PARTIALLY_CANCELED
        int64_t createTime = 0;
    };
    bool hasPrivOrder = false;
    PrivOrder pord;
    struct PrivDeal {
        double price = 0, qty = 0, amount = 0;
        int tradeType = 0;        // 1=BUY, 2=SELL (НАША сторона)
        bool isMaker = false, isSelfTrade = false;
        std::string tradeId, clientOrderId, orderId;
        double feeAmount = 0;
        std::string feeCurrency;
        int64_t timeMs = 0;
    };
    bool hasPrivDeal = false;
    PrivDeal pdeal;
    struct PrivAccount {
        std::string coin;
        double balance = 0, balanceChange = 0, frozen = 0, frozenChange = 0;
    };
    bool hasPrivAcct = false;
    PrivAccount pacct;
};

// [v2.7 C] поток сделок. [v2.12] события применяются по одному из

bool parsePush(const std::string& msg, MexcPush& out);

} // namespace spred
