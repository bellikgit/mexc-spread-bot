#pragma once

#include "spred/platform.hpp"

namespace spred {

struct Level { double price = 0, qty = 0; };

struct BookState {
    std::vector<Level> bids, asks;
    // [v2.16 P0-2] две метки свежести:
    //   lastRecv   — последнее ПРИМЕНЁННОЕ событие (stale-gate, как было);
    //   lastPacket — последний ПРИНЯТЫЙ кадр (диагностика: сеть жива,
    //                пока evq копит события внутри reorder-окна).
    // Пакет в reorder-очереди НЕ делает книгу безопасной — stale-гейт
    // остаётся на lastRecv.
    double lastRecv = 0;
    double lastPacket = 0;

    // [v2.11 FIX 6] валидация BBO: finite, price>0, qty>0, не пересечены
    bool ok() const {
        if (bids.empty() || asks.empty()) return false;
        const Level& b = bids[0];
        const Level& a = asks[0];
        return std::isfinite(b.price) && std::isfinite(b.qty) &&
               std::isfinite(a.price) && std::isfinite(a.qty) &&
               b.price > 0 && a.price > 0 && b.qty > 0 && a.qty > 0 &&
               a.price > b.price;
    }
    double bestBid() const { return bids.empty() ? 0 : bids[0].price; }
    double bestAsk() const { return asks.empty() ? 0 : asks[0].price; }
    double mid() const { return ok() ? (bestBid() + bestAsk()) / 2 : 0; }
    double spreadBps() const {
        if (!ok()) return 0;
        return (bestAsk() - bestBid()) / mid() * 1e4;
    }
    double imbalance() const {
        double bb = 0, ba = 0;
        for (size_t i = 0; i < bids.size() && i < 10; ++i) bb += bids[i].qty;
        for (size_t i = 0; i < asks.size() && i < 10; ++i) ba += asks[i].qty;
        return (bb + ba) > 0 ? bb / (bb + ba) : 0.5;
    }
};

double walkBook(const BookState& b, bool isBuy, double qty, double& filled);
double walkBookSkip(const BookState& b, bool isBuy, double qty,
                           double& filled, int skipLevels);
double walkBudgetBuy(const BookState& b, double budgetQuote);
void subOwnLevel(BookState& b, int side, double px, double q, double eps);

} // namespace spred
