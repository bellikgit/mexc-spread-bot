#include "spred/market/book.hpp"

namespace spred {

double walkBook(const BookState& b, bool isBuy, double qty, double& filled) {
    const std::vector<Level>& lv = isBuy ? b.asks : b.bids;
    double rem = qty, cost = 0, q = 0;
    for (const auto& l : lv) {
        double take = std::min(rem, l.qty);
        cost += take * l.price;
        q += take;
        rem -= take;
        if (rem <= 1e-12) break;
    }
    filled = q;
    return q > 0 ? cost / q : 0.0;
}

// [v2.15 ФИКС 22] стресс-escape: walk с уровня N+1 — симуляция withdrawal
// (все MM сняли ближние котировки одновременно). skip=0 == walkBook.
// filled = фактически заполненное (книга может кончиться раньше qty).
double walkBookSkip(const BookState& b, bool isBuy, double qty,
                           double& filled, int skipLevels) {
    const std::vector<Level>& lv = isBuy ? b.asks : b.bids;
    double rem = qty, cost = 0, q = 0;
    for (size_t i = (size_t)std::max(0, skipLevels); i < lv.size(); ++i) {
        double take = std::min(rem, lv[i].qty);
        cost += take * lv[i].price;
        q += take;
        rem -= take;
        if (rem <= 1e-12) break;
    }
    filled = q;
    return q > 0 ? cost / q : 0.0;
}

// [v2.9.1 FIX A / v2.10 FIX C] закупка по бюджету, битые уровни пропускаем
double walkBudgetBuy(const BookState& b, double budgetQuote) {
    double rem = budgetQuote, q = 0;
    for (const auto& l : b.asks) {
        if (rem <= 0) break;
        if (l.price <= 0 || l.qty <= 0) continue;
        double afford = rem / l.price;
        double take = std::min(l.qty, afford);
        q += take;
        rem -= take * l.price;
        if (take < l.qty) break;
    }
    return q;
}

void subOwnLevel(BookState& b, int side, double px, double q, double eps) {
    auto& lv = side == 0 ? b.bids : b.asks;
    for (size_t k = 0; k < lv.size(); ++k)
        if (std::fabs(lv[k].price - px) < eps) {
            lv[k].qty -= q;
            if (lv[k].qty <= std::max(1e-12, q * 1e-6))
                lv.erase(lv.begin() + (long)k);
            break;
        }
}

} // namespace spred
