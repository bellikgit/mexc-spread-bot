#include "spred/core/rate_limit.hpp"

namespace spred {

SharedRate g_sr;
double g_restBlockUntil = 0;
bool g_restFormBody = false;
int g_rate429 = 0, g_rate418 = 0;
WeightBucket g_ipW;

double realSecs() {
    return std::chrono::duration<double>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

void restNoteLimit(int http, double retryAfter) {
    if (http != 429 && http != 418) return;
    if (http == 429) ++g_rate429; else ++g_rate418;
    double wait = retryAfter > 0 ? retryAfter : (http == 418 ? 120.0 : 2.0);
    wait = std::min(wait, 3.0 * 86400.0);
    double now = realSecs();
    if (now + wait > g_restBlockUntil) {
        bool fresh = now >= g_restBlockUntil;
        g_restBlockUntil = now + wait;
        g_sr.noteBlock(SharedRate::mono() + wait);   // [F61]
        if (fresh) lprintf("[live] HTTP %d от MEXC: пауза REST %.0f c (отмены разрешены)\n",
                           http, wait);
    }
}

void restWaitBlock(double maxSec) {   // [fix F41] для стартовых повторов
    double w = std::min(maxSec, g_restBlockUntil - realSecs());
    if (w > 0) std::this_thread::sleep_for(std::chrono::milliseconds(long(w * 1000) + 50));
}

double mexcWeight(const std::string& m, const std::string& path) {
    if (path.rfind("/api/v3/openOrders", 0) == 0) return m == "GET" ? 3 : 1;
    if (path.rfind("/api/v3/order", 0) == 0) return m == "GET" ? 2 : 1;
    if (path.rfind("/api/v3/account", 0) == 0) return 10;
    if (path.rfind("/api/v3/myTrades", 0) == 0) return 10;
    if (path.rfind("/api/v3/exchangeInfo", 0) == 0) return 25;
    return 1;
}

} // namespace spred
