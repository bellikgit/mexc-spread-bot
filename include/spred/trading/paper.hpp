#pragma once

#include "spred/trading/config.hpp"
#include "spred/trading/order.hpp"
#include "spred/core/rate_limit.hpp"
#include "spred/market/flow.hpp"

namespace spred {

struct PaperTrader {
    double base = 0;
    double quote = 0;
    double initialQuote = 0;
    double avgEntry = 0;
    int orderSeq = 1;

    double base0 = 0, mid0 = 0, setupCost = 0;

    Config cfg;
    SideBook buy, sell;
    TokenBucket rate;

    double qtyStep = 1e-8;
    // [v2.13.1 ФИКС 16] два часовых домена:
    //   exchOffMs     — steady -> send-домен (по push.sendTimeMs), только
    //                   сортировка/fallback/диагностика, филл-гейт НЕ трогает;
    //   exchOffDealMs — steady -> deal-домен (по d.timeMs), филл-гейт,
    //                   ghost-окно, конвертация времени для flow.
    double exchOffMs = 0;
    double exchOffDealMs = 0;
    double rttMs = 200;          // md-rtt по ping — только диагностика
    bool exchOffInit = false;
    bool dealOffInit = false;

    double orderLatMs = 150.0;   // база place-latency
    double cancelLatMs = 150.0;  // база cancel-latency

    std::mt19937 rng{(unsigned)nowMsSteady()};   // [v2.13]

    bool pauseBuy = false, pauseSell = false;

    // [v2.14] паузы сторон (steady-секунды; 0 = неактивна)
    double escPauseBuyUntil = 0, escPauseSellUntil = 0;   // ФИКС 18
    double cutPauseBuyUntil = 0, cutPauseSellUntil = 0;   // ФИКС 19
    int cutCnt = 0;                                        // событий cutoff
    bool curPrintInside = false;   // [F45] текущий принт строго внутри спреда
    long prAll = 0, prInside = 0, prCutFills = 0;          // [F45] счётчики
    long prPassFills = 0;          // [F49] внутренние принты, пропущенные с вероятностью p
    std::mt19937 prng{12345u};     // [F49] отдельный RNG (не сдвигает латентности rng)
    double prUrand();
    int realSyncCnt = 0;           // [F50] shadow-ордер закрыт по реальному полному филлу
    int realSweepCnt = 0;          // [F50] cooldown/cutoff от реального филла
    std::deque<std::array<double, 3>> recentPr;   // [F50] (now, price, side) публичных принтов, 3 c
    // [F50] реальный ордер стороны исполнен полностью, а shadow ещё стоит:
    // закрываем shadow-ордер БЕЗ проводки в paper-P&L (fill_cal не искажается),
    // дальше та же последовательность, что после полного paper-филла.
    bool realSync(SideBook& sb, double now, std::vector<std::string>& ev);

    // [v2.16 P0-3] окно живости ордера: [startExchMs, untilExchMs].
    // Сделка старше активации ордера физически не могла его заполнить.
    struct Ghost { bool active = false; int side = 0; double price = 0, qty = 0;
                   double startExchMs = 0, untilExchMs = 0; };
    Ghost ghostBuy, ghostSell;

    struct GapOrd { int side; double price, qty; };
    std::vector<GapOrd> gapCheck;
    bool gapPending = false;

    struct FillRec { double t; int sgn; double px, proxyPx, takerPx, qty; int done; int reason; };
    std::deque<FillRec> mo;
    double moSum[3] = {0, 0, 0}, moVol[3] = {0, 0, 0};
    double pmoSum[3] = {0, 0, 0}, pmoVol[3] = {0, 0, 0};
    double tmoSum[3] = {0, 0, 0}, tmoVol[3] = {0, 0, 0};
    static constexpr double MO_H[3] = {5.0, 30.0, 60.0};

    // [v2.12 ФИКС 10] mo и объём по типам филлов
    double rmoSum[FR_N][3] = {};
    double rmoVol[FR_N][3] = {};
    int rCnt[FR_N] = {};

    // [v2.13 ФИКС 12] распределения mo (копятся при закрытии горизонтов)
    std::vector<double> moH[3];            // все филлы (кроме ESCAPE), по горизонтам
    std::vector<double> moBuy30, moSell30; // стороны, 30c
    std::vector<double> moR30[FR_N];       // по reason, 30c (вкл. ESCAPE)

    // [v2.13 ФИКС 13] escape slippage
    int escCnt = 0;
    double escSlipSum = 0, escSlipWorst = 0;

    // [v2.13 ФИКС 14] fees
    double makerFees = 0, takerFees = 0;

    // [v2.13.1] часовая диагностика
    int skewQuar = 0;            // сделки в карантине (future-метка)
    double skewMaxMs = 0;        // max |send-vs-deal skew| за сессию
    bool skewAlerted = false;

    int fills = 0, buyFills = 0, sellFills = 0;
    int partialFills = 0, forcedFills = 0, throttled = 0, cancels = 0, replaces = 0;
    int lateFills = 0, toxicFills = 0;
    int gapFills = 0;
    int gapCertain = 0, gapPossible = 0;   // [v2.12 ФИКС 8]
    double realized = 0;
    double feesPaid = 0;
    double volumeTraded = 0;
    double lastMid = 0;
    double sellSmallSecs = 0;
    bool invInitialized = false, halted = false;

    double equity(double mid) const;
    double totalPnl(double mid) const;
    double posValue(double mid) const;

    double roundQty(double q) const;

    // [v2.13.1 ФИКС 16] калибровка deal-часов: sample = steadyRecv - d.timeMs.
    // Вниз (свежее) — быстро (0.3): отслеживаем конвергенцию/сдвиг часов;
    // вверх (старение/задержка доставки) — медленно (0.05): не накапливаем
    // bias доставки. Стейл-батчи (sample выше оценки на >5с) и мусорные
    // future-метки (ниже на >30с) отбрасываются; шаг вниз клампится 1с —
    // одиночная мусорная метка не утащит оценку.
    void calibDealClock(double sample);

    // [v2.13.1] + = send-часы ОТСТАЮТ от deal-часов (метки сделок «впереди»)
    double clockSkewMs() const;

    // [v2.13 ФИКС 11] множитель латентности: кусочно-линейный inverse-CDF.
    // p50 = 1.0, p90 = latP90Mult, p99 = latP99Mult, хвост до 1.5x p99.
    double sampleLatMult();

    void applyTrade(double signedQty, double px);

    static double levelQty(const BookState& b, Order::Side side, double px, double tick);

    // [v2.16 P0-3] makeGhost с ЯВНЫМИ параметрами (не sb-состоянием) —
    // меньше шансов перепутать cur/nxt при будущих правках.
    void makeGhost(const Order& o, double placeSentMs, double placeLatMs,
                   double cancelSentMs, double cancelLat);

    void requestQuote(SideBook& sb, Order::Side side, double px, double qty,
                      double rqThresh, double now, std::vector<std::string>& ev);

    void requestCancel(SideBook& sb, double now, std::vector<std::string>& ev);

    void advance(SideBook& sb, const BookState& book, double tick, double now);

    // [v2.11 FIX 4] queue-модель
    void onDepth(SideBook& sb, const BookState& book, double tick);

    // [v2.11 FIX 1] раздельные знаменатели; [v2.13] распределения + ESCAPE
    // ESCAPE-филлы идут ТОЛЬКО в per-reason статистику — вынужденный выход
    // не должен разбавнять mo/pxmo/txmo стратегии.
    void updateMarkouts(double now, double mid);
    double markout(int i) const;
    double proxyMarkout(int i) const;
    double takerMarkout(int i) const;
    double reasonMo(int r, int i) const;
    double reasonUsd(int r, int i) const;
    int reasonCnt(int r) const;

    // [v2.16 P0-4] fillAnchorSec — БИРЖЕВОЕ время сделки (deal-домен, СЕКУНДЫ),
    // явный параметр (не shared-поле: applyEvent обрабатывает сделки пачками,
    // поле бы «переживало» породившую его сделку). mo-окна отсчитываются
    // от него; вся остальная логика doFill (pendingUntil, lastSweepAt...)
    // остаётся на реальном now.
    void doFill(SideBook& sb, Order::Side side, double px, double qty, bool toxic,
                double now, double fillAnchorSec, const BookState& book,
                std::vector<std::string>& ev, int reason);

    void onPrint(SideBook& sb, Order::Side side, const MexcPush::Deal& d,
                 const BookState& book, double tick, double now,
                 std::vector<std::string>& ev);
    void onGhost(Ghost& g, const MexcPush::Deal& d, const BookState& book,
                 double now, std::vector<std::string>& ev);
};

} // namespace spred
