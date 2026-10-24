#pragma once

#include "spred/exchange/rest_client.hpp"
#include "spred/trading/live_ledger.hpp"
#include "spred/trading/live_config.hpp"
#include "spred/protocol/mexc.hpp"

namespace spred {

struct LiveExecutor {
    RestClient rc;
    LiveCfg* lcfg = nullptr;
    LiveSide s[2];                 // 0=BUY, 1=SELL
    std::string baseAsset = "BASE", quoteAsset = "USDT";
    LiveLedger ledger;
    TokenBucket ownRate;
    TokenBucket* liveRate = &ownRate;   // [v3.1] в main — общий бюджет аккаунта
    std::set<std::string> escCids;          // cid маркет-ордеров escape
    struct EscPend { std::string cid, oid; bool isBuy = false; double at = 0; };
    std::vector<EscPend> escPending;        // [fix F23] ждут сверки getOrder
    std::set<std::string> knownForeignOids; // чужие ордера (залогированы)
    std::set<std::string> ownOids;          // [fix F27] oid ордеров ЭТОЙ сессии
    std::deque<std::string> ownOidsOrder;
    int foreignDeals = 0;
    void addOwnOid(const std::string& oid);
    // [fix F4] КРЕДИТ-УЧЁТ филлов. dealtQty — qty, уже проведённая в ledger из
    // ЛЮБОГО источника (deals, дельта cumQty из order-апдейта, resolve);
    // dealSeenQty — сумма qty, подтверждённая приватными deals. Разница —
    // «кредит»: в ledger уже есть, сама сделка ещё не пришла. Пришедшая сделка
    // гасит кредит (проводится только её fee), всё сверх кредита — новый филл.
    // Не зависит от порядка апдейт/сделка и от того, закрыт ордер или нет.
    struct ClosedOrd { std::string cid, oid; double credit = 0, at = 0; };
    std::vector<ClosedOrd> closedOrds;
    // [fix F9] дедуп приватных сделок: повторная доставка одной сделки
    // (tradeId парсился, но нигде не использовался) = двойной учёт позиции
    std::set<std::string> seenDeals;
    // [fix F21] qty, проведённая по WS-сделкам ордеров без стороны (сироты)
    std::map<std::string, double> unknownDealt;
    std::deque<std::string> seenDealsOrder;
    int dupDeals = 0;

    void archiveClosed(const LiveSide& ls, double now);
    void pruneClosed(double now);
    ClosedOrd* findClosed(const std::string& cid, const std::string& oid);
    // [F50] события реальных филлов по сторонам для main-loop (sweep-cooldown,
    // cutoff и REAL_SYNC shadow-ордера). realFillEv растёт при любой новой
    // реальной qty по стороне; realFull — ордер стороны исполнен целиком.
    long realFillEv[2] = {0, 0};
    long realFillSeen[2] = {0, 0};
    double realFillPx[2] = {0, 0};
    double realFillAt[2] = {0, 0};
    bool realFull[2] = {false, false};
    int mirId[2] = {0, 0};         // id shadow-ордера, который зеркалит реальный
    int fullMirId[2] = {0, 0};     // id shadow-ордера в момент полного реального филла
    std::string mirCid[2];
    void noteDealt(const LiveSide& ls, bool wasOpen);
    // [F54] комиссии филлов, пришедших БЕЗ приватной сделки (cumQty/REST):
    // через ~2 c после филла GET /api/v3/myTrades?orderId= -> commission.
    // Дедуп по tradeId с WS-сделками: комиссия каждой сделки — ровно один раз.
    bool feeLookup = true;
    std::set<std::string> feeTid;
    std::deque<std::string> feeTidOrder;
    struct FeeQ { std::string oid; double at; int tries; };
    std::vector<FeeQ> feeQ;
    int feeLookups = 0, feeTrades = 0, feeTaker = 0, feeFail = 0;
    bool markFeeTid(const std::string& tid);
    void queueFee(const std::string& oid, double at);
    void pollFees(double now, std::vector<std::string>& ev);
    int reconEvents = 0, orphanCancels = 0, resolves = 0;
    int wsPrivOrders = 0, wsPrivDeals = 0, wsPrivAcct = 0;
    double lastRecon = 0, lastBalance = 0;
    bool initDone = false;

    LiveExecutor(const std::string& host, bool noVerify);

    std::string newCid();

    // [fix F38/F39] cid бота ЛЮБОГО запуска: sp<base36 pid>x<base36 seq>
    static bool botCid(const std::string& c);
    bool ourCid(const std::string& c) const;

    // ------------------------- приватные WS-события
    // сделка: истина по филлам. Ищет сторону по cid/oid; не нашла — всё
    // равно применяем (ордер мог быть снят до этого — GHOST-класс).
    void onPrivDeal(const MexcPush::PrivDeal& d, double anchorSec,
                    std::vector<std::string>& ev);

    // ордер-апдейт: терминальные статусы закрывают сторону; расхождение
    // cumQty vs dealtQty => пропущенная сделка => дельта-филл (GHOST-WS)
    void onPrivOrder(const MexcPush::PrivOrder& o, double now,
                     std::vector<std::string>& ev);

    void onPrivAccount(const MexcPush::PrivAccount& a);

    // ------------------------- REST-действия (синхронные, с разруливанием)
    bool transportUnknown(LiveSide& ls, const char* what, double now,
                          std::vector<std::string>& ev);

    static bool orderNotFound(const RestResult& r);

    void resolve(LiveSide& ls, bool isBuySide, double now,
                 std::vector<std::string>& ev, bool countFail = true);

    void place(LiveSide& ls, bool isBuy, double px, double qty, double now,
               std::vector<std::string>& ev);

    void cancel(LiveSide& ls, bool isBuy, double now,
                std::vector<std::string>& ev);

    // ------------------------- главная точка: привести сторону к желаемому
    void ensureSide(int idx, bool want, double px, double qty, double rqThresh,
                    double now, std::vector<std::string>& ev);

    void cancelAll(std::vector<std::string>& ev);

    // escape: реальный MARKET-ордер на излишек позиции
    bool escapeMarket(bool isBuy, double qty, double quoteQty, double now,
                      std::vector<std::string>& ev);

    // ------------------------- реконсиляция (паранойя вдвойне, по пакету)
    // [fix F23] сверка escape MARKET-ордеров: executedQty сверх уже
    // проведённого по WS -> ledger; остаток -> кредит против поздних сделок
    void reconcileEscapes(double now, std::vector<std::string>& ev);

    // [fix F38] усыновить ордер бота в свободный слот той же стороны.
    // thisSession: филлы сверх пришедших по WS (unknownDealt) — наши и
    // проводятся дельтой; ордер прошлого запуска: прежние филлы уже в
    // балансе, учитываем только будущие.
    bool adoptOrder(const std::string& ob, const std::string& oid,
                    const std::string& cid, bool thisSession, double now,
                    std::vector<std::string>& ev);

    void reconcile(double now, std::vector<std::string>& ev);

    // ------------------------- баланс (истина для гейтов)
    void pollBalance(std::vector<std::string>& ev);
};

} // namespace spred
