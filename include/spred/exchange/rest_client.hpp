#pragma once

#include "spred/net/http.hpp"
#include "spred/core/rate_limit.hpp"
#include "spred/core/encoding.hpp"
#include "spred/parsing/json.hpp"

namespace spred {

struct RestResult {
    bool ok = false;          // HTTP 200 и нет {"code":...} ошибки
    bool transportOk = false; // false = ответа нет, состояние НЕИЗВЕСТНО
    int httpCode = 0;
    long errCode = 0;         // из {"code":X} при ошибке
    std::string errMsg;
    std::string body;
    double rttMs = 0;
};


std::string buildSignedQs(const std::string& params, long long tsMs,
                                 long recvWindowMs, const std::string& secret);
struct RestClient {
    HttpClient http;
    std::string key, secret;
    std::string symbol;
    long long timeOffMs = 0;      // server - local
    double lastTimeSync = 0;
    double timeSyncSec = 60;
    long recvWindowMs = 10000;
    bool postOnly = true;         // [fix F26] LIMIT_MAKER
    int errCount = 0;
    int timeSyncs = 0;

    RestClient(const std::string& host, bool noVerify);

    long long srvNowMs() const;

    bool timeSync();

    // подписанный запрос: params — строка "a=1&b=2" БЕЗ timestamp/signature
    RestResult signedReq(const std::string& method, const std::string& path,
                         std::string params);

    // запрос с только apikey (userDataStream — подпись не нужна по докам;
    // при отказе автоматически пробуем подписанный вариант)
    RestResult keyOnlyReq(const std::string& method, const std::string& path,
                          const std::string& params);

    // ----------------------------- торговые операции
    // place: ok + orderId; transportFail => состояние неизвестно (resolve!)
    std::string stpMode;   // [fix F35] MM_STP_MODE; пусто = не отправлять
    std::string stpParam() const;
    RestResult placeLimit(const std::string& side, double px, double qty,
                          const std::string& cid);
    RestResult marketOrder(const std::string& side, double qty, double quoteQty,
                           const std::string& cid);
    RestResult cancelOrder(const std::string& oid, const std::string& cid);
    RestResult getOrder(const std::string& oid, const std::string& cid);
    RestResult openOrders();
    RestResult account();
    // [F54] сделки ордера с комиссией (SPOT_ACCOUNT_READ, вес 10)
    RestResult myTrades(const std::string& oid);
    // [F51] по докам MEXC userDataStream — SIGNED; key-only — запасной вариант
    RestResult listenKeyCreate();
    RestResult listenKeyKeepalive(const std::string& lk);
    RestResult listenKeyClose(const std::string& lk);
};

} // namespace spred
