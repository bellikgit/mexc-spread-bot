#include "spred/exchange/rest_client.hpp"

namespace spred {

std::string buildSignedQs(const std::string& params, long long tsMs,
                                 long recvWindowMs, const std::string& secret) {
    std::string qs = params;
    if (!qs.empty()) qs += "&";
    qs += "timestamp=" + std::to_string(tsMs) +
          "&recvWindow=" + std::to_string(recvWindowMs);
    qs += "&signature=" + hmacSha256Hex(secret, qs);
    return qs;
}

RestClient::RestClient(const std::string& host, bool noVerify) : http(host, noVerify) {}

long long RestClient::srvNowMs() const { return epochMs() + timeOffMs; }

bool RestClient::timeSync() {
    long long t0 = epochMs();
    HttpResp r = http.request("GET", "/api/v3/time", "", {}, 4000);
    long long t1 = epochMs();
    if (!r.transportOk || r.code != 200) return false;
    double srv = 0;
    if (!jsonFindNum(r.body, "serverTime", srv) || srv <= 0) return false;
    timeOffMs = (long long)srv - (t0 + t1) / 2;
    lastTimeSync = nowSecs();
    ++timeSyncs;
    return true;
}

RestResult RestClient::signedReq(const std::string& method, const std::string& path,
                         std::string params) {
    // [fix F41] пауза после 429/418: запрос НЕ отправлен (биржа его не
    // видела) -> обычный отказ; отмены пропускаем — снять риск важнее
    if (method != "DELETE" && (realSecs() < g_restBlockUntil ||
                               SharedRate::mono() < g_sr.blockUntil())) {   // [F61]
        RestResult b;
        b.transportOk = true;
        b.httpCode = 429;
        b.errCode = 429;
        b.errMsg = "local backoff after 429/418";
        return b;
    }
    g_ipW.wait(mexcWeight(method, path));   // [fix F42]
    std::string qs = buildSignedQs(params, srvNowMs(), recvWindowMs, secret);
    RestResult rr;
    std::vector<std::pair<std::string, std::string>> hdrs = {
        {"X-MEXC-APIKEY", key}};
    HttpResp r;
    // [F51] все параметры в query, тело пустое (как официальный MEXC SDK);
    // MM_REST_FORM_BODY=1 — старый вариант (POST/PUT в form-теле)
    if (method == "GET" || method == "DELETE" || !g_restFormBody)
        r = http.request(method, path + "?" + qs, "", hdrs, 4000);
    else
        r = http.request(method, path, qs, hdrs, 4000);
    restNoteLimit(r.code, r.retryAfter);   // [fix F41]
    rr.transportOk = r.transportOk;
    rr.httpCode = r.code;
    rr.rttMs = r.rttMs;
    rr.body = r.body;
    if (!r.transportOk) return rr;
    // MEXC ошибка: HTTP 4xx/5xx и/или тело {"code":X,"msg":"..."}.
    // Успех: HTTP 200 без error-"code" (некоторые эндпоинты шлют code:0).
    {
        double code = 0;
        std::string msg;
        bool hasCode = jsonFindNum(r.body, "code", code);
        bool hasMsg = jsonFindStr(r.body, "msg", msg);
        bool errBody = hasCode && hasMsg && code != 0 && code != 200;
        if (r.code >= 200 && r.code < 300 && !errBody) {
            rr.ok = true;
            return rr;
        }
        rr.ok = false;
        rr.errCode = hasCode ? (long)code : r.code;
        rr.errMsg = msg;
        ++errCount;
        return rr;
    }
}

RestResult RestClient::keyOnlyReq(const std::string& method, const std::string& path,
                          const std::string& params) {
    RestResult rr;
    std::vector<std::pair<std::string, std::string>> hdrs = {
        {"X-MEXC-APIKEY", key}};
    // параметры в query, тело пустое (Binance-совместимый стиль)
    std::string full = params.empty() ? path : path + "?" + params;
    g_ipW.wait(1);   // [fix F42]
    HttpResp r = http.request(method, full, "", hdrs, 4000);
    restNoteLimit(r.code, r.retryAfter);   // [fix F41]
    rr.transportOk = r.transportOk;
    rr.httpCode = r.code;
    rr.rttMs = r.rttMs;
    rr.body = r.body;
    if (!r.transportOk) return rr;
    double code = 0;
    bool hasCode = jsonFindNum(r.body, "code", code);
    if (r.code == 200 && (!hasCode || code == 0 || code == 200)) {
        rr.ok = true;
        return rr;
    }
    std::string msg;
    jsonFindStr(r.body, "msg", msg);
    rr.errCode = hasCode ? (long)code : r.code;
    rr.errMsg = msg;
    ++errCount;
    return rr;
}

std::string RestClient::stpParam() const {
    return stpMode.empty() ? std::string() : "&stpMode=" + stpMode;
}

RestResult RestClient::placeLimit(const std::string& side, double px, double qty,
                          const std::string& cid) {
    // [fix F26] post-only: при пересечении книги биржа отклоняет ордер
    // (обычный place-fail), а не исполняет его тейкером
    return signedReq("POST", "/api/v3/order",
                     "symbol=" + symbol + "&side=" + side +
                     (postOnly ? std::string("&type=LIMIT_MAKER")
                               : std::string("&type=LIMIT&timeInForce=GTC")) +
                     "&quantity=" + fmtDec(qty) +
                     "&price=" + fmtDec(px) + "&newClientOrderId=" + cid + stpParam());
}

RestResult RestClient::marketOrder(const std::string& side, double qty, double quoteQty,
                           const std::string& cid) {
    std::string p = "symbol=" + symbol + "&side=" + side + "&type=MARKET" +
                    "&newClientOrderId=" + cid;
    if (side == "BUY") p += "&quoteOrderQty=" + fmtDec(quoteQty);
    else               p += "&quantity=" + fmtDec(qty);
    p += stpParam();   // [fix F35]
    return signedReq("POST", "/api/v3/order", p);
}

RestResult RestClient::cancelOrder(const std::string& oid, const std::string& cid) {
    std::string p = "symbol=" + symbol;
    if (!oid.empty()) p += "&orderId=" + oid;
    else              p += "&origClientOrderId=" + cid;
    return signedReq("DELETE", "/api/v3/order", p);
}

RestResult RestClient::getOrder(const std::string& oid, const std::string& cid) {
    std::string p = "symbol=" + symbol;
    if (!oid.empty()) p += "&orderId=" + oid;
    else              p += "&origClientOrderId=" + cid;
    return signedReq("GET", "/api/v3/order", p);
}

RestResult RestClient::openOrders() {
    return signedReq("GET", "/api/v3/openOrders", "symbol=" + symbol);
}

RestResult RestClient::account() { return signedReq("GET", "/api/v3/account", ""); }

RestResult RestClient::myTrades(const std::string& oid) {
    return signedReq("GET", "/api/v3/myTrades", "symbol=" + symbol + "&orderId=" + oid);
}

RestResult RestClient::listenKeyCreate() {
    RestResult r = signedReq("POST", "/api/v3/userDataStream", "");
    if (r.ok) return r;
    RestResult k = keyOnlyReq("POST", "/api/v3/userDataStream", "");
    if (k.ok) return k;
    lprintf("[live] listenKey: signed code %ld %s | key-only code %ld %s\n",
            r.errCode, r.errMsg.c_str(), k.errCode, k.errMsg.c_str());
    return r;
}

RestResult RestClient::listenKeyKeepalive(const std::string& lk) {
    RestResult r = signedReq("PUT", "/api/v3/userDataStream", "listenKey=" + lk);
    if (r.ok) return r;
    return keyOnlyReq("PUT", "/api/v3/userDataStream", "listenKey=" + lk);
}

RestResult RestClient::listenKeyClose(const std::string& lk) {
    RestResult r = signedReq("DELETE", "/api/v3/userDataStream", "listenKey=" + lk);
    if (r.ok) return r;
    return keyOnlyReq("DELETE", "/api/v3/userDataStream", "listenKey=" + lk);
}

} // namespace spred
