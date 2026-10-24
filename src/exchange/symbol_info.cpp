#include "spred/exchange/symbol_info.hpp"
#include "spred/net/http.hpp"
#include "spred/parsing/json.hpp"
#include "spred/core/rate_limit.hpp"

namespace spred {

std::string g_apiHost = "api.mexc.com";

SymbolInfo fetchSymbolInfo(const std::string& symbol) {
    SymbolInfo si;
    std::string body;
    g_ipW.wait(25);   // [fix F42] exchangeInfo = 25 весов
    if (!httpsGet(g_apiHost.c_str(), "/api/v3/exchangeInfo?symbol=" + symbol, body)) {
        std::fprintf(stderr, "[init] exchangeInfo: запрос не прошёл (curl+native)\n");
        return si;
    }
    jsonFindStr(body, "baseAsset", si.baseAsset);
    jsonFindStr(body, "quoteAsset", si.quoteAsset);

    double v = 0;
    bool haveTick = false, haveQty = false, haveMinNot = false;

    if (jsonFindNum(body, "tickSize", v) && v > 0)        { si.tick = v;        haveTick = true; }
    if (jsonFindNum(body, "stepSize", v) && v > 0)        { si.qtyStep = v;     haveQty = true; }
    if (jsonFindNum(body, "minQuoteAmount", v) && v > 0)  { si.minNotional = v; haveMinNot = true; }

    double qp;
    if (!haveTick && jsonFindNum(body, "quotePrecision", qp) && qp >= 0 && qp <= 10) {
        si.tick = std::pow(10.0, -qp);
        haveTick = true;
    }
    double bsp;
    if (!haveQty && jsonFindNum(body, "baseSizePrecision", bsp) && bsp >= 0) {
        si.qtyStep = (bsp == 0.0) ? 1.0 : bsp;
        haveQty = true;
    }
    double qap;
    if (!haveMinNot && jsonFindNum(body, "quoteAmountPrecision", qap) && qap > 0) {
        si.minNotional = qap;
        haveMinNot = true;
    }

    si.ok = haveTick || haveQty;
    si.complete = haveTick && haveQty;
    if (body.find("\"orderTypes\"") != std::string::npos &&
        body.find("\"LIMIT_MAKER\"") == std::string::npos)
        si.makerOk = false;
    if (!si.ok)
        std::fprintf(stderr, "[init] exchangeInfo unparsed, raw: %.400s\n", body.c_str());
    else if (!si.complete)
        std::fprintf(stderr, "[init] exchangeInfo %s: нет %s (raw: %.300s)\n",
                     symbol.c_str(), haveTick ? "qtyStep" : "tick", body.c_str());
    if (!haveMinNot)
        std::fprintf(stderr, "[init] exchangeInfo %s: minNotional не найден, "
                     "берётся 1.0\n", symbol.c_str());
    return si;
}

} // namespace spred
