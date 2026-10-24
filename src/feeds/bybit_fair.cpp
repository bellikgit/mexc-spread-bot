#include "spred/feeds/bybit_fair.hpp"
#include "spred/net/websocket.hpp"
#include "spred/parsing/json.hpp"

namespace spred {

int g_fairMode = 0;
std::mutex g_fairMu;
std::map<std::string, FairQ> g_fair;
std::vector<std::string> g_fairLog;
std::atomic<long> g_fairMsgs{0}, g_fairReconn{0};
void fairLog(const std::string& m) {
    std::lock_guard<std::mutex> lk(g_fairMu);
    if (g_fairLog.size() < 200) g_fairLog.push_back(m);
}
bool g_fairUsdcMap = true;   // [F64]
std::string fairRef(const std::string& sym) {
    if (g_fairUsdcMap && sym.size() > 4 && sym.compare(sym.size() - 4, 4, "USDC") == 0)
        return sym.substr(0, sym.size() - 4) + "USDT";
    return sym;
}
bool fairGet(const std::string& sym0, FairQ& q) {
    std::string sym = fairRef(sym0);
    std::lock_guard<std::mutex> lk(g_fairMu);
    auto it = g_fair.find(sym);
    if (it == g_fair.end()) return false;
    q = it->second;
    return true;
}
// "b":[["p","q"],...] -> лучший уровень с q>0; any=массив непуст; allZero=только удаления
void fairParseSide(const std::string& s, const char* key, bool isBid,
                          bool& any, bool& allZero, double& bp, double& bq) {
    any = false; allZero = true; bp = 0; bq = 0;
    size_t k = s.find(key);           // key = "\"b\"" / "\"a\""
    if (k == std::string::npos) return;
    size_t i = k + std::strlen(key);
    while (i < s.size() && (s[i] == ' ' || s[i] == ':')) ++i;
    if (i >= s.size() || s[i] != '[') return;
    ++i;
    while (i < s.size()) {
        while (i < s.size() && (s[i] == ' ' || s[i] == ',')) ++i;
        if (i >= s.size() || s[i] != '[') break;
        size_t q1 = s.find('"', i), q2 = q1 == std::string::npos ? q1 : s.find('"', q1 + 1);
        size_t q3 = q2 == std::string::npos ? q2 : s.find('"', q2 + 1);
        size_t q4 = q3 == std::string::npos ? q3 : s.find('"', q3 + 1);
        size_t e = s.find(']', i);
        if (q4 == std::string::npos || e == std::string::npos || q4 > e) break;
        double p = std::atof(s.substr(q1 + 1, q2 - q1 - 1).c_str());
        double q = std::atof(s.substr(q3 + 1, q4 - q3 - 1).c_str());
        any = true;
        if (q > 0 && p > 0) {
            allZero = false;
            if (bp == 0 || (isBid ? p > bp : p < bp)) { bp = p; bq = q; }
        }
        i = e + 1;
    }
}
void bybitFairThread(std::vector<std::string> syms, std::string host, int port,
                            std::string path) {
    double backoff = 1.0;
    std::map<std::string, std::array<double, 4>> book;
    while (!g_stop) {
        ws::Connection c(host, port, path);
        if (!c.connect()) {
            fairLog("[FAIR] Bybit connect не удался, повтор через " +
                    std::to_string((int)backoff) + " с");
            ++g_fairReconn;
            for (int i = 0; i < (int)(backoff * 10) && !g_stop; ++i)
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            backoff = std::min(30.0, backoff * 2);
            continue;
        }
        backoff = 1.0;
        book.clear();
        for (size_t i = 0; i < syms.size(); i += 10) {
            std::string a = "{\"op\":\"subscribe\",\"args\":[";
            for (size_t j = i; j < syms.size() && j < i + 10; ++j)
                a += std::string(j > i ? "," : "") + "\"orderbook.1." + syms[j] + "\"";
            c.sendText(a + "]}");
        }
        fairLog("[FAIR] Bybit WS подключён, подписка orderbook.1 x " +
                std::to_string(syms.size()));
        double lastPing = nowMsSteady(), lastRx = lastPing;
        int textLogged = 0;
        while (!g_stop) {
            int r = c.waitReadable(500);
            if (r < 0) { fairLog("[FAIR] Bybit socket error"); break; }
            double t = nowMsSteady();
            if (r > 0) {
                ws::Frame f;
                if (!c.readFrame(f)) { fairLog("[FAIR] Bybit соединение потеряно"); break; }
                lastRx = t;
                if (f.opcode == 0x8) { fairLog("[FAIR] Bybit close"); break; }
                if (f.opcode == 0x9) { c.sendPong(f.payload); continue; }
                if (f.opcode != 0x1) continue;
                const std::string& m = f.payload;
                size_t tp = m.find("\"topic\"") == std::string::npos ? std::string::npos
                                                                        : m.find("orderbook.1.");
                if (tp == std::string::npos) {
                    if (textLogged < 6 && m.find("\"pong\"") == std::string::npos) {
                        fairLog("[FAIR] Bybit text: " + m.substr(0, 200));
                        ++textLogged;
                    }
                    continue;
                }
                size_t s0 = tp + 12, s1 = m.find('"', s0);
                if (s1 == std::string::npos) continue;
                std::string sym = m.substr(s0, s1 - s0);
                bool snap = m.find("\"snapshot\"") != std::string::npos;
                auto& b = book[sym];
                for (int side = 0; side < 2; ++side) {
                    bool any, allZero; double p, q;
                    fairParseSide(m, side == 0 ? "\"b\"" : "\"a\"", side == 0,
                                  any, allZero, p, q);
                    int i = side * 2;
                    if (any && !allZero) { b[i] = p; b[i + 1] = q; }
                    else if ((any && allZero && !snap) || (!any && snap)) { b[i] = 0; b[i + 1] = 0; }
                }
                ++g_fairMsgs;
                if (b[0] > 0 && b[2] > b[0]) {
                    std::lock_guard<std::mutex> lk(g_fairMu);
                    FairQ& fq = g_fair[sym];
                    fq.bid = b[0]; fq.ask = b[2]; fq.bq = b[1]; fq.aq = b[3]; fq.tMs = t; ++fq.n;
                }
            }
            if (t - lastPing > 20000) { c.sendText("{\"op\":\"ping\"}"); lastPing = t; }
            if (t - lastRx > 30000) { fairLog("[FAIR] Bybit молчит 30 с -> реконнект"); break; }
        }
        ++g_fairReconn;
        c.close();
        for (int i = 0; i < 10 && !g_stop; ++i)
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

} // namespace spred
