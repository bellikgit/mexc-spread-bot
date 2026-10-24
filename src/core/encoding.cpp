#include "spred/core/encoding.hpp"

namespace spred {

std::string b64(const unsigned char* data, size_t len) {
    static const char* T =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve(4 * ((len + 2) / 3));
    for (size_t i = 0; i < len; i += 3) {
        unsigned int n = (unsigned int)data[i] << 16;
        if (i + 1 < len) n |= (unsigned int)data[i + 1] << 8;
        if (i + 2 < len) n |= (unsigned int)data[i + 2];
        out += T[(n >> 18) & 63];
        out += T[(n >> 12) & 63];
        out += (i + 1 < len) ? T[(n >> 6) & 63] : '=';
        out += (i + 2 < len) ? T[n & 63] : '=';
    }
    return out;
}

// [v3.0] эпохальные миллисекунды (для timestamp подписанных REST-запросов;
// сим-часы nowMsSteady эпоху не знают — replay использует запись)
long long epochMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(
               system_clock::now().time_since_epoch()).count();
}

// [v3.0] HMAC-SHA256 -> lowercase hex (подпись MEXC: hex от HMAC по
// query-строке ключом secret)
std::string hmacSha256Hex(const std::string& key, const std::string& data) {
    unsigned char mac[EVP_MAX_MD_SIZE];
    unsigned int macLen = 0;
    HMAC(EVP_sha256(), key.data(), (int)key.size(),
         reinterpret_cast<const unsigned char*>(data.data()), data.size(),
         mac, &macLen);
    static const char* H = "0123456789abcdef";
    std::string out;
    out.reserve(macLen * 2);
    for (unsigned int i = 0; i < macLen; ++i) {
        out += H[(mac[i] >> 4) & 15];
        out += H[mac[i] & 15];
    }
    return out;
}

// [v3.0] десятичная строка без хвостовых нулей (цены/количества в REST);
// 18 знаков покрывает тик 1e-12 тонких пар
// [fix F7] не более 15 значащих цифр: цена = floor(px/tick+0.5)*tick в double
// не точна, и %.18f давал 0.299999999999999989 вместо 0.3
std::string fmtDec(double v) {
    if (!std::isfinite(v) || v == 0.0) return "0";
    int e = (int)std::floor(std::log10(std::fabs(v)));
    int dec = 14 - e;
    if (dec < 0) dec = 0;
    if (dec > 18) dec = 18;
    char buf[400];
    std::snprintf(buf, sizeof buf, "%.*f", dec, v);
    std::string s = buf;
    size_t dot = s.find('.');
    if (dot != std::string::npos) {
        size_t last = s.find_last_not_of('0');
        if (last == dot) s.erase(dot);
        else s.erase(last + 1);
    }
    if (s.empty() || s == "-0") s = "0";
    return s;
}

// [v3.0] id клиентского ордера: уникален на процесс (pid) + порядковый
std::string base36(long long v) {
    static const char* D = "0123456789abcdefghijklmnopqrstuvwxyz";
    if (v == 0) return "0";
    bool neg = v < 0;
    unsigned long long u = neg ? (unsigned long long)(-v) : (unsigned long long)v;
    std::string s;
    while (u) { s += D[u % 36]; u /= 36; }
    if (neg) s += 'z';
    std::reverse(s.begin(), s.end());
    return s;
}

} // namespace spred
