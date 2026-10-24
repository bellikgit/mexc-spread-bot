#include "spred/parsing/json.hpp"

namespace spred {

bool jsonFindNum(const std::string& s, const std::string& key, double& out) {
    std::string pat = "\"" + key + "\"";
    size_t p = s.find(pat);
    if (p == std::string::npos) return false;
    p = s.find(':', p + pat.size());
    if (p == std::string::npos) return false;
    ++p;
    while (p < s.size() && (s[p] == ' ' || s[p] == '"')) ++p;
    char* e = nullptr;
    out = strtod(s.c_str() + p, &e);
    return e && e != s.c_str() + p;
}

// [v3.0] строковое значение JSON-ключа ("key":"value" или "key":123)
bool jsonFindStr(const std::string& s, const std::string& key,
                        std::string& out) {
    std::string pat = "\"" + key + "\"";
    size_t p = s.find(pat);
    if (p == std::string::npos) return false;
    p = s.find(':', p + pat.size());
    if (p == std::string::npos) return false;
    ++p;
    while (p < s.size() && (s[p] == ' ' || s[p] == '\t' || s[p] == '\n')) ++p;
    if (p >= s.size()) return false;
    if (s[p] == '"') {
        size_t e = p + 1;
        for (;;) {
            e = s.find('"', e);
            if (e == std::string::npos) return false;
            if (s[e - 1] != '\\') break;
            ++e;
        }
        out = s.substr(p + 1, e - p - 1);
        return true;
    }
    size_t e = p;
    while (e < s.size() && s[e] != ',' && s[e] != '}' && s[e] != ']' &&
           s[e] != ' ' && s[e] != '\n') ++e;
    if (e == p) return false;
    out = s.substr(p, e - p);
    return true;
}

// [v3.0] грубый разбор массива плоских объектов ("[{...},{...}]") —
// для openOrders/account.balances (вложенных объектов в элементах нет)
std::vector<std::string> jsonSplitObjects(const std::string& s) {
    std::vector<std::string> out;
    int depth = 0;
    size_t start = std::string::npos;
    bool inStr = false;
    for (size_t i = 0; i < s.size(); ++i) {
        char c = s[i];
        if (inStr) {
            if (c == '\\') ++i;
            else if (c == '"') inStr = false;
            continue;
        }
        if (c == '"') { inStr = true; continue; }
        if (c == '{' ) {
            if (depth == 0) start = i;
            ++depth;
        } else if (c == '}') {
            --depth;
            if (depth == 0 && start != std::string::npos) {
                out.push_back(s.substr(start, i - start + 1));
                start = std::string::npos;
            }
        }
    }
    return out;
}

// [v3.0 fix] тело массива по ключу: {"balances":[ {...},{...} ]} -> [...]
// (jsonSplitObjects работает только по depth-0; балансы вложены в wrapper)
std::string jsonExtractArray(const std::string& s, const char* key) {
    std::string pat = std::string("\"") + key + "\"";
    size_t p = s.find(pat);
    if (p == std::string::npos) return "";
    p = s.find('[', p + pat.size());
    if (p == std::string::npos) return "";
    int depth = 0;
    bool inStr = false;
    for (size_t i = p; i < s.size(); ++i) {
        char c = s[i];
        if (inStr) {
            if (c == '\\') ++i;
            else if (c == '"') inStr = false;
            continue;
        }
        if (c == '"') { inStr = true; continue; }
        if (c == '[') ++depth;
        else if (c == ']') {
            --depth;
            if (depth == 0) return s.substr(p, i - p + 1);
        }
    }
    return "";
}
// [v3.0 L1] keep-alive HTTPS клиент (api.mexc.com): POST/GET/DELETE/PUT.

} // namespace spred
