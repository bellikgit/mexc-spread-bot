#include "spred/core/config.hpp"

namespace spred {

std::string envOr(const char* name, const std::string& dflt) {
    const char* v = getenv(name);
    return (v && *v) ? std::string(v) : dflt;
}
double envD(const char* name, double dflt) {
    const char* v = getenv(name);
    if (!v || !*v) return dflt;
    char* e = nullptr;
    double d = strtod(v, &e);
    return (e && *e == '\0') ? d : dflt;
}
int envI(const char* name, int dflt) { return (int)envD(name, (double)dflt); }

// [v3.1] per-symbol override: MM_<name>@<SYMBOL> побеждает глобальный MM_<name>
double envDsym(const char* name, const std::string& sym, double dflt) {
    std::string key = std::string(name) + "@" + sym;
    const char* v = getenv(key.c_str());
    if (v && *v) {
        char* e = nullptr;
        double d = strtod(v, &e);
        if (e && *e == '\0') return d;
    }
    return envD(name, dflt);
}


std::vector<std::string> splitSymbols(const std::string& s) {
    std::vector<std::string> out;
    size_t p = 0;
    while (p <= s.size()) {
        size_t c = s.find(',', p);
        std::string t = s.substr(p, c == std::string::npos ? std::string::npos
                                                             : c - p);
        size_t a = t.find_first_not_of(" \t");
        size_t b = t.find_last_not_of(" \t");
        if (a != std::string::npos) out.push_back(t.substr(a, b - a + 1));
        if (c == std::string::npos) break;
        p = c + 1;
    }
    return out;
}

} // namespace spred
