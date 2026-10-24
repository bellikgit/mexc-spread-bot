#pragma once

#include "spred/core/runtime.hpp"

namespace spred {

// [F61] общий лимитер REST на UID (все процессы spred): окно по реальным
// часам CLOCK_MONOTONIC (общие для процессов), запись под flock.
enum { RK_NEW = 0, RK_CANCEL = 1, RK_OTHER = 2, RK_REPLACE = 3 };
struct SharedRateMem {
    uint64_t magic;
    double blockUntil;
    int hReq, hNew;
    double req[128];
    double nw[64];
    long granted[4], denied[4];
};
struct SharedRate {
    bool on = false;
    int fd = -1;
    SharedRateMem* m = nullptr;
    int limNew = 5, limTot = 12, reserve = 2;
    double win = 1.1;
    long granted[4] = {0, 0, 0, 0}, denied[4] = {0, 0, 0, 0}, forced = 0;
    std::string path;
    static double mono() {
        timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
        return ts.tv_sec + ts.tv_nsec * 1e-9;
    }
    bool init(const std::string& p) {
        path = p;
        fd = ::open(p.c_str(), O_RDWR | O_CREAT, 0600);
        if (fd < 0) return false;
        if (::flock(fd, LOCK_EX) != 0) { ::close(fd); fd = -1; return false; }
        struct stat st; fstat(fd, &st);
        if ((size_t)st.st_size < sizeof(SharedRateMem)) {
            if (ftruncate(fd, sizeof(SharedRateMem)) != 0) { ::flock(fd, LOCK_UN); ::close(fd); fd = -1; return false; }
        }
        void* a = mmap(nullptr, sizeof(SharedRateMem), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (a == MAP_FAILED) { ::flock(fd, LOCK_UN); ::close(fd); fd = -1; return false; }
        m = (SharedRateMem*)a;
        if (m->magic != 0x5350524431ULL) { std::memset(m, 0, sizeof *m); m->magic = 0x5350524431ULL; }
        ::flock(fd, LOCK_UN);
        on = true;
        return true;
    }
    static int cnt(const double* r, int n, double from) {
        int c = 0;
        for (int i = 0; i < n; ++i) if (r[i] > from) ++c;
        return c;
    }
    // record=false — только проверить; force — записать без проверки
    bool attempt(int kind, bool record, bool force = false) {
        if (!on) return true;
        ::flock(fd, LOCK_EX);
        double now = mono(), from = now - win;
        bool ok = true;
        if (!force) {
            if (kind != RK_CANCEL && now < m->blockUntil) ok = false;
            int tot = cnt(m->req, 128, from), nw = cnt(m->nw, 64, from);
            int needT = kind == RK_REPLACE ? 2 : 1;
            int capT = kind == RK_CANCEL ? limTot
                     : kind == RK_REPLACE ? limTot - reserve + 1 : limTot - reserve;
            if (tot + needT > capT) ok = false;
            if ((kind == RK_NEW || kind == RK_REPLACE) && nw + 1 > limNew) ok = false;
        }
        if (ok && record) {
            int needT = kind == RK_REPLACE ? 2 : 1;
            for (int k = 0; k < needT; ++k) { m->req[m->hReq] = now; m->hReq = (m->hReq + 1) % 128; }
            if (kind == RK_NEW || kind == RK_REPLACE) { m->nw[m->hNew] = now; m->hNew = (m->hNew + 1) % 64; }
            ++m->granted[kind]; ++granted[kind];
            if (force) ++forced;
        } else if (!ok && record) { ++m->denied[kind]; ++denied[kind]; }
        ::flock(fd, LOCK_UN);
        return ok;
    }
    void noteBlock(double untilMono) {
        if (!on) return;
        ::flock(fd, LOCK_EX);
        if (untilMono > m->blockUntil) m->blockUntil = untilMono;
        ::flock(fd, LOCK_UN);
    }
    double blockUntil() const { return on ? m->blockUntil : 0.0; }
};
extern SharedRate g_sr;

struct TokenBucket {
    double cap = 8, tokens = 8, rate = 8, t = 0;
    bool shared = false;            // [F61] решает общий лимитер
    bool forceNext = false;
    void init(double perSec) { cap = tokens = rate = perSec; t = nowSecs(); }
    bool take(int kind = RK_OTHER) {
        if (shared && g_sr.on) {
            if (forceNext) { forceNext = false; g_sr.attempt(kind, true, true); return true; }
            return g_sr.attempt(kind, true);
        }
        double now = nowSecs();
        tokens = std::min(cap, tokens + (now - t) * rate);
        t = now;
        if (tokens >= 1.0) { tokens -= 1.0; return true; }
        return false;
    }
    // [fix F36] дождаться токена (не расходуя), не дольше maxSec;
    // по таймауту — выдать один токен принудительно (shutdown/риск важнее)
    void waitToken(double maxSec, int kind = RK_OTHER) {
        if (shared && g_sr.on) {   // [F61]
            for (int it = 0;; ++it) {
                if (g_sr.attempt(kind, false)) return;
                if (it * 0.02 >= maxSec) { forceNext = true; return; }
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
        }
        // [fix F41] дедлайн по числу итераций (реальное время): в replay
        // сим-часы во время ожидания стоят -> раньше был возможен вечный цикл
        for (int it = 0;; ++it) {
            double now = nowSecs();
            tokens = std::min(cap, tokens + (now - t) * rate);
            t = now;
            if (tokens >= 1.0) return;
            if (it * 0.02 >= maxSec) { tokens = 1.0; return; }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
    }
};

// [fix F41] MEXC: после 429 клиент обязан прекратить запросы, иначе бан IP
// (2 мин..3 дня); 418 = бан уже действует. Пауза общая на процесс.
// Часы — РЕАЛЬНЫЕ (не replay-сим-часы nowSecs): лимит биржи живёт в реальном времени.
double realSecs();
extern double g_restBlockUntil;
extern bool g_restFormBody;   // [F51] MM_REST_FORM_BODY
extern int g_rate429, g_rate418;
void restNoteLimit(int http, double retryAfter);
void restWaitBlock(double maxSec);
// [fix F42] пул весов IP-эндпоинтов 300 / 10 c; держим не больше 250 / 10 c
struct WeightBucket {
    double cap = 250, tok = 250, rate = 25, t = -1;
    void wait(double w) {
        for (int i = 0; i < 1200; ++i) {
            double now = realSecs();
            if (t < 0) t = now;
            tok = std::min(cap, tok + (now - t) * rate);
            t = now;
            if (tok >= w) { tok -= w; return; }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        tok = 0;
    }
};
extern WeightBucket g_ipW;
double mexcWeight(const std::string& m, const std::string& path);

} // namespace spred
