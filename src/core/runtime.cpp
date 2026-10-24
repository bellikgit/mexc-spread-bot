#include "spred/core/runtime.hpp"

namespace spred {

// [v2.15 R1 REPLAY] сим-часы: в replay nowMsSteady() = время ЗАПИСИ raw
// (steady-домен исходной сессии). Вся timing-логика (fill-гейт, ghost-окно,
// бакет, escape, markout-горизонты, TFI) идёт через эти две функции ->
// воспроизводится в исходном временном домене автоматически; сон в
// frameWait при SPEED>0 — чистый pacing, на корректность не влияет.
// Прайминг по первому кадру raw — в main (R3), до t0: тогда duration
// в summary = длительность ЗАПИСАННОЙ сессии.
bool   g_replay = false;
double g_simNowMs = -1.0;      // <0 = ещё не заведены (первый кадр)

double nowMsSteady() {
    if (g_replay && g_simNowMs >= 0.0) return g_simNowMs;
    using namespace std::chrono;
    return duration<double, std::milli>(
               steady_clock::now().time_since_epoch()).count();
}
double nowSecs() { return nowMsSteady() / 1000.0; }

// [v2.7 J] локальное время с мс
std::string nowWallClock(bool withMs) {
    char buf[40];
    auto now = std::chrono::system_clock::now();
    std::time_t t = std::chrono::system_clock::to_time_t(now);
    long msec = std::chrono::duration_cast<std::chrono::milliseconds>(
                    now.time_since_epoch()).count() % 1000;
    std::tm tm{};
    localtime_r(&t, &tm);
    if (withMs)
        std::snprintf(buf, sizeof buf, "%02d:%02d:%02d.%03ld",
                      tm.tm_hour, tm.tm_min, tm.tm_sec, msec);
    else
        std::snprintf(buf, sizeof buf, "%02d:%02d:%02d",
                      tm.tm_hour, tm.tm_min, tm.tm_sec);
    return buf;
}

// [v2.7 J] события с меткой времени в момент создания
void evAdd(std::vector<std::string>& v, const std::string& s) {
    v.push_back("[" + nowWallClock(true) + "] " + s);
}

// ------------------------------------------------------------------- logger
FILE* g_logFile = nullptr;

void lprintf(const char* fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    std::vprintf(fmt, ap);
    va_end(ap);
    if (g_logFile) {
        va_start(ap, fmt);
        std::vfprintf(g_logFile, fmt, ap);
        va_end(ap);   // [fix F6]
    }
}

void lflush() {
    std::fflush(stdout);
    if (g_logFile) std::fflush(g_logFile);
}

volatile std::sig_atomic_t g_stop = 0;
void onSigint(int) { g_stop = 1; }

} // namespace spred
