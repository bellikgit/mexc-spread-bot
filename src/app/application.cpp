#include "spred/app/application.hpp"
#include "spred/app/session_steps.hpp"
#include "spred/app/replay.hpp"
#include "spred/core/config.hpp"
#include "spred/feeds/bybit_fair.hpp"
#include "spred/net/websocket.hpp"

namespace spred {

int runApplication(int argc, char** argv) {
    std::setvbuf(stdout, nullptr, _IOLBF, 0);
    {   // [fix F16] без SA_RESTART: Ctrl+C прерывает блокирующий read/connect
        struct sigaction sa{};
        sa.sa_handler = onSigint;
        sigemptyset(&sa.sa_mask);
        sa.sa_flags = 0;
        sigaction(SIGINT, &sa, nullptr);
        sigaction(SIGTERM, &sa, nullptr);
        sigaction(SIGHUP, &sa, nullptr);    // [fix F24] закрыли SSH/терминал
        sigaction(SIGQUIT, &sa, nullptr);
    }
    // [fix F3] запись в оборванный сокет (PING, REST keep-alive) давала SIGPIPE
    // и смерть процесса БЕЗ cancelAll -> ордера оставались на бирже
    std::signal(SIGPIPE, SIG_IGN);

    // [v3.1] пары: argv[1] "A,B,C" или MM_SYMBOLS; одиночный символ — как раньше
    std::vector<std::string> symbols = splitSymbols(
        argc > 1 ? argv[1] : envOr("MM_SYMBOLS", envOr("MM_SYMBOL", "BTCUSDT")));
    if (symbols.empty()) symbols.push_back("BTCUSDT");
    // [F46] MEXC: <=30 подписок на WS-коннект, у нас 3 канала на пару
    if (symbols.size() > 10 && envOr("MM_REPLAY", "").empty()) {
        std::fprintf(stderr, "[FATAL] %zu пар в одном процессе: MEXC даёт не более 30 "
                     "подписок на WS (3 на пару -> максимум 10 пар). Разбейте на "
                     "несколько процессов.\n", symbols.size());
        return 1;
    }
    double runSecs = argc > 2 ? std::atof(argv[2]) : 0;
    int depthLevels = argc > 3 ? std::atoi(argv[3]) : 20;
    double latencyMs = argc > 4 ? std::atof(argv[4]) : envD("MM_LATENCY_MS", 22);
    // [F55] внешняя fair-цена
    {
        std::string fm = envOr("MM_FAIR", "");
        for (auto& ch : fm) ch = (char)std::tolower((unsigned char)ch);
        g_fairMode = (fm == "bybit" || fm == "1") ? 1 : 0;
        if (g_fairMode && !envOr("MM_REPLAY", "").empty()) {
            std::fprintf(stderr, "[FAIR] MM_REPLAY: внешней цены в записи нет -> MM_FAIR выключен\n");
            g_fairMode = 0;
        }
    }
    double fairBasisSec = envD("MM_FAIR_BASIS_SEC", 600);
    double fairWarmSec = envD("MM_FAIR_WARM_SEC", 30);
    double lastFairStat = 0;
    bool logDeals = envI("MM_LOG_DEALS", 0) != 0;

    std::string logPath = envOr("MM_LOG", "");
    if (!logPath.empty()) {
        g_logFile = std::fopen(logPath.c_str(), "a");
        if (g_logFile)
            std::fprintf(g_logFile, "\n==== SESSION START %s ====\n",
                         nowWallClock().c_str());
    }

    // [v2.15 R3 REPLAY] — как было; raw-лента однопарная -> один символ
    std::string replayPath = envOr("MM_REPLAY", "");
    double replaySpeed = envD("MM_REPLAY_SPEED", 1.0);
    if (!replayPath.empty()) {
        if (symbols.size() > 1) {
            std::fprintf(stderr,
                "[replay] raw-лента однопарная: задай ОДИН символ "
                "(получено %zu)\n", symbols.size());
            return 1;
        }
        g_replay = true;
        RSrc.f = std::fopen(replayPath.c_str(), "rb");
        if (!RSrc.f) {
            std::fprintf(stderr, "[replay] cannot open %s\n", replayPath.c_str());
            return 1;
        }
        {
            double firstRecv = 0;
            uint32_t firstLen = 0;
            bool primed = std::fread(&firstRecv, sizeof firstRecv, 1, RSrc.f) == 1 &&
                          std::fread(&firstLen, sizeof firstLen, 1, RSrc.f) == 1;
            std::fseek(RSrc.f, 0, SEEK_SET);
            if (primed) g_simNowMs = firstRecv;
        }
        lprintf("[replay] %s | speed %s | seed %s\n", replayPath.c_str(),
                replaySpeed > 0 ? "1.0 (реальное время)" : "0 (мгновенно)",
                envOr("MM_SEED", "").c_str());
    }

    // [v2.13 ФИКС 15] RAW RECORDER (все пары в одну ленту; replay отфильтрует
    // по symbol — однопарный прогон мультипарной ленты корректен)
    std::string rawPath = envOr("MM_RAW_LOG", "");
    FILE* rawFile = nullptr;
    if (!rawPath.empty() && replayPath.empty()) {
        rawFile = std::fopen(rawPath.c_str(), "wb");
        if (!rawFile)
            std::fprintf(stderr, "[init] WARNING: cannot open MM_RAW_LOG %s\n",
                         rawPath.c_str());
    }

    // ---------------- конфиг-шаблон: [v3.1] дефолты = продакшн-набор n9/n10
    Config baseCfg;
    baseCfg.latencyS = latencyMs / 1000.0;
    baseCfg.allowShort = envI("MM_ALLOW_SHORT", 0) != 0;
    baseCfg.requoteFrac = envD("MM_REQUOTE_FRAC", 0.11);
    baseCfg.minRequoteSec = envD("MM_MIN_REQUOTE_SEC", 2);
    baseCfg.sweepCooldownS = envD("MM_SWEEP_COOLDOWN_SEC", 15);
    baseCfg.skewImb = envI("MM_SKEW_IMB", 1);
    baseCfg.skewTfi = envI("MM_SKEW_TFI", 1);
    baseCfg.imbSkewFrac = envD("MM_IMB_SKEW_FRAC", 0.10);
    baseCfg.tfiSkewFrac = envD("MM_TFI_SKEW_FRAC", 0.10);
    baseCfg.invSkewFrac = envD("MM_INV_SKEW_FRAC", 0.30);
    baseCfg.tfiWindowSec = envD("MM_TFI_WINDOW", 60.0);
    baseCfg.tfiThresh = envD("MM_TFI_THRESH", 0.40);
    baseCfg.runStop = envI("MM_RUN_STOP", 3);
    baseCfg.maxSpreadBps = envD("MM_MAX_SPREAD_BPS", 500);
    baseCfg.rateRps = envD("MM_RATE_RPS", 10);   // [fix F44] MEXC: place+cancel 12/с
    baseCfg.maxLoss = envD("MM_MAX_LOSS", 25);
    baseCfg.orderSizeQuote = envD("MM_ORDER_USDT", 50);
    baseCfg.maxPosQuote = envD("MM_MAX_POS", 150);
    baseCfg.staleSecs = envD("MM_STALE_SEC", 30);
    baseCfg.escapeSecs = envD("MM_ESCAPE_SECS", 30);
    baseCfg.lossBasis = envI("MM_LOSS_BASIS", 1);                 // [F56]
    baseCfg.haltFlat  = envI("MM_HALT_FLAT", 1);                  // [F57]
    baseCfg.trendBps  = envD("MM_TREND_BPS", 0);                  // [F58]
    baseCfg.trendSec  = envD("MM_TREND_SEC", 900);
    baseCfg.makerFeeBps = envD("MM_MAKER_BPS", 0.0);
    baseCfg.takerFeeBps = envD("MM_TAKER_BPS", 5.0);
    baseCfg.minInitFrac = envD("MM_MIN_INIT_FRAC", 0.0);
    baseCfg.cancelAheadFrac = envD("MM_CANCEL_AHEAD_FRAC", 0.5);
    if (baseCfg.cancelAheadFrac < 0.0) baseCfg.cancelAheadFrac = 0.0;
    if (baseCfg.cancelAheadFrac > 1.0) baseCfg.cancelAheadFrac = 1.0;
    baseCfg.reorderMs = envD("MM_REORDER_MS", 50);
    if (baseCfg.reorderMs < 0) baseCfg.reorderMs = 0;
    baseCfg.gapFillMode = envI("MM_GAP_FILL_MODE", 0);
    if (baseCfg.gapFillMode < 0) baseCfg.gapFillMode = 0;
    if (baseCfg.gapFillMode > 2) baseCfg.gapFillMode = 2;
    baseCfg.latDist = envI("MM_LAT_DIST", 0);
    baseCfg.latP90Mult = envD("MM_LAT_P90_MULT", 1.6);
    baseCfg.latP99Mult = envD("MM_LAT_P99_MULT", 2.5);
    baseCfg.latMinMult = envD("MM_LAT_MIN_MULT", 0.3);
    if (baseCfg.latP90Mult < 1.0) baseCfg.latP90Mult = 1.0;
    if (baseCfg.latP99Mult < baseCfg.latP90Mult) baseCfg.latP99Mult = baseCfg.latP90Mult;
    if (baseCfg.latMinMult <= 0.0) baseCfg.latMinMult = 0.05;
    if (baseCfg.latMinMult > 1.0) baseCfg.latMinMult = 1.0;
    baseCfg.quarantineMs = envD("MM_QUAR_MS", 2000);
    if (baseCfg.quarantineMs < 0.0) baseCfg.quarantineMs = 0.0;
    if (baseCfg.quarantineMs > 60000.0) baseCfg.quarantineMs = 60000.0;
    // [v3.1] RAMP — дефолт (решение ТЗ): инвентарь из филлов, setup cost 0
    baseCfg.initMode = envI("MM_INIT_MODE", 1);
    if (baseCfg.initMode < 0) baseCfg.initMode = 0;
    if (baseCfg.initMode > 1) baseCfg.initMode = 1;
    baseCfg.escapeCooldownS = envD("MM_ESCAPE_COOLDOWN_SEC", 90);
    if (baseCfg.escapeCooldownS < 0) baseCfg.escapeCooldownS = 0;
    baseCfg.cutoffBps = envD("MM_CUTOFF_BPS", 90);
    if (baseCfg.cutoffBps < 0) baseCfg.cutoffBps = 0;
    baseCfg.cutoffSec = envD("MM_CUTOFF_SEC", 120);
    baseCfg.printFilter = envI("MM_PRINT_FILTER", 1);               // [F45]
    if (baseCfg.printFilter < 0 || baseCfg.printFilter > 2) baseCfg.printFilter = 1;
    baseCfg.printFilterMs = envD("MM_PRINT_FILTER_MS", 1500);
    if (baseCfg.printFilterMs < 0) baseCfg.printFilterMs = 0;
    baseCfg.printFillP = envD("MM_PRINT_FILL_P", 0);                // [F49]
    if (!(baseCfg.printFillP > 0)) baseCfg.printFillP = 0;
    if (baseCfg.printFillP > 1) baseCfg.printFillP = 1;
    if (baseCfg.cutoffSec < 0) baseCfg.cutoffSec = 0;
    baseCfg.queueMode = envI("MM_QUEUE_MODE", 0);
    if (baseCfg.queueMode < 0) baseCfg.queueMode = 0;
    if (baseCfg.queueMode > 2) baseCfg.queueMode = 2;
    baseCfg.escapeSkipLevels = envI("MM_ESCAPE_SKIP_LEVELS", 0);
    if (baseCfg.escapeSkipLevels < 0) baseCfg.escapeSkipLevels = 0;
    if (baseCfg.escapeSkipLevels > 3) baseCfg.escapeSkipLevels = 3;
    baseCfg.selfInBook = envI("MM_SELF_IN_BOOK", 0);
    baseCfg.qtyDriftUsdt = envD("MM_QTY_DRIFT_USDT", 0.0);
    if (baseCfg.qtyDriftUsdt < 0) baseCfg.qtyDriftUsdt = 0;
    double cancelLatMs = envD("MM_CANCEL_LAT_MS", latencyMs);
    if (baseCfg.makerFeeBps <= 0.0)
        lprintf("[init] NOTE: maker fee = 0 (MEXC spot promo assumed). "
                "Если биржа берёт maker — задайте MM_MAKER_BPS, иначе "
                "paper P&L завышен на maker_volume * fee.\n");

    // ================= [v3.0] LIVE-конфиг и гейт безопасности =================
    LiveCfg lcfg;
    lcfg.live = envI("MM_LIVE", 0) != 0;
    lcfg.confirm = envOr("MM_LIVE_CONFIRM", "") == "YES";
    lcfg.tlsNoVerify = envI("MM_TLS_NOVERIFY", 0) != 0;
    lcfg.cleanMode = envI("MM_LIVE_CLEAN_START", 1);   // [fix F39] 0/1 бот/2 все
    lcfg.cleanStart = lcfg.cleanMode != 0;
    lcfg.keepOnExit = envI("MM_KEEP_ORDERS_ON_EXIT", 0) != 0;
    lcfg.reconAdopt = envI("MM_RECON_ADOPT", 0) != 0;
    lcfg.allowReplayLive = envI("MM_LIVE_ALLOW_REPLAY", 0) != 0;
    lcfg.sellWallet = envI("MM_SELL_WALLET_BASE", 0) != 0;
    lcfg.realSync = envI("MM_LIVE_SYNC", 1) != 0;   // [F50]
    g_restFormBody = envI("MM_REST_FORM_BODY", 0) != 0;   // [F51]
    lcfg.selfExcl = envI("MM_SELF_EXCL", 1) != 0;          // [F52]
    lcfg.selfExclSec = envD("MM_SELF_EXCL_SEC", 2.0);
    if (lcfg.selfExclSec < 0) lcfg.selfExclSec = 0;
    lcfg.maxFillsMin = envI("MM_LIVE_MAX_FILLS_MIN", 6);    // [F53]
    lcfg.feeLookup = envI("MM_FEE_LOOKUP", 1) != 0;          // [F54]
    lcfg.breakerSec = envD("MM_LIVE_BREAKER_SEC", 600);
    lcfg.reconSec = envD("MM_RECON_SEC", 15);
    lcfg.balanceSec = envD("MM_BALANCE_SEC", 30);
    lcfg.usdtReserve = envD("MM_USDT_RESERVE", 1.0);
    lcfg.startUsdt = envD("MM_START_USDT", 10000.0);
    lcfg.killFile = envOr("MM_KILL_FILE", "");
    g_apiHost = envOr("MM_API_HOST", "api.mexc.com");
    g_tlsNoVerify = lcfg.tlsNoVerify;
    if (g_replay && lcfg.live && !lcfg.allowReplayLive) {
        lprintf("[live] MM_REPLAY активен -> MM_LIVE игнорируется (replay — "
                "бумажный). Для мок-стенда: MM_LIVE_ALLOW_REPLAY=1\n");
        lcfg.live = false;
    }
    if (lcfg.live) {
        if (envOr("MM_API_KEY", "").empty() || envOr("MM_API_SECRET", "").empty()) {
            std::fprintf(stderr,
                "[live] ОТКАЗ: MM_LIVE=1 без MM_API_KEY/MM_API_SECRET.\n"
                "       Ключи только через env, в коде/логах их быть не должно.\n");
            return 2;
        }
        if (!lcfg.confirm) {
            std::fprintf(stderr,
                "[live] ОТКАЗ: live-торговля требует MM_LIVE_CONFIRM=YES\n"
                "       (осознанное подтверждение работы с реальными деньгами).\n"
                "       Проверь: MM_ORDER_USDT=%.0f MM_MAX_POS=%.0f MM_MAX_LOSS=%.0f\n",
                baseCfg.orderSizeQuote, baseCfg.maxPosQuote, baseCfg.maxLoss);
            return 2;
        }
        lprintf("[live] === LIVE РЕЖИМ: реальные ордера %zu пар через %s ===\n",
                symbols.size(), g_apiHost.c_str());
        lprintf("[live] риск-рамки (дефолт/глобально): order %.0f USDT, band %.0f, "
                "maxLoss %.0f, cleanStart %d, keepOnExit %d, killFile %s\n",
                baseCfg.orderSizeQuote, baseCfg.maxPosQuote, baseCfg.maxLoss,
                (int)lcfg.cleanStart, (int)lcfg.keepOnExit,
                lcfg.killFile.empty() ? "(off)" : lcfg.killFile.c_str());
    }

    // ================= [v3.1] построение per-symbol контекстов =================
    TokenBucket restRate;            // общий REST-бюджет аккаунта
    restRate.init(baseCfg.rateRps);
    // [F61] общий лимитер на UID
    bool srLive = envI("MM_SHARED_RATE", 0) != 0;
    bool srPaper = envI("MM_SR_PAPER", 0) != 0 && !lcfg.live && !g_replay;
    if (srLive || srPaper) {
        g_sr.limNew = envI("MM_SR_NEW", 5);
        g_sr.limTot = envI("MM_SR_TOTAL", 12);
        g_sr.reserve = envI("MM_SR_CANCEL_RESERVE", 2);
        g_sr.win = envD("MM_SR_WIN_MS", 1100) / 1000.0;
        std::string srf = envOr("MM_SR_FILE", "/dev/shm/spred_rate_" + std::to_string((long)getuid()));
        if (g_sr.init(srf)) {
            restRate.shared = srLive;
            lprintf("[F61] общий лимитер %s: новых %d/с, всего %d/с (резерв отмен %d), окно %.0f ms%s\n",
                    srf.c_str(), g_sr.limNew, g_sr.limTot, g_sr.reserve, g_sr.win * 1000,
                    srPaper ? ", paper: включён" : "");
        } else {
            lprintf("[F61] общий лимитер: не открыть %s (%s)%s\n", srf.c_str(), std::strerror(errno),
                    lcfg.live ? " — LIVE остановлен" : " — работаю без него");
            if (lcfg.live) return 1;
        }
    }
    std::vector<std::unique_ptr<SymCtx>> ctxOwn;
    std::vector<SymCtx*> ctxs;
    unsigned gseed = (unsigned)envD("MM_SEED", 0.0);
    for (size_t i = 0; i < symbols.size(); ++i) {
        const std::string& sym = symbols[i];
        auto up = std::unique_ptr<SymCtx>(new SymCtx(g_apiHost, lcfg.tlsNoVerify));
        SymCtx& C = *up;
        C.symbol = sym;
        C.tr.cfg = baseCfg;
        // per-symbol overrides @SYM (побеждают глобальные env)
        C.tr.cfg.orderSizeQuote   = envDsym("MM_ORDER_USDT", sym, baseCfg.orderSizeQuote);
        C.tr.cfg.maxPosQuote      = envDsym("MM_MAX_POS", sym, baseCfg.maxPosQuote);
        C.fairD       = envDsym("MM_FAIR_D_BPS", sym, 15);          // [F55]
        C.fairSkew    = envDsym("MM_FAIR_SKEW_BPS", sym, 0);
        C.fairRq      = envDsym("MM_FAIR_RQ_BPS", sym, std::max(2.0, 0.2 * C.fairD));
        C.fairStaleMs = envDsym("MM_FAIR_STALE_MS", sym, 1500);
        C.fairMaxDev  = envDsym("MM_FAIR_MAX_DEV_BPS", sym, 100);
        C.escMk       = (int)envDsym("MM_ESC_MAKER", sym, 0);          // [F60]
        C.escMkBps    = envDsym("MM_ESC_MAKER_BPS", sym, 0);
        C.escMkSec    = envDsym("MM_ESC_MAKER_SEC", sym, 180);
        C.escMkHard   = envDsym("MM_ESC_MAKER_HARD", sym, 1.5);
        C.fairMicro   = envDsym("MM_FAIR_MICRO", sym, 0);             // [F62]
        C.fairImbBps  = envDsym("MM_FAIR_IMB_BPS", sym, 0);
        C.fairSigMax  = envDsym("MM_FAIR_SIG_MAX_BPS", sym, 0.5 * C.fairD);
        C.asGamma     = envDsym("MM_AS_GAMMA", sym, 0);               // [F63]
        C.asTau       = envDsym("MM_AS_TAU", sym, 60);
        C.asVolSec    = envDsym("MM_AS_VOL_SEC", sym, 300);
        C.asMax       = envDsym("MM_AS_MAX_BPS", sym, C.fairD);
        C.tr.cfg.maxLoss          = envDsym("MM_MAX_LOSS", sym, baseCfg.maxLoss);
        C.tr.cfg.invSkewFrac      = envDsym("MM_INV_SKEW_FRAC", sym, baseCfg.invSkewFrac);
        C.tr.cfg.cutoffBps        = envDsym("MM_CUTOFF_BPS", sym, baseCfg.cutoffBps);
        C.tr.cfg.cutoffSec        = envDsym("MM_CUTOFF_SEC", sym, baseCfg.cutoffSec);
        C.tr.cfg.trendBps         = envDsym("MM_TREND_BPS", sym, baseCfg.trendBps);   // [F58]
        C.tr.cfg.takerFeeBps      = envDsym("MM_TAKER_BPS", sym, baseCfg.takerFeeBps); // [F59] @SYM
        C.tr.cfg.trendSec         = envDsym("MM_TREND_SEC", sym, baseCfg.trendSec);
        C.tr.cfg.printFilter      = baseCfg.printFilter;            // [F45]
        C.tr.cfg.printFilterMs    = baseCfg.printFilterMs;
        C.tr.cfg.printFillP       = envDsym("MM_PRINT_FILL_P", sym, baseCfg.printFillP);  // [F49]
        if (!(C.tr.cfg.printFillP > 0)) C.tr.cfg.printFillP = 0;
        if (C.tr.cfg.printFillP > 1) C.tr.cfg.printFillP = 1;
        C.tr.cfg.maxSpreadBps     = envDsym("MM_MAX_SPREAD_BPS", sym, baseCfg.maxSpreadBps);
        C.tr.cfg.requoteFrac      = envDsym("MM_REQUOTE_FRAC", sym, baseCfg.requoteFrac);
        C.tr.cfg.minRequoteSec    = envDsym("MM_MIN_REQUOTE_SEC", sym, baseCfg.minRequoteSec);
        C.tr.cfg.escapeCooldownS  = envDsym("MM_ESCAPE_COOLDOWN_SEC", sym, baseCfg.escapeCooldownS);
        C.tr.orderLatMs = latencyMs;
        C.tr.cancelLatMs = cancelLatMs;
        C.tr.rate.init(C.tr.cfg.rateRps);
        C.tr.rate.shared = srPaper && g_sr.on;   // [F61]
        C.startUsdt = envDsym("MM_START_USDT", sym, 10000.0);
        if (C.startUsdt <= 0) C.startUsdt = 10000.0;
        C.tr.quote = C.startUsdt;
        C.tr.initialQuote = C.startUsdt;
        C.tr.rttMs = envD("MM_RTT_MS", 23);
        // [v2.15 R] детерминизм; per-symbol подсеed, чтобы латентности пар
        // не шли в унисон при одном MM_SEED
        // [F49] RNG внутренних принтов: от MM_SEED (детерминизм replay)
        C.tr.prng.seed(gseed ? gseed * 2654435761u + (unsigned)i * 104729u + 17u
                             : (unsigned)nowMsSteady() ^ ((unsigned)i * 104729u));
        if (gseed) {
            C.tr.rng.seed(gseed + (unsigned)i * 7919u);
            if (i == 0) std::srand(gseed);
        } else if (i == 0) {   // [fix F33] маски WS не детерминированы
            std::srand((unsigned)std::time(nullptr) ^ (unsigned)getpid());
        }
        C.flow.windowSec = C.tr.cfg.tfiWindowSec;
        C.flow.runDecaySec = envD("MM_RUN_DECAY", 20.0);
        C.flow.minDeals = envI("MM_MIN_DEALS", 5);
        // доля свободных USDT кошелька: по умолчанию 1/N (равная нарезка)
        C.quoteShare = envDsym("MM_QUOTE_SHARE", sym,
                               envD("MM_QUOTE_SHARE", 1.0 / (double)symbols.size()));
        if (C.quoteShare <= 0 || C.quoteShare > 1.0)
            C.quoteShare = 1.0 / (double)symbols.size();
        C.killFileSym = envOr((std::string("MM_KILL_FILE@") + sym).c_str(), "");

        lprintf("[init] fetching exchangeInfo for %s ...\n", sym.c_str());
        C.si = fetchSymbolInfo(sym);
        // [fix F15] частично распарсенный exchangeInfo в live = тоже фатал
        if (C.si.ok && !C.si.complete && lcfg.live && !g_replay &&
            envI("MM_ALLOW_FALLBACK_INFO", 0) == 0) {
            std::fprintf(stderr,
                "[init] FATAL: exchangeInfo %s распарсен частично (tick и qtyStep "
                "обязательны для live). MM_ALLOW_FALLBACK_INFO=1 — только мок.\n",
                sym.c_str());
            return 1;
        }
        if (!C.si.ok) {
            // [fix F10] live не должен торговать на выдуманных tick/qtyStep/minNotional
            if (lcfg.live && !g_replay && envI("MM_ALLOW_FALLBACK_INFO", 0) == 0) {
                std::fprintf(stderr,
                    "[init] FATAL: exchangeInfo %s недоступен; live на запасных "
                    "tick=0.01/qtyStep=1e-8/minNotional=1 не запускаю. Повторите "
                    "запуск (MM_ALLOW_FALLBACK_INFO=1 — только для мок-стенда).\n",
                    sym.c_str());
                return 1;
            }
            lprintf("[init] WARNING: exchangeInfo %s unavailable, fallback "
                    "tick=0.01 qtyStep=1e-8 — tick will be auto-derived\n",
                    sym.c_str());
            C.si.tick = 0.01;
            C.si.qtyStep = 1e-8;
            C.si.minNotional = 1.0;
        } else {
            lprintf("[init] %s: tick=%.10g qtyStep=%.10g minNotional=%.4g "
                    "base=%s quote=%s\n", sym.c_str(), C.si.tick, C.si.qtyStep,
                    C.si.minNotional, C.si.baseAsset.c_str(),
                    C.si.quoteAsset.c_str());
        }
        if (envDsym("MM_TICK", sym, 0) > 0)     C.si.tick = envDsym("MM_TICK", sym, 0);
        if (envDsym("MM_QTY_STEP", sym, 0) > 0) C.si.qtyStep = envDsym("MM_QTY_STEP", sym, 0);
        C.tick = C.si.tick;
        C.tr.qtyStep = C.si.qtyStep;

        if (lcfg.live) {
            C.live.lcfg = &lcfg;
            C.live.rc.key = envOr("MM_API_KEY", "");
            C.live.rc.secret = envOr("MM_API_SECRET", "");
            C.live.rc.symbol = sym;
            C.live.rc.postOnly = envI("MM_POST_ONLY", 1) != 0;   // [fix F26]
            C.live.feeLookup = lcfg.feeLookup;   // [F54]
            C.live.rc.stpMode = envOr("MM_STP_MODE", "");         // [fix F35]
            for (auto& ch : C.live.rc.stpMode) ch = (char)toupper((unsigned char)ch);   // [fix F43]
            if (!C.live.rc.stpMode.empty() && C.live.rc.stpMode != "CANCEL_MAKER" &&
                C.live.rc.stpMode != "CANCEL_TAKER" && C.live.rc.stpMode != "CANCEL_BOTH") {
                std::fprintf(stderr, "[live] FATAL: MM_STP_MODE=%s (нужно cancel_maker|"
                             "cancel_taker|cancel_both или пусто)\n", C.live.rc.stpMode.c_str());
                return 1;
            }
            if (C.live.rc.postOnly && !C.si.makerOk) {
                C.live.rc.postOnly = false;
                lprintf("[init] WARNING: %s: LIMIT_MAKER нет в orderTypes -> LIMIT GTC "
                        "(ордер может уйти тейкером)\n", sym.c_str());
            }
            C.live.rc.recvWindowMs = (long)envD("MM_RECV_WINDOW_MS", 10000);
            C.live.rc.timeSyncSec = envD("MM_TIME_SYNC_SEC", 60);
            C.live.baseAsset = C.si.baseAsset.empty()
                ? sym.substr(0, sym.size() >= 4 &&
                                sym.compare(sym.size() - 4, 4, "USDT") == 0
                                ? sym.size() - 4 : sym.size())
                : C.si.baseAsset;
            C.live.quoteAsset = C.si.quoteAsset.empty() ? "USDT" : C.si.quoteAsset;
            C.live.liveRate = &restRate;   // [v3.1] общий бюджет на аккаунт
        }
        ctxs.push_back(&C);
        ctxOwn.push_back(std::move(up));
    }

    // ================= [v3.0/v3.1] LIVE-инициализация =================
    std::vector<std::string> globEvents;   // глобальные события (не привязаны к паре)
    std::string listenKey;
    ws::Connection* pconn = nullptr;       // приватный WS (один на аккаунт)
    double lastPrivReconnect = 0, lastListenKeep = 0, lastKillCheck = 0;
    double lastPrivPing = 0, lastPrivRx = 0;   // [fix F13]
    if (lcfg.live) {
        lprintf("[live] time sync...\n");
        bool tsOk = ctxs[0]->live.rc.timeSync();
        for (size_t i = 1; i < ctxs.size(); ++i) {   // раздаём офсет всем
            ctxs[i]->live.rc.timeOffMs = ctxs[0]->live.rc.timeOffMs;
            ctxs[i]->live.rc.lastTimeSync = nowSecs();
        }
        lprintf("[live] time sync %s (offset %+lld ms)\n",
                tsOk ? "OK" : "FAIL (подписанные запросы могут резаться 700003)",
                ctxs[0]->live.rc.timeOffMs);

        // стартовые балансы (истина для гейтов; base0 фиксируется per-pair)
        for (auto* C : ctxs) {
            C->live.pollBalance(C->events);
            for (auto& e : C->events) lprintf("      %s\n", e.c_str());
            C->events.clear();
        }
        lflush();

        // чистый старт: снять старые ордера по КАЖДОЙ паре
        if (lcfg.cleanStart) {
            for (auto* C : ctxs) {
                // [fix F17] раньше при сбое пара молча пропускалась, и ордера
                // прошлого запуска (другой pid в cid) оставались на бирже
                RestResult oo;
                for (int att = 0; att < 4; ++att) {
                    restWaitBlock(30);   // [fix F41]
                    oo = C->live.rc.openOrders();
                    if (oo.ok) break;
                    std::this_thread::sleep_for(std::chrono::seconds(1 + att));
                }
                if (!oo.ok) {
                    std::fprintf(stderr,
                        "[live] FATAL: clean start %s: openOrders не ответил "
                        "(code %ld %.80s) — старые ордера не сняты, live не "
                        "запускаю. MM_LIVE_CLEAN_START=0 — запуск без чистки.\n",
                        C->symbol.c_str(), oo.errCode, oo.errMsg.c_str());
                    return 1;
                }
                int n = 0, nFail = 0, nForeign = 0;
                // [fix F39] mode 1: только ордера бота; чужие/ручные не трогаем
                auto mine = [&](const std::string& ob) {
                    if (lcfg.cleanMode >= 2) return true;
                    std::string c;
                    jsonFindStr(ob, "clientOrderId", c);
                    return LiveExecutor::botCid(c);
                };
                for (auto& ob : jsonSplitObjects(oo.body)) {
                    std::string oid, cid;
                    if (!jsonFindStr(ob, "orderId", oid)) continue;
                    jsonFindStr(ob, "clientOrderId", cid);
                    if (!mine(ob)) { ++nForeign; continue; }
                    restRate.waitToken(1.0);
                    restRate.take();
                    RestResult cr = C->live.rc.cancelOrder(oid, "");
                    if (!cr.ok) ++nFail;   // [fix F28]
                    ++n;
                }
                // [fix F28] проверка: после чистки на паре не должно остаться ордеров
                if (n) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(nFail ? 1500 : 500));
                    restWaitBlock(30);   // [fix F41]
                    RestResult oo2 = C->live.rc.openOrders();
                    auto countMine = [&](const RestResult& rr) {
                        size_t k = 0;
                        if (rr.ok) for (auto& ob : jsonSplitObjects(rr.body)) if (mine(ob)) ++k;
                        return k;
                    };
                    size_t left = countMine(oo2);
                    if (left) {   // ещё одна попытка
                        for (auto& ob : jsonSplitObjects(oo2.body)) {
                            std::string oid;
                            if (!mine(ob)) continue;
                            restRate.waitToken(1.0);
                            restRate.take();
                            if (jsonFindStr(ob, "orderId", oid)) C->live.rc.cancelOrder(oid, "");
                        }
                        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                        restWaitBlock(30);   // [fix F41]
                        oo2 = C->live.rc.openOrders();
                        left = countMine(oo2);
                    }
                    if (!oo2.ok || left) {
                        std::fprintf(stderr,
                            "[live] FATAL: clean start %s: старые ордера не сняты "
                            "(осталось %zu, openOrders %s) — live не запускаю.\n",
                            C->symbol.c_str(), left, oo2.ok ? "ok" : "fail");
                        return 1;
                    }
                }
                if (n) lprintf("[live] %s clean start: снято %d старых ордеров бота\n",
                               C->symbol.c_str(), n);
                if (nForeign) lprintf("[live] %s clean start: %d чужих/ручных ордеров НЕ "
                                      "тронуто (MM_LIVE_CLEAN_START=2 — снять все)\n",
                                      C->symbol.c_str(), nForeign);
            }
        }

        // listenKey + приватный WS (один на аккаунт, все пары)
        bool skipPrivWs = g_replay && lcfg.allowReplayLive;
        if (!skipPrivWs) {
            RestResult lk;
            for (int att = 0; att < 3; ++att) {   // [fix F29]
                restWaitBlock(30);   // [fix F41]
                lk = ctxs[0]->live.rc.listenKeyCreate();
                if (lk.ok && jsonFindStr(lk.body, "listenKey", listenKey) &&
                    !listenKey.empty()) break;
                listenKey.clear();
                std::this_thread::sleep_for(std::chrono::seconds(1 + att));
            }
            if (listenKey.empty() && envI("MM_ALLOW_NO_PRIVWS", 0) == 0) {
                std::fprintf(stderr,
                    "[live] FATAL: listenKey не получен (code %ld %.80s) — без "
                    "приватного WS филлы видны только через recon. Live не "
                    "запускаю (MM_ALLOW_NO_PRIVWS=1 — работать на REST-поллинге).\n",
                    lk.errCode, lk.errMsg.c_str());
                return 1;
            }
            if (!listenKey.empty()) {
                lprintf("[live] listenKey получен (%.8s...), приватный WS\n",
                        listenKey.c_str());
                pconn = new ws::Connection("wbs-api.mexc.com", 443,
                                           "/ws?listenKey=" + listenKey);
                if (pconn->connect()) {
                    lastPrivRx = lastPrivPing = nowSecs();   // [fix F13]
                    pconn->sendText("{\"method\":\"SUBSCRIPTION\",\"params\":["
                                    "\"spot@private.orders.v3.api.pb\"]}");
                    pconn->sendText("{\"method\":\"SUBSCRIPTION\",\"params\":["
                                    "\"spot@private.deals.v3.api.pb\"]}");
                    pconn->sendText("{\"method\":\"SUBSCRIPTION\",\"params\":["
                                    "\"spot@private.account.v3.api.pb\"]}");
                    lprintf("[live] private WS подключён: orders+deals+account "
                            "(общий на %zu пар)\n", ctxs.size());
                } else {
                    lprintf("[live] WARNING: private WS не подключился — "
                            "работаем на REST-поллинге (recon %d с)\n",
                            (int)lcfg.reconSec);
                    delete pconn;
                    pconn = nullptr;
                }
            } else {
                lprintf("[live] WARNING: listenKey не получен (code %ld %.80s) — "
                        "приватного WS нет, филлы через recon/баланс\n",
                        lk.errCode, lk.errMsg.c_str());
            }
        } else {
            lprintf("[live] replay-мок: приватный WS пропущен\n");
        }
        for (auto* C : ctxs) C->live.initDone = true;
        lastListenKeep = nowSecs();
        // [v3.1 fix] стаггеринг recon/balance по парам: не долбим REST пачкой
        double tNow = nowSecs();
        for (size_t i = 0; i < ctxs.size(); ++i) {
            ctxs[i]->live.lastRecon = tNow - lcfg.reconSec
                + (double)i * lcfg.reconSec / (double)ctxs.size();
            ctxs[i]->live.lastBalance = tNow;   // стартовый полл уже сделан выше
        }
    }

    // ---------------------------------------------------------------- banner
    {
        std::string symList;
        for (size_t i = 0; i < symbols.size(); ++i) {
            if (i) symList += ",";
            symList += symbols[i];
        }
        lprintf(
            "================================================================\n"
            "  SPRED v3.3 — MEXC market-making: paper / LIVE (MM_LIVE=1)\n"
            "  мультипарный режим: один процесс, %zu пар(а)\n"
            "================================================================\n"
            "  symbols       : %s\n"
            "  venue ws      : wss://wbs-api.mexc.com/ws (protobuf v3)\n"
            "  channels/sym  : limit.depth(%d) + bookTicker@100ms + deals@100ms\n"
            "  virtual start : %.0f USDT/pair   order size: %.0f USDT/side\n"
            "  mode          : %s\n"
            "  latency       : order %.0f ms, cancel %.0f ms%s; fill-gate по\n"
            "                  БИРЖЕВОМУ времени (deal-домен, v2.13.1)\n"
            "  requote       : >= max(1.5 tick, %.1f%% спреда), min %.0f s\n"
            "  flow          : TFI окно %.0fs / min %d сделок / порог %.2f;\n"
            "                  run-stop %d, затухание %.0fs\n"
            "  skews (спреда): imb %.0f%% tfi %.0f%% inv %.0f%%\n"
            "  risk          : band +/-%.0f USDT, spread cap %.0f bps, loss halt %.0f,\n"
            "                  stale %.0fs (2x -> reconnect), escape %.0fs\n"
            "  risk-add      : init %s | esc-cooldown %.0f s | cutoff %.0f bps / %.0f s\n"
            "  fees          : maker %.1f bps, taker %.1f bps\n"
            "  engine        : v2.16 (event-time книга, монотонность, dual clock,\n"
            "                  ghost lower-bound, markout-якорь) + v3.0 LIVE\n"
            "  v3.1          : %s\n"
            "  log file      : %s   deals log: %s\n"
            "  run           : %s\n"
            "================================================================\n",
            symbols.size(), symList.c_str(), depthLevels,
            ctxs[0]->startUsdt, baseCfg.orderSizeQuote,
            baseCfg.allowShort ? "VIRTUAL SHORT"
                               : (baseCfg.initMode == 1 ? "SPOT RAMP (bid-only)"
                                                        : "SPOT (pre-bought inventory)"),
            latencyMs, cancelLatMs,
            baseCfg.latDist > 0 ? " (DIST)" : "",
            baseCfg.requoteFrac * 100.0, baseCfg.minRequoteSec,
            baseCfg.tfiWindowSec, ctxs[0]->flow.minDeals, baseCfg.tfiThresh,
            baseCfg.runStop, ctxs[0]->flow.runDecaySec,
            baseCfg.imbSkewFrac * 100.0, baseCfg.tfiSkewFrac * 100.0,
            baseCfg.invSkewFrac * 100.0,
            baseCfg.maxPosQuote, baseCfg.maxSpreadBps, baseCfg.maxLoss,
            baseCfg.staleSecs, baseCfg.escapeSecs,
            baseCfg.initMode == 1 ? "RAMP" : "classic",
            baseCfg.escapeCooldownS, baseCfg.cutoffBps, baseCfg.cutoffSec,
            baseCfg.makerFeeBps, baseCfg.takerFeeBps,
            lcfg.live ? "LIVE: реальные ордера (REST HMAC + private WS),\n"
                        "                  shadow v2.16 — валидатор; recon/balance со\n"
                        "                  стаггерингом; kill-file глоб.+per-pair;\n"
                        "                  cancelAll на выходе (все пары)"
                      : "paper (dry-run, shadow off)",
            logPath.empty() ? "(none)" : logPath.c_str(),
            logDeals ? "ON" : "off",
            g_replay ? "replay until raw EOF"
                     : (runSecs > 0 ? (std::to_string((int)runSecs) + "s then summary").c_str()
                                    : "until Ctrl+C"));
        // per-symbol строки для band-регексов sum9 и контроля тиков
        for (auto* C : ctxs)
            lprintf("  pair %-12s: tick %.10g  qtyStep %.10g  minNotional %.4g  "
                    "band +/-%.0f USDT  order %.0f  maxLoss %.0f  skew %.2f\n",
                    C->symbol.c_str(), C->tick, C->si.qtyStep, C->si.minNotional,
                    C->tr.cfg.maxPosQuote, C->tr.cfg.orderSizeQuote,
                    C->tr.cfg.maxLoss, C->tr.cfg.invSkewFrac);
        lprintf("================================================================\n\n");
    }

    double t0 = nowSecs();
    double lastReport = 0, lastPing = 0;
    long msgCount = 0, binFrames = 0, parseFails = 0;
    bool done = false, lost = false;
    int sessions = 0;
    double connAt = nowSecs();
    double rttMs = envD("MM_RTT_MS", 23);   // md-rtt по ping (общий коннект)

    SessionOptions sessionOptions{lcfg, logDeals, fairBasisSec, fairWarmSec, rttMs, t0, replayPath, sessions, msgCount, binFrames, parseFails, connAt, lost};

    auto findCtx = [&](const std::string& sym) -> SymCtx* {
        for (auto* C : ctxs) if (C->symbol == sym) return C;
        return nullptr;
    };
    auto flushEvents = [&](SymCtx& C) { spred::flushEvents(C, sessionOptions); };

    // [v2.12 Ф7] flush evq в порядке exchMs (per-sym)
    if (g_fairMode) {   // [F55]
        std::string fh = envOr("MM_FAIR_HOST", "stream.bybit.com");
        int fp_ = envI("MM_FAIR_PORT", 443);
        std::string fpath = envOr("MM_FAIR_PATH", "/v5/public/spot");
        g_fairUsdcMap = envI("MM_FAIR_USDC_MAP", 1) != 0;   // [F64]
        std::vector<std::string> fsyms;
        for (auto& sy : symbols) {
            std::string r = fairRef(sy);
            if (std::find(fsyms.begin(), fsyms.end(), r) == fsyms.end()) fsyms.push_back(r);
        }
        std::thread(bybitFairThread, fsyms, fh, fp_, fpath).detach();
        lprintf("[FAIR] MM_FAIR=bybit: wss://%s:%d%s, базис EWMA %.0f с, прогрев %.0f с\n",
                fh.c_str(), fp_, fpath.c_str(), fairBasisSec, fairWarmSec);
        for (auto* C : ctxs)
            lprintf("[FAIR] %s: d %.1f bps, skew %.1f bps, requote %.1f bps, stale %.0f ms, "
                    "max dev %.0f bps\n", C->symbol.c_str(), C->fairD, C->fairSkew, C->fairRq,
                    C->fairStaleMs, C->fairMaxDev);
    }

    auto flushEvq = [&](SymCtx& C) { spred::flushEvq(C, sessionOptions); };

    // ---------------- per-symbol тик: gap -> INIT -> advance -> стратегия ->
    // live-зеркало. Вызывается каждую итерацию цикла для каждой пары.
    auto tickSymbol = [&](SymCtx& C, double now) { spred::tickSymbol(C, now, sessionOptions); };

    // ---------------- periodic report (per-symbol; формат строки — как v2.16,
    // чтобы sum9/R_REP продолжал парсить; символ — в конце строки) -----------
    auto reportSymbol = [&](SymCtx& C, double now) { spred::reportSymbol(C, now, sessionOptions); };

    // ---------------- SUMMARY per-symbol в произвольный FILE* ----------------
    // Формат строк — идентичен v2.16/v3.0 (sum9-регексы не тронуты).
    // sessionHeader=true -> "SESSION SUMMARY" (per-pair файлы для sum9);
    // false -> "PAIR SUMMARY" (комбинированный лог/консоль; sum9 его
    // пропускает, не плодя мусорных записей).
    auto printSummary = [&](SymCtx& C, FILE* fp, bool sessionHeader) { spred::printSummary(C, fp, sessionHeader, sessionOptions); };

    auto flushGlob = [&]() {
        for (auto& e : globEvents) lprintf("      %s\n", e.c_str());
        globEvents.clear();
    };

    try {   // [fix F24] исключение -> штатный shutdown с cancelAll
    while (!done) {
        ++sessions;
        ws::Connection conn("wbs-api.mexc.com", 443, "/ws");

        // [v2.15 R4 REPLAY] источник кадров: live=WS, replay=raw
        auto frameWait = [&](int timeoutMs) -> int {
            if (!g_replay) return conn.waitReadable(timeoutMs);
            if (!RSrc.haveNext && !RSrc.readNext()) return 0;
            if (g_simNowMs < 0) g_simNowMs = RSrc.nextRecvMs;
            if (RSrc.nextRecvMs <= g_simNowMs) return 1;
            double adv = std::min((double)timeoutMs, RSrc.nextRecvMs - g_simNowMs);
            if (replaySpeed > 0)
                std::this_thread::sleep_for(
                    std::chrono::duration<double>(adv / 1000.0 / replaySpeed));
            g_simNowMs += adv;
            return 0;
        };
        auto frameRead = [&](ws::Frame& f) -> bool {
            if (!g_replay) return conn.readFrame(f);
            if (!RSrc.haveNext && !RSrc.readNext()) return false;
            f.opcode = 0x2;
            f.fin = true;
            f.payload = std::move(RSrc.nextPayload);
            RSrc.haveNext = false;
            return true;
        };

        // [v2.15 R5a] live: connect + subscribe; replay: без сети
        if (!g_replay) {
            bool connected = false;
            for (int attempt = 1; !connected; ++attempt) {
                lprintf("[net] connecting (session %d, attempt %d)...\n", sessions, attempt);
                if (conn.connect()) { connected = true; break; }
                if (runSecs > 0 && nowSecs() - t0 >= runSecs) { done = true; break; }
                if (g_stop) { done = true; break; }
                std::this_thread::sleep_for(std::chrono::seconds(3));
            }
            if (done) break;
            lprintf("[net] websocket connected\n");

            // [v3.1] подписки всех пар на одном коннекте (3 канала × N)
            for (auto* C : ctxs) {
                const std::string& symbol = C->symbol;
                conn.sendText("{\"method\":\"SUBSCRIPTION\",\"params\":["
                              "\"spot@public.limit.depth.v3.api.pb@" + symbol +
                              "@" + std::to_string(depthLevels) + "\"]}");
                conn.sendText("{\"method\":\"SUBSCRIPTION\",\"params\":["
                              "\"spot@public.aggre.bookTicker.v3.api.pb@100ms@" +
                              symbol + "\"]}");
                conn.sendText("{\"method\":\"SUBSCRIPTION\",\"params\":["
                              "\"spot@public.aggre.deals.v3.api.pb@100ms@" +
                              symbol + "\"]}");
            }
            lprintf("[net] subscribed: %zu pairs × (depth(%d) + bookTicker + deals)\n",
                    ctxs.size(), depthLevels);
            lastPing = nowSecs();
        } else {
            lprintf("[replay] session %d: frames from raw, sym %s\n",
                    sessions, symbols[0].c_str());
            // [v2.16 fix] строка-подписка для sum7/sum9 (R_SYM)
            lprintf("[ws] text: {\"id\":0,\"code\":0,\"msg\":\"spot@public.limit.depth.v3.api.pb@%s@20\"}\n",
                    symbols[0].c_str());
        }
        connAt = nowSecs();
        bool pingOutstanding = false;
        double pingSentMs = 0;
        int textLogged = 0;

        std::string msgBuf;
        int msgOpcode = -1;
        lost = false;

        while (!lost && !done) {
            if (g_replay && RSrc.eof) { done = true; break; }

            int rd = frameWait(g_fairMode ? 50 : 500);   // [F55] чаще тикаем за Bybit
            if (rd < 0) { lprintf("[net] socket error\n"); lost = true; break; }

            if (rd > 0) {
                ws::Frame f;
                if (!frameRead(f)) { lprintf("[net] connection lost\n"); lost = true; break; }
                ++msgCount;
                if (pingOutstanding) {
                    double m = nowMsSteady() - pingSentMs;
                    if (m >= 5 && m < 5000) rttMs = 0.7 * rttMs + 0.3 * m;
                    pingOutstanding = false;
                }
                if (f.opcode == 0x8) { lprintf("[net] server sent close\n"); lost = true; break; }
                if (f.opcode == 0x9) { conn.sendPong(f.payload); continue; }
                if (f.opcode == 0x1) {
                    if (textLogged < 5 + (int)ctxs.size() * 3) {
                        lprintf("[ws] text: %.200s\n", f.payload.c_str());
                        ++textLogged;
                    }
                    continue;
                }
                if (f.opcode == 0x0) {
                    msgBuf += f.payload;
                    if (f.fin) {
                        f.opcode = msgOpcode;
                        f.payload = std::move(msgBuf);
                        msgBuf.clear();
                        msgOpcode = -1;
                    } else continue;
                } else if (!f.fin) {
                    msgBuf = f.payload;
                    msgOpcode = f.opcode;
                    continue;
                }
                if (f.opcode != 0x2) continue;
                ++binFrames;

                // [v2.13 ФИКС 15] raw recorder
                if (rawFile) {
                    double rRecv = nowMsSteady();
                    uint32_t rLen = (uint32_t)f.payload.size();
                    std::fwrite(&rRecv, sizeof rRecv, 1, rawFile);
                    std::fwrite(&rLen, sizeof rLen, 1, rawFile);
                    if (rLen) std::fwrite(f.payload.data(), 1, rLen, rawFile);
                }

                MexcPush push;
                if (!parsePush(f.payload, push)) { ++parseFails; continue; }

                // [v3.1] диспетчер по символу
                SymCtx* C = findCtx(push.symbol);
                if (!C) continue;
                C->book.lastPacket = nowSecs();   // [v2.16 P0-2]
                if (push.hasTicker) ++C->tickerFrames;
                if (push.hasLimitDepth) ++C->depthFrames;
                if (push.hasDeals) ++C->dealFrames;

                if (push.sendTimeMs > 0) {
                    double sample = nowMsSteady() - (double)push.sendTimeMs;
                    if (!C->tr.exchOffInit) { C->tr.exchOffMs = sample; C->tr.exchOffInit = true; }
                    else if (std::fabs(sample - C->tr.exchOffMs) < 5000.0)
                        C->tr.exchOffMs = 0.9 * C->tr.exchOffMs + 0.1 * sample;
                }

                // [v2.12 Ф7] event-time очередь (санитайзинг при постановке)
                // [v2.13.1] сделки: калибровка deal-часов + маппинг в send-домен
                {
                    auto& tr = C->tr;
                    double recvMs = nowMsSteady();
                    double exchTs = (double)push.sendTimeMs;
                    if (exchTs <= 0 && tr.exchOffInit) exchTs = recvMs - tr.exchOffMs;
                    if (exchTs <= 0) exchTs = recvMs;
                    if (exchTs > C->evqMaxExchMs) C->evqMaxExchMs = exchTs;

                    if (push.hasLimitDepth) {
                        PendEv e;
                        e.kind = 0;
                        e.exchMs = exchTs;
                        e.recvMs = recvMs;
                        for (const auto& l : push.bids)
                            if (l.price > 0 && std::isfinite(l.price) &&
                                std::isfinite(l.qty) && l.qty >= 0)
                                e.bids.push_back(l);
                        for (const auto& l : push.asks)
                            if (l.price > 0 && std::isfinite(l.price) &&
                                std::isfinite(l.qty) && l.qty >= 0)
                                e.asks.push_back(l);
                        C->evq.push_back(std::move(e));
                    }
                    if (push.hasTicker) {
                        PendEv e;
                        e.kind = 1;
                        e.exchMs = exchTs;
                        e.recvMs = recvMs;
                        e.bidP = push.bidP; e.bidQ = push.bidQ;
                        e.askP = push.askP; e.askQ = push.askQ;
                        C->evq.push_back(std::move(e));
                    }
                    if (push.hasDeals) {
                        for (const auto& d : push.deals) {
                            PendEv e;
                            e.kind = 2;
                            e.recvMs = recvMs;
                            if (d.timeMs > 0) {
                                tr.calibDealClock(recvMs - (double)d.timeMs);
                                double skw = tr.clockSkewMs();
                                if (skw > 30000.0) skw = 30000.0;
                                if (skw < -30000.0) skw = -30000.0;
                                e.exchMs = (double)d.timeMs - skw;
                            } else {
                                e.exchMs = exchTs;
                            }
                            e.deal = d;
                            C->evq.push_back(std::move(e));
                        }
                    }
                }
            }

            // [v2.12 Ф7] flush всех per-symbol очередей
            for (auto* C : ctxs) flushEvq(*C);

            double now = nowSecs();

            // ============ [v3.0 L2] приватный WS: неблокирующий дрен =========
            // [v3.1] один коннект на аккаунт; события диспетчатся по symbol.
            // Сделка/ордер с неизвестным символом НЕ раздаётся всем (иначе
            // чужой cid считался бы ghost-филлом в каждой паре) — только лог.
            if (lcfg.live && !listenKey.empty()) {
                if (pconn && pconn->connected()) {
                    for (int guard = 0; guard < 50; ++guard) {
                        int pr = pconn->waitReadable(0);
                        if (pr <= 0) break;
                        ws::Frame pf;
                        if (!pconn->readFrame(pf)) {
                            lastPrivRx = 0;
                            lprintf("[live] private WS оборвался -> переподключение\n");
                            pconn->close();
                            lastPrivReconnect = now;
                            break;
                        }
                        if (pf.opcode == 0x8) {
                            lprintf("[live] private WS: server close -> reconnect\n");
                            pconn->close();
                            lastPrivReconnect = now;
                            break;
                        }
                        lastPrivRx = now;   // [fix F13] любой кадр = соединение живо
                        if (pf.opcode == 0x9) { pconn->sendPong(pf.payload); continue; }
                        if (pf.opcode == 0x1) {   // acks подписок / PONG
                            // [F53] первые ответы приватного WS — в лог (диагностика
                            // подписок: deals 0 при orders>0)
                            static int privTxt = 0;
                            if (privTxt < 12 && pf.payload.find("PONG") == std::string::npos) {
                                ++privTxt;
                                lprintf("[live] private WS text: %.300s\n", pf.payload.c_str());
                            }
                            continue;
                        }
                        if (pf.opcode != 0x2) continue;
                        MexcPush pp;
                        if (!parsePush(pf.payload, pp)) continue;
                        SymCtx* PC = findCtx(pp.symbol);
                        if (pp.hasPrivDeal) {
                            if (PC) {
                                double anchor = now;
                                if (pp.pdeal.timeMs > 0 && PC->tr.dealOffInit)
                                    anchor = std::min(now,
                                        ((double)pp.pdeal.timeMs + PC->tr.exchOffDealMs) / 1000.0);
                                PC->live.onPrivDeal(pp.pdeal, anchor, PC->events);
                            } else {
                                evAdd(globEvents, "[LIVE] private deal чужого символа " +
                                      pp.symbol + " — пропуск (не наша пара)");
                            }
                        }
                        if (pp.hasPrivOrder) {
                            if (PC) PC->live.onPrivOrder(pp.pord, now, PC->events);
                        }
                        if (pp.hasPrivAcct) {
                            for (auto* C : ctxs) C->live.onPrivAccount(pp.pacct);
                        }
                    }
                    // [fix F13] приватный поток тихий: без PING сервер рвёт
                    // соединение. PING раз в 15 с; 45 с без кадров -> реконнект.
                    if (pconn->connected()) {
                        if (now - lastPrivPing >= 15.0) {
                            lastPrivPing = now;
                            if (!pconn->sendText("{\"method\":\"PING\"}")) {
                                lprintf("[live] private WS: PING не ушёл -> reconnect\n");
                                pconn->close();
                                lastPrivReconnect = now;
                            }
                        }
                        if (pconn->connected() && lastPrivRx > 0 && now - lastPrivRx > 45.0) {
                            lprintf("[live] private WS: 45 с тишины -> reconnect\n");
                            pconn->close();
                            lastPrivReconnect = now;
                        }
                    }
                } else if (now - lastPrivReconnect > 5.0) {
                    lastPrivReconnect = now;
                    lprintf("[live] private WS reconnect...\n");
                    delete pconn;
                    pconn = new ws::Connection("wbs-api.mexc.com", 443,
                                               "/ws?listenKey=" + listenKey);
                    if (pconn->connect()) {
                        pconn->sendText("{\"method\":\"SUBSCRIPTION\",\"params\":["
                                        "\"spot@private.orders.v3.api.pb\"]}");
                        pconn->sendText("{\"method\":\"SUBSCRIPTION\",\"params\":["
                                        "\"spot@private.deals.v3.api.pb\"]}");
                        pconn->sendText("{\"method\":\"SUBSCRIPTION\",\"params\":["
                                        "\"spot@private.account.v3.api.pb\"]}");
                        lastPrivRx = lastPrivPing = now;   // [fix F13]
                        lprintf("[live] private WS восстановлен; сверка ордеров\n");
                        for (auto* C : ctxs) {
                            C->live.reconcile(now, C->events);
                            // [fix F13] добор филлов окна обрыва по живым ордерам:
                            // resolve сравнит executedQty с учтённым dealtQty
                            for (int i = 0; i < 2; ++i)
                                if (C->live.s[i].open)
                                    C->live.resolve(C->live.s[i], i == 0, now, C->events, false);
                        }
                    } else {
                        delete pconn;
                        pconn = nullptr;
                    }
                }
            }

            // [v3.0/v3.1] kill-file: глобальный (все пары) + per-symbol
            if (lcfg.live && now - lastKillCheck >= 5.0) {
                lastKillCheck = now;
                struct stat st;
                if (!lcfg.killFile.empty() && stat(lcfg.killFile.c_str(), &st) == 0) {
                    lprintf("[KILL] глобальный kill-file %s -> HALT ALL + cancelAll\n",
                            lcfg.killFile.c_str());
                    for (auto* C : ctxs) {
                        C->tr.halted = true;
                        C->live.cancelAll(C->events);
                        flushEvents(*C);
                    }
                }
                for (auto* C : ctxs) {
                    if (C->killFileSym.empty() || C->tr.halted) continue;
                    if (stat(C->killFileSym.c_str(), &st) == 0) {
                        C->tr.halted = true;
                        lprintf("[KILL] %s kill-file -> HALT + cancelAll\n",
                                C->symbol.c_str());
                        C->live.cancelAll(C->events);
                        flushEvents(*C);
                    }
                }
            }

            // [v3.0/v3.1] периодические live-задачи: recon (GHOST-детект),
            // балансы, ресинк времени, keepalive listenKey.
            // [v3.1 fix] таймеры lastRecon/lastBalance обновляются ЗДЕСЬ —
            // в v3.0 они не обновлялись нигде и поллинг шёл каждую итерацию.
            if (lcfg.live) {
                for (auto* C : ctxs) {
                    if (now - C->live.lastRecon >= lcfg.reconSec) {
                        C->live.lastRecon = now;
                        C->live.reconcile(now, C->events);
                    }
                    if (now - C->live.lastBalance >= lcfg.balanceSec) {
                        C->live.lastBalance = now;
                        C->live.pollBalance(C->events);
                    }
                }
                if (now - ctxs[0]->live.rc.lastTimeSync >= ctxs[0]->live.rc.timeSyncSec) {
                    if (ctxs[0]->live.rc.timeSync())
                        for (size_t i = 1; i < ctxs.size(); ++i) {
                            ctxs[i]->live.rc.timeOffMs = ctxs[0]->live.rc.timeOffMs;
                            ctxs[i]->live.rc.lastTimeSync = now;
                        }
                }
                if (!listenKey.empty() && now - lastListenKeep >= 1800.0) {
                    lastListenKeep = now;
                    RestResult kk = ctxs[0]->live.rc.listenKeyKeepalive(listenKey);
                    if (!kk.ok) {
                        evAdd(globEvents, std::string(
                            "[LIVE] listenKey keepalive FAIL -> пересоздаю ключ"));
                        RestResult nlk = ctxs[0]->live.rc.listenKeyCreate();
                        if (nlk.ok) {
                            std::string nk;
                            if (jsonFindStr(nlk.body, "listenKey", nk) && !nk.empty()) {
                                listenKey = nk;
                                if (pconn) { delete pconn; pconn = nullptr; }
                                lastPrivReconnect = now;
                            }
                        } else {
                            lastListenKeep = now - 1740.0;  // повтор через минуту
                        }
                    }
                }
            }

            // ---------------- per-symbol тики (стратегия + live-зеркало)
            for (auto* C : ctxs) tickSymbol(*C, now);

            // keepalive PING + rtt-замер (общий публичный коннект)
            if (!g_replay) {
                if (now - lastPing > 10) {
                    conn.sendText("{\"method\":\"PING\"}");
                    lastPing = now;
                    pingOutstanding = true;
                    pingSentMs = nowMsSteady();
                }
                if (pingOutstanding && nowMsSteady() - pingSentMs > 15000) {
                    lprintf("[net] ping timeout -> reconnecting\n");
                    lost = true;
                    break;
                }
            }

            // periodic report (все пары + глобальные события)
            if (g_fairMode) {   // [F55] логи потока Bybit + статистика раз в минуту
                std::vector<std::string> fl;
                { std::lock_guard<std::mutex> lk(g_fairMu); fl.swap(g_fairLog); }
                for (auto& m : fl) lprintf("%s\n", m.c_str());
                if (now - lastFairStat >= 60.0) {
                    lastFairStat = now;
                    std::string st = "[FAIR] stat: msgs " + std::to_string(g_fairMsgs.load()) +
                                     ", reconn " + std::to_string(g_fairReconn.load());
                    for (auto* C : ctxs) {
                        char b[200];
                        std::snprintf(b, sizeof b, " | %s age %.0fms basis %+.1f ok %.0f%%",
                                      C->symbol.c_str(), C->lastYAge > 1e8 ? -1.0 : C->lastYAge,
                                      C->basis * 1e4,
                                      C->fairTicks ? 100.0 * C->fairOk / C->fairTicks : 0.0);
                        st += b;
                    }
                    lprintf("%s\n", st.c_str());
                }
            }
            if (now - lastReport >= 5.0) {
                lastReport = now;
                for (auto* C : ctxs) reportSymbol(*C, now);
                flushGlob();
                lflush();
            }

            // [v2.15 R5d] replay идёт до EOF raw; runSecs — только live
            if (runSecs > 0 && !g_replay && now - t0 >= runSecs) { done = true; break; }
            if (g_stop) { done = true; break; }
        }

        if (lost && !done) {
            if (g_replay) break;   // [v2.15 R5e] replay не реконнектится
            // [fix F12] данных нет -> котировки «вслепую»: снять реальные
            // ордера ВСЕХ пар до реконнекта (блокирующий connect может идти
            // долго, а tickSymbol в это время не вызывается)
            if (lcfg.live && envI("MM_CANCEL_ON_WS_LOSS", 1) != 0) {   // [fix F37]
                lprintf("[live] публичный WS потерян -> cancelAll ВСЕХ пар\n");
                for (auto* C : ctxs) {
                    C->live.cancelAll(C->events);
                    flushEvents(*C);
                }
                flushGlob();
                lflush();
            }
            // [v3.1] обрыв общего коннекта: gap-сохранение ВСЕХ пар
            for (auto* C : ctxs) {
                auto& tr = C->tr;
                C->gapMid0 = C->book.ok() ? C->book.mid() : tr.lastMid;
                C->gapMid0Valid = C->book.ok() || tr.lastMid > 0;
                // [v2.13.1] оба офсета пересчитываются на новом коннекте
                tr.exchOffInit = false;
                tr.dealOffInit = false;
                C->evq.clear();
                C->evqMaxExchMs = 0;
                C->lastAppliedExchMs = 0;   // [v2.16 P0-1] новая шкала
                auto saveGap = [&](SideBook& sb) {
                    if ((sb.st == QState::LIVE || sb.st == QState::WAIT_CANCEL) &&
                        sb.cur.qty - sb.cur.filled > 1e-15)
                        tr.gapCheck.push_back({(int)sb.cur.side, sb.cur.price,
                                               sb.cur.qty - sb.cur.filled});
                    sb.st = QState::NONE;
                    sb.hasNext = false;
                    sb.cur = Order{};
                };
                saveGap(tr.buy);
                saveGap(tr.sell);
                tr.ghostBuy.active = false;
                tr.ghostSell.active = false;
                if (!tr.gapCheck.empty()) tr.gapPending = true;
                C->book = BookState{};
                C->depthReady = false;
                C->warnedNoDepth = false;
            }
            lprintf("[net] reconnecting (orders saved for gap-check, books reset)...\n");
        }
    }

    } catch (const std::exception& e) {
        lprintf("[FATAL] исключение в главном цикле: %s -> shutdown + cancelAll\n", e.what());
        lflush();
    } catch (...) {
        lprintf("[FATAL] неизвестное исключение в главном цикле -> shutdown + cancelAll\n");
        lflush();
    }

    // ------------- [v3.0/v3.1] shutdown: снять реальные ордера ВСЕХ пар,
    // финальная сверка (ghost-добор), закрыть listenKey. По решению ТЗ —
    // всегда cancelAll при остановке; MM_KEEP_ORDERS_ON_EXIT=1 отменяет.
    if (g_sr.on)   // [F61]
        lprintf("[F61] лимитер (этот процесс): new %ld/%ld cancel %ld/%ld replace %ld/%ld "
                "other %ld/%ld (разрешено/отказ), принудительно %ld\n",
                g_sr.granted[RK_NEW], g_sr.denied[RK_NEW], g_sr.granted[RK_CANCEL],
                g_sr.denied[RK_CANCEL], g_sr.granted[RK_REPLACE], g_sr.denied[RK_REPLACE],
                g_sr.granted[RK_OTHER], g_sr.denied[RK_OTHER], g_sr.forced);
    if (lcfg.live) {
        if (lcfg.live && (g_rate429 || g_rate418))   // [fix F41]
            lprintf("[live] за сессию: HTTP 429 x%d, 418 x%d — снизить MM_RATE_RPS/"
                    "число пар\n", g_rate429, g_rate418);
        if (!lcfg.keepOnExit) {
            lprintf("[live] shutdown: cancelAll ВСЕХ пар (MM_KEEP_ORDERS_ON_EXIT=0)\n");
            for (auto* C : ctxs) {
                C->live.cancelAll(C->events);
                C->live.reconcile(nowSecs(), C->events);
                C->live.cancelAll(C->events);   // [fix F14] воскрешённые reconcile'ом
                flushEvents(*C);
            }
        } else {
            lprintf("[live] shutdown: MM_KEEP_ORDERS_ON_EXIT=1 — ордера "
                    "ОСТАВЛЕНЫ на бирже!\n");
        }
        flushGlob();
        if (!listenKey.empty()) ctxs[0]->live.rc.listenKeyClose(listenKey);
        if (pconn) { delete pconn; pconn = nullptr; }
    }

    // ------------------------------------------------------ final summary
    // [v3.1] в консоль/комбинированный лог — заголовок PAIR SUMMARY (sum9
    // такие файлы пропускает); per-pair копии в <лог>_<SYM>.log — с
    // SESSION SUMMARY (sum9 читает их как раньше, по одной записи на пару).
    bool multi = ctxs.size() > 1;
    for (auto* C : ctxs) {
        bool sessHdr = !multi;
        printSummary(*C, stdout, sessHdr);
        if (g_logFile) printSummary(*C, g_logFile, sessHdr);
        if (multi && !logPath.empty()) {
            std::string base = logPath;
            size_t dot = base.find_last_of('.');
            size_t slash = base.find_last_of('/');
            if (dot != std::string::npos &&
                (slash == std::string::npos || dot > slash))
                base = base.substr(0, dot);
            std::string pf = base + "_" + C->symbol + ".log";
            FILE* sf = std::fopen(pf.c_str(), "w");
            if (sf) {
                // строки для R_SYM (символ) и R_BAND (band) из sum9
                std::fprintf(sf,
                    "[ws] text: {\"id\":0,\"code\":0,\"msg\":\"spot@public.limit.depth.v3.api.pb@%s@20\"}\n"
                    "  pair %-12s: tick %.10g  qtyStep %.10g  minNotional %.4g  "
                    "band +/-%.0f USDT  order %.0f  maxLoss %.0f  skew %.2f\n",
                    C->symbol.c_str(), C->symbol.c_str(), C->tick, C->si.qtyStep,
                    C->si.minNotional, C->tr.cfg.maxPosQuote,
                    C->tr.cfg.orderSizeQuote, C->tr.cfg.maxLoss,
                    C->tr.cfg.invSkewFrac);
                printSummary(*C, sf, true);
                std::fclose(sf);
                lprintf("[summary] per-pair лог для sum9: %s\n", pf.c_str());
            }
        }
    }

    // [v3.1] портфельный итог (по всем парам)
    if (multi) {
        double sumPnl = 0, sumVol = 0, sumLivePnl = 0, sumLiveVol = 0;
        int sumFills = 0, sumLiveFills = 0;
        for (auto* C : ctxs) {
            double mid = C->book.ok() ? C->book.mid() : C->tr.lastMid;
            sumPnl += C->tr.totalPnl(mid);
            sumVol += C->tr.volumeTraded;
            sumFills += C->tr.fills;
            if (lcfg.live) {
                sumLivePnl += C->live.ledger.sessionPnl(mid);
                sumLiveVol += C->live.ledger.volume;
                sumLiveFills += C->live.ledger.fills;
            }
        }
        lprintf(
            "\n================================================================\n"
            "  PORTFOLIO SUMMARY (%zu пар, spred v3.1)\n"
            "================================================================\n"
            "  shadow P&L суммарно : %+.4f USDT   vol %.0f   %+.2f bps/vol   fills %d\n",
            ctxs.size(), sumPnl, sumVol,
            sumVol > 0 ? sumPnl / sumVol * 1e4 : 0.0, sumFills);
        if (lcfg.live)
            lprintf(
            "  LIVE P&L суммарно   : %+.4f USDT   vol %.0f   %+.2f bps/vol   fills %d\n",
            sumLivePnl, sumLiveVol,
            sumLiveVol > 0 ? sumLivePnl / sumLiveVol * 1e4 : 0.0, sumLiveFills);
        lprintf("================================================================\n");
    }
    lflush();

    if (rawFile) { std::fclose(rawFile); rawFile = nullptr; }   // [v2.13]
    if (RSrc.f) { std::fclose(RSrc.f); RSrc.f = nullptr; }      // [v2.15 R]
    if (g_logFile) {
        std::fprintf(g_logFile, "==== SESSION END %s ====\n", nowWallClock().c_str());
        std::fclose(g_logFile);
        g_logFile = nullptr;
    }
    return 0;
}

} // namespace spred
