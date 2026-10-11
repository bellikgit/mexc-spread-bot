// ============================================================================
// spred.cpp v3.3 — Paper + LIVE-Trading Market-Making Bot (spread collection)
//
// v3.3 over v3.2.1 (всё за флагами, по умолчанию поведение v3.2.1):
//   F60  MM_ESC_MAKER(0): при |pos| > MM_MAX_POS вместо MARKET-escape —
//        мейкерская заявка на сокращение по fair*(1±MM_ESC_MAKER_BPS/1e4)
//        размером до излишка (|pos|-0.75*band), наращивающая сторона снята.
//        MARKET — только если не ушли за MM_ESC_MAKER_SEC (180) или
//        |pos| > MM_ESC_MAKER_HARD (1.5) * MM_MAX_POS. Режим до |pos|<=0.75*band.
//   F61  MM_SHARED_RATE(0): общий лимитер REST для всех процессов одного UID
//        (/dev/shm/spred_rate_<uid>, flock): MM_SR_NEW 5 новых/с, MM_SR_TOTAL
//        12 запросов/с (окно MM_SR_WIN_MS 1100), MM_SR_CANCEL_RESERVE 2 слота
//        только для отмен (приоритет отмен), пауза после 429/418 общая.
//        Перестановка = отмена+новая; при отказе лимитера ставится последняя
//        желаемая цена (повторные перестановки сливаются). MM_SR_PAPER=1 —
//        тот же лимитер в paper (оценка ограничений на 12 парах).
//   F62  fair-сигналы: MM_FAIR_MICRO w (0) — микроцена Bybit вместо мида
//        (fair = mid + w*(micro-mid)); MM_FAIR_IMB_BPS (0) — сдвиг котировки
//        (imb-0.5)*2*X bps по дисбалансу 10 уровней MEXC; MM_FAIR_SIG_MAX_BPS
//        (0.5*d) — потолок сигнального сдвига.
//   F63  Avellaneda–Stoikov: MM_AS_GAMMA g (0=выкл): сдвиг резервной цены
//        -(pos/band)*g*sigma^2*MM_AS_TAU (60 c), sigma^2 — EWMA (MM_AS_VOL_SEC
//        300) секундных приращений fair в bps^2/c; потолок MM_AS_MAX_BPS (d).
//        Задумано вместо F58 (MM_TREND_BPS=0).
//   F64  USDC-пары: Bybit-символ для fair = BASE+USDT (MM_FAIR_USDC_MAP=1).
//
// v3.2.1 over v3.2.0 (по итогам ночи 05->06.10: HALT держал позицию, стоп
//   срабатывал от стартового запаса, бот докупал в падающих парах):
//   F56  MM_LOSS_BASIS(1): paper-стоп MM_MAX_LOSS по mm-P&L стратегии
//        (totalPnl + setupCost - base0*(mid-mid0)), как sessionPnl в live.
//        0 = как раньше (полный P&L со стартовым запасом).
//   F57  MM_HALT_FLAT(1): при HALT закрыть позицию стратегии тейкером
//        (paper: через pendEsc с задержкой MM_LATENCY; live: MARKET, повтор
//        каждые MM_ESCAPE_SECS, до 5 попыток, пока |pos| >= minNotional).
//        0 = как раньше (снять ордера и держать позицию).
//   F58  MM_TREND_BPS(0=выкл, @SYM) / MM_TREND_SEC(900, @SYM): если mid за окно
//        упал больше порога — не наращивать лонг (снять bid, ask остаётся);
//        вырос — не наращивать шорт. Лог [TREND], в сводке секунды блока.
//   F59  MM_TAKER_BPS@SYM — тейкер-комиссия по парам (0.064% -> 6.4, 0.04% -> 4).
//
// v3.2.0 over v3.1.10:
//   F55  MM_FAIR=bybit — котирование от ВНЕШНЕЙ справедливой цены. Отдельный поток
//        держит WS wss://stream.bybit.com/v5/public/spot (orderbook.1.SYM, ping 20 с,
//        реконнект с backoff). fair = Bybit-мид * exp(базис), базис = EWMA
//        log(MEXC-мид/Bybit-мид) с постоянной MM_FAIR_BASIS_SEC(600). Котировки
//        bid = fair*(1-d), ask = fair*(1+d), d = MM_FAIR_D_BPS(15, @SYM), bid вниз /
//        ask вверх к тику, post-only кламп как раньше. Сдвиги imb/tfi/inv-от-спреда
//        выключены; инвентарный сдвиг MM_FAIR_SKEW_BPS(0) * pos/MAX_POS. Реквот при
//        сдвиге >= MM_FAIR_RQ_BPS(max(2, 0.2*d)). Нет свежей цены Bybit дольше
//        MM_FAIR_STALE_MS(1500) / базис уехал > MM_FAIR_MAX_DEV_BPS(100) / прогрев
//        MM_FAIR_WARM_SEC(30) -> обе стороны снимаются. Тик стратегии каждые <=50 мс.
//        Логи [FAIR], строка "fair (F55)" в итогах. В replay выключен (нет Bybit в
//        записи). MM_FAIR_HOST/PORT/PATH — для теста. Без MM_FAIR = v3.1.10 побайтно.
// C++17, OpenSSL only. Linux / WSL.
//
// v3.1.1 over v3.1 — ФИКСЫ (аудит кода + проверка на локальном TLS-моке):
//   F1 [CRIT] signedReq: буфер подписи char[64] обрезал POST/DELETE (place/
//          cancel/getOrder/market -> 700002, при этом openOrders/account
//          проходили). Теперь std::string (buildSignedQs).
//   F2 [SEC]  TLS: проверка сертификата и имени хоста (REST, нативный
//          fallback, публичный и приватный WS). MM_TLS_NOVERIFY=1 — только
//          для мок-стенда. Нужен пакет ca-certificates.
//   F3 [CRIT] SIGPIPE игнорируется: запись в оборванный сокет убивала процесс
//          без cancelAll (ордера оставались на бирже).
//   F4 [LIVE] учёт филлов: кредит-схема (dealtQty/dealSeenQty/closedOrds) —
//          нет двойного учёта, если апдейт ордера пришёл раньше сделки, при
//          PARTIALLY_CANCELED и при цепочке реквотов; fee не теряется.
//   F5 [DIAG] pxmo/txmo: знаменатели копились как числитель (метрики были
//          тождественно 1/0). На торговлю не влияло.
//   F6 разное: длины в protobuf/WS без переполнения (hostile frame), утечка
//          при неудачном WS-handshake, cid-счётчик общий (нет коллизий между
//          парами), sell в live только из инвентаря СЕССИИ
//          (MM_SELL_WALLET_BASE=1 = старое поведение), MM_QTY_DRIFT_USDT
//          (0 = как в v3.1), va_end в lprintf, баннер читает реальные
//          minDeals/runDecay, дефолты в шапке приведены к коду.
//   F7 [LIVE] fmtDec: цены/qty уходили с мусорными знаками (0.2999..89).
//   F8 [LIVE] HttpClient: ответ из 3+ HTTP-чанков висел до таймаута.
//   F9 [LIVE] дедуп приватных сделок по tradeId (повтор = двойной учёт).
//   F10 [LIVE] нет exchangeInfo -> live не стартует на запасном tick/qtyStep.
//
// v3.1.3 over v3.1.2:
//   F11 [LIVE] HttpClient: fd сокета не закрывался при disconnect (BIO_NOCLOSE)
//          -> утечка одного дескриптора на каждый переподключение.
//   F12 [LIVE] обрыв публичного WS -> cancelAll всех пар до реконнекта
//          (как обещано в шапке; MM_CANCEL_ON_WS_LOSS=0 отключает).
//   F13 [LIVE] приватный WS: PING раз в 15 с, watchdog 45 с тишины ->
//          реконнект; после реконнекта resolve открытых ордеров (добор
//          филлов, пропущенных в окне обрыва).
//   F14 [LIVE] resolve: «закрыт» только по -2011/-2013 «Unknown order /
//          does not exist». Rate-limit/5xx/700003 -> backoff, ордер
//          считается возможно живым. reconcile «воскрешает» сторону, если
//          живой ордер биржи совпал с её cid; на shutdown повторный cancelAll.
//   F15 [LIVE] F10 строже: live требует И tick, И qtyStep из exchangeInfo.
//   F16 [NET]  WS: таймаут connect (5 с) и чтения/записи кадра (10 с);
//          SIGINT/SIGTERM без SA_RESTART (Ctrl+C прерывает блокирующий read).
//   F17 [LIVE] cleanStart: openOrders с 3 попытками; не ответил -> live не
//          стартует (иначе ордера прошлого запуска остаются висеть).
//   F18 [LIVE] 700003 при place: timeSync и один немедленный повтор.
//
// v3.1.4 over v3.1.3 (найдено smoke-прогоном live-контура на моке с отказами):
//   F19 [CRIT] HttpClient автоматически ПОВТОРЯЛ POST после обрыва без ответа.
//          Если биржа ордер уже приняла, повтор с тем же cid отклонялся,
//          сторона считалась «не выставленной», живой ордер становился
//          сиротой, а его филлы терялись (на моке позиция бота разошлась с
//          биржей на 65 X). Теперь POST повторяется только если запрос не
//          ушёл в сокет; иначе transportOk=false -> resolve по cid.
//          Плюс проверка keep-alive перед запросом: сервер закрыл коннект ->
//          переподключение ДО отправки (меньше неоднозначных обрывов).
//   F20 [LIVE] успешный cancel: executedQty из ответа не учитывался.
//          Частичный филл, не пришедший по приватному WS, терялся.
//   F21 [LIVE] снятие сироты в reconcile: её executedQty не учитывался.
//          Теперь дельта сверх уже пришедших по WS сделок проводится в ledger.
//   F22 [LIVE] PLACE-FAIL пишет cid (для разбора логов).
//   F23 [LIVE] escape MARKET: исполнение учитывалось ТОЛЬКО по приватному WS.
//          Пропуск сделки (обрыв WS) или транспортный сбой POST = позиция
//          бота расходится с биржей. Теперь escape-ордер сверяется через
//          getOrder на ближайшем reconcile (дельта сверх WS + кредит).
//
// v3.1.10 over v3.1.9:
//   F49 [PAPER] MM_PRINT_FILL_P (0..1, дефолт 0; можно @SYM): при
//          MM_PRINT_FILTER>=1 внутренний принт, касающийся нашего уровня,
//          с вероятностью p обрабатывается как при =0 (очередь + филл,
//          THROUGH/TOXIC/QUEUE/GHOST). p=0 = v3.1.9, p=1 = как =0. RNG
//          отдельный, сеется от MM_SEED (replay детерминирован, латентности
//          не сдвигаются). В итогах строка print-fill-p (только при p>0).
//          Подбор p: сравнить real fills micro-live с replay при разных p.
//   F50 [LIVE] зеркало: (1) sweep-cooldown и cutoff ставились только от
//          paper-филлов; реальный филл с пробитым уровнем их не включал.
//          Теперь: новая реальная qty по стороне + публичный принт агрессора
//          за ценой нашего ордера (<=3 c) -> lastSweepAt (MM_SWEEP_COOLDOWN_SEC),
//          при глубине > MM_CUTOFF_BPS ещё и cutoff-пауза. Лог [REAL_SWEEP].
//          (2) REAL_SYNC: реальный ордер исполнен целиком, а shadow-ордер,
//          который он зеркалил, ещё стоит -> shadow-ордер закрывается БЕЗ
//          paper-филла (fill_cal не искажается). Раньше зеркало через тик
//          ставило такой же ордер повторно = двойной набор позиции.
//          Лог [REAL_SYNC]; итог: строка LIVE sync. MM_LIVE_SYNC=0 — выкл.
//          Paper/replay F50 не затрагивает.
//   F51 [LIVE] MEXC 700013 Invalid content Type на POST /api/v3/userDataStream:
//          POST/PUT с пустым телом слались с form-urlencoded. Теперь все
//          параметры подписанных запросов — в query, тело пустое, Content-Type
//          application/json (как официальный SDK); listenKey — SIGNED
//          (key-only как запасной). MM_REST_FORM_BODY=1 — старый формат.
//   F52 [LIVE][CRIT] погоня за собственным ордером: котировки считались от
//          bestBid/bestAsk публичной книги, где лучшим уровнем был НАШ
//          ордер -> каждый реквот (2 c) на шаг к середине, спред сжимался до
//          встречной стороны; бот покупал у ask и продавал у bid (TAG
//          05.10: 17 филлов за 1.5 мин, ~-37 bps на круг). Теперь в live
//          тик стратегии видит книгу БЕЗ наших ордеров (живые + снятые за
//          MM_SELF_EXCL_SEC=2 c, пока книга не обновилась). MM_SELF_EXCL=0 —
//          старое поведение. Paper/replay не затронуты.
//   F53 [LIVE] breaker: > MM_LIVE_MAX_FILLS_MIN=6 реальных филлов за 60 c ->
//          ордера пары сняты, пауза MM_LIVE_BREAKER_SEC=600 ([LIVE BREAKER]).
//          Первые ответы приватного WS пишутся в лог (диагностика deals=0).
//   F54 [LIVE] комиссии: филлы, пришедшие без приватной сделки (cumQty
//          из private.orders, executedQty из REST: GHOST-WS/GHOST-REST,
//          escape, сироты), проводились с комиссией 0 -> P&L и позиция
//          завышены (05.10 TAG: бот 6219 TAG, биржа 6186). Теперь через ~2 c
//          после такого филла GET /api/v3/myTrades?symbol&orderId (права
//          SPOT_ACCOUNT_READ, вес 10, <=1 запроса за тик, общий REST-бюджет)
//          -> commission/commissionAsset в ledger; maker/TAKER в лог
//          [LIVE FEE]. Дедуп по tradeId с private.deals: комиссия каждой
//          сделки учитывается ровно один раз. MM_FEE_LOOKUP=0 — выкл.
//          Итог: строка LIVE fees (запросы, сделки, TAKER, privWS deals).
//
// v3.1.9 over v3.1.8:
//   F47 [PAPER] MM_PRINT_FILTER=1 закрывал только THROUGH/TOXIC/GHOST, а
//          принт строго внутри спреда РОВНО по цене нашего ордера давал
//          FR_QUEUE-филл (внутри спреда мы одни -> очередь 0). На GME/TAG это
//          был единственный оставшийся при =1 филл. Теперь при >=1 внутренний
//          принт не даёт филлов вообще и не двигает очередь; режимы 1 и 2
//          для филлов эквивалентны (2 оставлен для совместимости).
//   F48 [DIAG] «срезано филлов» в print-filter считает только реально
//          предотвращённые филлы (раньше — каждый внутренний принт x сторона:
//          у ZIL 87 при фактической разнице в 1 филл).
//
// v3.1.8 over v3.1.7 (разбор ленты MEXC: синтетические принты внутри спреда):
//   F45 [PAPER] на неликвидных парах ~100% сделок проходят СТРОГО ВНУТРИ
//          видимого спреда (регулярно, ~раз в 25-30 c, в середине спреда) —
//          признак биржевого объёмного бота. Бумажный движок считал такой
//          принт ниже нашего bid «пробитием» (THROUGH) и давал филл, хотя
//          принтер, видя наш ордер в книге, поставил бы цену иначе.
//          MM_PRINT_FILTER: 0 = как раньше (оптимистично);
//          1 (дефолт) = принт строго внутри видимого спреда (выше max bid и
//          ниже min ask за окно MM_PRINT_FILTER_MS=1500 до принта, биржевое
//          время) НЕ даёт THROUGH/TOXIC/GHOST-филл; [v3.1.9 F47: и филл «в
//          уровень» тоже]; 2 = то же (для совместимости).
//          В итогах: строка print-filter (всего/внутри/срезано филлов).
//          Сравнивайте paper с 0 и 1: правда — между ними, решает LIVE.
//   F46 [LIVE] MEXC: не более 30 подписок на одно WS-соединение, у бота 3
//          канала на пару -> >10 пар молча теряли подписки. Теперь >10 пар
//          в одном процессе = FATAL (запускайте несколько процессов).
//
// v3.1.7 over v3.1.6 (сверка с актуальной документацией MEXC Spot V3):
//   F41 [CRIT] после HTTP 429/418 бот продолжал слать запросы (другие пары,
//          recon, balance) -> по докам это путь к бану IP (2 мин..3 дня).
//          Теперь глобальная пауза REST по Retry-After (429 без заголовка ->
//          2 c, 418 -> 120 c); во время паузы проходят только отмены (DELETE).
//   F42 [LIVE] пул весов IP-эндпоинтов 300/10 c: локальный бюджет 250/10 c
//          (openOrders 3, getOrder 2, account 10, exchangeInfo 25, order 1).
//          Важно на старте с многими парами: exchangeInfo = 25 на пару.
//   F43 [LIVE] stpMode по докам — CANCEL_MAKER|CANCEL_TAKER|CANCEL_BOTH
//          (верхний регистр). MM_STP_MODE принимается в любом регистре.
//   F44 [LIVE] лимит ордеров изменён биржей (change log 2026-03-11): place и
//          cancel делят ОДИН бакет 12 запросов/с на аккаунт (старый анонс
//          «5 ордеров/с» устарел). Дефолт MM_RATE_RPS 5 -> 10 (запас 2/с).
//
// v3.1.6 over v3.1.5 (второе внешнее ревью, каждый пункт проверен):
//   F34 [CRIT] escape MARKET с ответом HTTP 5xx считался «не исполнен» и не
//          сверялся. Теперь 5xx = статус неизвестен -> escPending -> getOrder.
//   F35 [LIVE] self-trade: escape MARKET мог исполниться о СОБСТВЕННЫЙ
//          встречный лимит. Перед escape встречный ордер снимается; не снялся
//          -> escape откладывается. Плюс MM_STP_MODE (cancel_maker|cancel_taker|
//          cancel_both) -> stpMode в place/market (на MEXC работает только при
//          созданной strategy group; по умолчанию пусто = не отправляется).
//   F36 [LIVE] лимит MEXC 5 ордеров/с: дефолт MM_RATE_RPS 8 -> 5; escape
//          теперь тоже берёт токен; cancelAll ждёт токен, а не обнуляет бюджет.
//   F37 [LIVE] MM_KEEP_ORDERS_ON_EXIT=1 больше НЕ отключает cancelAll при
//          потере публичного WS (котировки вслепую опаснее, чем выход).
//   F38 [LIVE] MM_RECON_ADOPT=1 реально усыновляет ордер бота (свободный
//          слот той же стороны: cid/oid/px/qty/executedQty), иначе снимает.
//          Ордера прошлых запусков бота (префикс sp*x*) тоже усыновляются.
//   F39 [LIVE] clean start снимал ВСЕ ордера пары, включая ручные/чужие.
//          MM_LIVE_CLEAN_START=1 — только ордера бота (sp<pid>x<seq>, любой
//          pid), =2 — все ордера пары (старое поведение), =0 — без чистки.
//   F40 мелочь: unused parameter 'now' в onGhost.
//
// v3.1.5 over v3.1.4 (внешнее ревью, каждый пункт проверен по коду):
//   F24 [CRIT] SIGHUP (закрыли SSH/терминал) убивал процесс без cancelAll.
//          Теперь SIGHUP/SIGQUIT = штатная остановка. Исключение в главном
//          цикле -> лог + штатный shutdown (cancelAll), а не std::terminate.
//   F25 [LIVE] бюджет BUY считался от quoteFree, из которого уже вычтен
//          НАШ залоченный BUY -> цикл «поставил/снял» при балансе около
//          N*(2*ордер+резерв). Свой залоченный quote теперь прибавляется.
//   F26 [LIVE] LIMIT_MAKER (post-only) вместо LIMIT GTC: на устаревшей книге
//          ордер не исполнится тейкером. MM_POST_ONLY=0 — старое поведение;
//          если в exchangeInfo пары нет LIMIT_MAKER — LIMIT с предупреждением.
//   F27 [LIVE] чужие приватные сделки (ручная торговля/другой бот на паре)
//          больше не попадают в ledger: учитываются только cid/oid этой сессии.
//   F28 [LIVE] clean start: результат cancel проверяется, затем повторный
//          openOrders; остались ордера -> live не стартует.
//   F29 [LIVE] без listenKey (нет приватного WS) live не стартует
//          (3 попытки). MM_ALLOW_NO_PRIVWS=1 — работать на REST-поллинге.
//   F30 [LIVE] reconcile: executedQty известных открытых ордеров сверяется
//          с учтённым -> частичные филлы видны без приватного WS.
//   F31 [LIVE] HTTP 5xx на place = статус неизвестен -> resolve по cid
//          (раньше «не создан» -> возможный дубль до reconcile).
//   F32 [LIVE] qtyDrift сравнивал желаемый ОСТАТОК с исходным qty ордера ->
//          лишний cancel/replace после частичного филла >= 50%.
//   F33 мелочи: подсказка MM_LIVE_CLEAN_START=0 (было MM_CLEAN_START);
//          rand() (маски WS) сеется от времени, если MM_SEED не задан.
//   НЕ исправлено (архитектура): REST синхронный — зависший запрос блокирует
//          цикл всех пар до таймаута. Лечится отдельным потоком/async.
//
// v3.1 over v3.0 — МУЛЬТИПАРНЫЙ РЕЖИМ: один процесс — N символов
//   (argv[1] через запятую или MM_SYMBOLS="TAGUSDT,ZILUSDT,...").
//   Общие ресурсы: публичный WS (подписки всех пар на одном коннекте),
//   приватный WS (аккаунт-уровень, диспетчер событий по полю symbol),
//   REST rate-бюджет на аккаунт (один TokenBucket на всех executor'ов),
//   listenKey, серверное время (ресинк один — раздаётся всем).
//   Per-символ: shadow-движок v2.16 ЦЕЛИКОМ (book/flow/evq/PaperTrader)
//   + LiveExecutor (свой keep-alive HTTPS) + LiveLedger + kill-file.
//   Решения ТЗ зашиты в дефолты: остановка (Ctrl+C / halt / потеря
//   данных) -> cancelAll ВСЕХ реальных ордеров (MM_KEEP_ORDERS_ON_EXIT=1
//   отменяет); стартовый инвентарь — RAMP (MM_INIT_MODE=1 дефолт);
//   dry-run по умолчанию, live только MM_LIVE=1 + MM_LIVE_CONFIRM=YES.
//   Дефолты конфига = продакшн-набор ночей n9/n10 ($50/$150/$25,
//   cutoff 90bps/120s, reorder 50ms, lat 22ms, skew 0.30); per-symbol
//   переопределения — суффиксом @SYMBOL: MM_INV_SKEW_FRAC@TAGUSDT=0.40.
//   fix v3.0: таймеры recon/balance реально обновляются (в v3.0 поллинг
//   шёл КАЖДУЮ итерацию цикла после истечения интервала) + стаггеринг
//   по парам; SUMMARY per-pair пишется ещё и в <лог>_<SYM>.log в формате
//   sum9 (комбинированный лог sum9 пропускает — заголовок PAIR SUMMARY).
//   Replay — строго один символ (raw-лента однопарная).
//
// v3.0 over v2.16 — LIVE-ТРАНСФОРМАЦИЯ (MM_LIVE=1). Реальные ордера MEXC.
//   Архитектура: paper-движок v2.16 НЕ тронут ни в одной строке логики —
//   в live-режиме он продолжает работать как SHADOW (решения о котировках,
//   виртуальные филлы = предсказание модели), а поверх него LiveExecutor
//   зеркалит РЕШЕНИЯ на биржу (REST HMAC-SHA256, clientOrderId-идемпо-
//   тентность) и ведёт РЕАЛЬНЫЙ учёт (LiveLedger) по приватным WS-филлам.
//   Расхождение shadow/real = живой fill-cal (печатается в SUMMARY).
//
//   L1 (торговый REST): нативный keep-alive HTTPS к api.mexc.com (без
//           fork+curl в горячем пути), HMAC-SHA256 подпись query-строки,
//           timestamp по серверным часам (GET /api/v3/time, ресинк 60с),
//           recvWindow настраивается. placeLimit/cancelOrder/getOrder/
//           openOrders/account/marketOrder/userDataStream.
//   L2 (приватный WS): listenKey (POST /api/v3/userDataStream, PUT-
//           keepalive 30 мин, DELETE на выходе), второй ws::Connection на
//           /ws?listenKey=..., подписки spot@private.orders.v3.api.pb /
//           spot@private.deals.v3.api.pb / spot@private.account.v3.api.pb
//           (protobuf, wrapper-поля 304/306/307). Филлы приходят СРАЗУ,
//           REST-поллинг openOrders — только страховка.
//   L3 (машина состояния реального ордера): IDLE <-> OPEN, реквот =
//           cancel->подтверждение->place. Филл в полёте отмены = реальный
//           GHOST (класс ошибок из paper-модели): ловится (а) приватной
//           сделкой после cancel, (б) recon-запросом GET /api/v3/order
//           (executedQty > known -> дельта-филл), (в) сверкой openOrders.
//   L4 (реконсиляция): каждые MM_RECON_SEC — openOrders vs локальное
//           состояние (пропавшие -> resolve, чужие/сироты -> лог/+cancel
//           по политике), каждые MM_BALANCE_SEC — /api/v3/account, дрифт
//           внутреннего учёта vs биржа -> предупреждение. Баланс и дрифт
//           позиции для ГЕЙТОВ стратегии — РЕАЛЬНЫЕ (не shadow).
//   L5 (риск live): kill-file (MM_KILL_FILE) -> halt+cancel; maxLoss по
//           РЕАЛЬНОМУ equity; escape = реальный MARKET-ордер; стейл данных
//           -> снять реальные котировки; на SIGINT — cancelAll (если не
//           MM_KEEP_ORDERS_ON_EXIT=1). Запуск БЕЗ подтверждения невозможен:
//           MM_LIVE=1 требует MM_LIVE_CONFIRM=YES + ключи в env.
//   Тест-стенд: MM_API_HOST + MM_TLS_NOVERIFY=1 + MM_LIVE_ALLOW_REPLAY=1
//           позволяют гонять live-контур против локального мок-сервера на
//           записанной ленте (интеграционный тест ордер-машины).
//
//   Запуск — один процесс на ВСЕ пары (мультипарный, v3.1):
//     MM_API_KEY=... MM_API_SECRET=... MM_LIVE=1 MM_LIVE_CONFIRM=YES \
//       MM_LOG=night_live.log ./spred TAGUSDT,ZILUSDT,CAWUSDT 90000 20 22
//   Dry-run (по умолчанию, без ключей): ./spred TAGUSDT,ZILUSDT 3600 20 22
//
// v2.16 over v2.15 — P0: гигиена движка (event integrity):
//   P0-1 (монотонность): событие старее последнего применённого
//           (e.exchMs < lastAppliedExchMs) НЕ применяется. Раньше поздний
//           depth-снапшот мог откатить коммиченную книгу и при этом
//           освежить lastRecv -> откат книги + ложная свежесть -> quote
//           по устаревшему миду. Равные метки проходят (stable_sort
//           сохраняет порядок постановки). Счётчик lateEv в summary:
//           0-единицы на спокойных лентах = guard страховочный; десятки
//           = проблема была реальной и поймана.
//   P0-2 (lastPacket): время последнего ПРИНЯТОГО кадра — отдельно от
//           lastRecv (последнее ПРИМЕНЁННОЕ событие). Диагностика
//           «сеть жива / сортировка копит»; stale-gate ОСТАЁТСЯ на
//           lastRecv — пакет в reorder-очереди не делает книгу безопасной.
//   P0-3 (ghost lower-bound): окно живости ордера [startExchMs,
//           untilExchMs]. Сделка старше активации ордера физически не
//           могла его заполнить; раньше проверялась только верхняя
//           граница. makeGhost теперь с явными параметрами (placeSent/
//           placeLat/cancelSent/cancelLat) — не sb-состоянием.
//   P0-4 (markout-якорь): mo-окна отсчитываются от БИРЖЕВОГО времени
//           сделки (d.timeMs + exchOffDealMs, СЕКУНДЫ — mo.f.t в сек),
//           а не от времени обработки. Явный параметр fillAnchorSec
//           через сигнатуру doFill/onGhost (не shared-поле класса —
//           applyEvent обрабатывает сделки пачками, поле бы «переживало»
//           породившую его сделку). GAP/ESCAPE остаются на now — у них
//           нет биржевой сделки (контрфакт после реконнекта / решение
//           симулятора). После фикса MO5 может сдвинуться заметно
//           (якорь честнее, delivery lag ~100мс на окне 5с = 2%), MO30/60
//           — слегка. Это починка, не регрессия.
//   fix:    replay печатает строку-подписку -> sum7 видит символ
//           (матрицы больше не с "?").
//
// v2.15 over v2.14 — REPLAY + границы реализма для тонких пар:
//   R (replay): MM_REPLAY=<raw> MM_REPLAY_SPEED=<0|1> — прогон записанной
//           ленты (MM_RAW_LOG) через ТОТ ЖЕ путь, что живой WS. Сим-часы:
//           в replay nowSecs/nowMsSteady = время ЗАПИСИ (steady-домен
//           исходной сессии) -> fill-гейт, ghost-окна, бакет, escape,
//           markout, TFI воспроизводятся в исходных временах автоматически;
//           сон при SPEED>0 — чистый pacing, на корректность не влияет.
//           SPEED=0 валиден: вся ночь прогоняется за секунды (матрицы).
//           Прайминг по первому кадру: duration в summary = длительность
//           ЗАПИСАННОЙ сессии. Replay до EOF; runSecs игнорируется.
//   MM_SEED: детерминизм (латентности ордеров — из tr.rng). Два прогона
//           одного raw при одном seed -> идентичные SUMMARY. Смена
//           стратегии меняет порядок сэмплов даже при одном seed —
//           сравнивай распределения итогов, не построчные логи.
//   ФИКС 20 (queue bounding): MM_QUEUE_MODE — 0=frac (как v2.14),
//           1=pessimistic (чужие отмены НЕ двигают нашу очередь),
//           2=optimistic (убирают очередь полностью). Матрицы TAG/ZIL
//           показали: при requote ~10с очередь эпизодическая, frac не
//           меняет поток филлов — ось закрыта, флаг остаётся страховкой.
//   ФИКС 21 (self-in-book): MM_SELF_IN_BOOK=1 — сигнальный имбаланс
//           включает наш LIVE-ордер. ТОЛЬКО для сигналов; очередь/
//           levelQty — по чужой книге. Дефолт OFF (feedback-loop риск).
//   ФИКС 22 (escape stress): MM_ESCAPE_SKIP_LEVELS=N — escape-walk с
//           уровня N+1 (withdrawal ближней ликвидности). expAvg при
//           РЕШЕНИИ считается по живой книге -> escSlip меряет стоимость
//           withdrawal. В сделку идёт фактически заполненное fq.
//
// v2.14 over v2.13.1 — ФИКС 17 (ramp entry MM_INIT_MODE=1);
//   ФИКС 18 (MM_ESCAPE_COOLDOWN_SEC); ФИКС 19 (MM_CUTOFF_BPS/SEC);
//   MM_START_USDT.
// v2.13.1 — ФИКС 16 (dual clock): fill-гейт в часах СДЕЛОК
//   (exchOffDealMs), сортировка с компенсацией skew, карантин
//   future-сделок (MM_QUAR_MS).
// v2.13 — ФИКС 11..15: latency dist; mo-распределения; escape
//   slippage; fees maker/taker; MM_RAW_LOG.
// v2.12 — event-time книга (MM_REORDER_MS); gap certain/possible;
//   MM_CANCEL_LAT_MS; fill reasons + $-вклад.
// v2.11 — раздельные знаменатели mo/pxmo (+txmo); очередь: сброс на
//   PLACE + MM_CANCEL_AHEAD_FRAC; flow по биржевому времени; escape с
//   латентностью; санитайзинг книги.
// v2.10 — вычитка INIT. v2.9.1 — FIX INIT. v2.9 — FIX SELL-SIZING.
// v2.8 — PROXY-MARKOUT. v2.7 — честная модель исполнения, flow, P&L-разбивка.
//
// Usage:
//   MM_LOG=run.log ./spred TAGUSDT 3600 20 150
// Replay:
//   MM_REPLAY=TAG_n9.raw MM_REPLAY_SPEED=0 MM_SEED=42 MM_LOG=rp.log \
//     <env той же ночи> ./spred TAGUSDT 7200 20 90
// Env:
//   MM_LOG(off) MM_LOG_DEALS(0) MM_TICK MM_QTY_STEP
//   MM_MAKER_BPS(0) MM_TAKER_BPS(5) MM_ORDER_USDT(50) MM_MAX_POS(150)
//   MM_MAX_SPREAD_BPS(500) MM_REQUOTE_FRAC(0.11) MM_MIN_REQUOTE_SEC(2)
//   MM_RATE_RPS(10) MM_RTT_MS(23) MM_SWEEP_COOLDOWN_SEC(15)
//   MM_SKEW_IMB(1) MM_SKEW_TFI(1) MM_IMB_SKEW_FRAC(0.10)
//   MM_TFI_SKEW_FRAC(0.10) MM_INV_SKEW_FRAC(0.30)
//   MM_TFI_WINDOW(60) MM_TFI_THRESH(0.40) MM_RUN_STOP(3)
//   MM_RUN_DECAY(20) MM_MIN_DEALS(5)
//   MM_MAX_LOSS(25) MM_STALE_SEC(30) MM_ESCAPE_SECS(30) MM_ALLOW_SHORT(0)
//   MM_MIN_INIT_FRAC(0) MM_CANCEL_AHEAD_FRAC(0.5)
//   MM_REORDER_MS(50) MM_GAP_FILL_MODE(0) MM_CANCEL_LAT_MS(=order)
//   MM_LAT_DIST(0) MM_LAT_P90_MULT(1.6) MM_LAT_P99_MULT(2.5)
//   MM_LAT_MIN_MULT(0.3)   [v2.13]
//   MM_RAW_LOG(off)        [v2.13] бинарный дамп фреймов для replay
//   MM_QUAR_MS(2000)       [v2.13.1] карантин сделок «из будущего»
//   MM_START_USDT(10000)   [v2.14] стартовый виртуальный баланс
//   MM_INIT_MODE(1)        [v2.14/v3.1] 0=classic taker pre-buy, 1=ramp bid-only
//   MM_ESCAPE_COOLDOWN_SEC(90) [v2.14] пауза стороны после escape
//   MM_CUTOFF_BPS(90) MM_CUTOFF_SEC(120) [v2.14] cutoff при пробитии уровня
//   MM_PRINT_FILTER(1) MM_PRINT_FILTER_MS(1500) [v3.1.8/F47] фильтр внутренних принтов
//   MM_PRINT_FILL_P(0)     [v3.1.10/F49] вероятность филла от внутреннего принта
//   MM_LIVE_SYNC(1)        [v3.1.10/F50] реальные филлы -> shadow (cooldown/sync)
//   MM_SELF_EXCL(1) MM_SELF_EXCL_SEC(2)  [v3.1.10/F52] live: книга без наших ордеров
//   MM_LIVE_MAX_FILLS_MIN(6) MM_LIVE_BREAKER_SEC(600) [v3.1.10/F53] breaker
//   MM_FEE_LOOKUP(1)       [v3.1.10/F54] комиссии через myTrades
//   MM_REPLAY(off) MM_REPLAY_SPEED(1.0) MM_SEED(0)      [v2.15] replay
//   MM_QUEUE_MODE(0) MM_ESCAPE_SKIP_LEVELS(0)           [v2.15] Ф20/Ф22
//   MM_SELF_IN_BOOK(0)                                   [v2.15] Ф21
//   MM_SELL_WALLET_BASE(0) MM_QTY_DRIFT_USDT(0)          [v3.1.1] см. changelog
//   MM_LIVE(0) MM_LIVE_CONFIRM("") MM_API_KEY MM_API_SECRET   [v3.0] LIVE
//   MM_RECV_WINDOW_MS(10000) MM_TIME_SYNC_SEC(60)
//   MM_RECON_SEC(15) MM_BALANCE_SEC(30) MM_LIVE_CLEAN_START(1)
//   MM_KILL_FILE(off) MM_KEEP_ORDERS_ON_EXIT(0) MM_RECON_ADOPT(0)
//   MM_API_HOST(api.mexc.com) MM_TLS_NOVERIFY(0) MM_LIVE_ALLOW_REPLAY(0)
//   MM_SYMBOLS(off)            [v3.1] пары через запятую (= argv[1])
//   MM_QUOTE_SHARE(1/N)        [v3.1] доля свободных USDT кошелька на пару
//   MM_<ПАРАМ>@<SYMBOL>        [v3.1] per-symbol override (INV_SKEW_FRAC,
//                             CUTOFF_BPS/SEC, ORDER_USDT, MAX_POS, MAX_LOSS,
//                             MAX_SPREAD_BPS, REQUOTE_FRAC, MIN_REQUOTE_SEC,
//                             ESCAPE_COOLDOWN_SEC, START_USDT, TICK, QTY_STEP,
//                             QUOTE_SHARE, KILL_FILE)
// ============================================================================

#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>

#include <sys/select.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/file.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <unistd.h>

#include <fcntl.h>
#include <poll.h>
#include <cerrno>
#include <signal.h>
#include <csignal>
#include <cstdarg>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <deque>
#include <array>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>
#include <mutex>
#include <atomic>

// ---------------------------------------------------------------- utilities
static std::string envOr(const char* name, const std::string& dflt) {
    const char* v = getenv(name);
    return (v && *v) ? std::string(v) : dflt;
}
static double envD(const char* name, double dflt) {
    const char* v = getenv(name);
    if (!v || !*v) return dflt;
    char* e = nullptr;
    double d = strtod(v, &e);
    return (e && *e == '\0') ? d : dflt;
}
static int envI(const char* name, int dflt) { return (int)envD(name, (double)dflt); }

// [v3.1] per-symbol override: MM_<name>@<SYMBOL> побеждает глобальный MM_<name>
static double envDsym(const char* name, const std::string& sym, double dflt) {
    std::string key = std::string(name) + "@" + sym;
    const char* v = getenv(key.c_str());
    if (v && *v) {
        char* e = nullptr;
        double d = strtod(v, &e);
        if (e && *e == '\0') return d;
    }
    return envD(name, dflt);
}


// [v2.15 R1 REPLAY] сим-часы: в replay nowMsSteady() = время ЗАПИСИ raw
// (steady-домен исходной сессии). Вся timing-логика (fill-гейт, ghost-окно,
// бакет, escape, markout-горизонты, TFI) идёт через эти две функции ->
// воспроизводится в исходном временном домене автоматически; сон в
// frameWait при SPEED>0 — чистый pacing, на корректность не влияет.
// Прайминг по первому кадру raw — в main (R3), до t0: тогда duration
// в summary = длительность ЗАПИСАННОЙ сессии.
static bool   g_replay = false;
static double g_simNowMs = -1.0;      // <0 = ещё не заведены (первый кадр)

static double nowMsSteady() {
    if (g_replay && g_simNowMs >= 0.0) return g_simNowMs;
    using namespace std::chrono;
    return duration<double, std::milli>(
               steady_clock::now().time_since_epoch()).count();
}
static double nowSecs() { return nowMsSteady() / 1000.0; }

// [v2.7 J] локальное время с мс
static std::string nowWallClock(bool withMs = false) {
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
static void evAdd(std::vector<std::string>& v, const std::string& s) {
    v.push_back("[" + nowWallClock(true) + "] " + s);
}

// ------------------------------------------------------------------- logger
static FILE* g_logFile = nullptr;

static void lprintf(const char* fmt, ...) {
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

static void lflush() {
    std::fflush(stdout);
    if (g_logFile) std::fflush(g_logFile);
}

static volatile std::sig_atomic_t g_stop = 0;
static void onSigint(int) { g_stop = 1; }

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
static SharedRate g_sr;

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
static double realSecs() {
    return std::chrono::duration<double>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}
static double g_restBlockUntil = 0;
static bool g_restFormBody = false;   // [F51] MM_REST_FORM_BODY
static int g_rate429 = 0, g_rate418 = 0;
static void restNoteLimit(int http, double retryAfter) {
    if (http != 429 && http != 418) return;
    if (http == 429) ++g_rate429; else ++g_rate418;
    double wait = retryAfter > 0 ? retryAfter : (http == 418 ? 120.0 : 2.0);
    wait = std::min(wait, 3.0 * 86400.0);
    double now = realSecs();
    if (now + wait > g_restBlockUntil) {
        bool fresh = now >= g_restBlockUntil;
        g_restBlockUntil = now + wait;
        g_sr.noteBlock(SharedRate::mono() + wait);   // [F61]
        if (fresh) lprintf("[live] HTTP %d от MEXC: пауза REST %.0f c (отмены разрешены)\n",
                           http, wait);
    }
}
static void restWaitBlock(double maxSec) {   // [fix F41] для стартовых повторов
    double w = std::min(maxSec, g_restBlockUntil - realSecs());
    if (w > 0) std::this_thread::sleep_for(std::chrono::milliseconds(long(w * 1000) + 50));
}
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
static WeightBucket g_ipW;
static double mexcWeight(const std::string& m, const std::string& path) {
    if (path.rfind("/api/v3/openOrders", 0) == 0) return m == "GET" ? 3 : 1;
    if (path.rfind("/api/v3/order", 0) == 0) return m == "GET" ? 2 : 1;
    if (path.rfind("/api/v3/account", 0) == 0) return 10;
    if (path.rfind("/api/v3/myTrades", 0) == 0) return 10;
    if (path.rfind("/api/v3/exchangeInfo", 0) == 0) return 25;
    return 1;
}

// ------------------------------------------------------------------- base64
static std::string b64(const unsigned char* data, size_t len) {
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
static long long epochMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(
               system_clock::now().time_since_epoch()).count();
}

// [v3.0] HMAC-SHA256 -> lowercase hex (подпись MEXC: hex от HMAC по
// query-строке ключом secret)
static std::string hmacSha256Hex(const std::string& key, const std::string& data) {
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
static std::string fmtDec(double v) {
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
static std::string base36(long long v) {
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
// [fix F2] TLS: проверка сертификата + имени хоста. g_tlsNoVerify — ТОЛЬКО для
// мок-стенда с self-signed (MM_TLS_NOVERIFY=1); в проде проверка включена.
static bool g_tlsNoVerify = false;

static void tlsConfigCtx(SSL_CTX* ctx) {
    if (!ctx) return;
    SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);
    if (g_tlsNoVerify) { SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, nullptr); return; }
    SSL_CTX_set_default_verify_paths(ctx);
    SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, nullptr);
}

// имя хоста (или IP-литерал) должно совпасть с сертификатом
static void tlsSetPeer(SSL* ssl, const std::string& host) {
    unsigned char buf[sizeof(struct in6_addr)];
    if (inet_pton(AF_INET, host.c_str(), buf) == 1 ||
        inet_pton(AF_INET6, host.c_str(), buf) == 1)
        X509_VERIFY_PARAM_set1_ip_asc(SSL_get0_param(ssl), host.c_str());
    else
        SSL_set1_host(ssl, host.c_str());
}

// [fix F16] TCP connect с таймаутом (неблокирующий connect + poll).
// Возвращает блокирующий fd или -1.
static int tcpConnectTimeout(const std::string& host, int port, int timeoutMs) {
    struct addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo* res = nullptr;
    std::string portStr = std::to_string(port);
    if (getaddrinfo(host.c_str(), portStr.c_str(), &hints, &res) != 0 || !res)
        return -1;
    int out = -1;
    for (struct addrinfo* ai = res; ai && out < 0; ai = ai->ai_next) {
        int fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fd < 0) continue;
        int fl = fcntl(fd, F_GETFL, 0);
        fcntl(fd, F_SETFL, fl | O_NONBLOCK);
        int rc = ::connect(fd, ai->ai_addr, ai->ai_addrlen);
        if (rc != 0 && errno == EINPROGRESS) {
            struct pollfd pfd{fd, POLLOUT, 0};
            int pr = ::poll(&pfd, 1, timeoutMs);
            int err = 0;
            socklen_t el = sizeof err;
            if (pr == 1 && getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &el) == 0 && err == 0)
                rc = 0;
        }
        if (rc == 0) {
            fcntl(fd, F_SETFL, fl & ~O_NONBLOCK);
            out = fd;
        } else {
            ::close(fd);
        }
    }
    freeaddrinfo(res);
    return out;
}

namespace ws {

struct Frame {
    int opcode = -1;
    bool fin = true;
    std::string payload;
};

class Connection {
public:
    Connection(const std::string& host, int port, const std::string& path)
        : host_(host), port_(port), path_(path) {
        SSL_load_error_strings();
        OpenSSL_add_ssl_algorithms();
        ctx_ = SSL_CTX_new(TLS_client_method());
        tlsConfigCtx(ctx_);   // [fix F2] VERIFY_PEER + min TLS1.2
        ssl_ = nullptr;
    }
    ~Connection() {
        close();
        if (ctx_) SSL_CTX_free(ctx_);
    }

    // [fix F16] свой TCP-connect с таймаутом (BIO_do_connect блокировал без
    // ограничения) + SO_RCVTIMEO/SO_SNDTIMEO: недочитанный кадр или молчащий
    // посреди кадра сервер больше не вешают цикл навсегда.
    bool connect() {
        close();
        early_.clear();
        int fd = tcpConnectTimeout(host_, port_, 5000);
        if (fd < 0) {
            std::fprintf(stderr, "[ws] TCP connect %s:%d не удался/таймаут\n",
                         host_.c_str(), port_);
            return false;
        }
        struct timeval tv{10, 0};
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
        SSL* ssl = SSL_new(ctx_);
        if (!ssl) { ::close(fd); return false; }
        SSL_set_fd(ssl, fd);
        SSL_set_tlsext_host_name(ssl, host_.c_str());  // CloudFront needs SNI
        if (!g_tlsNoVerify) tlsSetPeer(ssl, host_);    // [fix F2]
        SSL_set_mode(ssl, SSL_MODE_AUTO_RETRY);
        if (SSL_connect(ssl) != 1) {
            ERR_print_errors_fp(stderr);
            SSL_free(ssl);
            ::close(fd);
            return false;
        }
        if (!g_tlsNoVerify && SSL_get_verify_result(ssl) != X509_V_OK) {   // [fix F2]
            std::fprintf(stderr, "[ws] TLS: сертификат %s не прошёл проверку (код %ld)\n",
                         host_.c_str(), SSL_get_verify_result(ssl));
            SSL_free(ssl);
            ::close(fd);
            return false;
        }
        BIO* bio = BIO_new(BIO_f_ssl());
        if (!bio) { SSL_free(ssl); ::close(fd); return false; }
        BIO_set_ssl(bio, ssl, BIO_CLOSE);
        ssl_ = ssl;
        bio_ = bio;
        fd_ = fd;
        if (!handshakeWs()) { close(); return false; }   // [fix F6] не течь BIO
        return true;
    }

    bool connected() const { return ssl_ != nullptr; }
    void close() {
        if (bio_) { BIO_free_all(bio_); bio_ = nullptr; ssl_ = nullptr; }
        if (fd_ >= 0) { ::close(fd_); fd_ = -1; }   // [fix F16] сокет наш
    }

    bool sendText(const std::string& text) { return sendFrame(0x1, text); }
    bool sendPong(const std::string& payload) { return sendFrame(0xA, payload); }

    int waitReadable(int timeoutMs) {
        if (!ssl_) return -1;
        if (SSL_pending(ssl_) > 0) return 1;
        if (!early_.empty()) return 1;   // байты после handshake уже в буфере
        int fd = fd_;
        if (fd < 0) return -1;
        fd_set rs;
        FD_ZERO(&rs);
        FD_SET(fd, &rs);
        struct timeval tv;
        tv.tv_sec = timeoutMs / 1000;
        tv.tv_usec = (timeoutMs % 1000) * 1000;
        int r = ::select(fd + 1, &rs, nullptr, nullptr, &tv);
        return (r > 0) ? 1 : (r == 0 ? 0 : -1);
    }

    bool readFrame(Frame& f) {
        unsigned char h[2];
        if (!readExact(h, 2)) return false;
        bool fin = h[0] & 0x80;
        int opcode = h[0] & 0x0F;
        bool masked = h[1] & 0x80;
        uint64_t len = h[1] & 0x7F;
        if (len == 126) {
            unsigned char e[2];
            if (!readExact(e, 2)) return false;
            len = ((uint64_t)e[0] << 8) | e[1];
        } else if (len == 127) {
            unsigned char e[8];
            if (!readExact(e, 8)) return false;
            len = 0;
            for (int i = 0; i < 8; ++i) len = (len << 8) | e[i];
        }
        if (len > (64ull << 20)) return false;   // [fix F6] hostile length -> bad_alloc
        unsigned char mask[4] = {0, 0, 0, 0};
        if (masked && !readExact(mask, 4)) return false;
        std::string payload(len, '\0');
        if (len && !readExact(reinterpret_cast<unsigned char*>(&payload[0]), len))
            return false;
        if (masked)
            for (uint64_t i = 0; i < len; ++i) payload[i] ^= mask[i % 4];
        f.opcode = opcode;
        f.fin = fin;
        f.payload = std::move(payload);
        return true;
    }

    bool sendFrame(int opcode, const std::string& payload) {
        std::string out;
        out.push_back(char(0x80 | opcode));
        size_t len = payload.size();
        if (len < 126) {
            out.push_back(char(0x80 | len));
        } else if (len < 65536) {
            out.push_back(char(0x80 | 126));
            out.push_back(char((len >> 8) & 0xFF));
            out.push_back(char(len & 0xFF));
        } else {
            out.push_back(char(0x80 | 127));
            for (int i = 7; i >= 0; --i) out.push_back(char((len >> (8 * i)) & 0xFF));
        }
        unsigned char mask[4] = {(unsigned char)(rand() & 0xFF), (unsigned char)(rand() & 0xFF),
                                 (unsigned char)(rand() & 0xFF), (unsigned char)(rand() & 0xFF)};
        for (int i = 0; i < 4; ++i) out.push_back(char(mask[i]));
        for (size_t i = 0; i < len; ++i)
            out.push_back(char(payload[i] ^ mask[i % 4]));
        return writeAll(out);
    }

private:
    bool handshakeWs() {
        unsigned char randKey[16];
        for (auto& b : randKey) b = rand() & 0xFF;
        std::string key = b64(randKey, 16);
        std::string req =
            "GET " + path_ + " HTTP/1.1\r\n"
            "Host: " + host_ + "\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            "Sec-WebSocket-Key: " + key + "\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n";
        if (!writeAll(req)) return false;
        std::string response;
        while (response.find("\r\n\r\n") == std::string::npos) {
            char buf[2048];
            int n = BIO_read(bio_, buf, sizeof buf);
            if (n <= 0) return false;
            response.append(buf, size_t(n));
            if (response.size() > 65536) return false;
        }
        if (response.compare(0, 12, "HTTP/1.1 101") != 0) {
            std::fprintf(stderr, "[ws] handshake failed:\n%.200s\n", response.c_str());
            return false;
        }
        size_t hdr = response.find("\r\n\r\n") + 4;
        early_.assign(response.begin() + long(hdr), response.end());
        return true;
    }

    bool readExact(unsigned char* dst, size_t n) {
        size_t fromEarly = std::min(n, early_.size());
        for (size_t i = 0; i < fromEarly; ++i) dst[i] = (unsigned char)early_[i];
        early_.erase(0, fromEarly);
        size_t remaining = n - fromEarly;
        if (!remaining) return true;
        size_t got = 0;
        while (got < remaining) {
            int r = BIO_read(bio_, dst + got, int(remaining - got));
            if (r <= 0) return false;
            got += size_t(r);
        }
        return true;
    }

    bool writeAll(const std::string& s) {
        size_t sent = 0;
        while (sent < s.size()) {
            int r = BIO_write(bio_, s.data() + sent, int(s.size() - sent));
            if (r <= 0) return false;
            sent += size_t(r);
        }
        return true;
    }

    std::string host_;
    int port_;
    std::string path_;
    SSL_CTX* ctx_ = nullptr;
    SSL* ssl_ = nullptr;
    BIO* bio_ = nullptr;
    int fd_ = -1;
    std::string early_;
};

} // namespace ws

// ------------------------------------------------------------------- REST GET
static std::string dechunk(const std::string& s) {
    std::string out;
    size_t p = 0;
    while (p < s.size()) {
        size_t e = s.find("\r\n", p);
        if (e == std::string::npos) break;
        long len = strtol(s.substr(p, e - p).c_str(), nullptr, 16);
        if (len <= 0) break;
        p = e + 2;
        if (p + (size_t)len > s.size()) break;
        out.append(s, p, (size_t)len);
        p += (size_t)len + 2;
    }
    return out;
}

// [v3.0] тестовый режим TLS (мок-стенд с self-signed): MM_TLS_NOVERIFY=1.
// В проде НЕ использовать — отключает проверку сертификата.
// [v2.5.1] REST через curl БЕЗ shell: fork + execlp.
static bool httpsGetCurl(const std::string& host, const std::string& path,
                         std::string& out) {
    std::string url = "https://" + host + path;

    int pipefd[2];
    if (pipe(pipefd) != 0) return false;

    pid_t pid = fork();
    if (pid < 0) { close(pipefd[0]); close(pipefd[1]); return false; }

    if (pid == 0) {
        close(pipefd[0]);
        dup2(pipefd[1], STDOUT_FILENO);
        close(pipefd[1]);
        int devnull = open("/dev/null", O_WRONLY);
        if (devnull >= 0) { dup2(devnull, STDERR_FILENO); close(devnull); }
        alarm(15);
        if (g_tlsNoVerify)
            execlp("curl", "curl",
                   "-4", "-s", "-k", "--max-time", "10", "--compressed",
                   url.c_str(),
                   (char*)nullptr);
        execlp("curl", "curl",
               "-4", "-s", "--max-time", "10", "--compressed",
               url.c_str(),
               (char*)nullptr);
        _exit(127);
    }

    close(pipefd[1]);
    out.clear();
    char buf[65536];
    ssize_t n;
    while ((n = read(pipefd[0], buf, sizeof buf)) > 0) {
        out.append(buf, size_t(n));
        if (out.size() > 16 * 1024 * 1024) break;
    }
    close(pipefd[0]);

    int status = 0;
    waitpid(pid, &status, 0);
    return (WIFEXITED(status) && WEXITSTATUS(status) == 0) && !out.empty();
}

static bool httpsGet(const std::string& host, const std::string& path, std::string& out) {
    if (httpsGetCurl(host, path, out)) return true;
    std::fprintf(stderr, "[rest] curl failed for https://%s%s, trying native...\n",
                 host.c_str(), path.c_str());

    // [v3.0 fix] host может быть "127.0.0.1:PORT" (мок-стенд) — выделяем порт,
    // в DNS/SNI отдаём чистый хост
    std::string h = host;
    std::string portStr = "443";
    size_t cpos = h.rfind(':');
    if (cpos != std::string::npos && h.find(':') == cpos) {   // один ':' = host:port
        portStr = h.substr(cpos + 1);
        h = h.substr(0, cpos);
    }

    struct addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo* res = nullptr;
    if (getaddrinfo(h.c_str(), portStr.c_str(), &hints, &res) != 0 || !res) return false;

    SSL_CTX* ctx = SSL_CTX_new(TLS_client_method());
    if (!ctx) { freeaddrinfo(res); return false; }
    tlsConfigCtx(ctx);   // [fix F2] раньше: SSL_VERIFY_NONE безусловно

    bool ok = false;
    for (struct addrinfo* ai = res; ai && !ok; ai = ai->ai_next) {
        int fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fd < 0) continue;
        struct timeval tv{10, 0};
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
        if (connect(fd, ai->ai_addr, ai->ai_addrlen) != 0) { close(fd); continue; }

        SSL* ssl = SSL_new(ctx);
        if (!ssl) { close(fd); continue; }
        SSL_set_fd(ssl, fd);
        SSL_set_tlsext_host_name(ssl, h.c_str());
        if (!g_tlsNoVerify) tlsSetPeer(ssl, h);   // [fix F2]
        if (SSL_connect(ssl) == 1 &&
            (g_tlsNoVerify || SSL_get_verify_result(ssl) == X509_V_OK)) {
            std::string req = "GET " + path + " HTTP/1.1\r\nHost: " + host +
                              "\r\nUser-Agent: curl/7.81.0\r\nAccept: */*\r\n"
                              "Connection: close\r\n\r\n";
            if (SSL_write(ssl, req.data(), (int)req.size()) > 0) {
                std::string resp;
                char buf[8192];
                for (;;) {
                    int n = SSL_read(ssl, buf, sizeof buf);
                    if (n <= 0) break;
                    resp.append(buf, size_t(n));
                    if (resp.size() > 16 * 1024 * 1024) break;
                }
                size_t hp = resp.find("\r\n\r\n");
                if (hp != std::string::npos) {
                    std::string head = resp.substr(0, hp);
                    std::string body = resp.substr(hp + 4);
                    if (head.find("chunked") != std::string::npos) body = dechunk(body);
                    out = body;
                    ok = !body.empty();
                }
            }
        }
        SSL_free(ssl);
        close(fd);
    }
    freeaddrinfo(res);
    SSL_CTX_free(ctx);
    return ok;
}

static bool jsonFindNum(const std::string& s, const std::string& key, double& out) {
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
static bool jsonFindStr(const std::string& s, const std::string& key,
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
static std::vector<std::string> jsonSplitObjects(const std::string& s) {
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
static std::string jsonExtractArray(const std::string& s, const char* key) {
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
// curl+fork в горячем пути недопустим (100-300 мс на вызов); здесь один
// TLS-коннект на сессию, переподключение при обрыве. Возвращает HTTP-код
// и тело; transportOk=false = сетевой сбой (ответа нет, состояние запроса
// НЕИЗВЕСТНО — вызывающий обязан разрулить через query/recon).
// ============ [F55] внешняя справедливая цена: Bybit spot orderbook.1 ============
// Отдельный поток держит WS к Bybit и кладёт лучшие bid/ask по символу в g_fair.
// Стратегия (MM_FAIR=bybit) котирует fair*(1±d) вместо bestBid+1/4 спреда MEXC.
struct FairQ { double bid = 0, ask = 0, tMs = 0, bq = 0, aq = 0; long n = 0; };
static int g_fairMode = 0;
static std::mutex g_fairMu;
static std::map<std::string, FairQ> g_fair;
static std::vector<std::string> g_fairLog;
static std::atomic<long> g_fairMsgs{0}, g_fairReconn{0};
static void fairLog(const std::string& m) {
    std::lock_guard<std::mutex> lk(g_fairMu);
    if (g_fairLog.size() < 200) g_fairLog.push_back(m);
}
static bool g_fairUsdcMap = true;   // [F64]
static std::string fairRef(const std::string& sym) {
    if (g_fairUsdcMap && sym.size() > 4 && sym.compare(sym.size() - 4, 4, "USDC") == 0)
        return sym.substr(0, sym.size() - 4) + "USDT";
    return sym;
}
static bool fairGet(const std::string& sym0, FairQ& q) {
    std::string sym = fairRef(sym0);
    std::lock_guard<std::mutex> lk(g_fairMu);
    auto it = g_fair.find(sym);
    if (it == g_fair.end()) return false;
    q = it->second;
    return true;
}
// "b":[["p","q"],...] -> лучший уровень с q>0; any=массив непуст; allZero=только удаления
static void fairParseSide(const std::string& s, const char* key, bool isBid,
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
static void bybitFairThread(std::vector<std::string> syms, std::string host, int port,
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

struct HttpResp {
    int code = 0;
    double retryAfter = 0;   // [fix F41] заголовок Retry-After, c
    std::string body;
    bool transportOk = false;
    double rttMs = 0;
};

class HttpClient {
public:
    explicit HttpClient(const std::string& hostWithOptPort, bool noVerify) {
        host_ = hostWithOptPort;
        port_ = 443;
        size_t c = host_.find(':');
        if (c != std::string::npos) {
            port_ = std::atoi(host_.c_str() + c + 1);
            host_ = host_.substr(0, c);
        }
        noVerify_ = noVerify;
        ctx_ = SSL_CTX_new(TLS_client_method());
        if (ctx_) {
            SSL_CTX_set_min_proto_version(ctx_, TLS1_2_VERSION);
            if (noVerify_) {
                SSL_CTX_set_verify(ctx_, SSL_VERIFY_NONE, nullptr);
            } else {   // [fix F2] раньше VERIFY не включался -> сертификат не проверялся
                SSL_CTX_set_default_verify_paths(ctx_);
                SSL_CTX_set_verify(ctx_, SSL_VERIFY_PEER, nullptr);
            }
        }
    }
    ~HttpClient() {
        disconnect();
        if (ctx_) SSL_CTX_free(ctx_);
    }

    void disconnect() {
        if (bio_) { BIO_free_all(bio_); bio_ = nullptr; }
        if (fd_ >= 0) { ::close(fd_); fd_ = -1; }   // [fix F11] BIO_NOCLOSE -> fd наш
    }

    HttpResp request(const std::string& method, const std::string& path,
                     const std::string& body,
                     const std::vector<std::pair<std::string, std::string>>& hdrs,
                     int timeoutMs = 4000) {
        HttpResp r;
        double t0 = nowMsSteady();
        for (int attempt = 0; attempt < 2; ++attempt) {
            if (!ensureConnected()) {
                if (attempt == 0) continue;
                r.rttMs = nowMsSteady() - t0;
                return r;
            }
            wrote_ = false;
            if (doRequest(method, path, body, hdrs, r, timeoutMs)) {
                r.transportOk = true;
                r.rttMs = nowMsSteady() - t0;
                return r;
            }
            disconnect();   // обрыв/таймаут — новый коннект на retry
            // [fix F19] неидемпотентный POST, уже ушедший в сокет, НЕ повторяем:
            // биржа могла его исполнить. Решает вызывающий (resolve по cid).
            if (wrote_ && method == "POST") break;
        }
        r.rttMs = nowMsSteady() - t0;
        return r;
    }

private:
    bool ensureConnected() {
        // [fix F19] keep-alive мог быть закрыт сервером, пока мы молчали:
        // в простое сокет не должен быть читаемым (EOF / close_notify / RST)
        if (bio_ && fd_ >= 0) {
            struct pollfd pfd{fd_, POLLIN, 0};
            if (::poll(&pfd, 1, 0) != 0) disconnect();
        }
        if (bio_) return true;
        struct addrinfo hints{};
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        struct addrinfo* res = nullptr;
        std::string portStr = std::to_string(port_);
        if (getaddrinfo(host_.c_str(), portStr.c_str(), &hints, &res) != 0 || !res)
            return false;
        bool ok = false;
        for (struct addrinfo* ai = res; ai && !ok; ai = ai->ai_next) {
            int fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
            if (fd < 0) continue;
            struct timeval tv{5, 0};
            setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
            setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);
            if (connect(fd, ai->ai_addr, ai->ai_addrlen) != 0) { close(fd); continue; }
            SSL* ssl = SSL_new(ctx_);
            if (!ssl) { close(fd); continue; }
            SSL_set_fd(ssl, fd);
            SSL_set_tlsext_host_name(ssl, host_.c_str());
            if (!noVerify_) tlsSetPeer(ssl, host_);   // [fix F2]
            if (SSL_connect(ssl) == 1 &&
                (noVerify_ || SSL_get_verify_result(ssl) == X509_V_OK)) {
                BIO* cbio = BIO_new(BIO_s_socket());
                BIO_set_fd(cbio, fd, BIO_NOCLOSE);
                BIO* sbio = BIO_new(BIO_f_ssl());
                BIO_set_ssl(sbio, ssl, BIO_CLOSE);
                bio_ = BIO_push(sbio, cbio);
                fd_ = fd;   // [fix F11]
                ok = true;
            } else {
                SSL_free(ssl);
                close(fd);
            }
        }
        freeaddrinfo(res);
        return ok;
    }

    bool writeAll(const std::string& s) {
        size_t sent = 0;
        while (sent < s.size()) {
            int r = BIO_write(bio_, s.data() + sent, int(s.size() - sent));
            if (r <= 0) return false;
            wrote_ = true;   // [fix F19] хоть что-то ушло
            sent += size_t(r);
        }
        return true;
    }

    bool doRequest(const std::string& method, const std::string& path,
                   const std::string& body,
                   const std::vector<std::pair<std::string, std::string>>& hdrs,
                   HttpResp& r, int timeoutMs) {
        std::string req = method + " " + path + " HTTP/1.1\r\nHost: " + host_ +
                          "\r\nUser-Agent: spred/3.0\r\nAccept: */*\r\n"
                          "Connection: keep-alive\r\n";
        for (const auto& h : hdrs)
            req += h.first + ": " + h.second + "\r\n";
        // [F51] MEXC (700013 Invalid content Type): пустое тело ->
        // application/json, как в официальном SDK; form — только с телом
        if (!body.empty())
            req += "Content-Type: application/x-www-form-urlencoded\r\n"
                   "Content-Length: " + std::to_string(body.size()) + "\r\n";
        else if (method == "POST" || method == "PUT" || method == "DELETE")
            req += "Content-Type: application/json\r\n"
                   "Content-Length: 0\r\n";
        req += "\r\n";
        req += body;
        if (!writeAll(req)) return false;

        // заголовки
        std::string head;
        char buf[8192];
        double deadline = nowMsSteady() + timeoutMs;
        while (head.find("\r\n\r\n") == std::string::npos) {
            if (nowMsSteady() > deadline) return false;
            int n = BIO_read(bio_, buf, sizeof buf);
            if (n <= 0) return false;
            head.append(buf, size_t(n));
            if (head.size() > 65536) return false;
        }
        size_t hp = head.find("\r\n\r\n");
        std::string rest = head.substr(hp + 4);
        head = head.substr(0, hp);
        {
            // "HTTP/1.1 200 OK"
            size_t sp = head.find(' ');
            if (sp != std::string::npos)
                r.code = std::atoi(head.c_str() + sp + 1);
        }
        bool chunked = false;
        long contentLen = -1;
        {
            std::string low = head;
            for (auto& c : low) c = (char)tolower((unsigned char)c);
            chunked = low.find("transfer-encoding: chunked") != std::string::npos;
            size_t cl = low.find("content-length:");
            if (cl != std::string::npos)
                contentLen = std::atol(low.c_str() + cl + 15);
            size_t ra = low.find("\nretry-after:");   // [fix F41]
            if (ra != std::string::npos) r.retryAfter = std::atof(low.c_str() + ra + 13);
        }
        std::string rawBody = std::move(rest);
        auto readMore = [&](size_t need) -> bool {
            while (rawBody.size() < need) {
                if (nowMsSteady() > deadline) return false;
                int n = BIO_read(bio_, buf, sizeof buf);
                if (n <= 0) return false;
                rawBody.append(buf, size_t(n));
                if (rawBody.size() > 16 * 1024 * 1024) return false;
            }
            return true;
        };
        if (chunked) {
            // [fix F8] инкрементальный разбор чанков. Раньше цикл каждый раз
            // заново разбирал ПЕРВЫЙ чанк и не двигался дальше: ответ из 3+
            // чанков (или нулевой чанк, пришедший отдельной записью) висел до
            // таймаута -> transportOk=false при нормальном ответе сервера.
            std::string out;
            size_t pos = 0;   // начало строки размера текущего чанка в rawBody
            for (;;) {
                size_t e;
                while ((e = rawBody.find("\r\n", pos)) == std::string::npos)
                    if (!readMore(rawBody.size() + 1)) return false;
                std::string szs = rawBody.substr(pos, e - pos);
                size_t semi = szs.find(';');              // chunk-extension
                if (semi != std::string::npos) szs.resize(semi);
                if (szs.empty() || !isxdigit((unsigned char)szs[0])) return false;
                unsigned long long len = strtoull(szs.c_str(), nullptr, 16);
                if (len > (16ull << 20)) return false;
                pos = e + 2;
                if (len == 0) {
                    // trailer-заголовки (если есть) и пустая строка = конец ответа
                    for (;;) {
                        size_t te;
                        while ((te = rawBody.find("\r\n", pos)) == std::string::npos)
                            if (!readMore(rawBody.size() + 1)) return false;
                        if (te == pos) break;
                        pos = te + 2;
                    }
                    r.body = std::move(out);
                    return true;
                }
                if (!readMore(pos + (size_t)len + 2)) return false;
                out.append(rawBody, pos, (size_t)len);
                if (out.size() > (16u << 20)) return false;
                pos += (size_t)len + 2;
            }
        } else if (contentLen >= 0) {
            if (!readMore((size_t)contentLen)) return false;
            r.body = rawBody.substr(0, (size_t)contentLen);
            return true;
        } else {
            // до закрытия (Connection: close)
            for (;;) {
                int n = BIO_read(bio_, buf, sizeof buf);
                if (n <= 0) break;
                rawBody.append(buf, size_t(n));
                if (rawBody.size() > 16 * 1024 * 1024) break;
            }
            r.body = rawBody;
            return true;
        }
    }

    std::string host_;
    int port_ = 443;
    bool noVerify_ = false;
    SSL_CTX* ctx_ = nullptr;
    BIO* bio_ = nullptr;
    int fd_ = -1;   // [fix F11]
    bool wrote_ = false;   // [fix F19]
};

// [v3.0] api-хост настраиваемый (мок-стенд для тестов live-контура)
static std::string g_apiHost = "api.mexc.com";

struct SymbolInfo {
    bool ok = false;
    bool complete = false;   // [fix F15] есть И tick, И qtyStep (обязательно для live)
    bool makerOk = true;     // [fix F26] LIMIT_MAKER разрешён (или orderTypes не указан)
    double tick = 0.01;
    double qtyStep = 1e-8;
    double minNotional = 1.0;
    std::string baseAsset;    // [v3.0] для live: что покупаем/продаём
    std::string quoteAsset;   // [v3.0] валюта котировки (USDT)
};

// [v2.7 A] MEXC v3: quotePrecision — ЧИСЛО ЗНАКОВ (4 -> тик 1e-4);
// baseSizePrecision — УЖЕ ШАГ ("0.00001"; 0 -> шаг 1);
// quoteAmountPrecision — МИНИМАЛЬНЫЙ НОМИНАЛ.
static SymbolInfo fetchSymbolInfo(const std::string& symbol) {
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

// [v2.15 R2 REPLAY] источник кадров из raw-записи MM_RAW_LOG:
// [double recvMs][uint32 len][payload]xN, только бинарные фреймы.
struct ReplaySrc {
    FILE* f = nullptr;
    bool haveNext = false, eof = false;
    double nextRecvMs = 0;
    std::string nextPayload;
    bool readNext() {
        if (eof || !f) return false;
        double recvMs = 0;
        uint32_t len = 0;
        if (std::fread(&recvMs, sizeof recvMs, 1, f) != 1 ||
            std::fread(&len, sizeof len, 1, f) != 1) { eof = true; return false; }
        nextPayload.assign(len, '\0');
        if (len && std::fread(&nextPayload[0], 1, len, f) != len) {
            eof = true;
            return false;
        }
        nextRecvMs = recvMs;
        haveNext = true;
        return true;
    }
};
static ReplaySrc RSrc;

// ------------------------------------------------------------- protobuf-lite
namespace pb {

struct Reader {
    const unsigned char* p;
    const unsigned char* end;

    bool varint(uint64_t& out) {
        out = 0;
        int shift = 0;
        while (p < end) {
            unsigned char b = *p++;
            out |= (uint64_t)(b & 0x7F) << shift;
            if (!(b & 0x80)) return true;
            shift += 7;
            if (shift >= 64) return false;
        }
        return false;
    }

    // [fix F6] проверка остатка ДО сдвига указателя: p += v с огромным v
    // давал переполнение указателя (UB) и обход проверки p <= end
    bool skipField(uint64_t tag) {
        int wire = int(tag & 7);
        uint64_t v;
        switch (wire) {
            case 0: return varint(v);
            case 1: if ((uint64_t)(end - p) < 8) return false; p += 8; return true;
            case 2: {
                if (!varint(v)) return false;
                if (v > (uint64_t)(end - p)) return false;
                p += v;
                return true;
            }
            case 5: if ((uint64_t)(end - p) < 4) return false; p += 4; return true;
            default: return false;
        }
    }
};

inline bool readTag(Reader& r, uint64_t& tag) { return r.varint(tag); }

} // namespace pb

// ------------------------------------------------------------------- models
struct Level { double price = 0, qty = 0; };

struct BookState {
    std::vector<Level> bids, asks;
    // [v2.16 P0-2] две метки свежести:
    //   lastRecv   — последнее ПРИМЕНЁННОЕ событие (stale-gate, как было);
    //   lastPacket — последний ПРИНЯТЫЙ кадр (диагностика: сеть жива,
    //                пока evq копит события внутри reorder-окна).
    // Пакет в reorder-очереди НЕ делает книгу безопасной — stale-гейт
    // остаётся на lastRecv.
    double lastRecv = 0;
    double lastPacket = 0;

    // [v2.11 FIX 6] валидация BBO: finite, price>0, qty>0, не пересечены
    bool ok() const {
        if (bids.empty() || asks.empty()) return false;
        const Level& b = bids[0];
        const Level& a = asks[0];
        return std::isfinite(b.price) && std::isfinite(b.qty) &&
               std::isfinite(a.price) && std::isfinite(a.qty) &&
               b.price > 0 && a.price > 0 && b.qty > 0 && a.qty > 0 &&
               a.price > b.price;
    }
    double bestBid() const { return bids.empty() ? 0 : bids[0].price; }
    double bestAsk() const { return asks.empty() ? 0 : asks[0].price; }
    double mid() const { return ok() ? (bestBid() + bestAsk()) / 2 : 0; }
    double spreadBps() const {
        if (!ok()) return 0;
        return (bestAsk() - bestBid()) / mid() * 1e4;
    }
    double imbalance() const {
        double bb = 0, ba = 0;
        for (size_t i = 0; i < bids.size() && i < 10; ++i) bb += bids[i].qty;
        for (size_t i = 0; i < asks.size() && i < 10; ++i) ba += asks[i].qty;
        return (bb + ba) > 0 ? bb / (bb + ba) : 0.5;
    }
};

static bool parseDouble(const std::string& s, double& out) {
    if (s.empty()) return false;
    char* e = nullptr;
    out = strtod(s.c_str(), &e);
    return e && *e == '\0';
}

// ---------------------------------------------------- MEXC protobuf decoding
struct MexcPush {
    std::string channel, symbol;
    int64_t sendTimeMs = 0;
    bool hasTicker = false;
    double bidP = 0, bidQ = 0, askP = 0, askQ = 0;
    bool hasLimitDepth = false;
    std::vector<Level> bids, asks;
    struct Deal {
        double price = 0, qty = 0;
        int side = 0;
        int64_t timeMs = 0;
    };
    std::vector<Deal> deals;
    bool hasDeals = false;

    // [v3.0 L2] приватные каналы (wrapper-поля 304/306/307, proto из
    // mexcdevelop/websocket-proto). Заполнены только на приватном коннекте.
    struct PrivOrder {
        std::string id, clientId;
        double price = 0, qty = 0, amount = 0, avgPrice = 0;
        int orderType = 0;        // 1=LIMIT, 2=MARKET
        int tradeType = 0;        // 1=BUY, 2=SELL
        bool isMaker = false;
        double remainQty = 0, lastDealQty = 0, cumQty = 0, cumAmt = 0;
        int status = 0;           // 1=NEW 2=FILLED 3=PARTIALLY_FILLED
                                  // 4=CANCELED 5=PARTIALLY_CANCELED
        int64_t createTime = 0;
    };
    bool hasPrivOrder = false;
    PrivOrder pord;
    struct PrivDeal {
        double price = 0, qty = 0, amount = 0;
        int tradeType = 0;        // 1=BUY, 2=SELL (НАША сторона)
        bool isMaker = false, isSelfTrade = false;
        std::string tradeId, clientOrderId, orderId;
        double feeAmount = 0;
        std::string feeCurrency;
        int64_t timeMs = 0;
    };
    bool hasPrivDeal = false;
    PrivDeal pdeal;
    struct PrivAccount {
        std::string coin;
        double balance = 0, balanceChange = 0, frozen = 0, frozenChange = 0;
    };
    bool hasPrivAcct = false;
    PrivAccount pacct;
};

// [v2.7 C] поток сделок. [v2.12] события применяются по одному из
// event-time очереди, уже в порядке биржевого времени.
// [v2.13.1] конвертация метки — через DEAL-офсет (exchOffDealMs).
struct FlowTracker {
    struct Ev { double t, signedQty; };
    std::deque<Ev> ev;
    double windowSec = 60.0;
    double runDecaySec = 20.0;
    double lastRunT = -1e9;
    int runLen = 0;
    int minDeals = 5;

    void prune(double now) {
        while (!ev.empty() && now - ev.front().t > windowSec) ev.pop_front();
    }

    void onDeal(const MexcPush::Deal& d, double now,
                bool haveExchClock, double exchOffMs) {
        double s = (d.side == 1) ? d.qty : (d.side == 2) ? -d.qty : 0.0;
        if (s == 0.0) return;
        double t = now;
        if (haveExchClock && d.timeMs > 0)
            t = ((double)d.timeMs + exchOffMs) / 1000.0;
        if (t > now) t = now;   // защита от рассинхрона часов
        ev.push_back({t, s});
        int sg = (s > 0) ? 1 : -1;
        if (t - lastRunT > runDecaySec) runLen = 0;
        runLen = (runLen != 0 && ((runLen > 0) == (sg > 0))) ? runLen + sg : sg;
        lastRunT = t;
        prune(now);
    }

    double tfi(double now) {
        prune(now);
        if ((int)ev.size() < minDeals) return 0.0;
        double b = 0, s = 0;
        for (const auto& e : ev) (e.signedQty > 0 ? b : s) += std::fabs(e.signedQty);
        return (b + s) > 0 ? (b - s) / (b + s) : 0.0;
    }

    int run(double now) const {
        return (now - lastRunT > runDecaySec) ? 0 : runLen;
    }
};

static double walkBook(const BookState& b, bool isBuy, double qty, double& filled) {
    const std::vector<Level>& lv = isBuy ? b.asks : b.bids;
    double rem = qty, cost = 0, q = 0;
    for (const auto& l : lv) {
        double take = std::min(rem, l.qty);
        cost += take * l.price;
        q += take;
        rem -= take;
        if (rem <= 1e-12) break;
    }
    filled = q;
    return q > 0 ? cost / q : 0.0;
}

// [v2.15 ФИКС 22] стресс-escape: walk с уровня N+1 — симуляция withdrawal
// (все MM сняли ближние котировки одновременно). skip=0 == walkBook.
// filled = фактически заполненное (книга может кончиться раньше qty).
static double walkBookSkip(const BookState& b, bool isBuy, double qty,
                           double& filled, int skipLevels) {
    const std::vector<Level>& lv = isBuy ? b.asks : b.bids;
    double rem = qty, cost = 0, q = 0;
    for (size_t i = (size_t)std::max(0, skipLevels); i < lv.size(); ++i) {
        double take = std::min(rem, lv[i].qty);
        cost += take * lv[i].price;
        q += take;
        rem -= take;
        if (rem <= 1e-12) break;
    }
    filled = q;
    return q > 0 ? cost / q : 0.0;
}

// [v2.9.1 FIX A / v2.10 FIX C] закупка по бюджету, битые уровни пропускаем
static double walkBudgetBuy(const BookState& b, double budgetQuote) {
    double rem = budgetQuote, q = 0;
    for (const auto& l : b.asks) {
        if (rem <= 0) break;
        if (l.price <= 0 || l.qty <= 0) continue;
        double afford = rem / l.price;
        double take = std::min(l.qty, afford);
        q += take;
        rem -= take * l.price;
        if (take < l.qty) break;
    }
    return q;
}

static Level parseLimitDepth(const unsigned char* data, size_t len) {
    Level lv;
    pb::Reader r{data, data + len};
    uint64_t tag;
    while (pb::readTag(r, tag) && r.p < r.end) {
        if ((tag & 7) == 2) {
            uint64_t l;
            if (!r.varint(l)) break;
            if (l > (uint64_t)(r.end - r.p)) break;
            std::string s(reinterpret_cast<const char*>(r.p), size_t(l));
            if ((tag >> 3) == 1) parseDouble(s, lv.price);
            else if ((tag >> 3) == 2) parseDouble(s, lv.qty);
            r.p += l;
        } else if (!r.skipField(tag)) break;
    }
    return lv;
}

static void parseLimitDepths(const unsigned char* data, size_t len, MexcPush& out) {
    pb::Reader r{data, data + len};
    uint64_t tag;
    while (pb::readTag(r, tag) && r.p < r.end) {
        if ((tag & 7) == 2) {
            uint64_t l;
            if (!r.varint(l)) break;
            if (l > (uint64_t)(r.end - r.p)) break;
            int field = int(tag >> 3);
            if (field == 1) out.asks.push_back(parseLimitDepth(r.p, size_t(l)));
            else if (field == 2) out.bids.push_back(parseLimitDepth(r.p, size_t(l)));
            r.p += l;
        } else if (!r.skipField(tag)) break;
    }
    out.hasLimitDepth = !out.bids.empty() && !out.asks.empty();
}

static void parseBookTicker(const unsigned char* data, size_t len, MexcPush& out) {
    pb::Reader r{data, data + len};
    uint64_t tag;
    while (pb::readTag(r, tag) && r.p < r.end) {
        if ((tag & 7) == 2) {
            uint64_t l;
            if (!r.varint(l)) break;
            if (l > (uint64_t)(r.end - r.p)) break;
            std::string s(reinterpret_cast<const char*>(r.p), size_t(l));
            switch (tag >> 3) {
                case 1: parseDouble(s, out.bidP); break;
                case 2: parseDouble(s, out.bidQ); break;
                case 3: parseDouble(s, out.askP); break;
                case 4: parseDouble(s, out.askQ); break;
                default: break;
            }
            r.p += l;
        } else if (!r.skipField(tag)) break;
    }
    out.hasTicker = out.bidP > 0 && out.askP > 0 && out.askP > out.bidP;
}

static void parseDeal(const unsigned char* data, size_t len, MexcPush::Deal& d) {
    pb::Reader r{data, data + len};
    uint64_t tag;
    while (pb::readTag(r, tag) && r.p < r.end) {
        if ((tag & 7) == 2) {
            uint64_t l;
            if (!r.varint(l)) break;
            if (l > (uint64_t)(r.end - r.p)) break;
            std::string s(reinterpret_cast<const char*>(r.p), size_t(l));
            if ((tag >> 3) == 1) parseDouble(s, d.price);
            else if ((tag >> 3) == 2) parseDouble(s, d.qty);
            r.p += l;
        } else if ((tag & 7) == 0) {
            uint64_t v;
            if (!r.varint(v)) break;
            if ((tag >> 3) == 3) d.side = int(v);
            else if ((tag >> 3) == 4) d.timeMs = (int64_t)v;
        } else if (!r.skipField(tag)) break;
    }
}

static void parseDeals(const unsigned char* data, size_t len, MexcPush& out) {
    pb::Reader r{data, data + len};
    uint64_t tag;
    while (pb::readTag(r, tag) && r.p < r.end) {
        if ((tag & 7) == 2) {
            uint64_t l;
            if (!r.varint(l)) break;
            if (l > (uint64_t)(r.end - r.p)) break;
            if ((tag >> 3) == 1) {
                MexcPush::Deal d;
                parseDeal(r.p, size_t(l), d);
                if (d.price > 0 && d.qty > 0) out.deals.push_back(d);
            }
            r.p += l;
        } else if (!r.skipField(tag)) break;
    }
    out.hasDeals = !out.deals.empty();
}

// [v3.0 L2] приватные protobuf-парсеры (поля — из официальных .proto MEXC)
static void parsePrivOrder(const unsigned char* data, size_t len,
                           MexcPush::PrivOrder& o) {
    pb::Reader r{data, data + len};
    uint64_t tag;
    while (pb::readTag(r, tag) && r.p < r.end) {
        int field = int(tag >> 3);
        int wire = int(tag & 7);
        if (wire == 2) {
            uint64_t l;
            if (!r.varint(l)) break;
            if (l > (uint64_t)(r.end - r.p)) break;
            std::string s(reinterpret_cast<const char*>(r.p), size_t(l));
            double dv;
            switch (field) {
                case 1: o.id = s; break;
                case 2: o.clientId = s; break;
                case 3: if (parseDouble(s, dv)) o.price = dv; break;
                case 4: if (parseDouble(s, dv)) o.qty = dv; break;
                case 5: if (parseDouble(s, dv)) o.amount = dv; break;
                case 6: if (parseDouble(s, dv)) o.avgPrice = dv; break;
                case 11: if (parseDouble(s, dv)) o.remainQty = dv; break;
                case 12: if (parseDouble(s, dv)) o.lastDealQty = dv; break;
                case 13: if (parseDouble(s, dv)) o.cumQty = dv; break;
                case 14: if (parseDouble(s, dv)) o.cumAmt = dv; break;
                default: break;
            }
            r.p += l;
        } else if (wire == 0) {
            uint64_t v;
            if (!r.varint(v)) break;
            switch (field) {
                case 7: o.orderType = int(v); break;
                case 8: o.tradeType = int(v); break;
                case 9: o.isMaker = (v != 0); break;
                case 15: o.status = int(v); break;
                case 16: o.createTime = (int64_t)v; break;
                default: break;
            }
        } else if (!r.skipField(tag)) break;
    }
}

static void parsePrivDeal(const unsigned char* data, size_t len,
                          MexcPush::PrivDeal& d) {
    pb::Reader r{data, data + len};
    uint64_t tag;
    while (pb::readTag(r, tag) && r.p < r.end) {
        int field = int(tag >> 3);
        int wire = int(tag & 7);
        if (wire == 2) {
            uint64_t l;
            if (!r.varint(l)) break;
            if (l > (uint64_t)(r.end - r.p)) break;
            std::string s(reinterpret_cast<const char*>(r.p), size_t(l));
            double dv;
            switch (field) {
                case 1: if (parseDouble(s, dv)) d.price = dv; break;
                case 2: if (parseDouble(s, dv)) d.qty = dv; break;
                case 3: if (parseDouble(s, dv)) d.amount = dv; break;
                case 7: d.tradeId = s; break;
                case 8: d.clientOrderId = s; break;
                case 9: d.orderId = s; break;
                case 10: if (parseDouble(s, dv)) d.feeAmount = dv; break;
                case 11: d.feeCurrency = s; break;
                default: break;
            }
            r.p += l;
        } else if (wire == 0) {
            uint64_t v;
            if (!r.varint(v)) break;
            switch (field) {
                case 4: d.tradeType = int(v); break;
                case 5: d.isMaker = (v != 0); break;
                case 6: d.isSelfTrade = (v != 0); break;
                case 12: d.timeMs = (int64_t)v; break;
                default: break;
            }
        } else if (!r.skipField(tag)) break;
    }
}

static void parsePrivAccount(const unsigned char* data, size_t len,
                             MexcPush::PrivAccount& a) {
    pb::Reader r{data, data + len};
    uint64_t tag;
    while (pb::readTag(r, tag) && r.p < r.end) {
        int field = int(tag >> 3);
        int wire = int(tag & 7);
        if (wire == 2) {
            uint64_t l;
            if (!r.varint(l)) break;
            if (l > (uint64_t)(r.end - r.p)) break;
            std::string s(reinterpret_cast<const char*>(r.p), size_t(l));
            double dv;
            switch (field) {
                case 1: a.coin = s; break;
                case 3: if (parseDouble(s, dv)) a.balance = dv; break;
                case 4: if (parseDouble(s, dv)) a.balanceChange = dv; break;
                case 5: if (parseDouble(s, dv)) a.frozen = dv; break;
                case 6: if (parseDouble(s, dv)) a.frozenChange = dv; break;
                default: break;
            }
            r.p += l;
        } else if (!r.skipField(tag)) break;
    }
}

static bool parsePush(const std::string& msg, MexcPush& out) {
    pb::Reader r{reinterpret_cast<const unsigned char*>(msg.data()),
                 reinterpret_cast<const unsigned char*>(msg.data()) + msg.size()};
    uint64_t tag;
    while (pb::readTag(r, tag) && r.p < r.end) {
        int field = int(tag >> 3);
        int wire = int(tag & 7);
        if (wire == 2) {
            uint64_t l;
            if (!r.varint(l)) break;
            if (l > (uint64_t)(r.end - r.p)) break;
            if (field == 1) {
                out.channel.assign(reinterpret_cast<const char*>(r.p), size_t(l));
            } else if (field == 3) {
                out.symbol.assign(reinterpret_cast<const char*>(r.p), size_t(l));
            } else if (field == 303) {
                parseLimitDepths(r.p, size_t(l), out);
            } else if (field == 314) {
                parseDeals(r.p, size_t(l), out);
            } else if (field == 315) {
                parseBookTicker(r.p, size_t(l), out);
            } else if (field == 304) {   // [v3.0] spot@private.orders.v3.api.pb
                parsePrivOrder(r.p, size_t(l), out.pord);
                out.hasPrivOrder = true;
            } else if (field == 306) {   // [v3.0] spot@private.deals.v3.api.pb
                parsePrivDeal(r.p, size_t(l), out.pdeal);
                out.hasPrivDeal = true;
            } else if (field == 307) {   // [v3.0] spot@private.account.v3.api.pb
                parsePrivAccount(r.p, size_t(l), out.pacct);
                out.hasPrivAcct = true;
            }
            r.p += l;
        } else if (wire == 0) {
            uint64_t v;
            if (!r.varint(v)) break;
            if (field == 6) out.sendTimeMs = (int64_t)v;
        } else if (!r.skipField(tag)) break;
    }
    return out.hasTicker || out.hasLimitDepth || out.hasDeals ||
           out.hasPrivOrder || out.hasPrivDeal || out.hasPrivAcct;
}

// [v2.12 ФИКС 7] event-time очередь
struct PendEv {
    double exchMs = 0;
    double recvMs = 0;
    int kind = 0;                 // 0=depth, 1=ticker, 2=deal
    std::vector<Level> bids, asks;
    double bidP = 0, bidQ = 0, askP = 0, askQ = 0;
    MexcPush::Deal deal;
};

// ------------------------------------------------------------- virtual trader
struct Order {
    enum Side { BUY, SELL } side = BUY;
    double price = 0, qty = 0, filled = 0;
    int id = 0;
};

enum class QState { NONE, WAIT_PLACE, LIVE, WAIT_CANCEL };

// [v2.12 ФИКС 10] классификация филлов. [v2.13] + ESCAPE (только per-reason)
enum FillReason { FR_QUEUE = 0, FR_THROUGH, FR_TOXIC, FR_GHOST, FR_GAP, FR_ESCAPE, FR_N };
static const char* FR_NAMES[FR_N] = {"QUEUE", "THROUGH", "TOXIC", "GHOST", "GAP", "ESCAPE"};

struct SideBook {
    Order cur;
    Order nxt;
    bool hasNext = false;
    QState st = QState::NONE;
    double pendingUntil = 0;
    double placeSentMs = 0;
    double cancelSentMs = 0;
    double queueAhead = 0;
    double lastLevelQty = 0;
    double tradedAtLevel = 0;
    bool queueInited = false;      // [v2.11 FIX 3]
    double curPlaceLatMs = 150.0;  // [v2.13] латентность ЭТОГО ордера
    double curCancelLatMs = 150.0; // [v2.13] латентность ЭТОЙ отмены
    double lastRequoteAt = 0;
    double lastSweepAt = 0;
};

struct Config {
    double orderSizeQuote = 100;
    double maxPosQuote = 500;
    double makerFeeBps = 0.0;
    double takerFeeBps = 5.0;
    double latencyS = 0.15;
    double maxSpreadBps = 250;
    double requoteFrac = 0.05;
    double minRequoteSec = 0;
    double sweepCooldownS = 15;
    int skewImb = 1;
    int skewTfi = 1;
    double imbSkewFrac = 0.10;
    double tfiSkewFrac = 0.10;
    double invSkewFrac = 0.25;
    double tfiWindowSec = 60.0;
    double tfiThresh = 0.40;
    int runStop = 3;
    double rateRps = 8;
    double maxLoss = 100;
    double staleSecs = 30;
    double escapeSecs = 30;
    int lossBasis = 1;              // [F56] 1 = стоп по mm-P&L, 0 = полный P&L
    int haltFlat = 1;               // [F57] закрыть позицию при HALT
    double trendBps = 0;            // [F58] 0 = выкл
    double trendSec = 900;          // [F58]
    bool allowShort = false;
    double minInitFrac = 0.0;
    double cancelAheadFrac = 0.5;   // [v2.11 FIX 4]
    // [v2.12]
    double reorderMs = 250;
    int gapFillMode = 0;            // 0=песс, 1=50%, 2=опт
    // [v2.13] latency как распределение (множители к базе)
    int latDist = 0;                // 0 = константа (как v2.12), 1 = распределение
    double latP90Mult = 1.6;
    double latP99Mult = 2.5;
    double latMinMult = 0.3;
    // [v2.13.1] карантин «сделок из будущего»
    double quarantineMs = 2000;
    // [v2.14] ФИКС 17/18/19 — все OFF по умолчанию (= поведение v2.13.1)
    int initMode = 0;               // 0=classic (taker pre-buy), 1=ramp (bid-only)
    double escapeCooldownS = 0;     // пауза стороны после escape (0=off)
    double cutoffBps = 0;           // порог пробития уровня для cutoff (0=off)
    double cutoffSec = 60;          // длительность cutoff-паузы
    int printFilter = 1;            // [F45] 0=off 1=no THROUGH из внутр. принтов 2=игнор
    double printFilterMs = 1500;    // [F45] окно истории BBO до принта (биржевое время)
    double printFillP = 0;          // [F49] вероятность, что внутренний принт всё же даёт филл
    // [v2.15 ФИКС 20/21/22] — все OFF по умолчанию (= поведение v2.14)
    int queueMode = 0;              // Ф20: 0=frac(как v2.14) 1=pess 2=opt
    int escapeSkipLevels = 0;       // Ф22: 0..3 — escape-walk с уровня N+1
    int selfInBook = 0;             // Ф21: 1 = сигнальный имбаланс вкл. наш ордер
    double qtyDriftUsdt = 0;        // [v3.1.1] 0 = порог 1000 ед. (как v3.1); >0 = порог в USDT
};

struct PaperTrader {
    double base = 0;
    double quote = 0;
    double initialQuote = 0;
    double avgEntry = 0;
    int orderSeq = 1;

    double base0 = 0, mid0 = 0, setupCost = 0;

    Config cfg;
    SideBook buy, sell;
    TokenBucket rate;

    double qtyStep = 1e-8;
    // [v2.13.1 ФИКС 16] два часовых домена:
    //   exchOffMs     — steady -> send-домен (по push.sendTimeMs), только
    //                   сортировка/fallback/диагностика, филл-гейт НЕ трогает;
    //   exchOffDealMs — steady -> deal-домен (по d.timeMs), филл-гейт,
    //                   ghost-окно, конвертация времени для flow.
    double exchOffMs = 0;
    double exchOffDealMs = 0;
    double rttMs = 200;          // md-rtt по ping — только диагностика
    bool exchOffInit = false;
    bool dealOffInit = false;

    double orderLatMs = 150.0;   // база place-latency
    double cancelLatMs = 150.0;  // база cancel-latency

    std::mt19937 rng{(unsigned)nowMsSteady()};   // [v2.13]

    bool pauseBuy = false, pauseSell = false;

    // [v2.14] паузы сторон (steady-секунды; 0 = неактивна)
    double escPauseBuyUntil = 0, escPauseSellUntil = 0;   // ФИКС 18
    double cutPauseBuyUntil = 0, cutPauseSellUntil = 0;   // ФИКС 19
    int cutCnt = 0;                                        // событий cutoff
    bool curPrintInside = false;   // [F45] текущий принт строго внутри спреда
    long prAll = 0, prInside = 0, prCutFills = 0;          // [F45] счётчики
    long prPassFills = 0;          // [F49] внутренние принты, пропущенные с вероятностью p
    std::mt19937 prng{12345u};     // [F49] отдельный RNG (не сдвигает латентности rng)
    double prUrand() { return (double)(prng() >> 8) * (1.0 / 16777216.0); }
    int realSyncCnt = 0;           // [F50] shadow-ордер закрыт по реальному полному филлу
    int realSweepCnt = 0;          // [F50] cooldown/cutoff от реального филла
    std::deque<std::array<double, 3>> recentPr;   // [F50] (now, price, side) публичных принтов, 3 c
    // [F50] реальный ордер стороны исполнен полностью, а shadow ещё стоит:
    // закрываем shadow-ордер БЕЗ проводки в paper-P&L (fill_cal не искажается),
    // дальше та же последовательность, что после полного paper-филла.
    bool realSync(SideBook& sb, double now, std::vector<std::string>& ev) {
        if (!(sb.st == QState::LIVE || sb.st == QState::WAIT_PLACE)) return false;
        if (sb.cur.qty - sb.cur.filled <= 1e-15) return false;
        ++realSyncCnt;
        char buf[200];
        std::snprintf(buf, sizeof buf,
                      "[REAL_SYNC] %s #%d: реальный ордер исполнен, shadow-ордер "
                      "(%.10g @ %.10g, filled %.10g) закрыт без paper-филла",
                      sb.cur.side == Order::BUY ? "BUY " : "SELL", sb.cur.id,
                      sb.cur.qty, sb.cur.price, sb.cur.filled);
        evAdd(ev, buf);
        if (sb.hasNext) {
            sb.cur = sb.nxt;
            sb.hasNext = false;
            sb.st = QState::WAIT_PLACE;
            sb.curPlaceLatMs = orderLatMs * sampleLatMult();
            sb.pendingUntil = now + sb.curPlaceLatMs / 1000.0;
            sb.placeSentMs = nowMsSteady();
            sb.lastRequoteAt = now;
            sb.queueInited = false;
        } else {
            sb.cur = Order{};
            sb.st = QState::NONE;
        }
        return true;
    }

    // [v2.16 P0-3] окно живости ордера: [startExchMs, untilExchMs].
    // Сделка старше активации ордера физически не могла его заполнить.
    struct Ghost { bool active = false; int side = 0; double price = 0, qty = 0;
                   double startExchMs = 0, untilExchMs = 0; };
    Ghost ghostBuy, ghostSell;

    struct GapOrd { int side; double price, qty; };
    std::vector<GapOrd> gapCheck;
    bool gapPending = false;

    struct FillRec { double t; int sgn; double px, proxyPx, takerPx, qty; int done; int reason; };
    std::deque<FillRec> mo;
    double moSum[3] = {0, 0, 0}, moVol[3] = {0, 0, 0};
    double pmoSum[3] = {0, 0, 0}, pmoVol[3] = {0, 0, 0};
    double tmoSum[3] = {0, 0, 0}, tmoVol[3] = {0, 0, 0};
    static constexpr double MO_H[3] = {5.0, 30.0, 60.0};

    // [v2.12 ФИКС 10] mo и объём по типам филлов
    double rmoSum[FR_N][3] = {};
    double rmoVol[FR_N][3] = {};
    int rCnt[FR_N] = {};

    // [v2.13 ФИКС 12] распределения mo (копятся при закрытии горизонтов)
    std::vector<double> moH[3];            // все филлы (кроме ESCAPE), по горизонтам
    std::vector<double> moBuy30, moSell30; // стороны, 30c
    std::vector<double> moR30[FR_N];       // по reason, 30c (вкл. ESCAPE)

    // [v2.13 ФИКС 13] escape slippage
    int escCnt = 0;
    double escSlipSum = 0, escSlipWorst = 0;

    // [v2.13 ФИКС 14] fees
    double makerFees = 0, takerFees = 0;

    // [v2.13.1] часовая диагностика
    int skewQuar = 0;            // сделки в карантине (future-метка)
    double skewMaxMs = 0;        // max |send-vs-deal skew| за сессию
    bool skewAlerted = false;

    int fills = 0, buyFills = 0, sellFills = 0;
    int partialFills = 0, forcedFills = 0, throttled = 0, cancels = 0, replaces = 0;
    int lateFills = 0, toxicFills = 0;
    int gapFills = 0;
    int gapCertain = 0, gapPossible = 0;   // [v2.12 ФИКС 8]
    double realized = 0;
    double feesPaid = 0;
    double volumeTraded = 0;
    double lastMid = 0;
    double sellSmallSecs = 0;
    bool invInitialized = false, halted = false;

    double equity(double mid) const { return quote + base * mid; }
    double totalPnl(double mid) const { return equity(mid) - initialQuote; }
    double posValue(double mid) const { return (base - base0) * mid; }

    double roundQty(double q) const { return std::floor(q / qtyStep + 1e-9) * qtyStep; }

    // [v2.13.1 ФИКС 16] калибровка deal-часов: sample = steadyRecv - d.timeMs.
    // Вниз (свежее) — быстро (0.3): отслеживаем конвергенцию/сдвиг часов;
    // вверх (старение/задержка доставки) — медленно (0.05): не накапливаем
    // bias доставки. Стейл-батчи (sample выше оценки на >5с) и мусорные
    // future-метки (ниже на >30с) отбрасываются; шаг вниз клампится 1с —
    // одиночная мусорная метка не утащит оценку.
    void calibDealClock(double sample) {
        if (!dealOffInit) {
            exchOffDealMs = sample;
            dealOffInit = true;
        } else {
            double d = sample - exchOffDealMs;
            if (d <= -30000.0 || d >= 5000.0) return;
            double step = (d < 0.0 ? 0.30 : 0.05) * d;
            if (step < -1000.0) step = -1000.0;
            exchOffDealMs += step;
        }
        if (exchOffInit) {
            double s = std::fabs(exchOffMs - exchOffDealMs);
            if (s > skewMaxMs) skewMaxMs = s;
        }
    }

    // [v2.13.1] + = send-часы ОТСТАЮТ от deal-часов (метки сделок «впереди»)
    double clockSkewMs() const {
        return (exchOffInit && dealOffInit) ? (exchOffMs - exchOffDealMs) : 0.0;
    }

    // [v2.13 ФИКС 11] множитель латентности: кусочно-линейный inverse-CDF.
    // p50 = 1.0, p90 = latP90Mult, p99 = latP99Mult, хвост до 1.5x p99.
    double sampleLatMult() {
        if (cfg.latDist <= 0) return 1.0;
        std::uniform_real_distribution<double> u(0.0, 1.0);
        double x = u(rng);
        double m;
        if (x < 0.5)
            m = cfg.latMinMult + (1.0 - cfg.latMinMult) * (x / 0.5);
        else if (x < 0.9)
            m = 1.0 + (cfg.latP90Mult - 1.0) * ((x - 0.5) / 0.4);
        else if (x < 0.99)
            m = cfg.latP90Mult + (cfg.latP99Mult - cfg.latP90Mult) * ((x - 0.9) / 0.09);
        else
            m = cfg.latP99Mult * (1.0 + 0.5 * ((x - 0.99) / 0.01));
        if (m < cfg.latMinMult) m = cfg.latMinMult;
        return m;
    }

    void applyTrade(double signedQty, double px) {
        double notional = signedQty * px;
        double fee = std::fabs(notional) * cfg.makerFeeBps / 1e4;
        feesPaid += fee;
        makerFees += fee;   // [v2.13] (escape добавляет taker-дельту отдельно)
        quote -= notional + fee;
        volumeTraded += std::fabs(notional);
        const double EPS = 1e-12;
        bool wasLong = base > EPS, wasShort = base < -EPS;
        double nb = base + signedQty;
        if (!wasLong && !wasShort) {
            avgEntry = px;
        } else if (wasLong) {
            if (signedQty < 0) {
                double close = std::min(-signedQty, base);
                realized += close * (px - avgEntry);
                if (nb < -EPS) avgEntry = px;
                else if (std::fabs(nb) <= EPS) avgEntry = 0;
            } else {
                avgEntry = (avgEntry * base + px * signedQty) / nb;
            }
        } else {
            if (signedQty > 0) {
                double close = std::min(signedQty, -base);
                realized += close * (avgEntry - px);
                if (nb > EPS) avgEntry = px;
                else if (std::fabs(nb) <= EPS) avgEntry = 0;
            } else {
                avgEntry = (avgEntry * (-base) + px * (-signedQty)) / (-nb);
            }
        }
        base = nb;
    }

    static double levelQty(const BookState& b, Order::Side side, double px, double tick) {
        double eps = tick * 0.49, sum = 0;
        const std::vector<Level>& v = (side == Order::BUY) ? b.bids : b.asks;
        for (const auto& l : v)
            if (std::fabs(l.price - px) <= eps) sum += l.qty;
        return sum;
    }

    // [v2.16 P0-3] makeGhost с ЯВНЫМИ параметрами (не sb-состоянием) —
    // меньше шансов перепутать cur/nxt при будущих правках.
    void makeGhost(const Order& o, double placeSentMs, double placeLatMs,
                   double cancelSentMs, double cancelLat) {
        // [v2.13.1] окно живости ордера на бирже — в deal-домене: границы
        // окна сравниваются с d.timeMs в onGhost.
        if (!dealOffInit) return;
        Ghost& g = (o.side == Order::BUY) ? ghostBuy : ghostSell;
        g.active = true;
        g.side = (int)o.side;
        g.price = o.price;
        g.qty = o.qty - o.filled;
        g.startExchMs = placeSentMs + placeLatMs - exchOffDealMs;   // [P0-3]
        g.untilExchMs = cancelSentMs + cancelLat - exchOffDealMs;
    }

    void requestQuote(SideBook& sb, Order::Side side, double px, double qty,
                      double rqThresh, double now, std::vector<std::string>& ev) {
        if (sb.st == QState::NONE) {
            if (!rate.take(RK_NEW)) { ++throttled; return; }
            sb.cur = Order{side, px, qty, 0, orderSeq++};
            sb.hasNext = false;
            sb.st = QState::WAIT_PLACE;
            // [v2.13] каждому ордеру — своя латентность
            sb.curPlaceLatMs = orderLatMs * sampleLatMult();
            sb.pendingUntil = now + sb.curPlaceLatMs / 1000.0;
            sb.placeSentMs = nowMsSteady();
            sb.lastRequoteAt = now;
            // [v2.11 FIX 3] новый ордер — очередь с чистого листа
            sb.queueAhead = 0;
            sb.lastLevelQty = 0;
            sb.tradedAtLevel = 0;
            sb.queueInited = false;
            char buf[192];
            std::snprintf(buf, sizeof buf,
                          "[PLACE %s] #%d %.10g @ %.10g (live in %.0f ms)",
                          side == Order::BUY ? "BUY " : "SELL", sb.cur.id, qty, px,
                          sb.curPlaceLatMs);
            evAdd(ev, buf);
        } else if (sb.st == QState::WAIT_PLACE) {
            if (std::fabs(sb.cur.price - px) >= rqThresh) {
                if (!rate.take(RK_NEW)) { ++throttled; return; }
                sb.cur.price = px;
                sb.cur.qty = qty;
                sb.lastRequoteAt = now;
            }
        } else {
            // LIVE or WAIT_CANCEL: replace if price moved enough OR qty drifted
            double target = sb.hasNext ? sb.nxt.price : sb.cur.price;
            double targetQty = sb.hasNext ? sb.nxt.qty : sb.cur.qty;
            bool pxMoved = std::fabs(target - px) >= rqThresh;
            double denom = std::max(targetQty, qty);
            // [fix F6] порог дрейфа qty: по умолчанию 1000 ед. (как v3.1, историю
            // ночей не ломаем); MM_QTY_DRIFT_USDT>0 — в USDT, как в LiveExecutor
            double minDrift = (cfg.qtyDriftUsdt > 0 && px > 0) ? cfg.qtyDriftUsdt / px
                                                                : 1000.0;
            bool qtyDrift = denom > 0 &&
                            std::fabs(targetQty - qty) / denom >= 0.50 &&
                            std::fabs(targetQty - qty) >= minDrift;
            if (!pxMoved && !qtyDrift) return;
            if (cfg.minRequoteSec > 0 && now - sb.lastRequoteAt < cfg.minRequoteSec) return;
            if (!rate.take(sb.st == QState::LIVE ? RK_REPLACE : RK_NEW)) { ++throttled; return; }
            ++replaces;
            sb.nxt = Order{side, px, qty, 0, orderSeq++};
            sb.hasNext = true;
            sb.lastRequoteAt = now;
            if (sb.st == QState::LIVE) {
                sb.st = QState::WAIT_CANCEL;
                // [v2.13] каждой отмене — своя латентность
                sb.curCancelLatMs = cancelLatMs * sampleLatMult();
                sb.pendingUntil = now + sb.curCancelLatMs / 1000.0;
                sb.cancelSentMs = nowMsSteady();
                ++cancels;
                char buf[256];
                std::snprintf(buf, sizeof buf,
                              "[REQUOTE   ] cancel #%d @ %.10g%s -> #%d @ %.10g x%.10g",
                              sb.cur.id, sb.cur.price,
                              qtyDrift && !pxMoved ? " (qty-sync)" : "",
                              sb.nxt.id, px, qty);
                evAdd(ev, buf);
            }
        }
    }

    void requestCancel(SideBook& sb, double now, std::vector<std::string>& ev) {
        if (sb.st == QState::NONE) return;
        if (sb.st == QState::WAIT_CANCEL) { sb.hasNext = false; return; }
        if (!rate.take(RK_CANCEL)) { ++throttled; return; }
        ++cancels;
        if (sb.st == QState::WAIT_PLACE) {
            sb.st = QState::NONE;
            sb.hasNext = false;
            return;
        }
        sb.st = QState::WAIT_CANCEL;
        // [v2.13] каждой отмене — своя латентность
        sb.curCancelLatMs = cancelLatMs * sampleLatMult();
        sb.pendingUntil = now + sb.curCancelLatMs / 1000.0;
        sb.cancelSentMs = nowMsSteady();
        char buf[128];
        std::snprintf(buf, sizeof buf, "[CANCEL    ] #%d @ %.10g", sb.cur.id, sb.cur.price);
        evAdd(ev, buf);
        sb.hasNext = false;
    }

    void advance(SideBook& sb, const BookState& book, double tick, double now) {
        if (sb.st == QState::WAIT_PLACE && now >= sb.pendingUntil) {
            sb.st = QState::LIVE;
            if (!sb.queueInited) {
                sb.queueAhead = levelQty(book, sb.cur.side, sb.cur.price, tick);
                sb.lastLevelQty = sb.queueAhead;
                sb.tradedAtLevel = 0;
                sb.queueInited = true;
            }
        } else if (sb.st == QState::WAIT_CANCEL && now >= sb.pendingUntil) {
            if (sb.cur.qty - sb.cur.filled > 1e-15)
                makeGhost(sb.cur, sb.placeSentMs, sb.curPlaceLatMs,     // [v2.16 P0-3]
                          sb.cancelSentMs, sb.curCancelLatMs);
            sb.cur = Order{};
            if (sb.hasNext) {
                sb.cur = sb.nxt;
                sb.hasNext = false;
                sb.st = QState::WAIT_PLACE;
                sb.curPlaceLatMs = orderLatMs * sampleLatMult();   // [v2.13]
                sb.pendingUntil = now + sb.curPlaceLatMs / 1000.0;
                sb.placeSentMs = nowMsSteady();
                sb.queueInited = false;
            } else {
                sb.st = QState::NONE;
            }
        }
    }

    // [v2.11 FIX 4] queue-модель
    void onDepth(SideBook& sb, const BookState& book, double tick) {
        if (sb.st != QState::LIVE && sb.st != QState::WAIT_CANCEL) return;
        if (sb.cur.price <= 0) return;
        double eps = tick * 0.49;
        const std::vector<Level>& v = (sb.cur.side == Order::BUY) ? book.bids : book.asks;
        bool outOfWindow =
            v.empty() ||
            (sb.cur.side == Order::BUY
                 ? (v.back().price > sb.cur.price + eps)
                 : (v.back().price < sb.cur.price - eps));
        double lq = levelQty(book, sb.cur.side, sb.cur.price, tick);
        if (outOfWindow) {
            sb.lastLevelQty = lq;
            sb.tradedAtLevel = 0;
            return;
        }
        double cancelled = sb.lastLevelQty - sb.tradedAtLevel - lq;
        // [v2.15 ФИКС 20] MM_QUEUE_MODE: bounding вместо точки. Матрицы
        // TAG/ZIL (n9): при requote ~10с очередь эпизодическая, frac не
        // меняет поток филлов — ось закрыта, флаг остаётся страховкой
        // для пар с редкими requot'ами.
        if (cancelled > 0) {
            double frac = (cfg.queueMode == 1) ? 0.0
                        : (cfg.queueMode == 2) ? 1.0
                                               : cfg.cancelAheadFrac;
            sb.queueAhead -= frac * cancelled;
        }
        if (sb.queueAhead < 0) sb.queueAhead = 0;
        sb.lastLevelQty = lq;
        sb.tradedAtLevel = 0;
    }

    // [v2.11 FIX 1] раздельные знаменатели; [v2.13] распределения + ESCAPE
    // ESCAPE-филлы идут ТОЛЬКО в per-reason статистику — вынужденный выход
    // не должен разбавнять mo/pxmo/txmo стратегии.
    void updateMarkouts(double now, double mid) {
        if (mid <= 0) return;
        for (auto& f : mo)
            while (f.done < 3 && now - f.t >= MO_H[f.done]) {
                double bps = f.sgn * (mid - f.px) / f.px * 1e4;
                if (f.reason >= 0 && f.reason < FR_N) {
                    rmoSum[f.reason][f.done] += bps * f.qty * f.px;
                    rmoVol[f.reason][f.done] += f.qty * f.px;
                    if (f.done == 1) moR30[f.reason].push_back(bps);
                }
                if (f.reason == FR_ESCAPE) { ++f.done; continue; }
                moSum[f.done] += bps * f.qty * f.px;
                moVol[f.done] += f.qty * f.px;
                double pbps = f.sgn * (mid - f.proxyPx) / f.proxyPx * 1e4;
                pmoSum[f.done] += pbps * f.qty * f.proxyPx;
                pmoVol[f.done] += f.qty * f.proxyPx;   // [fix F5]
                double tbps = f.sgn * (mid - f.takerPx) / f.takerPx * 1e4;
                tmoSum[f.done] += tbps * f.qty * f.takerPx;
                tmoVol[f.done] += f.qty * f.takerPx;   // [fix F5]
                moH[f.done].push_back(bps);
                if (f.done == 1)
                    (f.sgn > 0 ? moBuy30 : moSell30).push_back(bps);
                ++f.done;
            }
        while (!mo.empty() && mo.front().done >= 3) mo.pop_front();
    }
    double markout(int i) const { return moVol[i] > 0 ? moSum[i] / moVol[i] : 0.0; }
    double proxyMarkout(int i) const { return pmoVol[i] > 0 ? pmoSum[i] / pmoVol[i] : 0.0; }
    double takerMarkout(int i) const { return tmoVol[i] > 0 ? tmoSum[i] / tmoVol[i] : 0.0; }
    double reasonMo(int r, int i) const { return rmoVol[r][i] > 0 ? rmoSum[r][i] / rmoVol[r][i] : 0.0; }
    double reasonUsd(int r, int i) const { return rmoSum[r][i] / 1e4; }
    int reasonCnt(int r) const { return rCnt[r]; }

    // [v2.16 P0-4] fillAnchorSec — БИРЖЕВОЕ время сделки (deal-домен, СЕКУНДЫ),
    // явный параметр (не shared-поле: applyEvent обрабатывает сделки пачками,
    // поле бы «переживало» породившую его сделку). mo-окна отсчитываются
    // от него; вся остальная логика doFill (pendingUntil, lastSweepAt...)
    // остаётся на реальном now.
    void doFill(SideBook& sb, Order::Side side, double px, double qty, bool toxic,
                double now, double fillAnchorSec, const BookState& book,
                std::vector<std::string>& ev, int reason) {
        qty = std::min(qty, sb.cur.qty - sb.cur.filled);
        if (side == Order::SELL && !cfg.allowShort) {
            if (qty > base) qty = std::floor(base / qtyStep + 1e-9) * qtyStep;
        }
        if (qty <= 1e-15) return;
        applyTrade(side == Order::BUY ? qty : -qty, px);
        ++fills;
        if (side == Order::BUY) ++buyFills; else ++sellFills;
        if (toxic) { ++forcedFills; ++toxicFills; sb.lastSweepAt = now; }
        if (reason >= 0 && reason < FR_N) ++rCnt[reason];
        sb.cur.filled += qty;
        bool partial = sb.cur.filled < sb.cur.qty - 1e-15;
        if (partial) ++partialFills;
        double proxyPx = (side == Order::BUY) ? book.bestBid() : book.bestAsk();
        if (proxyPx <= 0) proxyPx = px;
        double takerPx = (side == Order::BUY) ? book.bestAsk() : book.bestBid();
        if (takerPx <= 0) takerPx = px;
        mo.push_back({fillAnchorSec, side == Order::BUY ? 1 : -1, px, proxyPx,
                      takerPx, qty, 0, reason});   // [v2.16 P0-4]
        char buf[256];
        std::snprintf(buf, sizeof buf,
                      "[FILL %s%s] #%d %.10g @ %.10g%s (pos %+.10g, realized %+.4f)",
                      side == Order::BUY ? "BUY " : "SELL",
                      toxic ? " SWEEP" : "      ",
                      sb.cur.id, qty, px, partial ? " PARTIAL" : "", base, realized);
        evAdd(ev, buf);
        if (!partial) {
            if (sb.hasNext) {
                sb.cur = sb.nxt;
                sb.hasNext = false;
                sb.st = QState::WAIT_PLACE;
                sb.curPlaceLatMs = orderLatMs * sampleLatMult();   // [v2.13]
                sb.pendingUntil = now + sb.curPlaceLatMs / 1000.0;
                sb.placeSentMs = nowMsSteady();
                sb.lastRequoteAt = now;
                sb.queueInited = false;
            } else {
                sb.cur = Order{};
                sb.st = QState::NONE;
            }
        }
    }

    void onPrint(SideBook& sb, Order::Side side, const MexcPush::Deal& d,
                 const BookState& book, double tick, double now,
                 std::vector<std::string>& ev) {
        if (sb.st == QState::NONE) return;
        double P = sb.cur.price, eps = tick * 0.49;
        double remaining = sb.cur.qty - sb.cur.filled;
        if (remaining <= 1e-15) return;
        bool isBuy = (side == Order::BUY);
        if (isBuy ? (d.side != 2) : (d.side != 1)) return;

        // [v2.13.1 ФИКС 16] гейт ЦЕЛИКОМ в deal-домене: d.timeMs сравнивается
        // с приходом ордера, пересчитанным через deal-офсет. Раньше
        // arriveExch считался через send-офсет (exchOffMs): при расхождении
        // часов шлюза и матчинга на S гейт уезжал на S и пропускал сделки,
        // случившиеся ДО ордера (лаг до -26319 мс в логе 16:48-17:06).
        bool haveExchTime = dealOffInit && d.timeMs > 0;
        // [v2.16 P0-4] якорь markout-окон: биржевое время сделки (deal-домен,
        // сек; exchOffDealMs маппит d.timeMs в steady). Нет метки — now.
        double fillAnchorSec = haveExchTime
            ? ((double)d.timeMs + exchOffDealMs) / 1000.0
            : now;
        if (haveExchTime) {
            double dealExchMs = (double)d.timeMs;
            // [v2.13] гейт по латентности КОНКРЕТНОГО ордера
            double arriveExch = sb.placeSentMs + sb.curPlaceLatMs - exchOffDealMs;
            if (sb.st == QState::WAIT_PLACE) {
                if (dealExchMs < arriveExch) return;
                // сделка прошла гейт => ордер уже активен на бирже
                sb.st = QState::LIVE;
                if (!sb.queueInited) {
                    sb.queueAhead = levelQty(book, sb.cur.side, sb.cur.price, tick);
                    sb.lastLevelQty = sb.queueAhead;
                    sb.tradedAtLevel = 0;
                    sb.queueInited = true;
                }
            } else if (sb.st == QState::WAIT_CANCEL) {
                double cancelArrive = sb.cancelSentMs + sb.curCancelLatMs - exchOffDealMs;
                if (dealExchMs > cancelArrive) return;
            } else {
                if (dealExchMs < arriveExch) return;
            }
        } else if (sb.st == QState::WAIT_PLACE) {
            return;
        }

        bool through = isBuy ? (d.price < P - eps) : (d.price > P + eps);
        // [F45] внутренний принт не пробивает наш уровень (консервативно)
        // [F47] внутренний принт (строго внутри видимого спреда) не даёт
        // НИКАКОГО филла: ни THROUGH/TOXIC, ни «в уровень» (цена == наш
        // ордер: внутри спреда мы одни, queueAhead = 0, и любой синтетический
        // принт ровно по нашей цене засчитывался FR_QUEUE). Очередь таким
        // принтом тоже не уменьшается. Счётчик — только реально срезанные
        // филлы (раньше считался каждый внутренний принт на каждой стороне).
        if (curPrintInside && cfg.printFilter >= 1) {
            bool beyond = isBuy ? (d.price > P + eps) : (d.price < P - eps);
            bool wouldFill = through || (!beyond && sb.queueAhead - d.qty <= 1e-15);
            // [F49] принт, касающийся нашего уровня, с вероятностью p
            // обрабатывается как при =0 (очередь + филл); p=0 -> как v3.1.9
            bool pass = cfg.printFillP > 0 && !beyond &&
                        (cfg.printFillP >= 1 || prUrand() < cfg.printFillP);
            if (!pass) {
                if (wouldFill) ++prCutFills;
                return;
            }
            if (wouldFill) ++prPassFills;
        }
        if (through) {
            // Полный филл на toxic — корректный контрфакт price-time priority
            bool toxic = isBuy ? (d.price < book.bestBid() - eps)
                               : (d.price > book.bestAsk() + eps);
            // [v2.14 ФИКС 19] CUTOFF: глубина пробития НАШЕГО уровня —
            // не зависит от порядка событий (книга к этому моменту уже
            // обновлена принтом, из-за чего toxic-флаг почти не срабатывал:
            // sw0 всю сессию, включая нож -2% в 22:15:56). Филл остаётся —
            // мы реально стоим в книге; пауза запрещает перекотироваться
            // в ту же сторону до истечения MM_CUTOFF_SEC.
            if (cfg.cutoffBps > 0) {
                double depthBps = (P > 0) ? std::fabs(P - d.price) / P * 1e4 : 0.0;
                if (depthBps > cfg.cutoffBps || toxic) {
                    ++cutCnt;
                    double until = now + cfg.cutoffSec;
                    if (isBuy) cutPauseBuyUntil = until;
                    else       cutPauseSellUntil = until;
                    char cb[192];
                    std::snprintf(cb, sizeof cb,
                                  "[CUTOFF  ] %s: пробитие уровня на %.0f бп "
                                  "(порог %.0f) -> пауза %.0f s",
                                  isBuy ? "BUY " : "SELL", depthBps,
                                  cfg.cutoffBps, cfg.cutoffSec);
                    evAdd(ev, cb);
                }
            }
            double fq = toxic ? roundQty(remaining)
                              : roundQty(std::min(remaining, d.qty));
            if (fq > 1e-15)
                doFill(sb, side, P, fq, toxic, now, fillAnchorSec, book, ev,
                       toxic ? FR_TOXIC : FR_THROUGH);   // [v2.16 P0-4]
            return;
        }
        if (isBuy ? (d.price > P + eps) : (d.price < P - eps)) return;
        sb.tradedAtLevel += d.qty;
        sb.queueAhead -= d.qty;
        if (sb.queueAhead <= 1e-15) {
            double reached = -sb.queueAhead;
            sb.queueAhead = 0;
            double fq = roundQty(std::min(remaining, reached));
            if (fq > 1e-15)
                doFill(sb, side, P, fq, false, now, fillAnchorSec, book, ev,
                       FR_QUEUE);   // [v2.16 P0-4]
        }
    }
    void onGhost(Ghost& g, const MexcPush::Deal& d, const BookState& book,
                 double now, std::vector<std::string>& ev) {
        (void)now;   // [fix F40]
        if (!g.active) return;
        // [v2.13.1] сравнение d.timeMs с окном живости — оба в deal-домене
        if (!(dealOffInit && d.timeMs > 0)) { g.active = false; return; }
        double dealExchMs = (double)d.timeMs;
        if (dealExchMs > g.untilExchMs) { g.active = false; return; }
        // [v2.16 P0-3] нижняя граница: сделка старше активации ордера
        // физически не могла нас заполнить
        if (dealExchMs < g.startExchMs) return;
        bool isBuy = (g.side == (int)Order::BUY);
        if (isBuy ? (d.side != 2) : (d.side != 1)) return;
        bool through = isBuy ? (d.price < g.price) : (d.price > g.price);
        if (!through) return;
        if (curPrintInside && cfg.printFilter >= 1) {   // [F45]
            bool pass = cfg.printFillP > 0 &&
                        (cfg.printFillP >= 1 || prUrand() < cfg.printFillP);   // [F49]
            if (!pass) { ++prCutFills; return; }
            ++prPassFills;
        }
        double fq = roundQty(std::min(g.qty, d.qty));
        if (fq <= 1e-15) return;
        applyTrade(isBuy ? fq : -fq, g.price);
        g.qty -= fq;
        if (g.qty <= 1e-15) g.active = false;
        ++fills;
        if (isBuy) ++buyFills; else ++sellFills;
        ++forcedFills; ++lateFills;
        ++rCnt[FR_GHOST];
        double proxyPx = isBuy ? book.bestBid() : book.bestAsk();
        if (proxyPx <= 0) proxyPx = g.price;
        double takerPx = isBuy ? book.bestAsk() : book.bestBid();
        if (takerPx <= 0) takerPx = g.price;
        double fillAnchorSec = ((double)d.timeMs + exchOffDealMs) / 1000.0;  // [P0-4]
        mo.push_back({fillAnchorSec, isBuy ? 1 : -1, g.price, proxyPx,
                      takerPx, fq, 0, FR_GHOST});
        char buf[256];
        std::snprintf(buf, sizeof buf,
                      "[FILL %s LATE  ] ghost %.10g @ %.10g (pos %+.10g, realized %+.4f)",
                      isBuy ? "BUY " : "SELL", fq, g.price, base, realized);
        evAdd(ev, buf);
    }
};

// ============================================================================
// [v3.0] LIVE-СЛОЙ: RestClient (HMAC), LiveLedger (реальный учёт),
// LiveExecutor (машина состояний ордеров + реконсиляция).
// Всё это НЕ трогает paper-движок: он остаётся shadow-предсказателем.
// ============================================================================

// --------------------------------------------------------------- RestClient
struct RestResult {
    bool ok = false;          // HTTP 200 и нет {"code":...} ошибки
    bool transportOk = false; // false = ответа нет, состояние НЕИЗВЕСТНО
    int httpCode = 0;
    long errCode = 0;         // из {"code":X} при ошибке
    std::string errMsg;
    std::string body;
    double rttMs = 0;
};

// [fix F1] сборка подписанной query-строки. Раньше — char tb[64] + snprintf:
// POST/DELETE (>63 символов) обрезались, timestamp/recvWindow терялись, биржа
// отвечала 700002. Здесь длина не ограничена.
static std::string buildSignedQs(const std::string& params, long long tsMs,
                                 long recvWindowMs, const std::string& secret) {
    std::string qs = params;
    if (!qs.empty()) qs += "&";
    qs += "timestamp=" + std::to_string(tsMs) +
          "&recvWindow=" + std::to_string(recvWindowMs);
    qs += "&signature=" + hmacSha256Hex(secret, qs);
    return qs;
}

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

    RestClient(const std::string& host, bool noVerify) : http(host, noVerify) {}

    long long srvNowMs() const { return epochMs() + timeOffMs; }

    bool timeSync() {
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

    // подписанный запрос: params — строка "a=1&b=2" БЕЗ timestamp/signature
    RestResult signedReq(const std::string& method, const std::string& path,
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

    // запрос с только apikey (userDataStream — подпись не нужна по докам;
    // при отказе автоматически пробуем подписанный вариант)
    RestResult keyOnlyReq(const std::string& method, const std::string& path,
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

    // ----------------------------- торговые операции
    // place: ok + orderId; transportFail => состояние неизвестно (resolve!)
    std::string stpMode;   // [fix F35] MM_STP_MODE; пусто = не отправлять
    std::string stpParam() const {
        return stpMode.empty() ? std::string() : "&stpMode=" + stpMode;
    }
    RestResult placeLimit(const std::string& side, double px, double qty,
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
    RestResult marketOrder(const std::string& side, double qty, double quoteQty,
                           const std::string& cid) {
        std::string p = "symbol=" + symbol + "&side=" + side + "&type=MARKET" +
                        "&newClientOrderId=" + cid;
        if (side == "BUY") p += "&quoteOrderQty=" + fmtDec(quoteQty);
        else               p += "&quantity=" + fmtDec(qty);
        p += stpParam();   // [fix F35]
        return signedReq("POST", "/api/v3/order", p);
    }
    RestResult cancelOrder(const std::string& oid, const std::string& cid) {
        std::string p = "symbol=" + symbol;
        if (!oid.empty()) p += "&orderId=" + oid;
        else              p += "&origClientOrderId=" + cid;
        return signedReq("DELETE", "/api/v3/order", p);
    }
    RestResult getOrder(const std::string& oid, const std::string& cid) {
        std::string p = "symbol=" + symbol;
        if (!oid.empty()) p += "&orderId=" + oid;
        else              p += "&origClientOrderId=" + cid;
        return signedReq("GET", "/api/v3/order", p);
    }
    RestResult openOrders() {
        return signedReq("GET", "/api/v3/openOrders", "symbol=" + symbol);
    }
    RestResult account() { return signedReq("GET", "/api/v3/account", ""); }
    // [F54] сделки ордера с комиссией (SPOT_ACCOUNT_READ, вес 10)
    RestResult myTrades(const std::string& oid) {
        return signedReq("GET", "/api/v3/myTrades", "symbol=" + symbol + "&orderId=" + oid);
    }
    // [F51] по докам MEXC userDataStream — SIGNED; key-only — запасной вариант
    RestResult listenKeyCreate() {
        RestResult r = signedReq("POST", "/api/v3/userDataStream", "");
        if (r.ok) return r;
        RestResult k = keyOnlyReq("POST", "/api/v3/userDataStream", "");
        if (k.ok) return k;
        lprintf("[live] listenKey: signed code %ld %s | key-only code %ld %s\n",
                r.errCode, r.errMsg.c_str(), k.errCode, k.errMsg.c_str());
        return r;
    }
    RestResult listenKeyKeepalive(const std::string& lk) {
        RestResult r = signedReq("PUT", "/api/v3/userDataStream", "listenKey=" + lk);
        if (r.ok) return r;
        return keyOnlyReq("PUT", "/api/v3/userDataStream", "listenKey=" + lk);
    }
    RestResult listenKeyClose(const std::string& lk) {
        RestResult r = signedReq("DELETE", "/api/v3/userDataStream", "listenKey=" + lk);
        if (r.ok) return r;
        return keyOnlyReq("DELETE", "/api/v3/userDataStream", "listenKey=" + lk);
    }
};

// ---------------------------------------------------------------- LiveLedger
// [v3.0 L4] РЕАЛЬНЫЙ учёт сессии (internal — по приватным филлам от нуля,
// exchange — free/locked с биржи). Гейты стратегии читают ОТСЮДА, а не из
// shadow. Сессионный P&L: quoteNet + baseInt*mid (идентичен paper-RAMP по
// построению: quoteNet=0/baseInt=0 на старте).
struct LiveLedger {
    // exchange truth (поллинг + private.account)
    double baseFree = 0, baseLocked = 0, quoteFree = 0, quoteLocked = 0;
    bool acctInit = false;
    // internal session (с нуля, по филлам)
    double baseInt = 0, quoteNet = 0;
    double realized = 0, avgEntry = 0;
    double feesQuote = 0, feesBase = 0;      // в валюте сбора
    double feesOther = 0; std::string feesOtherCur;
    int fills = 0, buyFills = 0, sellFills = 0;
    int ghostFillsWs = 0, ghostFillsRecon = 0;
    double volume = 0;
    double base0 = 0;                        // стартовый инвентарь кошелька
    double driftMaxBase = 0;                 // max |internal - exchange|
    // реальные маркауты (те же горизонты, что у shadow)
    struct LFill { double t; int sgn; double px, qty; int done; int esc; };
    std::deque<LFill> mo;
    double moSum[3] = {0, 0, 0}, moVol[3] = {0, 0, 0};
    double moSumE[3] = {0, 0, 0}, moVolE[3] = {0, 0, 0};   // escape-филлы
    static constexpr double MO_H[3] = {5.0, 30.0, 60.0};

    double totalBase() const { return baseFree + baseLocked; }
    double totalQuote() const { return quoteFree + quoteLocked; }
    double sessionPnl(double mid) const { return quoteNet + baseInt * mid; }
    double posValue(double mid) const { return baseInt * mid; }  // как paper: от base0

    // [fix F4] комиссия отдельно: нужна, когда сама сделка уже проведена
    // дельтой cumQty (кредит), а fee приходит только с deal
    void applyFee(double fee, const std::string& feeCur,
                  const std::string& baseAsset, const std::string& quoteAsset) {
        if (!(fee > 0)) return;
        if (feeCur == quoteAsset) { quoteNet -= fee; feesQuote += fee; }
        else if (feeCur == baseAsset) { baseInt -= fee; feesBase += fee; }
        else { feesOther += fee; feesOtherCur = feeCur; }
    }

    void applyRealFill(bool isBuy, double px, double qty, double fee,
                       const std::string& feeCur, const std::string& baseAsset,
                       const std::string& quoteAsset, double anchorSec,
                       int isEscape) {
        double notional = qty * px;
        if (isBuy) { quoteNet -= notional; baseInt += qty; }
        else       { quoteNet += notional; baseInt -= qty; }
        applyFee(fee, feeCur, baseAsset, quoteAsset);
        volume += notional;
        ++fills;
        if (isBuy) ++buyFills; else ++sellFills;
        // avg-cost realized (та же семантика, что PaperTrader.applyTrade)
        const double EPS = 1e-12;
        double signedQty = isBuy ? qty : -qty;
        double pos = baseInt - signedQty;    // позиция ДО филла (с учётом fee-в-base нетто — приближение)
        bool wasLong = pos > EPS, wasShort = pos < -EPS;
        if (!wasLong && !wasShort) {
            avgEntry = px;
        } else if (wasLong) {
            if (signedQty < 0) {
                double close = std::min(-signedQty, pos);
                realized += close * (px - avgEntry);
                if (baseInt < -EPS) avgEntry = px;
                else if (std::fabs(baseInt) <= EPS) avgEntry = 0;
            } else {
                avgEntry = (avgEntry * pos + px * signedQty) / baseInt;
            }
        } else {
            if (signedQty > 0) {
                double close = std::min(signedQty, -pos);
                realized += close * (avgEntry - px);
                if (baseInt > EPS) avgEntry = px;
                else if (std::fabs(baseInt) <= EPS) avgEntry = 0;
            } else {
                avgEntry = (avgEntry * (-pos) + px * (-signedQty)) / (-baseInt);
            }
        }
        mo.push_back({anchorSec, isBuy ? 1 : -1, px, qty, 0, isEscape});
    }

    void updateMarkouts(double now, double mid) {
        if (mid <= 0) return;
        for (auto& f : mo)
            while (f.done < 3 && now - f.t >= MO_H[f.done]) {
                double bps = f.sgn * (mid - f.px) / f.px * 1e4;
                if (f.esc) { moSumE[f.done] += bps * f.qty * f.px;
                             moVolE[f.done] += f.qty * f.px; }
                else       { moSum[f.done] += bps * f.qty * f.px;
                             moVol[f.done] += f.qty * f.px; }
                ++f.done;
            }
        while (!mo.empty() && mo.front().done >= 3) mo.pop_front();
    }
    double markout(int i) const { return moVol[i] > 0 ? moSum[i] / moVol[i] : 0.0; }
    double markoutE(int i) const { return moVolE[i] > 0 ? moSumE[i] / moVolE[i] : 0.0; }
};
constexpr double LiveLedger::MO_H[3];

// -------------------------------------------------------------- LiveExecutor
// [v3.0 L3] машина состояний одного реального ордера на сторону.
// ensure() приводит биржевое состояние к ЖЕЛАЕМОМУ (shadow-решение).
// REST синхронный (~RTT десятки мс) — вопрос «ордер в полёте» решается
// через resolve-after-transport-fail, а не через async.
struct LiveSide {
    bool open = false;             // по нашим данным ордер стоит на бирже
    std::string cid, oid;
    double px = 0, qty = 0;        // параметры ВЫСТАВЛЕННОГО ордера
    double dealtQty = 0;           // [F4] qty, УЖЕ проведённая в ledger (любой источник)
    double dealtAmt = 0;
    double openedAt = 0;
    double dealSeenQty = 0;        // [F4] сумма qty, подтверждённая приватными deals
    int fails = 0;
    double nextRetryAt = 0;
    // статистика
    int places = 0, cancels = 0, placeFails = 0, cancelFails = 0;
    double placeLatSum = 0, placeLatMax = 0, cancelLatSum = 0, cancelLatMax = 0;
};

struct LiveCfg {
    bool live = false;
    bool confirm = false;
    bool tlsNoVerify = false;
    bool cleanStart = true;
    int cleanMode = 1;             // [fix F39] 1 = только ордера бота, 2 = все
    bool keepOnExit = false;
    bool reconAdopt = false;
    bool allowReplayLive = false;
    bool sellWallet = false;       // [fix F6] MM_SELL_WALLET_BASE: sell из ВСЕГО кошелька (v3.1)
    bool realSync = true;          // [F50] MM_LIVE_SYNC: реальные филлы -> shadow (cooldown/sync)
    bool selfExcl = true;          // [F52] MM_SELF_EXCL: стратегия видит книгу без наших ордеров
    double selfExclSec = 2.0;      // [F52] MM_SELF_EXCL_SEC: сколько помнить снятый ордер
    int maxFillsMin = 6;           // [F53] MM_LIVE_MAX_FILLS_MIN: breaker (0 = выкл)
    bool feeLookup = true;         // [F54] MM_FEE_LOOKUP: комиссии через myTrades
    double breakerSec = 600;       // [F53] MM_LIVE_BREAKER_SEC: пауза пары
    double reconSec = 15, balanceSec = 30;
    double usdtReserve = 1.0;      // не трогаем последний USDT кошелька
    double startUsdt = 10000;      // локальный лимит задействования quote
    std::string apiHost = "api.mexc.com";
    std::string killFile;
};

// [fix F6] общий счётчик cid на процесс (раньше у каждой пары свой с 1)
static long g_cidSeq = 1;

struct LiveExecutor {
    RestClient rc;
    LiveCfg* lcfg = nullptr;
    LiveSide s[2];                 // 0=BUY, 1=SELL
    std::string baseAsset = "BASE", quoteAsset = "USDT";
    LiveLedger ledger;
    TokenBucket ownRate;
    TokenBucket* liveRate = &ownRate;   // [v3.1] в main — общий бюджет аккаунта
    std::set<std::string> escCids;          // cid маркет-ордеров escape
    struct EscPend { std::string cid, oid; bool isBuy = false; double at = 0; };
    std::vector<EscPend> escPending;        // [fix F23] ждут сверки getOrder
    std::set<std::string> knownForeignOids; // чужие ордера (залогированы)
    std::set<std::string> ownOids;          // [fix F27] oid ордеров ЭТОЙ сессии
    std::deque<std::string> ownOidsOrder;
    int foreignDeals = 0;
    void addOwnOid(const std::string& oid) {
        if (oid.empty() || !ownOids.insert(oid).second) return;
        ownOidsOrder.push_back(oid);
        if (ownOidsOrder.size() > 4096) { ownOids.erase(ownOidsOrder.front()); ownOidsOrder.pop_front(); }
    }
    // [fix F4] КРЕДИТ-УЧЁТ филлов. dealtQty — qty, уже проведённая в ledger из
    // ЛЮБОГО источника (deals, дельта cumQty из order-апдейта, resolve);
    // dealSeenQty — сумма qty, подтверждённая приватными deals. Разница —
    // «кредит»: в ledger уже есть, сама сделка ещё не пришла. Пришедшая сделка
    // гасит кредит (проводится только её fee), всё сверх кредита — новый филл.
    // Не зависит от порядка апдейт/сделка и от того, закрыт ордер или нет.
    struct ClosedOrd { std::string cid, oid; double credit = 0, at = 0; };
    std::vector<ClosedOrd> closedOrds;
    // [fix F9] дедуп приватных сделок: повторная доставка одной сделки
    // (tradeId парсился, но нигде не использовался) = двойной учёт позиции
    std::set<std::string> seenDeals;
    // [fix F21] qty, проведённая по WS-сделкам ордеров без стороны (сироты)
    std::map<std::string, double> unknownDealt;
    std::deque<std::string> seenDealsOrder;
    int dupDeals = 0;

    void archiveClosed(const LiveSide& ls, double now) {
        double credit = ls.dealtQty - ls.dealSeenQty;
        if (credit > 1e-12 && (!ls.cid.empty() || !ls.oid.empty())) {
            closedOrds.push_back({ls.cid, ls.oid, credit, now});
            if (closedOrds.size() > 64) closedOrds.erase(closedOrds.begin());
        }
    }
    void pruneClosed(double now) {
        closedOrds.erase(
            std::remove_if(closedOrds.begin(), closedOrds.end(),
                           [&](const ClosedOrd& c) {
                               return c.credit <= 1e-12 || now - c.at > 60.0;
                           }),
            closedOrds.end());
    }
    ClosedOrd* findClosed(const std::string& cid, const std::string& oid) {
        for (auto& c : closedOrds)
            if ((!cid.empty() && c.cid == cid) || (!oid.empty() && c.oid == oid))
                return &c;
        return nullptr;
    }
    // [F50] события реальных филлов по сторонам для main-loop (sweep-cooldown,
    // cutoff и REAL_SYNC shadow-ордера). realFillEv растёт при любой новой
    // реальной qty по стороне; realFull — ордер стороны исполнен целиком.
    long realFillEv[2] = {0, 0};
    long realFillSeen[2] = {0, 0};
    double realFillPx[2] = {0, 0};
    double realFillAt[2] = {0, 0};
    bool realFull[2] = {false, false};
    int mirId[2] = {0, 0};         // id shadow-ордера, который зеркалит реальный
    int fullMirId[2] = {0, 0};     // id shadow-ордера в момент полного реального филла
    std::string mirCid[2];
    void noteDealt(const LiveSide& ls, bool wasOpen) {
        int idx = (int)(&ls - s);
        if (idx < 0 || idx > 1) return;
        ++realFillEv[idx];
        realFillPx[idx] = ls.px;
        realFillAt[idx] = nowSecs();
        if (wasOpen && ls.qty > 0 &&
            ls.dealtQty >= ls.qty - std::max(1e-12, ls.qty * 1e-9)) {
            realFull[idx] = true;
            fullMirId[idx] = mirId[idx];
        }
    }
    // [F54] комиссии филлов, пришедших БЕЗ приватной сделки (cumQty/REST):
    // через ~2 c после филла GET /api/v3/myTrades?orderId= -> commission.
    // Дедуп по tradeId с WS-сделками: комиссия каждой сделки — ровно один раз.
    bool feeLookup = true;
    std::set<std::string> feeTid;
    std::deque<std::string> feeTidOrder;
    struct FeeQ { std::string oid; double at; int tries; };
    std::vector<FeeQ> feeQ;
    int feeLookups = 0, feeTrades = 0, feeTaker = 0, feeFail = 0;
    bool markFeeTid(const std::string& tid) {   // true = комиссия ещё не учтена
        if (tid.empty()) return true;
        if (!feeTid.insert(tid).second) return false;
        feeTidOrder.push_back(tid);
        if (feeTidOrder.size() > 4096) { feeTid.erase(feeTidOrder.front()); feeTidOrder.pop_front(); }
        return true;
    }
    void queueFee(const std::string& oid, double at) {
        if (!feeLookup || oid.empty()) return;
        for (auto& f : feeQ) if (f.oid == oid) { f.at = std::max(f.at, at); return; }
        if (feeQ.size() < 256) feeQ.push_back({oid, at, 0});
    }
    void pollFees(double now, std::vector<std::string>& ev) {
        for (size_t i = 0; i < feeQ.size(); ++i) {
            if (now < feeQ[i].at) continue;
            FeeQ f = feeQ[i];
            if (!liveRate->take()) return;          // бюджет REST — позже
            feeQ.erase(feeQ.begin() + (long)i);
            RestResult r = rc.myTrades(f.oid);
            ++feeLookups;
            if (!r.ok) {
                ++feeFail;
                ++f.tries;
                if (f.tries < 3) { f.at = now + 5.0; feeQ.push_back(f); }
                char b[200];
                std::snprintf(b, sizeof b, "[LIVE FEE] myTrades %s: code %ld %s%s",
                              f.oid.c_str(), r.errCode, r.errMsg.c_str(),
                              f.tries < 3 ? " (повтор)" : " (сдаюсь)");
                evAdd(ev, b);
                return;
            }
            for (auto& ob : jsonSplitObjects(r.body)) {
                std::string tid, cur;
                if (!jsonFindStr(ob, "id", tid) || tid.empty()) continue;
                if (!markFeeTid(tid)) continue;     // уже учтена (WS-сделка/прошлый запрос)
                double fee = 0, qty = 0, px = 0;
                jsonFindNum(ob, "commission", fee);
                jsonFindStr(ob, "commissionAsset", cur);
                jsonFindNum(ob, "qty", qty);
                jsonFindNum(ob, "price", px);
                bool maker = ob.find("\"isMaker\":true") != std::string::npos ||
                             ob.find("\"isMaker\": true") != std::string::npos;
                if (!maker) ++feeTaker;
                ledger.applyFee(fee, cur, baseAsset, quoteAsset);
                ++feeTrades;
                char b[240];
                std::snprintf(b, sizeof b,
                              "[LIVE FEE] сделка %.24s: %.10g @ %.10g, комиссия %.10g %s, %s",
                              tid.c_str(), qty, px, fee, cur.c_str(),
                              maker ? "maker" : "TAKER");
                evAdd(ev, b);
            }
            return;                                  // не больше 1 запроса за вызов
        }
    }
    int reconEvents = 0, orphanCancels = 0, resolves = 0;
    int wsPrivOrders = 0, wsPrivDeals = 0, wsPrivAcct = 0;
    double lastRecon = 0, lastBalance = 0;
    bool initDone = false;

    LiveExecutor(const std::string& host, bool noVerify) : rc(host, noVerify) { ownRate.init(10); }

    std::string newCid() {
        std::string c = "sp" + base36((long long)getpid()) + "x" + base36(g_cidSeq++);
        if (c.size() > 30) c.resize(30);
        return c;
    }

    // [fix F38/F39] cid бота ЛЮБОГО запуска: sp<base36 pid>x<base36 seq>
    static bool botCid(const std::string& c) {
        if (c.size() < 5 || c.compare(0, 2, "sp") != 0) return false;
        size_t x = c.find('x', 3);
        if (x == std::string::npos || x + 1 >= c.size()) return false;
        for (size_t i = 2; i < c.size(); ++i) {
            char ch = c[i];
            if (!((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'z'))) return false;
        }
        return true;
    }
    bool ourCid(const std::string& c) const {
        std::string pfx = "sp" + base36((long long)getpid()) + "x";
        return c.rfind(pfx, 0) == 0;
    }

    // ------------------------- приватные WS-события
    // сделка: истина по филлам. Ищет сторону по cid/oid; не нашла — всё
    // равно применяем (ордер мог быть снят до этого — GHOST-класс).
    void onPrivDeal(const MexcPush::PrivDeal& d, double anchorSec,
                    std::vector<std::string>& ev) {
        ++wsPrivDeals;
        if (!(d.qty > 0)) return;
        if (!d.tradeId.empty()) {   // [fix F9]
            std::string key = d.tradeId + "|" + d.clientOrderId;
            if (!seenDeals.insert(key).second) { ++dupDeals; return; }
            seenDealsOrder.push_back(key);
            if (seenDealsOrder.size() > 4096) {
                seenDeals.erase(seenDealsOrder.front());
                seenDealsOrder.pop_front();
            }
        }
        const double EPS = 1e-12;
        bool isBuy = (d.tradeType == 1);
        pruneClosed(nowSecs());
        int idx = -1;
        for (int i = 0; i < 2; ++i)
            if ((!d.clientOrderId.empty() && s[i].cid == d.clientOrderId) ||
                (!d.orderId.empty() && s[i].oid == d.orderId)) { idx = i; break; }
        ClosedOrd* co = (idx < 0) ? findClosed(d.clientOrderId, d.orderId) : nullptr;
        // [fix F27] сделка не нашей сессии (ручная торговля, другой бот, ордер
        // прошлого запуска) -> в ledger не проводим: искажала бы позицию/maxLoss
        if (idx < 0 && !co && !ourCid(d.clientOrderId) && !escCids.count(d.clientOrderId) &&
            !(!d.orderId.empty() && ownOids.count(d.orderId))) {
            if (++foreignDeals <= 20 || foreignDeals % 100 == 0) {
                char fb[224];
                std::snprintf(fb, sizeof fb,
                              "[LIVE DEAL] чужая сделка (cid %.24s oid %.20s) %s %.10g @ %.10g "
                              "— не учитываю (#%d)", d.clientOrderId.c_str(),
                              d.orderId.c_str(), d.tradeType == 1 ? "BUY" : "SELL",
                              d.qty, d.price, foreignDeals);
                evAdd(ev, fb);
            }
            return;
        }

        // кредит: уже проведено дельтой cumQty/resolve, сама сделка ещё не пришла
        double credit = 0;
        if (idx >= 0) credit = std::max(0.0, s[idx].dealtQty - s[idx].dealSeenQty);
        else if (co)  credit = co->credit;

        double covered = std::min(d.qty, credit);   // уже в ledger -> только fee
        double fresh = d.qty - covered;             // реально новый филл
        double frac = fresh / d.qty;
        // сделка по снятой/неизвестной стороне, не покрытая кредитом = GHOST
        bool ghost = ((idx < 0) || !s[idx].open) && fresh > EPS;
        if (ghost) ++ledger.ghostFillsWs;

        int isEsc = escCids.count(d.clientOrderId) ? 1 : 0;
        // [F54] комиссия этой сделки уже взята из myTrades -> не дублировать
        double dFee = markFeeTid(d.tradeId) ? d.feeAmount : 0.0;
        if (fresh > EPS) {
            ledger.applyRealFill(isBuy, d.price, fresh, dFee * frac,
                                 d.feeCurrency, baseAsset, quoteAsset, anchorSec,
                                 isEsc);
            if (idx >= 0)
                s[idx].dealtAmt += (d.amount > 0 ? d.amount : d.qty * d.price) * frac;
        }
        if (covered > EPS)
            ledger.applyFee(dFee * (covered / d.qty), d.feeCurrency,
                            baseAsset, quoteAsset);
        if (idx >= 0) {
            s[idx].dealSeenQty += d.qty;
            s[idx].dealtQty += fresh;
            if (fresh > EPS) noteDealt(s[idx], s[idx].open);   // [F50]
        } else if (co) {
            co->credit = std::max(0.0, co->credit - covered);
        } else if (fresh > EPS) {   // [fix F21] сирота: запомнить проведённое
            const std::string& k = !d.clientOrderId.empty() ? d.clientOrderId : d.orderId;
            if (!k.empty()) {
                unknownDealt[k] += fresh;
                if (unknownDealt.size() > 512) unknownDealt.erase(unknownDealt.begin());
            }
        }

        char buf[320];
        if (fresh > EPS) {
            std::snprintf(buf, sizeof buf,
                          "[LIVE FILL %s%s] %.10g @ %.10g (fee %.10g %s, maker %d%s%s) "
                          "pos %+.10g, quoteNet %+.4f, realized %+.4f",
                          isBuy ? "BUY " : "SELL", ghost ? " GHOST" : "     ",
                          fresh, d.price, d.feeAmount * frac, d.feeCurrency.c_str(),
                          (int)d.isMaker, isEsc ? " ESC" : "",
                          covered > EPS ? " +cov" : "",
                          ledger.baseInt, ledger.quoteNet, ledger.realized);
        } else {
            std::snprintf(buf, sizeof buf,
                          "[LIVE DEAL ok] %s %.10g @ %.10g уже учтена по cumQty "
                          "(fee %.10g %s проведена)",
                          isBuy ? "BUY " : "SELL", d.qty, d.price, d.feeAmount,
                          d.feeCurrency.c_str());
        }
        evAdd(ev, buf);
    }

    // ордер-апдейт: терминальные статусы закрывают сторону; расхождение
    // cumQty vs dealtQty => пропущенная сделка => дельта-филл (GHOST-WS)
    void onPrivOrder(const MexcPush::PrivOrder& o, double now,
                     std::vector<std::string>& ev) {
        ++wsPrivOrders;
        int idx = -1;
        for (int i = 0; i < 2; ++i)
            if ((!o.clientId.empty() && s[i].cid == o.clientId) ||
                (!o.id.empty() && s[i].oid == o.id)) { idx = i; break; }
        if (idx < 0) return;            // чужой/старый — recon разберётся
        LiveSide& ls = s[idx];
        if (!o.id.empty() && ls.oid.empty()) ls.oid = o.id;
        bool terminal = (o.status == 2 || o.status == 4 || o.status == 5);
        if (o.cumQty > ls.dealtQty + 1e-12) {
            double dq = o.cumQty - ls.dealtQty;
            double dAmt = o.cumAmt - ls.dealtAmt;
            double px = dq > 0 ? dAmt / dq : (o.avgPrice > 0 ? o.avgPrice : o.price);
            if (px > 0 && dq > 0) {
                ++ledger.ghostFillsWs;
                ledger.applyRealFill(o.tradeType == 1, px, dq, 0, "",
                                     baseAsset, quoteAsset, now, 0);
                ls.dealtQty = o.cumQty;
                ls.dealtAmt = o.cumAmt;
                noteDealt(ls, ls.open);   // [F50]
                queueFee(ls.oid.empty() ? o.id : ls.oid, now + 2.0);   // [F54]
                char buf[224];
                std::snprintf(buf, sizeof buf,
                              "[LIVE GHOST-WS] %s дельта-филл %.10g @ ~%.10g "
                              "(cumQty биржи опередила deals-ленту)",
                              o.tradeType == 1 ? "BUY " : "SELL", dq, px);
                evAdd(ev, buf);
            }
        }
        if (terminal) {
            char buf[224];
            std::snprintf(buf, sizeof buf,
                          "[LIVE ORD  ] %s %s status %d (filled %.10g/%.10g)",
                          o.tradeType == 1 ? "BUY " : "SELL", ls.cid.c_str(),
                          o.status, o.cumQty, ls.qty);
            evAdd(ev, buf);
            // [fix F4] кредит (cumQty-дельта без подтверждающей сделки) уходит в
            // closedOrds: хвостовые deals гасят его, а не считаются заново.
            // Раньше хвост учитывался только для статусов FILLED, а
            // CANCELED/PARTIALLY_CANCELED с филлами считались дважды.
            archiveClosed(ls, now);
            ls.open = false;
            ls.cid.clear();
            ls.oid.clear();
            ls.dealtQty = 0;
            ls.dealtAmt = 0;
            ls.dealSeenQty = 0;
        }
    }

    void onPrivAccount(const MexcPush::PrivAccount& a) {
        ++wsPrivAcct;
        if (a.coin == baseAsset) {
            ledger.baseFree = a.balance;
            ledger.baseLocked = a.frozen;
        } else if (a.coin == quoteAsset) {
            ledger.quoteFree = a.balance;
            ledger.quoteLocked = a.frozen;
        }
    }

    // ------------------------- REST-действия (синхронные, с разруливанием)
    bool transportUnknown(LiveSide& ls, const char* what, double now,
                          std::vector<std::string>& ev) {
        // транспортный сбой: состояние запроса неизвестно -> resolve по cid
        ++ls.fails;
        ls.nextRetryAt = now + std::min(30.0, std::pow(2.0, ls.fails));
        char buf[224];
        std::snprintf(buf, sizeof buf,
                      "[LIVE REST ] %s: транспортный сбой (%s) -> resolve по cid, "
                      "retry через %.0f с", ls.cid.c_str(), what, ls.nextRetryAt - now);
        evAdd(ev, buf);
        return false;
    }

    static bool orderNotFound(const RestResult& r) {
        if (r.errCode == -2011 || r.errCode == -2013) return true;
        const std::string& m = r.errMsg;
        return m.find("Unknown order") != std::string::npos ||
               m.find("does not exist") != std::string::npos ||
               m.find("not exist") != std::string::npos;
    }

    void resolve(LiveSide& ls, bool isBuySide, double now,
                 std::vector<std::string>& ev, bool countFail = true) {
        ++resolves;
        RestResult r = rc.getOrder(ls.oid, ls.cid);
        if (!r.transportOk) {
            // [fix 3.1-F4] при вызове после transportUnknown в place
            // fails уже инкрементирован — не удваиваем backoff
            if (countFail) transportUnknown(ls, "resolve", now, ev);
            else ls.nextRetryAt = now + std::min(30.0, std::pow(2.0, ls.fails));
            return;
        }
        if (!r.ok) {
            // [fix F14] закрытым считаем ТОЛЬКО явный «ордера нет» (-2011
            // Unknown order sent; -2013 Binance-совместимый). Rate-limit, 5xx,
            // 700003 и прочее = состояние неизвестно: ордер, возможно, жив ->
            // open=true (cancel/cancelAll его попробуют снять), backoff.
            if (orderNotFound(r)) {
                ls.open = false;
                ls.fails = 0;
                char buf[192];
                std::snprintf(buf, sizeof buf,
                              "[LIVE RESOLVE] %s: ордер не найден (code %ld %.60s) -> закрыт",
                              ls.cid.c_str(), r.errCode, r.errMsg.c_str());
                evAdd(ev, buf);
                return;
            }
            if (r.errCode == 700003) rc.timeSync();
            if (countFail) ++ls.fails;
            ls.nextRetryAt = now + std::min(30.0, std::pow(2.0, std::max(1, ls.fails)));
            if (!ls.cid.empty() || !ls.oid.empty()) ls.open = true;
            char buf[224];
            std::snprintf(buf, sizeof buf,
                          "[LIVE RESOLVE] %s: ошибка запроса (http %d code %ld %.60s) "
                          "-> статус неизвестен, считаю живым, retry через %.0f с",
                          ls.cid.c_str(), r.httpCode, r.errCode, r.errMsg.c_str(),
                          ls.nextRetryAt - now);
            evAdd(ev, buf);
            return;
        }
        std::string status;
        jsonFindStr(r.body, "status", status);
        double execQty = 0, cumQuote = 0;
        jsonFindNum(r.body, "executedQty", execQty);
        jsonFindNum(r.body, "cummulativeQuoteQty", cumQuote);
        if (execQty > ls.dealtQty + 1e-12) {
            double dq = execQty - ls.dealtQty;
            double px = dq > 0 ? (cumQuote - ls.dealtAmt) / dq : 0;
            if (px <= 0) px = ls.px;
            ++ledger.ghostFillsRecon;
            ledger.applyRealFill(isBuySide, px, dq, 0, "", baseAsset, quoteAsset,
                                 now, 0);
            ls.dealtQty = execQty;
            ls.dealtAmt = cumQuote;
            noteDealt(ls, ls.open);   // [F50]
            queueFee(ls.oid, now + 2.0);   // [F54]
            char buf[224];
            std::snprintf(buf, sizeof buf,
                          "[LIVE GHOST-REST] %s дельта-филл %.10g @ ~%.10g "
                          "(resolve: executedQty опередил deals)",
                          isBuySide ? "BUY " : "SELL", dq, px);
            evAdd(ev, buf);
        }
        bool terminal = (status == "FILLED" || status == "CANCELED" ||
                         status == "PARTIALLY_CANCELED" || status == "EXPIRED" ||
                         status == "REJECTED");
        if (terminal) {
            ls.open = false;
        } else {
            ls.open = true;     // NEW/PARTIALLY_FILLED — жив
            if (ls.oid.empty()) {
                std::string oid;
                if (jsonFindStr(r.body, "orderId", oid)) ls.oid = oid;
            }
            addOwnOid(ls.oid);   // [fix F27]
        }
        ls.fails = 0;
        char buf[192];
        std::snprintf(buf, sizeof buf,
                      "[LIVE RESOLVE] %s -> %s (exec %.10g)",
                      ls.cid.c_str(), status.c_str(), execQty);
        evAdd(ev, buf);
    }

    void place(LiveSide& ls, bool isBuy, double px, double qty, double now,
               std::vector<std::string>& ev) {
        if (!liveRate->take(RK_NEW)) return;
        archiveClosed(ls, now);   // [fix F4] кредит прежнего ордера не теряем
        ls.cid = newCid();
        ls.oid.clear();
        ls.dealtQty = 0;
        ls.dealtAmt = 0;
        ls.dealSeenQty = 0;
        RestResult r = rc.placeLimit(isBuy ? "BUY" : "SELL", px, qty, ls.cid);
        // [fix F18] 700003 (часы уехали): ресинк и один немедленный повтор;
        // биржа запрос отклонила, тот же cid повторно использовать можно
        if (r.transportOk && !r.ok && r.errCode == 700003 && rc.timeSync())
            r = rc.placeLimit(isBuy ? "BUY" : "SELL", px, qty, ls.cid);
        // [fix F31] 5xx от шлюза: биржа могла ордер принять -> как транспортный сбой
        if (r.transportOk && !r.ok && r.httpCode >= 500) r.transportOk = false;
        if (!r.transportOk) { ls.px = px; ls.qty = qty; ls.openedAt = now;   // [F14]
                              transportUnknown(ls, "place", now, ev);
                              resolve(ls, isBuy, now, ev, false); return; }
        if (r.ok) {
            ls.open = true;
            ls.px = px;
            ls.qty = qty;
            ls.openedAt = now;
            ls.fails = 0;
            ++ls.places;
            ls.placeLatSum += r.rttMs;
            if (r.rttMs > ls.placeLatMax) ls.placeLatMax = r.rttMs;
            std::string oid;
            if (jsonFindStr(r.body, "orderId", oid)) ls.oid = oid;
            addOwnOid(ls.oid);   // [fix F27]
            char buf[256];
            std::snprintf(buf, sizeof buf,
                          "[LIVE PLACE %s] %s %.10g @ %.10g (oid %.16s, %.0f ms)",
                          isBuy ? "BUY " : "SELL", ls.cid.c_str(), qty, px,
                          ls.oid.c_str(), r.rttMs);
            evAdd(ev, buf);
            return;
        }
        ++ls.placeFails;
        ++ls.fails;
        // 700003 и после повтора -> ещё раз ресинк, дальше обычный backoff
        if (r.errCode == 700003) { rc.timeSync(); }
        ls.nextRetryAt = now + std::min(30.0, std::pow(2.0, ls.fails));
        char buf[320];
        std::snprintf(buf, sizeof buf,
                      "[LIVE PLACE-FAIL %s] %s code %ld %.120s (retry через %.0f с)",
                      isBuy ? "BUY " : "SELL", ls.cid.c_str(), r.errCode, r.errMsg.c_str(),
                      ls.nextRetryAt - now);
        evAdd(ev, buf);
    }

    void cancel(LiveSide& ls, bool isBuy, double now,
                std::vector<std::string>& ev) {
        if (!liveRate->take(RK_CANCEL)) return;
        if (ls.cid.empty() && ls.oid.empty()) { ls.open = false; return; }
        RestResult r = rc.cancelOrder(ls.oid, ls.cid);
        if (!r.transportOk) { transportUnknown(ls, "cancel", now, ev);
                              resolve(ls, isBuy, now, ev); return; }
        ++ls.cancels;
        ls.cancelLatSum += r.rttMs;
        if (r.rttMs > ls.cancelLatMax) ls.cancelLatMax = r.rttMs;
        if (r.ok) {
            ls.open = false;
            ls.fails = 0;
            char buf[224];
            std::snprintf(buf, sizeof buf, "[LIVE CANCEL %s] %s (%.0f ms)",
                          isBuy ? "BUY " : "SELL", ls.cid.c_str(), r.rttMs);
            evAdd(ev, buf);
            // [fix F20] ответ cancel несёт executedQty: частичный филл, не
            // пришедший по приватному WS, иначе терялся
            double execQty = 0, cumQuote = 0;
            if (jsonFindNum(r.body, "executedQty", execQty) &&
                execQty > ls.dealtQty + 1e-12) {
                jsonFindNum(r.body, "cummulativeQuoteQty", cumQuote);
                double dq = execQty - ls.dealtQty;
                double px = (cumQuote - ls.dealtAmt) / dq;
                if (!(px > 0)) px = ls.px;
                ++ledger.ghostFillsRecon;
                ledger.applyRealFill(isBuy, px, dq, 0, "", baseAsset, quoteAsset,
                                     now, 0);
                ls.dealtQty = execQty;
                if (cumQuote > 0) ls.dealtAmt = cumQuote;
                noteDealt(ls, ls.open);   // [F50]
                queueFee(ls.oid, now + 2.0);   // [F54]
                std::snprintf(buf, sizeof buf,
                              "[LIVE GHOST-REST] %s дельта-филл %.10g @ ~%.10g "
                              "(cancel: executedQty опередил deals)",
                              isBuy ? "BUY " : "SELL", dq, px);
                evAdd(ev, buf);
            }
            return;
        }
        ++ls.cancelFails;
        // неизвестный ордер при отмене = его уже нет (исполнился/снят) ->
        // resolve добирает executedQty (GHOST-REST при расхождении)
        resolve(ls, isBuy, now, ev, false);
        if (!ls.open) return;
        ++ls.fails;
        ls.nextRetryAt = now + std::min(30.0, std::pow(2.0, ls.fails));
    }

    // ------------------------- главная точка: привести сторону к желаемому
    void ensureSide(int idx, bool want, double px, double qty, double rqThresh,
                    double now, std::vector<std::string>& ev) {
        LiveSide& ls = s[idx];
        bool isBuy = (idx == 0);
        if (now < ls.nextRetryAt) return;
        if (!want) {
            if (ls.open) cancel(ls, isBuy, now, ev);
            return;
        }
        if (!ls.open) { place(ls, isBuy, px, qty, now, ev); return; }
        bool pxMoved = std::fabs(ls.px - px) >= rqThresh;
        // [fix F32] qty — желаемый ОСТАТОК; сравниваем с остатком живого ордера
        double liveRem = std::max(0.0, ls.qty - ls.dealtQty);
        double denom = std::max(liveRem, qty);
        bool qtyDrift = denom > 0 &&
                        std::fabs(liveRem - qty) / denom >= 0.50 &&
                        std::fabs(liveRem - qty) * px >= 1.0;   // ≥1 USDT разницы
        if (!pxMoved && !qtyDrift) return;
        // реквот: cancel; place произойдёт на следующем тике (open=false
        // после успешного cancel) — та же последовательность, что в paper
        cancel(ls, isBuy, now, ev);
    }

    void cancelAll(std::vector<std::string>& ev) {
        for (int i = 0; i < 2; ++i)
            if (s[i].open) {
                // [fix F36] ждём токен (лимит биржи 5/с), не обнуляем бюджет
                liveRate->waitToken(1.5, RK_CANCEL);
                cancel(s[i], i == 0, nowSecs(), ev);
            }
    }

    // escape: реальный MARKET-ордер на излишек позиции
    bool escapeMarket(bool isBuy, double qty, double quoteQty, double now,
                      std::vector<std::string>& ev) {
        // [fix F35] self-trade: встречный СОБСТВЕННЫЙ лимит снять до MARKET
        LiveSide& opp = s[isBuy ? 1 : 0];
        if (opp.open) {
            liveRate->waitToken(1.0, RK_CANCEL);
            cancel(opp, !isBuy, now, ev);
            if (opp.open) {
                evAdd(ev, std::string("[LIVE ESCAPE] ") + (isBuy ? "BUY " : "SELL") +
                          " отложен: встречный ордер бота не снят (self-trade)");
                return false;
            }
        }
        liveRate->waitToken(1.0, RK_NEW);   // [fix F36] escape тоже в лимите 5/с
        liveRate->take(RK_NEW);
        std::string cid = newCid();
        escCids.insert(cid);
        RestResult r = rc.marketOrder(isBuy ? "BUY" : "SELL", qty, quoteQty, cid);
        // [fix F34] 5xx: биржа могла исполнить -> статус неизвестен, сверить
        if (r.transportOk && !r.ok && r.httpCode >= 500) r.transportOk = false;
        if (!r.transportOk || r.ok) {   // [fix F23] сверим исполнение позже
            std::string eoid;
            if (r.ok) jsonFindStr(r.body, "orderId", eoid);
            addOwnOid(eoid);   // [fix F27]
            escPending.push_back({cid, eoid, isBuy, now});
            if (escPending.size() > 32) escPending.erase(escPending.begin());
        }
        if (!r.transportOk) {
            char buf[192];
            std::snprintf(buf, sizeof buf,
                          "[LIVE ESCAPE] %s: транспортный сбой — проверить "
                          "позицию вручную/через recon!", isBuy ? "BUY " : "SELL");
            evAdd(ev, buf);
            return false;
        }
        if (!r.ok) {
            char buf[288];
            std::snprintf(buf, sizeof buf,
                          "[LIVE ESCAPE-FAIL] %s code %ld %.120s",
                          isBuy ? "BUY " : "SELL", r.errCode, r.errMsg.c_str());
            evAdd(ev, buf);
            return false;
        }
        double execQty = 0, cumQuote = 0;
        jsonFindNum(r.body, "executedQty", execQty);
        jsonFindNum(r.body, "cummulativeQuoteQty", cumQuote);
        char buf[288];
        std::snprintf(buf, sizeof buf,
                      "[LIVE ESCAPE] %s market qty %.10g -> exec %.10g за %.10g USDT "
                      "(%.0f ms), филлы подтвердятся по deals-ленте",
                      isBuy ? "BUY " : "SELL", qty, execQty, cumQuote, r.rttMs);
        evAdd(ev, buf);
        return true;
    }

    // ------------------------- реконсиляция (паранойя вдвойне, по пакету)
    // [fix F23] сверка escape MARKET-ордеров: executedQty сверх уже
    // проведённого по WS -> ledger; остаток -> кредит против поздних сделок
    void reconcileEscapes(double now, std::vector<std::string>& ev) {
        for (size_t i = 0; i < escPending.size();) {
            EscPend& e = escPending[i];
            if (now - e.at < 1.0) { ++i; continue; }   // дать бирже исполнить
            RestResult g = rc.getOrder(e.oid, e.cid);
            if (!g.transportOk || (!g.ok && !orderNotFound(g))) { ++i; continue; }
            if (!g.ok) {   // не найден: не выставился (или ещё не виден)
                if (now - e.at > 30.0) escPending.erase(escPending.begin() + long(i));
                else ++i;
                continue;
            }
            std::string status;
            jsonFindStr(g.body, "status", status);
            bool terminal = (status == "FILLED" || status == "CANCELED" ||
                             status == "PARTIALLY_CANCELED" || status == "EXPIRED" ||
                             status == "REJECTED");
            if (!terminal && now - e.at < 60.0) { ++i; continue; }
            double execQty = 0, cumQuote = 0;
            jsonFindNum(g.body, "executedQty", execQty);
            jsonFindNum(g.body, "cummulativeQuoteQty", cumQuote);
            std::string oid = e.oid;
            if (oid.empty()) jsonFindStr(g.body, "orderId", oid);
            double seen = 0;
            auto it = unknownDealt.find(e.cid);
            if (it == unknownDealt.end() && !oid.empty()) it = unknownDealt.find(oid);
            if (it != unknownDealt.end()) seen = it->second;
            double dq = execQty - seen;
            if (dq > 1e-12) {
                double px = execQty > 0 ? cumQuote / execQty : 0;
                if (px > 0) {
                    ++ledger.ghostFillsRecon;
                    ledger.applyRealFill(e.isBuy, px, dq, 0, "", baseAsset, quoteAsset,
                                         now, 1);
                    queueFee(oid, now + 1.0);   // [F54] escape — тейкер, комиссия есть
                    closedOrds.push_back({e.cid, oid, dq, now});
                    if (closedOrds.size() > 64) closedOrds.erase(closedOrds.begin());
                    char buf[224];
                    std::snprintf(buf, sizeof buf,
                                  "[LIVE GHOST-REST] %s escape %.20s: дельта-филл "
                                  "%.10g @ ~%.10g (deals-лента не подтвердила)",
                                  e.isBuy ? "BUY " : "SELL", e.cid.c_str(), dq, px);
                    evAdd(ev, buf);
                }
            }
            escPending.erase(escPending.begin() + long(i));
        }
    }

    // [fix F38] усыновить ордер бота в свободный слот той же стороны.
    // thisSession: филлы сверх пришедших по WS (unknownDealt) — наши и
    // проводятся дельтой; ордер прошлого запуска: прежние филлы уже в
    // балансе, учитываем только будущие.
    bool adoptOrder(const std::string& ob, const std::string& oid,
                    const std::string& cid, bool thisSession, double now,
                    std::vector<std::string>& ev) {
        std::string side;
        if (!jsonFindStr(ob, "side", side) || (side != "BUY" && side != "SELL")) return false;
        int idx = side == "BUY" ? 0 : 1;
        LiveSide& ls = s[idx];
        if (ls.open) return false;
        double px = 0, oq = 0, eq = 0, cq = 0;
        jsonFindNum(ob, "price", px);
        jsonFindNum(ob, "origQty", oq);
        jsonFindNum(ob, "executedQty", eq);
        jsonFindNum(ob, "cummulativeQuoteQty", cq);
        if (!(px > 0) || !(oq > 0)) return false;
        archiveClosed(ls, now);
        double seen = 0;
        if (thisSession) {
            auto it = unknownDealt.find(cid);
            if (it == unknownDealt.end()) it = unknownDealt.find(oid);
            if (it != unknownDealt.end()) { seen = it->second; unknownDealt.erase(it); }
            double dq = eq - seen;
            if (dq > 1e-12) {
                ++ledger.ghostFillsRecon;
                ledger.applyRealFill(idx == 0, eq > 0 && cq > 0 ? cq / eq : px, dq, 0, "",
                                     baseAsset, quoteAsset, now, 0);
                queueFee(oid, now + 1.0);   // [F54]
            }
        } else {
            seen = eq;
        }
        ls.open = true;
        ls.cid = cid;
        ls.oid = oid;
        ls.px = px;
        ls.qty = oq;
        ls.dealtQty = eq;
        ls.dealtAmt = cq;
        ls.dealSeenQty = seen;
        ls.openedAt = now;
        ls.fails = 0;
        ls.nextRetryAt = 0;
        addOwnOid(oid);
        char buf[256];
        std::snprintf(buf, sizeof buf,
                      "[LIVE RECON] усыновлён %s %.20s (cid %.24s) %.10g @ %.10g, "
                      "исполнено %.10g%s", side.c_str(), oid.c_str(), cid.c_str(), oq, px,
                      eq, thisSession ? "" : " (прошлый запуск)");
        evAdd(ev, buf);
        return true;
    }

    void reconcile(double now, std::vector<std::string>& ev) {
        reconcileEscapes(now, ev);
        RestResult r = rc.openOrders();
        if (!r.transportOk || !r.ok) return;
        auto objs = jsonSplitObjects(r.body);
        std::set<std::string> exchOids;
        for (auto& ob : objs) {
            std::string oid, cid;
            if (!jsonFindStr(ob, "orderId", oid)) continue;
            exchOids.insert(oid);
            jsonFindStr(ob, "clientOrderId", cid);
            bool known = (s[0].open && s[0].oid == oid) ||
                         (s[1].open && s[1].oid == oid) ||
                         (s[0].open && !s[0].cid.empty() && s[0].cid == cid) ||
                         (s[1].open && !s[1].cid.empty() && s[1].cid == cid);
            if (known) {
                // [fix F30] частичный филл, не пришедший по WS: executedQty
                // из openOrders сверх учтённого -> дельта в ledger
                for (int i = 0; i < 2; ++i) {
                    LiveSide& ls = s[i];
                    if (!ls.open || !(ls.oid == oid || (!ls.cid.empty() && ls.cid == cid)))
                        continue;
                    double eq = 0, cq = 0;
                    if (!jsonFindNum(ob, "executedQty", eq) || eq <= ls.dealtQty + 1e-12)
                        break;
                    jsonFindNum(ob, "cummulativeQuoteQty", cq);
                    double dq = eq - ls.dealtQty;
                    double px = (cq - ls.dealtAmt) / dq;
                    if (!(px > 0)) px = ls.px;
                    ++ledger.ghostFillsRecon;
                    ledger.applyRealFill(i == 0, px, dq, 0, "", baseAsset, quoteAsset, now, 0);
                    ls.dealtQty = eq;
                    if (cq > 0) ls.dealtAmt = cq;
                    noteDealt(ls, ls.open);   // [F50]
                    queueFee(ls.oid.empty() ? oid : ls.oid, now + 2.0);   // [F54]
                    char buf[224];
                    std::snprintf(buf, sizeof buf,
                                  "[LIVE GHOST-REST] %s %s дельта-филл %.10g @ ~%.10g "
                                  "(recon: executedQty живого ордера опередил deals)",
                                  i == 0 ? "BUY " : "SELL", ls.cid.c_str(), dq, px);
                    evAdd(ev, buf);
                    break;
                }
                continue;
            }
            // [fix F14] ордер биржи совпал с cid стороны, которая локально
            // закрыта (ошибочный resolve / поздняя видимость после place) ->
            // сторона снова open, дальше ею управляют ensureSide/cancelAll.
            // Раньше такой ордер считался «известным» и не снимался никогда.
            bool revived = false;
            for (int i = 0; i < 2 && !revived; ++i) {
                LiveSide& ls = s[i];
                if (ls.open || ls.cid.empty() || ls.cid != cid) continue;
                ls.open = true;
                ls.oid = oid;
                addOwnOid(oid);   // [fix F27]
                double v = 0;
                if (jsonFindNum(ob, "price", v) && v > 0) ls.px = v;
                if (jsonFindNum(ob, "origQty", v) && v > 0) ls.qty = v;
                ls.nextRetryAt = 0;
                ++reconEvents;
                revived = true;
                char buf[224];
                std::snprintf(buf, sizeof buf,
                              "[LIVE RECON] %s: локально закрыт, но жив на бирже "
                              "(oid %.20s) -> снова open", cid.c_str(), oid.c_str());
                evAdd(ev, buf);
            }
            if (revived) continue;
            // [fix F38] ордер бота прошлого запуска: adopt=1 -> усыновить
            if (!ourCid(cid) && botCid(cid) && lcfg && lcfg->reconAdopt) {
                ++reconEvents;
                if (!adoptOrder(ob, oid, cid, false, now, ev)) {
                    RestResult cr = rc.cancelOrder(oid, "");
                    char buf[224];
                    std::snprintf(buf, sizeof buf,
                                  "[LIVE RECON] ордер прошлого запуска %.20s: слот занят "
                                  "-> снят (%s)", oid.c_str(), cr.ok ? "ok" : "fail");
                    evAdd(ev, buf);
                }
                continue;
            }
            if (!ourCid(cid)) {
                if (knownForeignOids.insert(oid).second) {
                    char buf[224];
                    std::snprintf(buf, sizeof buf,
                                  "[LIVE RECON] чужой ордер на бирже: %.20s "
                                  "(cid %.24s) — НЕ трогаем", oid.c_str(), cid.c_str());
                    evAdd(ev, buf);
                }
                continue;
            }
            // наш префикс, но неизвестен (перезапуск?) — политика:
            // adopt=0 -> снять (чистый лист), adopt=1 -> оставить и усыновить
            ++reconEvents;
            if (lcfg && lcfg->reconAdopt && adoptOrder(ob, oid, cid, true, now, ev)) {
                // [fix F38] усыновлён в свободный слот своей стороны
            } else {
                RestResult cr = rc.cancelOrder(oid, "");
                ++orphanCancels;
                char buf[224];
                std::snprintf(buf, sizeof buf,
                              "[LIVE RECON] снят сирота %.20s (%s)", oid.c_str(),
                              cr.ok ? "ok" : "fail");
                evAdd(ev, buf);
                // [fix F21] сирота этой сессии (pid в cid наш): её филлы — наши.
                // Проводим executedQty сверх уже пришедшего по WS; остаток
                // кладём кредитом, чтобы поздняя WS-сделка не учлась дважды.
                std::string srcBody = cr.ok ? cr.body : ob;
                if (!cr.ok) {   // уже исполнилась/снялась -> точный executedQty
                    RestResult gr = rc.getOrder(oid, "");
                    if (gr.ok) srcBody = gr.body;
                }
                double execQty = 0, cumQuote = 0;
                std::string side;
                jsonFindNum(srcBody, "executedQty", execQty);
                jsonFindNum(srcBody, "cummulativeQuoteQty", cumQuote);
                if (!jsonFindStr(srcBody, "side", side)) jsonFindStr(ob, "side", side);
                double seen = 0;
                auto it = unknownDealt.find(cid);
                if (it == unknownDealt.end()) it = unknownDealt.find(oid);
                if (it != unknownDealt.end()) seen = it->second;
                double dq = execQty - seen;
                if (dq > 1e-12 && (side == "BUY" || side == "SELL")) {
                    double px = execQty > 0 ? cumQuote / execQty : 0;
                    if (!(px > 0)) jsonFindNum(ob, "price", px);
                    if (px > 0) {
                        ++ledger.ghostFillsRecon;
                        ledger.applyRealFill(side == "BUY", px, dq, 0, "", baseAsset,
                                             quoteAsset, now, 0);
                        queueFee(oid, now + 1.0);   // [F54]
                        closedOrds.push_back({cid, oid, dq, now});
                        if (closedOrds.size() > 64) closedOrds.erase(closedOrds.begin());
                        std::snprintf(buf, sizeof buf,
                                      "[LIVE GHOST-REST] %s сирота %.20s: дельта-филл "
                                      "%.10g @ ~%.10g", side.c_str(), oid.c_str(), dq, px);
                        evAdd(ev, buf);
                    }
                }
            }
        }
        // наши open, которых нет на бирже -> resolve (филл в обход ленты?)
        for (int i = 0; i < 2; ++i) {
            LiveSide& ls = s[i];
            if (!ls.open || ls.oid.empty()) continue;
            if (exchOids.count(ls.oid)) continue;
            ++reconEvents;
            char buf[192];
            std::snprintf(buf, sizeof buf,
                          "[LIVE RECON] %s open локально, но нет на бирже -> resolve",
                          ls.cid.c_str());
            evAdd(ev, buf);
            resolve(ls, i == 0, now, ev);
        }
    }

    // ------------------------- баланс (истина для гейтов)
    void pollBalance(std::vector<std::string>& ev) {
        RestResult r = rc.account();
        if (!r.transportOk || !r.ok) return;
        bool wasInit = ledger.acctInit;
        // [v3.0 fix] ответ = {"balances":[{...}],...} — массив на ГЛУБИНЕ 1,
        // jsonSplitObjects по всему телу его не видит. Сначала вырезаем
        // вложенный массив с баланс-скобками, потом разбираем объекты.
        std::string arr = jsonExtractArray(r.body, "balances");
        if (arr.empty()) arr = r.body;   // fallback: вдруг отдали плоский массив
        for (auto& ob : jsonSplitObjects(arr)) {
            std::string asset;
            if (!jsonFindStr(ob, "asset", asset)) continue;
            double fr = 0, lk = 0;
            jsonFindNum(ob, "free", fr);
            jsonFindNum(ob, "locked", lk);
            if (asset == baseAsset) {
                ledger.baseFree = fr;
                ledger.baseLocked = lk;
                ledger.acctInit = true;
            } else if (asset == quoteAsset) {
                ledger.quoteFree = fr;
                ledger.quoteLocked = lk;
                // [fix 3.1-F5] RAMP-старт с пустым base: ассет может
                // отсутствовать в первом ответе (нулевые не отдаются);
                // acctInit по quote — иначе base0 перезапишется позже
                // пост-филлевым значением и sessionPnl потеряет позицию
                if (!ledger.acctInit) ledger.acctInit = true;
            }
        }
        if (ledger.acctInit && !wasInit) {
            ledger.base0 = ledger.totalBase();
            char buf[288];
            std::snprintf(buf, sizeof buf,
                          "[LIVE BAL] старт: %.10g %s (free %.10g) | %.4f %s free "
                          "(base0 зафиксирован, сессионный P&L — от нуля)",
                          ledger.totalBase(), baseAsset.c_str(), ledger.baseFree,
                          ledger.quoteFree, quoteAsset.c_str());
            evAdd(ev, buf);
        }
        // дрифт внутреннего учёта vs биржа (сигнал пропущенных филлов)
        if (ledger.acctInit) {
            double d = std::fabs(ledger.baseInt - (ledger.totalBase() - ledger.base0));
            if (d > ledger.driftMaxBase) ledger.driftMaxBase = d;
        }
    }
};

// [v2.13] percentile по вектору (копия сортируется)
static double moPct(std::vector<double> v, double p) {
    if (v.empty()) return 0.0;
    std::sort(v.begin(), v.end());
    double idx = p * (double)(v.size() - 1);
    size_t lo = (size_t)idx;
    size_t hi = std::min(lo + 1, v.size() - 1);
    double fr = idx - (double)lo;
    return v[lo] * (1.0 - fr) + v[hi] * fr;
}

// ---------------------------------------------------------------------- main
// [v3.1] мультипарный режим: один процесс — N символов. Всё per-symbol
// состояние живёт в SymCtx; общие: публичный WS, приватный WS, REST-бюджет,
// listenKey, серверное время, kill-политика (см. шапку).

// [v2.11] escape с латентностью (paper-ветка); [v2.13] + expected VWAP
struct PendEsc { bool active = false; bool isBuy = false;
                 bool flat = false;   // [F57] закрыть всю позицию (HALT)
                 double qty = 0, fireAt = 0, expAvg = 0; };

// [F52] вычесть наш ордер (side 0=bid,1=ask) из уровня книги; пустой уровень убрать
static void subOwnLevel(BookState& b, int side, double px, double q, double eps) {
    auto& lv = side == 0 ? b.bids : b.asks;
    for (size_t k = 0; k < lv.size(); ++k)
        if (std::fabs(lv[k].price - px) < eps) {
            lv[k].qty -= q;
            if (lv[k].qty <= std::max(1e-12, q * 1e-6))
                lv.erase(lv.begin() + (long)k);
            break;
        }
}

struct SymCtx {
    std::string symbol;
    SymbolInfo si;
    double tick = 0.01;
    PaperTrader tr;                 // shadow-движок v2.16 (не тронут)
    BookState book;
    BookState xbook;                // [F52] live: книга БЕЗ наших реальных ордеров
    struct OwnLvl { int side; double px, qty, until; };
    std::deque<OwnLvl> ownRecent;   // [F52] недавно снятые/исполненные наши ордера
    std::string ownCid[2];
    double ownPx[2] = {0, 0}, ownRem[2] = {0, 0};
    std::deque<double> fillTimes;   // [F53] времена реальных филлов (breaker)
    long fillEvSeenBr[2] = {0, 0};
    double breakerUntil = 0;
    int breakerCnt = 0;
    FlowTracker flow;
    std::deque<PendEv> evq;         // [v2.12 Ф7] event-time очередь (per-sym)
    double evqMaxExchMs = 0;
    double lastAppliedExchMs = 0;   // [v2.16 P0-1] монотонность (per-sym)
    long lateEvents = 0;
    double gapMid0 = 0;             // [v2.12 Ф8] мид до обрыва
    bool gapMid0Valid = false;
    std::vector<std::string> events;
    LiveExecutor live;              // свой RestClient (keep-alive HTTPS на пару)
    double startUsdt = 10000;       // локальный лимит задействования quote
    double quoteShare = 1.0;        // доля свободных USDT кошелька [v3.1]
    std::string killFileSym;        // per-symbol kill-switch [v3.1]
    std::vector<double> priceSamples;
    int depthSeen = 0;
    double retryInitAt = 0;
    bool depthReady = false, warnedNoDepth = false, warnedBothPaused = false;
    double lastStratT = 0, lastFlatten = 0;
    int haltFlatTries = 0; double haltFlatAt = 0;          // [F57]
    std::deque<std::pair<double, double>> trendHist;       // [F58] (t, mid) раз в 5 с
    double lastTrendSample = 0, trendBlockBuyS = 0, trendBlockSellS = 0, lastTrendT = 0;
    int trendState = 0;                                    // -1 блок bid, +1 блок ask
    PendEsc pendEsc;
    long depthFrames = 0, tickerFrames = 0, dealFrames = 0;
    std::deque<std::array<double, 3>> bboHist;   // [F45] (exchMs, bestBid, bestAsk)
    // [F55] MM_FAIR=bybit
    double fairD = 15, fairSkew = 0, fairRq = 3, fairStaleMs = 1500, fairMaxDev = 100;
    double basis = 0, basisT = 0, basisT0 = 0, lastFair = 0, lastYAge = 0, devSum = 0;
    bool basisInit = false;
    long fairTicks = 0, fairStale = 0, fairDevCnt = 0, fairWarm = 0, fairOk = 0;
    // [v3.3] F60 maker-escape
    int escMk = 0; double escMkBps = 0, escMkSec = 180, escMkHard = 1.5;
    bool escMkOn = false; double escMkSince = 0, escMkTime = 0;
    long escMkStarts = 0, escMkDone = 0, escMkMarket = 0, escMarketCnt = 0;
    // F62 сигналы, F63 Avellaneda–Stoikov
    double fairMicro = 0, fairImbBps = 0, fairSigMax = 0;
    double asGamma = 0, asTau = 60, asVolSec = 300, asMax = 0;
    double asVar = 0, asLastF = 0, asLastT = 0; bool asInit = false;
    double sigSum = 0, asSum = 0, asAbsMax = 0; long sigN = 0;
    SymCtx(const std::string& host, bool nv) : live(host, nv) {}
};

static std::vector<std::string> splitSymbols(const std::string& s) {
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

int main(int argc, char** argv) {
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

    auto findCtx = [&](const std::string& sym) -> SymCtx* {
        for (auto* C : ctxs) if (C->symbol == sym) return C;
        return nullptr;
    };
    auto flushEvents = [&](SymCtx& C) {
        for (auto& e : C.events) lprintf("      [%s] %s\n", C.symbol.c_str(), e.c_str());
        C.events.clear();
    };

    // [v2.12 ФИКС 7] применение одного события к коммиченной книге (per-sym)
    auto applyEvent = [&](SymCtx& C, const PendEv& e) {
        auto& tr = C.tr; auto& cfg = C.tr.cfg; auto& book = C.book;
        auto& flow = C.flow; auto& tick = C.tick; auto& events = C.events;
        // [v2.16 P0-1] монотонность: позднее событие НЕ откатывает книгу
        if (e.exchMs < C.lastAppliedExchMs) { ++C.lateEvents; return; }
        if (e.exchMs > C.lastAppliedExchMs) C.lastAppliedExchMs = e.exchMs;
        double now = nowSecs();
        if (e.kind == 0) {
            book.bids = e.bids;
            book.asks = e.asks;
            std::sort(book.bids.begin(), book.bids.end(),
                      [](const Level& a, const Level& b) { return a.price > b.price; });
            std::sort(book.asks.begin(), book.asks.end(),
                      [](const Level& a, const Level& b) { return a.price < b.price; });
            book.lastRecv = now;
            if (!book.bids.empty() && !book.asks.empty()) C.depthReady = true;
            tr.onDepth(tr.buy, book, tick);
            tr.onDepth(tr.sell, book, tick);
            if (book.ok()) C.bboHist.push_back({e.exchMs, book.bestBid(), book.bestAsk()});  // [F45]
            if (C.depthSeen < 30) {
                ++C.depthSeen;
                for (const auto& l : book.bids) C.priceSamples.push_back(l.price);
                for (const auto& l : book.asks) C.priceSamples.push_back(l.price);
            }
        } else if (e.kind == 1) {
            if (!book.ok()) {
                book.bids.assign(1, Level{e.bidP, e.bidQ});
                book.asks.assign(1, Level{e.askP, e.askQ});
            } else {
                double ep = tick * 0.49;
                if (e.bidP > 0) {
                    book.bids.erase(
                        std::remove_if(book.bids.begin(), book.bids.end(),
                            [&](const Level& l) { return l.price >= e.bidP - ep; }),
                        book.bids.end());
                    book.bids.insert(book.bids.begin(), Level{e.bidP, e.bidQ});
                }
                if (e.askP > 0) {
                    book.asks.erase(
                        std::remove_if(book.asks.begin(), book.asks.end(),
                            [&](const Level& l) { return l.price <= e.askP + ep; }),
                        book.asks.end());
                    book.asks.insert(book.asks.begin(), Level{e.askP, e.askQ});
                }
            }
            book.lastRecv = now;
            if (book.ok()) C.bboHist.push_back({e.exchMs, book.bestBid(), book.bestAsk()});  // [F45]
        } else {
            const MexcPush::Deal& d = e.deal;
            // [v2.13.1] карантин «сделок из будущего» (страховка гейта Ф16)
            bool quar = false;
            if (tr.dealOffInit && d.timeMs > 0) {
                double dlag = nowMsSteady() - tr.exchOffDealMs - (double)d.timeMs;
                quar = (dlag < -cfg.quarantineMs);
            }
            if (quar) ++tr.skewQuar;
            flow.onDeal(d, now, tr.dealOffInit, tr.exchOffDealMs);
            if (logDeals && book.ok()) {
                double lag = (d.timeMs > 0 && tr.dealOffInit)
                                 ? (nowMsSteady() - tr.exchOffDealMs - (double)d.timeMs)
                                 : -1.0;
                char ourB[32], ourS[32];
                if (tr.buy.st == QState::LIVE || tr.buy.st == QState::WAIT_CANCEL)
                    std::snprintf(ourB, sizeof ourB, "%.10g", tr.buy.cur.price);
                else std::snprintf(ourB, sizeof ourB, "-");
                if (tr.sell.st == QState::LIVE || tr.sell.st == QState::WAIT_CANCEL)
                    std::snprintf(ourS, sizeof ourS, "%.10g", tr.sell.cur.price);
                else std::snprintf(ourS, sizeof ourS, "-");
                lprintf("      [DEAL]%s %-10s %s %.10g @ %.10g | bbo %.10g/%.10g | "
                        "our %s/%s | lag %.0f ms\n",
                        quar ? "!" : " ", C.symbol.c_str(),
                        d.side == 1 ? "BUY " : d.side == 2 ? "SELL" : "?  ",
                        d.qty, d.price, book.bestBid(), book.bestAsk(),
                        ourB, ourS, lag);
            }
            // [F45] принт строго внутри видимого спреда за окно до принта?
            {
                double wMs = cfg.printFilterMs;
                while (C.bboHist.size() > 2 && C.bboHist.front()[0] < e.exchMs - wMs - 5000)
                    C.bboHist.pop_front();
                double hiBid = book.ok() ? book.bestBid() : 0;
                double loAsk = book.ok() ? book.bestAsk() : 0;
                for (const auto& h : C.bboHist) {
                    if (h[0] < e.exchMs - wMs || h[0] > e.exchMs) continue;
                    if (h[1] > hiBid) hiBid = h[1];
                    if (loAsk <= 0 || (h[2] > 0 && h[2] < loAsk)) loAsk = h[2];
                }
                double ep = tick * 0.49;
                tr.curPrintInside = hiBid > 0 && loAsk > 0 &&
                                    d.price > hiBid + ep && d.price < loAsk - ep;
                ++tr.prAll;
                if (tr.curPrintInside) ++tr.prInside;
            }
            if (lcfg.live && lcfg.realSync) {   // [F50] для REAL_SWEEP
                tr.recentPr.push_back({now, d.price, (double)d.side});
                while (!tr.recentPr.empty() && now - tr.recentPr.front()[0] > 6.0)
                    tr.recentPr.pop_front();
            }
            if (!quar) {
                tr.onGhost(tr.ghostBuy, d, book, now, events);
                tr.onGhost(tr.ghostSell, d, book, now, events);
                tr.onPrint(tr.buy, Order::BUY, d, book, tick, now, events);
                tr.onPrint(tr.sell, Order::SELL, d, book, tick, now, events);
            }
        }
    };

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

    auto flushEvq = [&](SymCtx& C) {
        if (C.evq.empty()) return;
        std::stable_sort(C.evq.begin(), C.evq.end(),
                         [](const PendEv& a, const PendEv& b) {
                             return a.exchMs < b.exchMs;
                         });
        double nowFlushMs = nowMsSteady();
        while (!C.evq.empty()) {
            const PendEv& e = C.evq.front();
            bool aged = (nowFlushMs - e.recvMs) >= C.tr.cfg.reorderMs;
            bool watermark = e.exchMs <= C.evqMaxExchMs - C.tr.cfg.reorderMs;
            if (!aged && !watermark) break;
            applyEvent(C, e);
            C.evq.pop_front();
        }
    };

    // ---------------- per-symbol тик: gap -> INIT -> advance -> стратегия ->
    // live-зеркало. Вызывается каждую итерацию цикла для каждой пары.
    auto tickSymbol = [&](SymCtx& C, double now) {
        auto& tr = C.tr; auto& cfg = C.tr.cfg;
        auto& tick = C.tick; auto& si = C.si; auto& flow = C.flow;
        auto& events = C.events; auto& live = C.live;
        // [F52] live: наши реальные ордера стоят в публичной книге. Котировки
        // считались от bestBid/bestAsk, которыми был НАШ же ордер -> каждый
        // реквот на шаг ближе к середине (погоня за собой, сжатие спреда до
        // встречной стороны). Теперь стратегия в live видит книгу без наших
        // ордеров (живые + снятые/исполненные за последние MM_SELF_EXCL_SEC).
        if (lcfg.live && lcfg.selfExcl) {
            for (int i = 0; i < 2; ++i) {
                const LiveSide& ls = live.s[i];
                std::string cid = ls.open ? ls.cid : std::string();
                if (cid != C.ownCid[i]) {
                    if (!C.ownCid[i].empty() && C.ownRem[i] > 0)
                        C.ownRecent.push_back({i, C.ownPx[i], C.ownRem[i],
                                               now + lcfg.selfExclSec});
                    C.ownCid[i] = cid;
                }
                if (ls.open) {
                    C.ownPx[i] = ls.px;
                    C.ownRem[i] = std::max(0.0, ls.qty - ls.dealtQty);
                } else {
                    C.ownRem[i] = 0;
                }
            }
            while (!C.ownRecent.empty() && C.ownRecent.front().until < now)
                C.ownRecent.pop_front();
            C.xbook = C.book;
            double eps = C.tick * 0.49;
            auto sub = [&](int side, double px, double q) {
                subOwnLevel(C.xbook, side, px, q, eps);
            };
            for (int i = 0; i < 2; ++i)
                if (live.s[i].open && C.ownRem[i] > 0) sub(i, C.ownPx[i], C.ownRem[i]);
            for (const auto& o : C.ownRecent) sub(o.side, o.px, o.qty);
        }
        const BookState& book = (lcfg.live && lcfg.selfExcl) ? C.xbook : C.book;

        // [v2.12 ФИКС 8] gap: CERTAIN / POSSIBLE
        if (tr.gapPending && book.ok()) {
            tr.gapPending = false;
            double midN = book.mid();
            for (auto& g : tr.gapCheck) {
                bool isBuy = (g.side == (int)Order::BUY);
                bool crossedNow = isBuy ? (midN <= g.price) : (midN >= g.price);
                bool crossedBefore = C.gapMid0Valid &&
                    (isBuy ? (C.gapMid0 <= g.price) : (C.gapMid0 >= g.price));
                double q = 0;
                if (crossedNow || crossedBefore) {
                    ++tr.gapCertain;
                    q = g.qty;
                } else {
                    ++tr.gapPossible;
                    if (cfg.gapFillMode == 2) q = g.qty;
                    else if (cfg.gapFillMode == 1) q = tr.roundQty(g.qty * 0.5);
                }
                if (q > 0) {
                    tr.applyTrade(isBuy ? q : -q, g.price);
                    ++tr.fills;
                    if (isBuy) ++tr.buyFills; else ++tr.sellFills;
                    ++tr.forcedFills; ++tr.lateFills; ++tr.gapFills;
                    ++tr.rCnt[FR_GAP];
                    double proxyPx = isBuy ? book.bestBid() : book.bestAsk();
                    if (proxyPx <= 0) proxyPx = g.price;
                    double takerPx = isBuy ? book.bestAsk() : book.bestBid();
                    if (takerPx <= 0) takerPx = g.price;
                    // [v2.16 P0-4] GAP — контрфакт после реконнекта: якорь = now
                    tr.mo.push_back({now, isBuy ? 1 : -1, g.price, proxyPx,
                                     takerPx, q, 0, FR_GAP});
                    char buf[224];
                    std::snprintf(buf, sizeof buf,
                                  "[FILL %s GAP%s ] %.10g @ %.10g (%s)",
                                  isBuy ? "BUY " : "SELL",
                                  (crossedNow || crossedBefore) ? "  " : "? ",
                                  q, g.price,
                                  (crossedNow || crossedBefore) ? "certain"
                                                                : "possible");
                    evAdd(events, buf);
                }
            }
            tr.gapCheck.clear();
        }

        // INIT
        if (!tr.invInitialized && !cfg.allowShort && !C.depthReady && book.ok() &&
            !C.warnedNoDepth) {
            C.warnedNoDepth = true;
            evAdd(events, std::string("[INIT] waiting for depth snapshot "
                                      "(book seeded from bookTicker only)"));
        }
        if (!tr.invInitialized && book.ok() && (C.depthReady || cfg.allowShort) &&
            now >= C.retryInitAt) {
            double sprLive = book.bestAsk() - book.bestBid();
            bool tickBad = !(tick > 0) || tick > sprLive + 1e-12;
            if (tickBad && C.priceSamples.size() >= 60) {
                std::sort(C.priceSamples.begin(), C.priceSamples.end());
                double best = 0;
                for (size_t i = 1; i < C.priceSamples.size(); ++i) {
                    double d = C.priceSamples[i] - C.priceSamples[i - 1];
                    if (d > 0 && (best == 0 || d < best)) best = d;
                }
                if (best > 0 && best <= sprLive + 1e-12) {
                    tick = best;
                    tickBad = false;
                    char tb[96];
                    std::snprintf(tb, sizeof tb,
                                  "[INIT] tick derived from live book: %.3g", tick);
                    evAdd(events, tb);
                }
                C.priceSamples.clear();
                C.depthSeen = 0;
            }
            if (tickBad) {
                C.retryInitAt = now + 30.0;
                char buf[224];
                std::snprintf(buf, sizeof buf,
                              "[INIT] DEFER 30s: tick %.10g vs live spread %.10g",
                              tick, sprLive);
                evAdd(events, buf);
            } else if (!cfg.allowShort) {
                // [v2.14 ФИКС 17] RAMP: вход пассивно — только bid, sell из
                // филлов. setup cost = 0, весь P&L — mm-компонента.
                if (cfg.initMode == 1) {
                    tr.invInitialized = true;
                    tr.base = 0;
                    tr.base0 = 0;
                    tr.mid0 = book.mid();
                    tr.avgEntry = 0;
                    tr.setupCost = 0;
                    evAdd(events, std::string(
                        "[INIT] RAMP mode: bid-only до первого филла "
                        "(setup cost 0, инвентарь пассивно)"));
                } else {
                double ask = book.bestAsk();
                double mid0 = book.mid();
                double budget = cfg.maxPosQuote;
                double lotCost = si.qtyStep * ask;
                double minBuy = std::max(si.minNotional, lotCost);
                double minInit = cfg.minInitFrac * cfg.orderSizeQuote;
                double minViable = std::max(minBuy, minInit);
                double fq = 0, avg = 0;
                bool budgetTooSmall = minViable > budget;
                if (!budgetTooSmall) {
                    fq = std::floor(walkBudgetBuy(book, budget) / si.qtyStep + 1e-9)
                         * si.qtyStep;
                    double filled = 0;
                    avg = fq > 0 ? walkBook(book, true, fq, filled) : 0.0;
                }
                if (budgetTooSmall) {
                    C.retryInitAt = now + 60.0;
                    char buf[352];
                    std::snprintf(buf, sizeof buf,
                                  "[INIT] DEFER 60s: budget %.2f USDT < min viable buy "
                                  "%.4f USDT (lot %.10g @ %.10g = %.4f, minNotional %.4g, "
                                  "minInit %.4f) — raise MM_MAX_POS%s",
                                  budget, minViable, si.qtyStep, ask, lotCost,
                                  si.minNotional, minInit,
                                  minInit > minBuy ? " or lower MM_MIN_INIT_FRAC" : "");
                    evAdd(events, buf);
                } else if (fq <= 0 || avg <= 0 || fq * avg < minViable) {
                    C.retryInitAt = now + 60.0;
                    char buf[320];
                    std::snprintf(buf, sizeof buf,
                                  "[INIT] DEFER 60s: ask side %.4f USDT < min viable "
                                  "%.4f USDT (min buy %.4f, minInit %.4f, budget %.2f)",
                                  fq * avg, minViable, minBuy, minInit, budget);
                    evAdd(events, buf);
                } else {
                    double cost = fq * avg;
                    double fee = cost * cfg.takerFeeBps / 1e4;
                    if (cost + fee > tr.quote) {
                        C.retryInitAt = now + 60.0;
                        evAdd(events, std::string(
                            "[INIT] DEFER 60s: cost+fee exceeds quote balance"));
                    } else {
                        tr.invInitialized = true;
                        tr.base = fq;
                        tr.quote -= cost + fee;
                        tr.feesPaid += fee;
                        tr.takerFees += fee;
                        tr.base0 = fq;
                        tr.mid0 = mid0;
                        tr.avgEntry = mid0;
                        tr.setupCost = cost + fee - fq * mid0;
                        char buf[288];
                        std::snprintf(buf, sizeof buf,
                                      "[INIT] spot inventory: bought %.10g %s @ avg %.10g "
                                      "= %.2f USDT (fee %.4f, setup cost %.4f)",
                                      fq, C.symbol.c_str(), avg, cost, fee, tr.setupCost);
                        evAdd(events, buf);
                        if (si.qtyStep * avg > cfg.orderSizeQuote) {
                            char w[224];
                            std::snprintf(w, sizeof w,
                                "[INIT] WARNING: 1 qtyStep = %.2f USDT > MM_ORDER_USDT "
                                "(%.0f) — quotes suppressed",
                                si.qtyStep * avg, cfg.orderSizeQuote);
                            evAdd(events, w);
                        }
                        if (tr.base * mid0 < cfg.orderSizeQuote) {
                            char w[256];
                            std::snprintf(w, sizeof w,
                                "[INIT] NOTE: inventory %.10g < order size "
                                "(%.0f USDT) — sell-side sized to available base",
                                tr.base, cfg.orderSizeQuote);
                            evAdd(events, w);
                        }
                    }
                }
                }
            } else {
                tr.invInitialized = true;
            }
        }

        if (book.ok()) {
            tr.advance(tr.buy, book, tick, now);
            tr.advance(tr.sell, book, tick, now);
        }

        // ---- strategy tick
        if (book.ok() && tr.invInitialized) {
            double mid = book.mid();
            tr.lastMid = mid;

            if (!cfg.allowShort && C.lastStratT > 0 && now > C.lastStratT &&
                now - C.lastStratT < 5.0 && tr.base * mid < cfg.orderSizeQuote)
                tr.sellSmallSecs += now - C.lastStratT;
            C.lastStratT = now;

            // [v2.11] исполнение отложенного escape (только paper-ветка;
            // в live излишек РЕАЛЬНОЙ позиции сносится MARKET-ордером ниже)
            if (!lcfg.live && C.pendEsc.active && now >= C.pendEsc.fireAt) {
                C.pendEsc.active = false;
                double excess = std::fabs(tr.posValue(mid)) -
                                (C.pendEsc.flat ? 0.0 : cfg.maxPosQuote * 0.75);   // [F57]
                C.pendEsc.flat = false;
                if (excess > 0) {
                    double over = std::min(
                        C.pendEsc.qty,
                        std::floor((excess / mid) / si.qtyStep + 1e-9) * si.qtyStep);
                    bool isBuy = C.pendEsc.isBuy;
                    if (isBuy) {
                        double maxAfford = tr.quote * 0.98 / mid;
                        if (over > maxAfford)
                            over = std::floor(maxAfford / si.qtyStep + 1e-9) * si.qtyStep;
                    } else {
                        if (over > tr.base)
                            over = std::floor(tr.base / si.qtyStep + 1e-9) * si.qtyStep;
                    }
                    if (over > 0) {
                        // [v2.15 ФИКС 22] MM_ESCAPE_SKIP_LEVELS: стресс-escape
                        double fq = 0;
                        double avg = walkBookSkip(book, isBuy, over, fq,
                                                  cfg.escapeSkipLevels);
                        if (fq > 0) {
                            tr.applyTrade(isBuy ? fq : -fq, avg);
                            double fee = fq * avg * (cfg.takerFeeBps - cfg.makerFeeBps) / 1e4;
                            if (fee > 0) { tr.quote -= fee; tr.feesPaid += fee;
                                           tr.takerFees += fee; }
                            ++tr.rCnt[FR_ESCAPE];
                            // [v2.16 P0-4] escape — решение симулятора: якорь = now
                            tr.mo.push_back({now, isBuy ? 1 : -1, avg, avg, avg,
                                             fq, 0, FR_ESCAPE});
                            double slipBps = 0.0;
                            if (C.pendEsc.expAvg > 0) {
                                double slip = isBuy ? (C.pendEsc.expAvg - avg)
                                                    : (avg - C.pendEsc.expAvg);
                                slipBps = slip / C.pendEsc.expAvg * 1e4;
                                ++tr.escCnt;
                                tr.escSlipSum += slipBps;
                                if (tr.escCnt == 1 || slipBps < tr.escSlipWorst)
                                    tr.escSlipWorst = slipBps;
                            }
                            char buf[224];
                            std::snprintf(buf, sizeof buf,
                                          "[ESCAPE ] taker %s %.10g @ avg %.10g "
                                          "(через %.0f ms, slip %+.1f bps)",
                                          isBuy ? "BUY " : "SELL", fq, avg,
                                          cfg.latencyS * 1000, slipBps);
                            evAdd(events, buf);
                            // [v2.14 ФИКС 18] cooldown после escape
                            if (cfg.escapeCooldownS > 0) {
                                if (isBuy) tr.escPauseSellUntil = now + cfg.escapeCooldownS;
                                else       tr.escPauseBuyUntil  = now + cfg.escapeCooldownS;
                            }
                        }
                    }
                } else {
                    evAdd(events, std::string(
                        "[ESCAPE ] skipped: позиция вернулась в band за время полёта"));
                }
            }

            double dataAge = book.lastRecv > 0 ? (now - book.lastRecv)
                                               : (now - connAt);
            if (dataAge > cfg.staleSecs) {
                if (tr.buy.st != QState::NONE || tr.sell.st != QState::NONE) {
                    evAdd(events, std::string("[risk] no data > ") +
                           std::to_string((int)dataAge) + "s -> pulling quotes");
                }
                tr.requestCancel(tr.buy, now, events);
                tr.requestCancel(tr.sell, now, events);
                if (dataAge > cfg.staleSecs * 2.0) {
                    // [v3.1] коннект общий — стейл любой пары реконнектит всех
                    if (!g_replay) {
                        lprintf("[net] %s no data > %.0f s -> reconnecting\n",
                                C.symbol.c_str(), dataAge);
                        lost = true;
                    }
                }
            } else if (lcfg.live && now < C.breakerUntil) {
                // [F53] breaker: слишком частые реальные филлы -> пара на паузе
                tr.requestCancel(tr.buy, now, events);
                tr.requestCancel(tr.sell, now, events);
            } else if (!tr.halted &&
                       (lcfg.live ? live.ledger.sessionPnl(mid)
                                  : (cfg.lossBasis == 1                       // [F56]
                                     ? tr.totalPnl(mid) + tr.setupCost - tr.base0 * (mid - tr.mid0)
                                     : tr.totalPnl(mid))) < -cfg.maxLoss) {
                tr.halted = true;
                char buf[192];
                std::snprintf(buf, sizeof buf,
                              "[risk] loss %.2f exceeded limit %.2f -> HALT%s",
                              lcfg.live ? live.ledger.sessionPnl(mid)
                                        : (cfg.lossBasis == 1
                                           ? tr.totalPnl(mid) + tr.setupCost - tr.base0 * (mid - tr.mid0)
                                           : tr.totalPnl(mid)),
                              cfg.maxLoss, lcfg.live ? " (REAL)" : "");
                evAdd(events, buf);
                tr.requestCancel(tr.buy, now, events);
                tr.requestCancel(tr.sell, now, events);
                if (lcfg.live) live.cancelAll(events);   // риск-офф немедленно
            } else if (tr.halted) {
                tr.requestCancel(tr.buy, now, events);
                tr.requestCancel(tr.sell, now, events);
                // [F57] закрыть позицию стратегии (первая попытка сразу после HALT)
                double posH = lcfg.live ? live.ledger.posValue(mid) : tr.posValue(mid);
                if (cfg.haltFlat && C.haltFlatTries < 5 &&
                    std::fabs(posH) >= std::max(si.minNotional, 1.0) &&
                    now - C.haltFlatAt >= (C.haltFlatTries == 0 ? 0.5 : std::max(5.0, cfg.escapeSecs)) &&
                    (lcfg.live ? live.ledger.acctInit : !C.pendEsc.active)) {
                    ++C.haltFlatTries; C.haltFlatAt = now;
                    bool isBuy = posH < 0;
                    double qty = std::floor((std::fabs(posH) / mid) / si.qtyStep + 1e-9) * si.qtyStep;
                    char buf[192];
                    if (lcfg.live) {
                        if (isBuy) {
                            double maxAfford = std::max(0.0, live.ledger.quoteFree * C.quoteShare
                                                             - lcfg.usdtReserve) / mid;
                            if (qty > maxAfford)
                                qty = std::floor(maxAfford / si.qtyStep + 1e-9) * si.qtyStep;
                            if (qty > 0 && qty * mid >= si.minNotional) {
                                double q = qty * mid * (1.0 + cfg.takerFeeBps / 1e4);
                                std::snprintf(buf, sizeof buf, "[HALT-FLAT] LIVE #%d BUY market %.6g USDT",
                                              C.haltFlatTries, q);
                                evAdd(events, buf);
                                live.escapeMarket(true, 0, q, now, events);
                            }
                        } else {
                            if (qty > live.ledger.baseFree)
                                qty = std::floor(live.ledger.baseFree / si.qtyStep + 1e-9) * si.qtyStep;
                            if (qty > 0 && qty * mid >= si.minNotional) {
                                std::snprintf(buf, sizeof buf, "[HALT-FLAT] LIVE #%d SELL market %.10g",
                                              C.haltFlatTries, qty);
                                evAdd(events, buf);
                                live.escapeMarket(false, qty, 0, now, events);
                            }
                        }
                    } else {
                        if (isBuy) {
                            double maxAfford = tr.quote * 0.98 / mid;
                            if (qty > maxAfford)
                                qty = std::floor(maxAfford / si.qtyStep + 1e-9) * si.qtyStep;
                        } else if (qty > tr.base) {
                            qty = std::floor(tr.base / si.qtyStep + 1e-9) * si.qtyStep;
                        }
                        if (qty > 0) {
                            double fqE = 0, avgE = walkBook(book, isBuy, qty, fqE);
                            C.pendEsc.active = true; C.pendEsc.flat = true;
                            C.pendEsc.isBuy = isBuy; C.pendEsc.qty = qty;
                            C.pendEsc.fireAt = now + cfg.latencyS;
                            C.pendEsc.expAvg = (fqE > 0) ? avgE : 0.0;
                            std::snprintf(buf, sizeof buf, "[HALT-FLAT] #%d %s %.10g (pos %+.2f USDT)",
                                          C.haltFlatTries, isBuy ? "BUY " : "SELL", qty, posH);
                            evAdd(events, buf);
                        }
                    }
                }
            } else {
                double spr = book.bestAsk() - book.bestBid();
                double sprTicks = spr / tick;
                double imb = book.imbalance();
                // [v2.15 ФИКС 21] MM_SELF_IN_BOOK (только сигналы)
                if (cfg.selfInBook) {
                    double bb2 = 0, ba2 = 0;
                    for (size_t i = 0; i < book.bids.size() && i < 10; ++i) bb2 += book.bids[i].qty;
                    for (size_t i = 0; i < book.asks.size() && i < 10; ++i) ba2 += book.asks[i].qty;
                    if (tr.buy.st == QState::LIVE || tr.buy.st == QState::WAIT_CANCEL)
                        bb2 += tr.buy.cur.qty - tr.buy.cur.filled;
                    if (tr.sell.st == QState::LIVE || tr.sell.st == QState::WAIT_CANCEL)
                        ba2 += tr.sell.cur.qty - tr.sell.cur.filled;
                    if (bb2 + ba2 > 0) imb = bb2 / (bb2 + ba2);
                }
                double posV = lcfg.live ? live.ledger.posValue(mid)
                                        : tr.posValue(mid);   // гейты — РЕАЛЬНЫЕ
                double tfi = flow.tfi(now);
                int run = flow.run(now);

                if (cfg.runStop > 0 && run <= -cfg.runStop) tr.pauseBuy = true;
                if (cfg.runStop > 0 && run >= +cfg.runStop) tr.pauseSell = true;
                if (tr.pauseBuy && run > -cfg.runStop && tfi > -cfg.tfiThresh)
                    tr.pauseBuy = false;
                if (tr.pauseSell && run < +cfg.runStop && tfi < +cfg.tfiThresh)
                    tr.pauseSell = false;

                tr.updateMarkouts(now, mid);

                // [F60] maker-escape: состояние
                bool escMarketOK = true;
                if (C.escMk) {
                    double ap = std::fabs(posV);
                    if (!C.escMkOn && ap > cfg.maxPosQuote) {
                        C.escMkOn = true; C.escMkSince = now; ++C.escMkStarts;
                        char eb[160];
                        std::snprintf(eb, sizeof eb, "[ESC-MAKER] старт: pos %+.2f > %.0f -> %s по fair%+.1f bps",
                                      posV, cfg.maxPosQuote, posV > 0 ? "SELL" : "BUY ", C.escMkBps);
                        evAdd(events, eb);
                    } else if (C.escMkOn && ap <= cfg.maxPosQuote * 0.75) {
                        C.escMkOn = false; ++C.escMkDone; C.escMkTime += now - C.escMkSince;
                        char eb[128];
                        std::snprintf(eb, sizeof eb, "[ESC-MAKER] готово за %.0f s: pos %+.2f",
                                      now - C.escMkSince, posV);
                        evAdd(events, eb);
                    }
                    escMarketOK = C.escMkOn && (now - C.escMkSince >= C.escMkSec ||
                                                ap > C.escMkHard * cfg.maxPosQuote);
                }
                // ESCAPE — решение; [v3.0] live: реальный MARKET-ордер сразу
                if (cfg.escapeSecs > 0 && std::fabs(posV) > cfg.maxPosQuote && escMarketOK &&
                    now - C.lastFlatten >= cfg.escapeSecs &&
                    (lcfg.live ? live.ledger.acctInit : !C.pendEsc.active)) {
                    C.lastFlatten = now;
                    ++C.escMarketCnt;
                    if (C.escMk) {   // [F60] мейкер не успел -> MARKET, новое окно
                        ++C.escMkMarket; C.escMkTime += now - C.escMkSince; C.escMkSince = now;
                        evAdd(events, "[ESC-MAKER] не вышли мейкером -> MARKET");
                    }
                    double excess = std::fabs(posV) - cfg.maxPosQuote * 0.75;
                    double over = std::floor((excess / mid) / si.qtyStep + 1e-9) * si.qtyStep;
                    bool isBuy = posV < 0;
                    if (lcfg.live) {
                        if (isBuy) {
                            double maxAfford =
                                std::max(0.0, live.ledger.quoteFree * C.quoteShare
                                              - lcfg.usdtReserve) / mid;
                            if (over > maxAfford)
                                over = std::floor(maxAfford / si.qtyStep + 1e-9) * si.qtyStep;
                            if (over > 0 && over * mid >= si.minNotional) {
                                // [fix 3.1-F6] +taker сверху: иначе bought
                                // base < over — хвост позиции после escape
                                double escQ = over * mid * (1.0 + cfg.takerFeeBps / 1e4);
                                evAdd(events, std::string("[ESCAPE ] LIVE решение: BUY market ") +
                                      fmtDec(escQ) + " USDT");
                                live.escapeMarket(true, 0, escQ, now, events);
                            }
                        } else {
                            if (over > live.ledger.baseFree)
                                over = std::floor(live.ledger.baseFree / si.qtyStep + 1e-9) * si.qtyStep;
                            if (over > 0 && over * mid >= si.minNotional) {
                                evAdd(events, std::string("[ESCAPE ] LIVE решение: SELL market ") +
                                      fmtDec(over));
                                live.escapeMarket(false, over, 0, now, events);
                            }
                        }
                        if (cfg.escapeCooldownS > 0) {
                            if (isBuy) tr.escPauseSellUntil = now + cfg.escapeCooldownS;
                            else       tr.escPauseBuyUntil  = now + cfg.escapeCooldownS;
                        }
                    } else {
                    if (isBuy) {
                        double maxAfford = tr.quote * 0.98 / mid;
                        if (over > maxAfford)
                            over = std::floor(maxAfford / si.qtyStep + 1e-9) * si.qtyStep;
                    } else {
                        if (over > tr.base)
                            over = std::floor(tr.base / si.qtyStep + 1e-9) * si.qtyStep;
                    }
                    if (over > 0) {
                        double fqE = 0, avgE = walkBook(book, isBuy, over, fqE);
                        C.pendEsc.active = true;
                        C.pendEsc.isBuy = isBuy;
                        C.pendEsc.qty = over;
                        C.pendEsc.fireAt = now + cfg.latencyS;
                        C.pendEsc.expAvg = (fqE > 0) ? avgE : 0.0;
                        char buf[192];
                        std::snprintf(buf, sizeof buf,
                                      "[ESCAPE ] решение: %s %.10g (исполнение через %.0f ms)",
                                      isBuy ? "BUY " : "SELL", over, cfg.latencyS * 1000);
                        evAdd(events, buf);
                    }
                    }   // end else (paper-ветка escape)
                }

                double bidPx, askPx;
                bool fairBlock = false;     // [F55]
                double fairPx = 0;
                if (g_fairMode) {
                    ++C.fairTicks;
                    FairQ fq;
                    double tms = nowMsSteady();
                    bool have = fairGet(C.symbol, fq);
                    double age = have ? tms - fq.tMs : 1e9;
                    C.lastYAge = age;
                    if (!have || age > C.fairStaleMs || fq.bid <= 0 || fq.ask <= fq.bid) {
                        fairBlock = true; ++C.fairStale;
                    } else {
                        double ymid = 0.5 * (fq.bid + fq.ask);
                        double inst = std::log(mid / ymid);
                        if (!C.basisInit) {
                            C.basisInit = true; C.basis = inst; C.basisT = now; C.basisT0 = now;
                        } else {
                            double dt = std::max(0.0, now - C.basisT);
                            double al = 1.0 - std::exp(-dt / std::max(1.0, fairBasisSec));
                            C.basis += al * (inst - C.basis);
                            C.basisT = now;
                        }
                        double dev = std::fabs(inst - C.basis) * 1e4;
                        C.devSum += dev;
                        fairPx = ymid * std::exp(C.basis);
                        if (C.fairMicro != 0 && fq.bq > 0 && fq.aq > 0) {   // [F62] микроцена Bybit
                            double micro = (fq.bid * fq.aq + fq.ask * fq.bq) / (fq.bq + fq.aq);
                            fairPx = (ymid + C.fairMicro * (micro - ymid)) * std::exp(C.basis);
                        }
                        C.lastFair = fairPx;
                        if (C.asGamma > 0) {   // [F63] EWMA дисперсии fair, bps^2/c
                            if (!C.asInit) { C.asInit = true; C.asLastF = fairPx; C.asLastT = now; }
                            else if (now - C.asLastT >= 1.0) {
                                double dt = now - C.asLastT;
                                double r = std::log(fairPx / C.asLastF) * 1e4;
                                double v = std::min(r * r / dt, 1e4);
                                double al = 1.0 - std::exp(-dt / std::max(1.0, C.asVolSec));
                                C.asVar = C.asVar <= 0 ? v : C.asVar + al * (v - C.asVar);
                                C.asLastF = fairPx; C.asLastT = now;
                            }
                        }
                        if (now - C.basisT0 < fairWarmSec) { fairBlock = true; ++C.fairWarm; }
                        else if (dev > C.fairMaxDev) { fairBlock = true; ++C.fairDevCnt; }
                        else ++C.fairOk;
                    }
                }
                if (g_fairMode) {
                    double f0 = fairPx > 0 ? fairPx : mid;
                    double sk = -(posV / cfg.maxPosQuote) * C.fairSkew;
                    if (C.fairImbBps != 0 && fairPx > 0) {   // [F62] дисбаланс MEXC
                        double sg = (imb - 0.5) * 2.0 * C.fairImbBps;
                        sg = std::max(-C.fairSigMax, std::min(C.fairSigMax, sg));
                        sk += sg; C.sigSum += sg; ++C.sigN;
                    }
                    if (C.asGamma > 0 && fairPx > 0) {     // [F63] Avellaneda–Stoikov
                        double qf = std::max(-1.5, std::min(1.5, posV / cfg.maxPosQuote));
                        double as = -qf * C.asGamma * C.asVar * C.asTau;
                        as = std::max(-C.asMax, std::min(C.asMax, as));
                        sk += as; C.asSum += std::fabs(as);
                        C.asAbsMax = std::max(C.asAbsMax, std::fabs(as));
                        if (C.fairImbBps == 0) ++C.sigN;
                    }
                    bidPx = f0 * (1.0 + (-C.fairD + sk) / 1e4);
                    askPx = f0 * (1.0 + (C.fairD + sk) / 1e4);
                    if (C.escMkOn) {   // [F60] сокращающая сторона — к fair
                        if (posV > 0) askPx = f0 * (1.0 + C.escMkBps / 1e4);
                        else          bidPx = f0 * (1.0 - C.escMkBps / 1e4);
                    }
                } else if (sprTicks >= 4) {
                    bidPx = book.bestBid() + spr * 0.25;
                    askPx = book.bestAsk() - spr * 0.25;
                } else {
                    bidPx = book.bestBid();
                    askPx = book.bestAsk();
                }
                if (cfg.skewImb && !g_fairMode) {
                    double s = (imb - 0.5) * 2.0 * cfg.imbSkewFrac * spr;
                    bidPx += s; askPx += s;
                }
                if (cfg.skewTfi && !g_fairMode && std::fabs(tfi) > cfg.tfiThresh) {
                    double s = tfi * cfg.tfiSkewFrac * spr;
                    bidPx += s; askPx += s;
                }
                if (!g_fairMode) {
                    double s = -(posV / cfg.maxPosQuote) * cfg.invSkewFrac * spr;
                    bidPx += s; askPx += s;
                }
                if (g_fairMode) {   // [F55] не лучше fair±d: bid вниз, ask вверх
                    bidPx = std::floor(bidPx / tick + 1e-9) * tick;
                    askPx = std::ceil(askPx / tick - 1e-9) * tick;
                } else {
                bidPx = std::floor(bidPx / tick + 0.5) * tick;
                askPx = std::floor(askPx / tick + 0.5) * tick;
                }
                bidPx = std::min(bidPx, book.bestAsk() - tick);
                askPx = std::max(askPx, book.bestBid() + tick);

                bool spreadOK = book.spreadBps() <= cfg.maxSpreadBps;
                bool sane = bidPx > 0 && bidPx < askPx;
                double notional = std::max(cfg.orderSizeQuote, si.minNotional);

                auto makeQty = [&](double px, double& qty) -> bool {
                    if (px <= 0) return false;
                    qty = std::floor((notional / px) / si.qtyStep + 1e-9) * si.qtyStep;
                    if (qty <= 0) return false;
                    if (qty * px < si.minNotional) qty += si.qtyStep;
                    return qty * px >= si.minNotional;
                };

                bool sweepCoolB = cfg.sweepCooldownS <= 0 ||
                                  now - tr.buy.lastSweepAt >= cfg.sweepCooldownS;
                bool sweepCoolS = cfg.sweepCooldownS <= 0 ||
                                  now - tr.sell.lastSweepAt >= cfg.sweepCooldownS;

                double rqThresh = std::max(1.5 * tick, cfg.requoteFrac * spr);
                if (g_fairMode && fairPx > 0)
                    rqThresh = std::max(1.5 * tick, fairPx * C.fairRq / 1e4);

                double bq = 0, sq = 0;
                bool mkB = makeQty(bidPx, bq);

                bool mkS = makeQty(askPx, sq);
                if (C.escMkOn && mid > 0) {   // [F60] объём до излишка
                    double exq = std::floor(((std::fabs(posV) - cfg.maxPosQuote * 0.75) / mid)
                                            / si.qtyStep + 1e-9) * si.qtyStep;
                    if (posV > 0 && mkS && exq > sq) sq = exq;
                    if (posV < 0 && mkB && exq > bq) bq = exq;
                }
                // [v3.0] sell клампится РЕАЛЬНЫМ базовым инвентарём
                // [fix F6] live: продаём только инвентарь СЕССИИ (baseInt от нуля), а не
                // весь кошелёк — иначе бот распродаёт ранее купленные монеты и уводит
                // baseInt в минус. MM_SELL_WALLET_BASE=1 возвращает поведение v3.1.
                double availBase = tr.base;
                if (lcfg.live)
                    availBase = lcfg.sellWallet
                        ? live.ledger.totalBase()
                        : std::min(std::max(0.0, live.ledger.baseInt),
                                   live.ledger.totalBase());
                if (!cfg.allowShort && availBase < sq - 1e-12) {
                    sq = std::floor(availBase / si.qtyStep + 1e-9) * si.qtyStep;
                    mkS = (sq > 0 && sq * askPx >= si.minNotional);
                }

                // [v3.0/v3.1] live: локальная аллокация (MM_START_USDT) и
                // ДОЛЯ свободных USDT кошелька (MM_QUOTE_SHARE, дефолт 1/N)
                bool liveReady = !lcfg.live || live.ledger.acctInit;
                double buyBudget = tr.quote;
                if (lcfg.live) {
                    // [fix F25] quoteFree уже без НАШЕГО залоченного BUY: без
                    // возврата его в бюджет стоящий ордер «не помещался» и
                    // снимался, баланс освобождался, ордер ставился снова.
                    const LiveSide& lb = live.s[0];
                    double ownLocked = lb.open
                        ? std::max(0.0, lb.qty - lb.dealtQty) * lb.px : 0.0;
                    double wallet = live.ledger.quoteFree + live.ledger.quoteLocked;
                    buyBudget = std::min({C.startUsdt + live.ledger.quoteNet,
                                          wallet * C.quoteShare - lcfg.usdtReserve,
                                          live.ledger.quoteFree + ownLocked
                                          - lcfg.usdtReserve});
                }

                // [v2.14] ФИКС 18/19: esc-/cut-паузы в гейтах котирования
                if (fairBlock) { spreadOK = false; sane = false; }   // [F55] нет fair -> снять
                // [F58] фильтр тренда: не наращивать позицию против движения
                bool trendNoBuy = false, trendNoSell = false;
                if (cfg.trendBps > 0 && mid > 0) {
                    if (now - C.lastTrendSample >= 5.0) {
                        C.lastTrendSample = now;
                        C.trendHist.push_back({now, mid});
                        while (C.trendHist.size() > 2 &&
                               now - C.trendHist[1].first >= cfg.trendSec)
                            C.trendHist.pop_front();
                    }
                    int st = 0;
                    if (!C.trendHist.empty() &&
                        now - C.trendHist.front().first >= cfg.trendSec * 0.9) {
                        double trd = (mid / C.trendHist.front().second - 1.0) * 1e4;
                        if (trd < -cfg.trendBps && posV >= -cfg.orderSizeQuote * 0.5) st = -1;
                        if (trd > +cfg.trendBps && posV <= +cfg.orderSizeQuote * 0.5) st = +1;
                        if (st != C.trendState) {
                            char tb[160];
                            std::snprintf(tb, sizeof tb, "[TREND] %+.0f bps / %.0f s -> %s",
                                          trd, cfg.trendSec,
                                          st < 0 ? "bid OFF" : st > 0 ? "ask OFF" : "оба ON");
                            evAdd(events, tb);
                            C.trendState = st;
                        }
                    }
                    double dt = (C.lastTrendT > 0 && now > C.lastTrendT && now - C.lastTrendT < 5.0)
                                ? now - C.lastTrendT : 0.0;
                    if (st < 0) { trendNoBuy = true;  C.trendBlockBuyS += dt; }
                    if (st > 0) { trendNoSell = true; C.trendBlockSellS += dt; }
                    C.lastTrendT = now;
                }
                bool wantBuy = spreadOK && sane && mkB && sweepCoolB && liveReady
                               && !trendNoBuy
                               && !(C.escMkOn && posV > 0)
                               && !tr.pauseBuy
                               && now >= tr.cutPauseBuyUntil
                               && now >= tr.escPauseBuyUntil
                               && posV < cfg.maxPosQuote
                               && bq * bidPx <= buyBudget;
                bool wantSell = spreadOK && sane && mkS && sweepCoolS && liveReady
                                && !trendNoSell
                                && !(C.escMkOn && posV < 0)
                                && !tr.pauseSell
                                && now >= tr.cutPauseSellUntil
                                && now >= tr.escPauseSellUntil
                                && posV > -cfg.maxPosQuote
                                && (cfg.allowShort || sq > 0);

                if (!C.warnedBothPaused && !wantBuy && !wantSell && spreadOK && sane) {
                    C.warnedBothPaused = true;
                    char buf[384];
                    std::snprintf(buf, sizeof buf,
                        "[WARN] both sides paused: buy(%s%s%s%s%s) sell(%s%s%s%s%s) "
                        "run=%+d tfi=%+.2f pos=%+.0f",
                        tr.pauseBuy ? "MOM" : "",
                        !sweepCoolB ? " COOL" : "",
                        !mkB ? " QTY" : "",
                        now < tr.escPauseBuyUntil ? " ESC" : "",
                        now < tr.cutPauseBuyUntil ? " CUT" : "",
                        tr.pauseSell ? "MOM" : "",
                        !sweepCoolS ? " COOL" : "",
                        !mkS ? " QTY" : "",
                        now < tr.escPauseSellUntil ? " ESC" : "",
                        now < tr.cutPauseSellUntil ? " CUT" : "",
                        run, tfi, posV);
                    evAdd(events, buf);
                }

                if (wantBuy)
                    tr.requestQuote(tr.buy, Order::BUY, bidPx, bq, rqThresh, now, events);
                else
                    tr.requestCancel(tr.buy, now, events);
                if (wantSell)
                    tr.requestQuote(tr.sell, Order::SELL, askPx, sq, rqThresh, now, events);
                else
                    tr.requestCancel(tr.sell, now, events);
            }
        }

        // ============ [v3.0 L3] зеркало решений на биржу ============
        // Желаемое состояние = состояние shadow-машины ПОСЛЕ решения.
        // Каждый тик (включая stale/halt): shadow NONE -> реальные снимаем.
        // [F50] реальные филлы -> shadow: sweep-cooldown/cutoff от РЕАЛЬНОГО
        // исполнения (раньше только от paper-филлов) и REAL_SYNC: реальный
        // ордер исполнен целиком, а shadow ещё стоит -> shadow-ордер закрыт,
        // иначе зеркало выставило бы его повторно (двойной набор позиции).
        if (lcfg.live) live.pollFees(now, events);   // [F54]
        // [F53] breaker: > MM_LIVE_MAX_FILLS_MIN реальных филлов за 60 c ->
        // снять ордера пары и пауза MM_LIVE_BREAKER_SEC (защита от «сжигания»)
        if (lcfg.live && lcfg.maxFillsMin > 0) {
            for (int i = 0; i < 2; ++i) {
                long d = live.realFillEv[i] - C.fillEvSeenBr[i];
                for (long k = 0; k < d && k < 1000; ++k) C.fillTimes.push_back(now);
                C.fillEvSeenBr[i] = live.realFillEv[i];
            }
            while (!C.fillTimes.empty() && now - C.fillTimes.front() > 60.0)
                C.fillTimes.pop_front();
            if ((int)C.fillTimes.size() > lcfg.maxFillsMin && now >= C.breakerUntil) {
                C.breakerUntil = now + lcfg.breakerSec;
                ++C.breakerCnt;
                char bb[200];
                std::snprintf(bb, sizeof bb,
                    "[LIVE BREAKER] %zu реальных филлов за 60 c (лимит %d) -> "
                    "ордера сняты, пауза %.0f s", C.fillTimes.size(),
                    lcfg.maxFillsMin, lcfg.breakerSec);
                evAdd(events, bb);
                lprintf("[%s] %s\n", C.symbol.c_str(), bb);
                C.fillTimes.clear();
                tr.requestCancel(tr.buy, now, events);
                tr.requestCancel(tr.sell, now, events);
                live.cancelAll(events);
            }
        }
        if (lcfg.live && lcfg.realSync) {
            for (int i = 0; i < 2; ++i) {
                SideBook& sbx = (i == 0) ? tr.buy : tr.sell;
                // проверка через 0.5 c после филла: публичный принт мог
                // прийти позже приватной сделки
                if (live.realFillEv[i] != live.realFillSeen[i] &&
                    now - live.realFillAt[i] >= 0.5) {
                    live.realFillSeen[i] = live.realFillEv[i];
                    // пробитие = публичный принт агрессора по нашей стороне
                    // (sell-агрессор для BUY) ЗА ценой нашего ордера в
                    // последние 3 c — уровень прошли насквозь, как toxic/
                    // cutoff в paper (там — по принту против цены ордера)
                    double P = live.realFillPx[i];
                    double worst = 0;
                    for (const auto& r : tr.recentPr) {
                        if (r[0] < live.realFillAt[i] - 3.0) continue;
                        if (i == 0 && (int)r[2] == 2 && (worst <= 0 || r[1] < worst)) worst = r[1];
                        if (i == 1 && (int)r[2] == 1 && r[1] > worst) worst = r[1];
                    }
                    if (P > 0 && worst > 0) {
                        double eps = tick * 0.49;
                        bool thru = (i == 0) ? (worst < P - eps) : (worst > P + eps);
                        double depthBps = std::fabs(P - worst) / P * 1e4;
                        double best = worst;
                        if (thru) {
                            sbx.lastSweepAt = now;
                            ++tr.realSweepCnt;
                            bool cut = cfg.cutoffBps > 0 && depthBps > cfg.cutoffBps;
                            if (cut) {
                                ++tr.cutCnt;
                                double until = now + cfg.cutoffSec;
                                if (i == 0) tr.cutPauseBuyUntil = std::max(tr.cutPauseBuyUntil, until);
                                else        tr.cutPauseSellUntil = std::max(tr.cutPauseSellUntil, until);
                            }
                            char rb[224];
                            std::snprintf(rb, sizeof rb,
                                "[REAL_SWEEP] %s: реальный филл @ %.10g, уровень пробит "
                                "(принт %.10g, %.0f бп) -> cooldown %.0f s%s",
                                i == 0 ? "BUY " : "SELL", P, best, depthBps,
                                cfg.sweepCooldownS, cut ? " + cutoff" : "");
                            evAdd(events, rb);
                        }
                    }
                }
                if (live.realFull[i]) {
                    live.realFull[i] = false;
                    // только если shadow всё ещё держит ТОТ ЖЕ ордер, что
                    // зеркалил реальный (shadow мог уже сам исполниться и
                    // выставить новый — его не трогаем)
                    int fid = live.fullMirId[i];
                    if (fid != 0 && sbx.hasNext && sbx.nxt.id == fid) {
                        // реальный зеркалил отложенный реквот: он уже исполнен
                        sbx.hasNext = false;
                        ++tr.realSyncCnt;
                        char rb[160];
                        std::snprintf(rb, sizeof rb,
                            "[REAL_SYNC] %s #%d: реальный ордер исполнен, отложенный "
                            "shadow-реквот снят", i == 0 ? "BUY " : "SELL", fid);
                        evAdd(events, rb);
                    } else if (fid != 0 && sbx.cur.id == fid) {
                        tr.realSync(sbx, now, events);
                    }
                }
            }
        }
        if (lcfg.live && tr.invInitialized) {
            double sprNow = book.ok() ? (book.bestAsk() - book.bestBid()) : 0;
            double rqLive = std::max(1.5 * tick, cfg.requoteFrac * sprNow);
            for (int sideIdx = 0; sideIdx < 2; ++sideIdx) {
                const SideBook& sb = (sideIdx == 0) ? tr.buy : tr.sell;
                bool want = !(sb.st == QState::NONE ||
                              (sb.st == QState::WAIT_CANCEL && !sb.hasNext));
                double dpx = 0, dqty = 0;
                if (want) {
                    dpx = sb.hasNext ? sb.nxt.price : sb.cur.price;
                    dqty = sb.hasNext ? (sb.nxt.qty - sb.nxt.filled)
                                      : (sb.cur.qty - sb.cur.filled);
                    // [fix 3.1-F1] желаемое -= РЕАЛЬНОЕ исполнение биржи:
                    // shadow.filled и ls.dealtQty расходятся на частичных
                    // филлах; без коррекции place создал бы дубль-ордер.
                    if (live.s[sideIdx].open && live.s[sideIdx].dealtQty > 1e-15)
                        dqty -= live.s[sideIdx].dealtQty;
                    if (dqty * dpx < si.minNotional) want = false;
                }
                live.ensureSide(sideIdx, want, dpx, dqty, rqLive, now, events);
                // [F50] запомнить, какой shadow-ордер зеркалит новый реальный
                if (live.s[sideIdx].open && live.s[sideIdx].cid != live.mirCid[sideIdx]) {
                    live.mirCid[sideIdx] = live.s[sideIdx].cid;
                    live.mirId[sideIdx] = sb.hasNext ? sb.nxt.id : sb.cur.id;
                }
            }
        }
    };

    // ---------------- periodic report (per-symbol; формат строки — как v2.16,
    // чтобы sum9/R_REP продолжал парсить; символ — в конце строки) -----------
    auto reportSymbol = [&](SymCtx& C, double now) {
        auto& tr = C.tr; auto& book = C.book; auto& flow = C.flow;
        auto& live = C.live;
        // [v2.13.1] сигнал о расхождении часов (send vs deal)
        {
            double skw = tr.clockSkewMs();
            if (std::fabs(skw) > 2000.0 && !tr.skewAlerted) {
                tr.skewAlerted = true;
                char b[160];
                std::snprintf(b, sizeof b,
                              "[SKEW   ] send-vs-deal clock offset %+.0f ms "
                              "(гейт в deal-домене, сортировка компенсируется)", skw);
                evAdd(C.events, b);
            } else if (std::fabs(skw) < 1000.0 && tr.skewAlerted) {
                tr.skewAlerted = false;
                char b[128];
                std::snprintf(b, sizeof b, "[SKEW   ] recovered: %+.0f ms", skw);
                evAdd(C.events, b);
            }
        }
        tr.rttMs = rttMs;
        double mid = book.ok() ? book.mid() : tr.lastMid;
        if (mid > 0) {
            double invPnl = tr.base0 * (mid - tr.mid0);
            double mmPnl = tr.totalPnl(mid) + tr.setupCost - invPnl;
            double qbuy = (tr.buy.st == QState::LIVE) ? tr.buy.queueAhead : -1.0;
            double qsell = (tr.sell.st == QState::LIVE) ? tr.sell.queueAhead : -1.0;
            lprintf(
                "[%s] mid %.10g | spr %.2f bps | imb %.2f | tfi %+.2f | run %+d | "
                "pos %+.1f | mm %+.2f inv %+.2f | eq %.2f | P&L %+.4f real %+.4f | "
                "fills %d (b%d/s%d p%d sw%d late%d) | vol %.0f | q %.1f/%.1f | "
                "thr %d rpl %d | mo %.0f/%.0f/%.0f | pxmo %.0f/%.0f/%.0f | "
                "txmo %.0f/%.0f/%.0f bps | %s\n",
                nowWallClock().c_str(), mid, book.spreadBps(), book.imbalance(),
                flow.tfi(now), flow.run(now),
                tr.posValue(mid), mmPnl, invPnl,
                tr.equity(mid), tr.totalPnl(mid), tr.realized,
                tr.fills, tr.buyFills, tr.sellFills, tr.partialFills,
                tr.forcedFills, tr.lateFills, tr.volumeTraded,
                qbuy, qsell, tr.throttled, tr.replaces,
                tr.markout(0), tr.markout(1), tr.markout(2),
                tr.proxyMarkout(0), tr.proxyMarkout(1), tr.proxyMarkout(2),
                tr.takerMarkout(0), tr.takerMarkout(1), tr.takerMarkout(2),
                C.symbol.c_str());
            // [v3.0] вторая строка — РЕАЛЬНЫЙ учёт (истина с биржи)
            if (lcfg.live) {
                live.ledger.updateMarkouts(now, mid);
                std::string bOrd = live.s[0].open
                    ? fmtDec(live.s[0].px) : std::string("-");
                std::string sOrd = live.s[1].open
                    ? fmtDec(live.s[1].px) : std::string("-");
                lprintf(
                    "      [LIVE %s] pos %+.10g (%+.1f USDT) | P&L %+.4f "
                    "real %+.4f | fills %d (b%d/s%d g%d/%d) | vol %.0f | "
                    "ord B %s / S %s | free %.2f USDT, %.10g %s | drift %.3g\n",
                    C.symbol.c_str(),
                    live.ledger.baseInt, live.ledger.posValue(mid),
                    live.ledger.sessionPnl(mid), live.ledger.realized,
                    live.ledger.fills, live.ledger.buyFills,
                    live.ledger.sellFills, live.ledger.ghostFillsWs,
                    live.ledger.ghostFillsRecon, live.ledger.volume,
                    bOrd.c_str(), sOrd.c_str(),
                    live.ledger.quoteFree, live.ledger.baseFree,
                    live.baseAsset.c_str(), live.ledger.driftMaxBase);
            }
        }
        if (!tr.invInitialized)
            lprintf("      [STATUS %s] WAIT-INIT (retry in %.0f s)\n",
                    C.symbol.c_str(), std::max(0.0, C.retryInitAt - now));
        else if (tr.halted)
            lprintf("      [STATUS %s] HALTED: loss limit hit\n", C.symbol.c_str());
        flushEvents(C);
    };

    // ---------------- SUMMARY per-symbol в произвольный FILE* ----------------
    // Формат строк — идентичен v2.16/v3.0 (sum9-регексы не тронуты).
    // sessionHeader=true -> "SESSION SUMMARY" (per-pair файлы для sum9);
    // false -> "PAIR SUMMARY" (комбинированный лог/консоль; sum9 его
    // пропускает, не плодя мусорных записей).
    auto printSummary = [&](SymCtx& C, FILE* fp, bool sessionHeader) {
        auto& tr = C.tr; auto& cfg = C.tr.cfg; auto& book = C.book;
        auto& flow = C.flow; auto& live = C.live;
        double mid = book.ok() ? book.mid() : tr.lastMid;
        tr.updateMarkouts(nowSecs(), mid);
        double invPnl = tr.base0 * (mid - tr.mid0);
        double mmPnl = tr.totalPnl(mid) + tr.setupCost - invPnl;
        double bb = book.ok() ? book.bestBid() : mid;
        double liqPnl = tr.totalPnl(mid) - std::max(0.0, tr.base) * (mid - bb);
        std::fprintf(fp,
            "\n================================================================\n"
            "  %s\n"
            "================================================================\n"
            "  symbol          : %s\n"
            "  duration        : %.0f s   (ws sessions: %d)\n"
            "  ws messages     : %ld (bin %ld, depth %ld, ticker %ld, deals %ld, parseFails %ld, lateEv %ld)\n"
            "  tick used       : %.10g   rtt: %.0f ms\n"
            "  last mid        : %.10g\n"
            "  ------------------------------\n"
            "  total P&L       : %+.4f USDT (%+.3f%%)\n"
            "    setup cost    : %+.4f  (полспреда + fee при INIT; 0 в RAMP)\n"
            "    inv drift     : %+.4f  (дрейф стартового инвентаря; 0 в RAMP)\n"
            "    MM P&L        : %+.4f  (заработок СТРАТЕГИИ)\n"
            "    liq P&L       : %+.4f  (с учётом ликвидации по bid)\n"
            "  realized (avg-cost): %+.4f    fees: %.4f\n"
            "  fills           : %d (b%d/s%d)  partial %d  toxic %d  LATE %d (gap %d)\n"
            "  cancels/replaces: %d / %d    throttled: %d\n"
            "  traded volume   : %.0f USDT   P&L/объём (mm): %+.2f bps\n"
            "  sell undersized : %.0f s\n"
            "  ------------------------------ markout ------------------------------\n"
            "  mo    5/30/60c: %+.1f / %+.1f / %+.1f bps\n"
            "  pxmo  5/30/60c: %+.1f / %+.1f / %+.1f bps\n"
            "  txmo  5/30/60c: %+.1f / %+.1f / %+.1f bps\n"
            "  ---------------------------------------------------------------------\n"
            "  final TFI/run   : %+.2f / %+d\n"
            "  INIT status     : %s\n"
            "================================================================\n",
            sessionHeader ? "SESSION SUMMARY (spred v3.1)"
                          : "PAIR SUMMARY (spred v3.1)",
            C.symbol.c_str(),
            nowSecs() - t0, sessions, msgCount, binFrames, C.depthFrames,
            C.tickerFrames, C.dealFrames, parseFails, C.lateEvents,
            C.tick, tr.rttMs, mid,
            tr.totalPnl(mid), tr.totalPnl(mid) / tr.initialQuote * 100,
            tr.setupCost, invPnl, mmPnl, liqPnl,
            tr.realized, tr.feesPaid,
            tr.fills, tr.buyFills, tr.sellFills, tr.partialFills,
            tr.toxicFills, tr.lateFills, tr.gapFills,
            tr.cancels, tr.replaces, tr.throttled,
            tr.volumeTraded,
            tr.volumeTraded > 0 ? mmPnl / tr.volumeTraded * 1e4 : 0.0,
            tr.sellSmallSecs,
            tr.markout(0), tr.markout(1), tr.markout(2),
            tr.proxyMarkout(0), tr.proxyMarkout(1), tr.proxyMarkout(2),
            tr.takerMarkout(0), tr.takerMarkout(1), tr.takerMarkout(2),
            flow.tfi(nowSecs()), flow.run(nowSecs()),
            tr.invInitialized
                ? (cfg.initMode == 1 ? "RAMP (passive inventory build)"
                                     : "inventory bought")
                : "NEVER INITIALIZED");
        // [v2.12 Ф10] + [v2.13] распределения в тех же строках
        std::fprintf(fp, "  ------------------------- fills by reason -------------------------\n");
        for (int r = 0; r < FR_N; ++r) {
            if (tr.reasonCnt(r) == 0) continue;
            const std::vector<double>& rv = tr.moR30[r];
            std::fprintf(fp, "  %-8s n=%-4d  mo %+.1f/%+.1f/%+.1f  med %+.1f  p05 %+.1f  "
                    "p95 %+.1f  $%+.3f (30c)\n",
                    FR_NAMES[r], tr.reasonCnt(r),
                    tr.reasonMo(r, 0), tr.reasonMo(r, 1), tr.reasonMo(r, 2),
                    moPct(rv, 0.5), moPct(rv, 0.05), moPct(rv, 0.95),
                    tr.reasonUsd(r, 1));
        }
        std::fprintf(fp, "  ---------------------- mo 30c distribution -----------------------\n");
        const std::vector<double>& a30 = tr.moH[1];
        if (!a30.empty())
            std::fprintf(fp, "  ALL    n=%-5ld p05 %+.1f | p25 %+.1f | med %+.1f | p75 %+.1f | "
                    "p95 %+.1f | worst %+.1f | best %+.1f bps\n",
                    (long)a30.size(),
                    moPct(a30, 0.05), moPct(a30, 0.25), moPct(a30, 0.5),
                    moPct(a30, 0.75), moPct(a30, 0.95),
                    moPct(a30, 0.0), moPct(a30, 1.0));
        if (!tr.moBuy30.empty())
            std::fprintf(fp, "  BUY    n=%-5ld med %+.1f  p05 %+.1f  p95 %+.1f bps\n",
                    (long)tr.moBuy30.size(), moPct(tr.moBuy30, 0.5),
                    moPct(tr.moBuy30, 0.05), moPct(tr.moBuy30, 0.95));
        if (!tr.moSell30.empty())
            std::fprintf(fp, "  SELL   n=%-5ld med %+.1f  p05 %+.1f  p95 %+.1f bps\n",
                    (long)tr.moSell30.size(), moPct(tr.moSell30, 0.5),
                    moPct(tr.moSell30, 0.05), moPct(tr.moSell30, 0.95));
        if (tr.escCnt > 0)
            std::fprintf(fp, "  escape slippage : n=%d mean %+.1f bps worst %+.1f bps "
                    "(realized vs expected VWAP)\n",
                    tr.escCnt, tr.escSlipSum / tr.escCnt, tr.escSlipWorst);
        std::fprintf(fp, "  fees            : maker %.4f / taker %.4f / total %.4f USDT\n",
                tr.makerFees, tr.takerFees, tr.feesPaid);
        std::fprintf(fp, "  gap             : certain %d / possible %d / filled %d "
                "(MM_GAP_FILL_MODE=%d)\n",
                tr.gapCertain, tr.gapPossible, tr.gapFills, cfg.gapFillMode);
        std::fprintf(fp, "  clock           : skew send-vs-deal max %.0f ms, last %.0f ms; "
                "quarantined deals %d (MM_QUAR_MS=%.0f)\n",
                tr.skewMaxMs, tr.clockSkewMs(), tr.skewQuar, cfg.quarantineMs);
        std::fprintf(fp, "  risk-add        : init %s | esc-cooldown %.0f s | cutoff "
                "%.0f bps / %.0f s (events %d)\n",
                cfg.initMode == 1 ? "RAMP" : "classic",
                cfg.escapeCooldownS, cfg.cutoffBps, cfg.cutoffSec, tr.cutCnt);
        std::fprintf(fp, "  v3.2.1 F56-58   : loss-basis %s | halt-flat %d (попыток %d) | "
                "trend %.0f bps / %.0f s: bid OFF %.0f s, ask OFF %.0f s\n",
                cfg.lossBasis == 1 ? "mm" : "total", cfg.haltFlat, C.haltFlatTries,
                cfg.trendBps, cfg.trendSec, C.trendBlockBuyS, C.trendBlockSellS);
        std::fprintf(fp, "  print-filter    : mode %d (окно %.0f ms) | принтов %ld, "
                "внутри спреда %ld (%.0f%%), срезано филлов %ld\n",
                cfg.printFilter, cfg.printFilterMs, tr.prAll, tr.prInside,
                tr.prAll > 0 ? 100.0 * tr.prInside / tr.prAll : 0.0, tr.prCutFills);
        std::fprintf(fp, "  v3.3 F60-63     : esc-maker %d (старт %ld, мейкером %ld, MARKET %ld, "
                "ср. %.0f s) | ESCAPE market %ld | imb %.1f bps micro %.2f ср.сдвиг %+.2f | "
                "AS g %.3g tau %.0f: ср.|сдвиг| %.2f макс %.2f bps, sigma %.2f bps/sqrt(s)\n",
                C.escMk, C.escMkStarts, C.escMkDone, C.escMkMarket,
                (C.escMkDone + C.escMkMarket) > 0 ? C.escMkTime / (C.escMkDone + C.escMkMarket) : 0.0,
                C.escMarketCnt, C.fairImbBps, C.fairMicro,
                C.sigN > 0 && C.fairImbBps != 0 ? C.sigSum / C.sigN : 0.0,
                C.asGamma, C.asTau, C.sigN > 0 ? C.asSum / C.sigN : 0.0, C.asAbsMax,
                std::sqrt(std::max(0.0, C.asVar)));
        if (g_fairMode)   // [F55]
            std::fprintf(fp, "  fair (F55)      : bybit | d %.1f bps skew %.1f rq %.1f | basis %+.1f bps "
                    "(ср. откл. %.1f) | тиков %ld: ok %ld, stale %ld, dev %ld, прогрев %ld | "
                    "bybit msgs %ld reconn %ld\n",
                    C.fairD, C.fairSkew, C.fairRq, C.basis * 1e4,
                    C.fairTicks > C.fairStale ? C.devSum / (double)(C.fairTicks - C.fairStale) : 0.0,
                    C.fairTicks, C.fairOk, C.fairStale, C.fairDevCnt, C.fairWarm,
                    g_fairMsgs.load(), g_fairReconn.load());
        if (cfg.printFillP > 0)   // [F49] строка только при p>0 (формат =0/=1 как v3.1.9)
            std::fprintf(fp, "  print-fill-p    : p %.3f | пропущено внутренних филлов %ld "
                    "(срезано %ld)\n", cfg.printFillP, tr.prPassFills, tr.prCutFills);
        std::fprintf(fp, "  sim-add         : queue %d | escSkip %d | self %d | replay %s\n",
                cfg.queueMode, cfg.escapeSkipLevels, cfg.selfInBook,
                g_replay ? replayPath.c_str() : "off");
        std::fprintf(fp, "  honesty         : mo — верхняя граница (нет конкурентов за\n"
                "                    очередь); fill-модель — контрфакт FIFO\n"
                "================================================================\n");
        // ================= [v3.0] LIVE-блок: РЕАЛЬНЫЙ учёт
        if (lcfg.live) {
            live.ledger.updateMarkouts(nowSecs(), mid);
            std::string feeOth = live.ledger.feesOther > 0
                ? " + " + fmtDec(live.ledger.feesOther) + " " + live.ledger.feesOtherCur
                : "";
            const LiveSide& sb = live.s[0];
            const LiveSide& ss = live.s[1];
            std::fprintf(fp,
                "  ===================== LIVE (реальная торговля) ====================\n"
                "  LIVE session P&L  : %+.4f USDT  (quoteNet %+.4f + pos %.10g x mid)\n"
                "  LIVE realized     : %+.4f (avg-cost)   fees: %.4f USDT + %.10g %s%s\n"
                "  LIVE позиция      : %+.10g %s (%+.2f USDT), base0 %.10g\n"
                "  LIVE кошелёк      : %.4f %s free (locked %.4f) | %.10g %s free "
                "(locked %.10g)\n"
                "  LIVE fills        : %d (b%d/s%d)   vol %.2f USDT   P&L/объём %+.2f bps\n"
                "  LIVE markout 5/30/60c   : %+.1f / %+.1f / %+.1f bps (vol-w)\n"
                "  LIVE esc-markout 5/30/60: %+.1f / %+.1f / %+.1f bps\n"
                "  LIVE ghost        : ws %d, recon %d, drift max %.6g %s\n"
                "  LIVE fill_cal     : shadow fills %d vs real %d; shadow P&L %+.4f vs "
                "real %+.4f (расхождение = цена модели)\n"
                "  LIVE REST         : places %d/%d (fail %d/%d), cancels %d/%d "
                "(fail %d/%d), resolves %d, reconEv %d, orphanCancels %d, "
                "errCount %d, timeSyncs %d\n"
                "  LIVE REST lat ms  : place avg %.0f max %.0f | cancel avg %.0f "
                "max %.0f\n"
                "  LIVE privWS msgs  : orders %d, deals %d, account %d\n"
                "================================================================\n",
                live.ledger.sessionPnl(mid), live.ledger.quoteNet, live.ledger.baseInt,
                live.ledger.realized, live.ledger.feesQuote, live.ledger.feesBase,
                live.baseAsset.c_str(), feeOth.c_str(),
                live.ledger.baseInt, live.baseAsset.c_str(),
                live.ledger.posValue(mid), live.ledger.base0,
                live.ledger.quoteFree, live.quoteAsset.c_str(),
                live.ledger.quoteLocked,
                live.ledger.baseFree, live.baseAsset.c_str(), live.ledger.baseLocked,
                live.ledger.fills, live.ledger.buyFills, live.ledger.sellFills,
                live.ledger.volume,
                live.ledger.volume > 0
                    ? live.ledger.sessionPnl(mid) / live.ledger.volume * 1e4 : 0.0,
                live.ledger.markout(0), live.ledger.markout(1), live.ledger.markout(2),
                live.ledger.markoutE(0), live.ledger.markoutE(1), live.ledger.markoutE(2),
                live.ledger.ghostFillsWs, live.ledger.ghostFillsRecon,
                live.ledger.driftMaxBase, live.baseAsset.c_str(),
                tr.fills, live.ledger.fills,
                tr.totalPnl(mid), live.ledger.sessionPnl(mid),
                sb.places, ss.places, sb.placeFails, ss.placeFails,
                sb.cancels, ss.cancels, sb.cancelFails, ss.cancelFails,
                live.resolves, live.reconEvents, live.orphanCancels,
                live.rc.errCount, live.rc.timeSyncs,
                sb.places + ss.places > 0
                    ? (sb.placeLatSum + ss.placeLatSum) / (sb.places + ss.places) : 0.0,
                std::max(sb.placeLatMax, ss.placeLatMax),
                sb.cancels + ss.cancels > 0
                    ? (sb.cancelLatSum + ss.cancelLatSum) / (sb.cancels + ss.cancels) : 0.0,
                std::max(sb.cancelLatMax, ss.cancelLatMax),
                live.wsPrivOrders, live.wsPrivDeals, live.wsPrivAcct);
            std::fprintf(fp, "  LIVE sync (F50)   : %s | REAL_SYNC %d, REAL_SWEEP %d "
                    "(cooldown/cutoff от реальных филлов)\n"
                    "================================================================\n",
                    lcfg.realSync ? "on" : "off (MM_LIVE_SYNC=0)",
                    tr.realSyncCnt, tr.realSweepCnt);
            std::fprintf(fp, "  LIVE fees (F54)   : myTrades запросов %d (ошибок %d), сделок с "
                    "комиссией %d, из них TAKER %d | privWS deals %d | breaker %d\n",
                    live.feeLookups, live.feeFail, live.feeTrades, live.feeTaker,
                    live.wsPrivDeals, C.breakerCnt);
        }
        std::fflush(fp);
    };

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
