# Original configuration and change history

Preserved from the original src/spred.cpp. Historical line references may no longer apply.

============================================================================

spred.cpp v3.3 — Paper + LIVE-Trading Market-Making Bot (spread collection)



v3.3 over v3.2.1 (всё за флагами, по умолчанию поведение v3.2.1):

  F60  MM_ESC_MAKER(0): при |pos| > MM_MAX_POS вместо MARKET-escape —

       мейкерская заявка на сокращение по fair*(1±MM_ESC_MAKER_BPS/1e4)

       размером до излишка (|pos|-0.75*band), наращивающая сторона снята.

       MARKET — только если не ушли за MM_ESC_MAKER_SEC (180) или

       |pos| > MM_ESC_MAKER_HARD (1.5) * MM_MAX_POS. Режим до |pos|<=0.75*band.

  F61  MM_SHARED_RATE(0): общий лимитер REST для всех процессов одного UID

       (/dev/shm/spred_rate_<uid>, flock): MM_SR_NEW 5 новых/с, MM_SR_TOTAL

       12 запросов/с (окно MM_SR_WIN_MS 1100), MM_SR_CANCEL_RESERVE 2 слота

       только для отмен (приоритет отмен), пауза после 429/418 общая.

       Перестановка = отмена+новая; при отказе лимитера ставится последняя

       желаемая цена (повторные перестановки сливаются). MM_SR_PAPER=1 —

       тот же лимитер в paper (оценка ограничений на 12 парах).

  F62  fair-сигналы: MM_FAIR_MICRO w (0) — микроцена Bybit вместо мида

       (fair = mid + w*(micro-mid)); MM_FAIR_IMB_BPS (0) — сдвиг котировки

       (imb-0.5)*2*X bps по дисбалансу 10 уровней MEXC; MM_FAIR_SIG_MAX_BPS

       (0.5*d) — потолок сигнального сдвига.

  F63  Avellaneda–Stoikov: MM_AS_GAMMA g (0=выкл): сдвиг резервной цены

       -(pos/band)*g*sigma^2*MM_AS_TAU (60 c), sigma^2 — EWMA (MM_AS_VOL_SEC

       300) секундных приращений fair в bps^2/c; потолок MM_AS_MAX_BPS (d).

       Задумано вместо F58 (MM_TREND_BPS=0).

  F64  USDC-пары: Bybit-символ для fair = BASE+USDT (MM_FAIR_USDC_MAP=1).



v3.2.1 over v3.2.0 (по итогам ночи 05->06.10: HALT держал позицию, стоп

  срабатывал от стартового запаса, бот докупал в падающих парах):

  F56  MM_LOSS_BASIS(1): paper-стоп MM_MAX_LOSS по mm-P&L стратегии

       (totalPnl + setupCost - base0*(mid-mid0)), как sessionPnl в live.

       0 = как раньше (полный P&L со стартовым запасом).

  F57  MM_HALT_FLAT(1): при HALT закрыть позицию стратегии тейкером

       (paper: через pendEsc с задержкой MM_LATENCY; live: MARKET, повтор

       каждые MM_ESCAPE_SECS, до 5 попыток, пока |pos| >= minNotional).

       0 = как раньше (снять ордера и держать позицию).

  F58  MM_TREND_BPS(0=выкл, @SYM) / MM_TREND_SEC(900, @SYM): если mid за окно

       упал больше порога — не наращивать лонг (снять bid, ask остаётся);

       вырос — не наращивать шорт. Лог [TREND], в сводке секунды блока.

  F59  MM_TAKER_BPS@SYM — тейкер-комиссия по парам (0.064% -> 6.4, 0.04% -> 4).



v3.2.0 over v3.1.10:

  F55  MM_FAIR=bybit — котирование от ВНЕШНЕЙ справедливой цены. Отдельный поток

       держит WS wss://stream.bybit.com/v5/public/spot (orderbook.1.SYM, ping 20 с,

       реконнект с backoff). fair = Bybit-мид * exp(базис), базис = EWMA

       log(MEXC-мид/Bybit-мид) с постоянной MM_FAIR_BASIS_SEC(600). Котировки

       bid = fair*(1-d), ask = fair*(1+d), d = MM_FAIR_D_BPS(15, @SYM), bid вниз /

       ask вверх к тику, post-only кламп как раньше. Сдвиги imb/tfi/inv-от-спреда

       выключены; инвентарный сдвиг MM_FAIR_SKEW_BPS(0) * pos/MAX_POS. Реквот при

       сдвиге >= MM_FAIR_RQ_BPS(max(2, 0.2*d)). Нет свежей цены Bybit дольше

       MM_FAIR_STALE_MS(1500) / базис уехал > MM_FAIR_MAX_DEV_BPS(100) / прогрев

       MM_FAIR_WARM_SEC(30) -> обе стороны снимаются. Тик стратегии каждые <=50 мс.

       Логи [FAIR], строка "fair (F55)" в итогах. В replay выключен (нет Bybit в

       записи). MM_FAIR_HOST/PORT/PATH — для теста. Без MM_FAIR = v3.1.10 побайтно.

C++17, OpenSSL only. Linux / WSL.



v3.1.1 over v3.1 — ФИКСЫ (аудит кода + проверка на локальном TLS-моке):

  F1 [CRIT] signedReq: буфер подписи char[64] обрезал POST/DELETE (place/

         cancel/getOrder/market -> 700002, при этом openOrders/account

         проходили). Теперь std::string (buildSignedQs).

  F2 [SEC]  TLS: проверка сертификата и имени хоста (REST, нативный

         fallback, публичный и приватный WS). MM_TLS_NOVERIFY=1 — только

         для мок-стенда. Нужен пакет ca-certificates.

  F3 [CRIT] SIGPIPE игнорируется: запись в оборванный сокет убивала процесс

         без cancelAll (ордера оставались на бирже).

  F4 [LIVE] учёт филлов: кредит-схема (dealtQty/dealSeenQty/closedOrds) —

         нет двойного учёта, если апдейт ордера пришёл раньше сделки, при

         PARTIALLY_CANCELED и при цепочке реквотов; fee не теряется.

  F5 [DIAG] pxmo/txmo: знаменатели копились как числитель (метрики были

         тождественно 1/0). На торговлю не влияло.

  F6 разное: длины в protobuf/WS без переполнения (hostile frame), утечка

         при неудачном WS-handshake, cid-счётчик общий (нет коллизий между

         парами), sell в live только из инвентаря СЕССИИ

         (MM_SELL_WALLET_BASE=1 = старое поведение), MM_QTY_DRIFT_USDT

         (0 = как в v3.1), va_end в lprintf, баннер читает реальные

         minDeals/runDecay, дефолты в шапке приведены к коду.

  F7 [LIVE] fmtDec: цены/qty уходили с мусорными знаками (0.2999..89).

  F8 [LIVE] HttpClient: ответ из 3+ HTTP-чанков висел до таймаута.

  F9 [LIVE] дедуп приватных сделок по tradeId (повтор = двойной учёт).

  F10 [LIVE] нет exchangeInfo -> live не стартует на запасном tick/qtyStep.



v3.1.3 over v3.1.2:

  F11 [LIVE] HttpClient: fd сокета не закрывался при disconnect (BIO_NOCLOSE)

         -> утечка одного дескриптора на каждый переподключение.

  F12 [LIVE] обрыв публичного WS -> cancelAll всех пар до реконнекта

         (как обещано в шапке; MM_CANCEL_ON_WS_LOSS=0 отключает).

  F13 [LIVE] приватный WS: PING раз в 15 с, watchdog 45 с тишины ->

         реконнект; после реконнекта resolve открытых ордеров (добор

         филлов, пропущенных в окне обрыва).

  F14 [LIVE] resolve: «закрыт» только по -2011/-2013 «Unknown order /

         does not exist». Rate-limit/5xx/700003 -> backoff, ордер

         считается возможно живым. reconcile «воскрешает» сторону, если

         живой ордер биржи совпал с её cid; на shutdown повторный cancelAll.

  F15 [LIVE] F10 строже: live требует И tick, И qtyStep из exchangeInfo.

  F16 [NET]  WS: таймаут connect (5 с) и чтения/записи кадра (10 с);

         SIGINT/SIGTERM без SA_RESTART (Ctrl+C прерывает блокирующий read).

  F17 [LIVE] cleanStart: openOrders с 3 попытками; не ответил -> live не

         стартует (иначе ордера прошлого запуска остаются висеть).

  F18 [LIVE] 700003 при place: timeSync и один немедленный повтор.



v3.1.4 over v3.1.3 (найдено smoke-прогоном live-контура на моке с отказами):

  F19 [CRIT] HttpClient автоматически ПОВТОРЯЛ POST после обрыва без ответа.

         Если биржа ордер уже приняла, повтор с тем же cid отклонялся,

         сторона считалась «не выставленной», живой ордер становился

         сиротой, а его филлы терялись (на моке позиция бота разошлась с

         биржей на 65 X). Теперь POST повторяется только если запрос не

         ушёл в сокет; иначе transportOk=false -> resolve по cid.

         Плюс проверка keep-alive перед запросом: сервер закрыл коннект ->

         переподключение ДО отправки (меньше неоднозначных обрывов).

  F20 [LIVE] успешный cancel: executedQty из ответа не учитывался.

         Частичный филл, не пришедший по приватному WS, терялся.

  F21 [LIVE] снятие сироты в reconcile: её executedQty не учитывался.

         Теперь дельта сверх уже пришедших по WS сделок проводится в ledger.

  F22 [LIVE] PLACE-FAIL пишет cid (для разбора логов).

  F23 [LIVE] escape MARKET: исполнение учитывалось ТОЛЬКО по приватному WS.

         Пропуск сделки (обрыв WS) или транспортный сбой POST = позиция

         бота расходится с биржей. Теперь escape-ордер сверяется через

         getOrder на ближайшем reconcile (дельта сверх WS + кредит).



v3.1.10 over v3.1.9:

  F49 [PAPER] MM_PRINT_FILL_P (0..1, дефолт 0; можно @SYM): при

         MM_PRINT_FILTER>=1 внутренний принт, касающийся нашего уровня,

         с вероятностью p обрабатывается как при =0 (очередь + филл,

         THROUGH/TOXIC/QUEUE/GHOST). p=0 = v3.1.9, p=1 = как =0. RNG

         отдельный, сеется от MM_SEED (replay детерминирован, латентности

         не сдвигаются). В итогах строка print-fill-p (только при p>0).

         Подбор p: сравнить real fills micro-live с replay при разных p.

  F50 [LIVE] зеркало: (1) sweep-cooldown и cutoff ставились только от

         paper-филлов; реальный филл с пробитым уровнем их не включал.

         Теперь: новая реальная qty по стороне + публичный принт агрессора

         за ценой нашего ордера (<=3 c) -> lastSweepAt (MM_SWEEP_COOLDOWN_SEC),

         при глубине > MM_CUTOFF_BPS ещё и cutoff-пауза. Лог [REAL_SWEEP].

         (2) REAL_SYNC: реальный ордер исполнен целиком, а shadow-ордер,

         который он зеркалил, ещё стоит -> shadow-ордер закрывается БЕЗ

         paper-филла (fill_cal не искажается). Раньше зеркало через тик

         ставило такой же ордер повторно = двойной набор позиции.

         Лог [REAL_SYNC]; итог: строка LIVE sync. MM_LIVE_SYNC=0 — выкл.

         Paper/replay F50 не затрагивает.

  F51 [LIVE] MEXC 700013 Invalid content Type на POST /api/v3/userDataStream:

         POST/PUT с пустым телом слались с form-urlencoded. Теперь все

         параметры подписанных запросов — в query, тело пустое, Content-Type

         application/json (как официальный SDK); listenKey — SIGNED

         (key-only как запасной). MM_REST_FORM_BODY=1 — старый формат.

  F52 [LIVE][CRIT] погоня за собственным ордером: котировки считались от

         bestBid/bestAsk публичной книги, где лучшим уровнем был НАШ

         ордер -> каждый реквот (2 c) на шаг к середине, спред сжимался до

         встречной стороны; бот покупал у ask и продавал у bid (TAG

         05.10: 17 филлов за 1.5 мин, ~-37 bps на круг). Теперь в live

         тик стратегии видит книгу БЕЗ наших ордеров (живые + снятые за

         MM_SELF_EXCL_SEC=2 c, пока книга не обновилась). MM_SELF_EXCL=0 —

         старое поведение. Paper/replay не затронуты.

  F53 [LIVE] breaker: > MM_LIVE_MAX_FILLS_MIN=6 реальных филлов за 60 c ->

         ордера пары сняты, пауза MM_LIVE_BREAKER_SEC=600 ([LIVE BREAKER]).

         Первые ответы приватного WS пишутся в лог (диагностика deals=0).

  F54 [LIVE] комиссии: филлы, пришедшие без приватной сделки (cumQty

         из private.orders, executedQty из REST: GHOST-WS/GHOST-REST,

         escape, сироты), проводились с комиссией 0 -> P&L и позиция

         завышены (05.10 TAG: бот 6219 TAG, биржа 6186). Теперь через ~2 c

         после такого филла GET /api/v3/myTrades?symbol&orderId (права

         SPOT_ACCOUNT_READ, вес 10, <=1 запроса за тик, общий REST-бюджет)

         -> commission/commissionAsset в ledger; maker/TAKER в лог

         [LIVE FEE]. Дедуп по tradeId с private.deals: комиссия каждой

         сделки учитывается ровно один раз. MM_FEE_LOOKUP=0 — выкл.

         Итог: строка LIVE fees (запросы, сделки, TAKER, privWS deals).



v3.1.9 over v3.1.8:

  F47 [PAPER] MM_PRINT_FILTER=1 закрывал только THROUGH/TOXIC/GHOST, а

         принт строго внутри спреда РОВНО по цене нашего ордера давал

         FR_QUEUE-филл (внутри спреда мы одни -> очередь 0). На GME/TAG это

         был единственный оставшийся при =1 филл. Теперь при >=1 внутренний

         принт не даёт филлов вообще и не двигает очередь; режимы 1 и 2

         для филлов эквивалентны (2 оставлен для совместимости).

  F48 [DIAG] «срезано филлов» в print-filter считает только реально

         предотвращённые филлы (раньше — каждый внутренний принт x сторона:

         у ZIL 87 при фактической разнице в 1 филл).



v3.1.8 over v3.1.7 (разбор ленты MEXC: синтетические принты внутри спреда):

  F45 [PAPER] на неликвидных парах ~100% сделок проходят СТРОГО ВНУТРИ

         видимого спреда (регулярно, ~раз в 25-30 c, в середине спреда) —

         признак биржевого объёмного бота. Бумажный движок считал такой

         принт ниже нашего bid «пробитием» (THROUGH) и давал филл, хотя

         принтер, видя наш ордер в книге, поставил бы цену иначе.

         MM_PRINT_FILTER: 0 = как раньше (оптимистично);

         1 (дефолт) = принт строго внутри видимого спреда (выше max bid и

         ниже min ask за окно MM_PRINT_FILTER_MS=1500 до принта, биржевое

         время) НЕ даёт THROUGH/TOXIC/GHOST-филл; [v3.1.9 F47: и филл «в

         уровень» тоже]; 2 = то же (для совместимости).

         В итогах: строка print-filter (всего/внутри/срезано филлов).

         Сравнивайте paper с 0 и 1: правда — между ними, решает LIVE.

  F46 [LIVE] MEXC: не более 30 подписок на одно WS-соединение, у бота 3

         канала на пару -> >10 пар молча теряли подписки. Теперь >10 пар

         в одном процессе = FATAL (запускайте несколько процессов).



v3.1.7 over v3.1.6 (сверка с актуальной документацией MEXC Spot V3):

  F41 [CRIT] после HTTP 429/418 бот продолжал слать запросы (другие пары,

         recon, balance) -> по докам это путь к бану IP (2 мин..3 дня).

         Теперь глобальная пауза REST по Retry-After (429 без заголовка ->

         2 c, 418 -> 120 c); во время паузы проходят только отмены (DELETE).

  F42 [LIVE] пул весов IP-эндпоинтов 300/10 c: локальный бюджет 250/10 c

         (openOrders 3, getOrder 2, account 10, exchangeInfo 25, order 1).

         Важно на старте с многими парами: exchangeInfo = 25 на пару.

  F43 [LIVE] stpMode по докам — CANCEL_MAKER|CANCEL_TAKER|CANCEL_BOTH

         (верхний регистр). MM_STP_MODE принимается в любом регистре.

  F44 [LIVE] лимит ордеров изменён биржей (change log 2026-03-11): place и

         cancel делят ОДИН бакет 12 запросов/с на аккаунт (старый анонс

         «5 ордеров/с» устарел). Дефолт MM_RATE_RPS 5 -> 10 (запас 2/с).



v3.1.6 over v3.1.5 (второе внешнее ревью, каждый пункт проверен):

  F34 [CRIT] escape MARKET с ответом HTTP 5xx считался «не исполнен» и не

         сверялся. Теперь 5xx = статус неизвестен -> escPending -> getOrder.

  F35 [LIVE] self-trade: escape MARKET мог исполниться о СОБСТВЕННЫЙ

         встречный лимит. Перед escape встречный ордер снимается; не снялся

         -> escape откладывается. Плюс MM_STP_MODE (cancel_maker|cancel_taker|

         cancel_both) -> stpMode в place/market (на MEXC работает только при

         созданной strategy group; по умолчанию пусто = не отправляется).

  F36 [LIVE] лимит MEXC 5 ордеров/с: дефолт MM_RATE_RPS 8 -> 5; escape

         теперь тоже берёт токен; cancelAll ждёт токен, а не обнуляет бюджет.

  F37 [LIVE] MM_KEEP_ORDERS_ON_EXIT=1 больше НЕ отключает cancelAll при

         потере публичного WS (котировки вслепую опаснее, чем выход).

  F38 [LIVE] MM_RECON_ADOPT=1 реально усыновляет ордер бота (свободный

         слот той же стороны: cid/oid/px/qty/executedQty), иначе снимает.

         Ордера прошлых запусков бота (префикс sp*x*) тоже усыновляются.

  F39 [LIVE] clean start снимал ВСЕ ордера пары, включая ручные/чужие.

         MM_LIVE_CLEAN_START=1 — только ордера бота (sp<pid>x<seq>, любой

         pid), =2 — все ордера пары (старое поведение), =0 — без чистки.

  F40 мелочь: unused parameter 'now' в onGhost.



v3.1.5 over v3.1.4 (внешнее ревью, каждый пункт проверен по коду):

  F24 [CRIT] SIGHUP (закрыли SSH/терминал) убивал процесс без cancelAll.

         Теперь SIGHUP/SIGQUIT = штатная остановка. Исключение в главном

         цикле -> лог + штатный shutdown (cancelAll), а не std::terminate.

  F25 [LIVE] бюджет BUY считался от quoteFree, из которого уже вычтен

         НАШ залоченный BUY -> цикл «поставил/снял» при балансе около

         N*(2*ордер+резерв). Свой залоченный quote теперь прибавляется.

  F26 [LIVE] LIMIT_MAKER (post-only) вместо LIMIT GTC: на устаревшей книге

         ордер не исполнится тейкером. MM_POST_ONLY=0 — старое поведение;

         если в exchangeInfo пары нет LIMIT_MAKER — LIMIT с предупреждением.

  F27 [LIVE] чужие приватные сделки (ручная торговля/другой бот на паре)

         больше не попадают в ledger: учитываются только cid/oid этой сессии.

  F28 [LIVE] clean start: результат cancel проверяется, затем повторный

         openOrders; остались ордера -> live не стартует.

  F29 [LIVE] без listenKey (нет приватного WS) live не стартует

         (3 попытки). MM_ALLOW_NO_PRIVWS=1 — работать на REST-поллинге.

  F30 [LIVE] reconcile: executedQty известных открытых ордеров сверяется

         с учтённым -> частичные филлы видны без приватного WS.

  F31 [LIVE] HTTP 5xx на place = статус неизвестен -> resolve по cid

         (раньше «не создан» -> возможный дубль до reconcile).

  F32 [LIVE] qtyDrift сравнивал желаемый ОСТАТОК с исходным qty ордера ->

         лишний cancel/replace после частичного филла >= 50%.

  F33 мелочи: подсказка MM_LIVE_CLEAN_START=0 (было MM_CLEAN_START);

         rand() (маски WS) сеется от времени, если MM_SEED не задан.

  НЕ исправлено (архитектура): REST синхронный — зависший запрос блокирует

         цикл всех пар до таймаута. Лечится отдельным потоком/async.



v3.1 over v3.0 — МУЛЬТИПАРНЫЙ РЕЖИМ: один процесс — N символов

  (argv[1] через запятую или MM_SYMBOLS="TAGUSDT,ZILUSDT,...").

  Общие ресурсы: публичный WS (подписки всех пар на одном коннекте),

  приватный WS (аккаунт-уровень, диспетчер событий по полю symbol),

  REST rate-бюджет на аккаунт (один TokenBucket на всех executor'ов),

  listenKey, серверное время (ресинк один — раздаётся всем).

  Per-символ: shadow-движок v2.16 ЦЕЛИКОМ (book/flow/evq/PaperTrader)

  + LiveExecutor (свой keep-alive HTTPS) + LiveLedger + kill-file.

  Решения ТЗ зашиты в дефолты: остановка (Ctrl+C / halt / потеря

  данных) -> cancelAll ВСЕХ реальных ордеров (MM_KEEP_ORDERS_ON_EXIT=1

  отменяет); стартовый инвентарь — RAMP (MM_INIT_MODE=1 дефолт);

  dry-run по умолчанию, live только MM_LIVE=1 + MM_LIVE_CONFIRM=YES.

  Дефолты конфига = продакшн-набор ночей n9/n10 ($50/$150/$25,

  cutoff 90bps/120s, reorder 50ms, lat 22ms, skew 0.30); per-symbol

  переопределения — суффиксом @SYMBOL: MM_INV_SKEW_FRAC@TAGUSDT=0.40.

  fix v3.0: таймеры recon/balance реально обновляются (в v3.0 поллинг

  шёл КАЖДУЮ итерацию цикла после истечения интервала) + стаггеринг

  по парам; SUMMARY per-pair пишется ещё и в <лог>_<SYM>.log в формате

  sum9 (комбинированный лог sum9 пропускает — заголовок PAIR SUMMARY).

  Replay — строго один символ (raw-лента однопарная).



v3.0 over v2.16 — LIVE-ТРАНСФОРМАЦИЯ (MM_LIVE=1). Реальные ордера MEXC.

  Архитектура: paper-движок v2.16 НЕ тронут ни в одной строке логики —

  в live-режиме он продолжает работать как SHADOW (решения о котировках,

  виртуальные филлы = предсказание модели), а поверх него LiveExecutor

  зеркалит РЕШЕНИЯ на биржу (REST HMAC-SHA256, clientOrderId-идемпо-

  тентность) и ведёт РЕАЛЬНЫЙ учёт (LiveLedger) по приватным WS-филлам.

  Расхождение shadow/real = живой fill-cal (печатается в SUMMARY).



  L1 (торговый REST): нативный keep-alive HTTPS к api.mexc.com (без

          fork+curl в горячем пути), HMAC-SHA256 подпись query-строки,

          timestamp по серверным часам (GET /api/v3/time, ресинк 60с),

          recvWindow настраивается. placeLimit/cancelOrder/getOrder/

          openOrders/account/marketOrder/userDataStream.

  L2 (приватный WS): listenKey (POST /api/v3/userDataStream, PUT-

          keepalive 30 мин, DELETE на выходе), второй ws::Connection на

          /ws?listenKey=..., подписки spot@private.orders.v3.api.pb /

          spot@private.deals.v3.api.pb / spot@private.account.v3.api.pb

          (protobuf, wrapper-поля 304/306/307). Филлы приходят СРАЗУ,

          REST-поллинг openOrders — только страховка.

  L3 (машина состояния реального ордера): IDLE <-> OPEN, реквот =

          cancel->подтверждение->place. Филл в полёте отмены = реальный

          GHOST (класс ошибок из paper-модели): ловится (а) приватной

          сделкой после cancel, (б) recon-запросом GET /api/v3/order

          (executedQty > known -> дельта-филл), (в) сверкой openOrders.

  L4 (реконсиляция): каждые MM_RECON_SEC — openOrders vs локальное

          состояние (пропавшие -> resolve, чужие/сироты -> лог/+cancel

          по политике), каждые MM_BALANCE_SEC — /api/v3/account, дрифт

          внутреннего учёта vs биржа -> предупреждение. Баланс и дрифт

          позиции для ГЕЙТОВ стратегии — РЕАЛЬНЫЕ (не shadow).

  L5 (риск live): kill-file (MM_KILL_FILE) -> halt+cancel; maxLoss по

          РЕАЛЬНОМУ equity; escape = реальный MARKET-ордер; стейл данных

          -> снять реальные котировки; на SIGINT — cancelAll (если не

          MM_KEEP_ORDERS_ON_EXIT=1). Запуск БЕЗ подтверждения невозможен:

          MM_LIVE=1 требует MM_LIVE_CONFIRM=YES + ключи в env.

  Тест-стенд: MM_API_HOST + MM_TLS_NOVERIFY=1 + MM_LIVE_ALLOW_REPLAY=1

          позволяют гонять live-контур против локального мок-сервера на

          записанной ленте (интеграционный тест ордер-машины).



  Запуск — один процесс на ВСЕ пары (мультипарный, v3.1):

    MM_API_KEY=... MM_API_SECRET=... MM_LIVE=1 MM_LIVE_CONFIRM=YES \

      MM_LOG=night_live.log ./spred TAGUSDT,ZILUSDT,CAWUSDT 90000 20 22

  Dry-run (по умолчанию, без ключей): ./spred TAGUSDT,ZILUSDT 3600 20 22



v2.16 over v2.15 — P0: гигиена движка (event integrity):

  P0-1 (монотонность): событие старее последнего применённого

          (e.exchMs < lastAppliedExchMs) НЕ применяется. Раньше поздний

          depth-снапшот мог откатить коммиченную книгу и при этом

          освежить lastRecv -> откат книги + ложная свежесть -> quote

          по устаревшему миду. Равные метки проходят (stable_sort

          сохраняет порядок постановки). Счётчик lateEv в summary:

          0-единицы на спокойных лентах = guard страховочный; десятки

          = проблема была реальной и поймана.

  P0-2 (lastPacket): время последнего ПРИНЯТОГО кадра — отдельно от

          lastRecv (последнее ПРИМЕНЁННОЕ событие). Диагностика

          «сеть жива / сортировка копит»; stale-gate ОСТАЁТСЯ на

          lastRecv — пакет в reorder-очереди не делает книгу безопасной.

  P0-3 (ghost lower-bound): окно живости ордера [startExchMs,

          untilExchMs]. Сделка старше активации ордера физически не

          могла его заполнить; раньше проверялась только верхняя

          граница. makeGhost теперь с явными параметрами (placeSent/

          placeLat/cancelSent/cancelLat) — не sb-состоянием.

  P0-4 (markout-якорь): mo-окна отсчитываются от БИРЖЕВОГО времени

          сделки (d.timeMs + exchOffDealMs, СЕКУНДЫ — mo.f.t в сек),

          а не от времени обработки. Явный параметр fillAnchorSec

          через сигнатуру doFill/onGhost (не shared-поле класса —

          applyEvent обрабатывает сделки пачками, поле бы «переживало»

          породившую его сделку). GAP/ESCAPE остаются на now — у них

          нет биржевой сделки (контрфакт после реконнекта / решение

          симулятора). После фикса MO5 может сдвинуться заметно

          (якорь честнее, delivery lag ~100мс на окне 5с = 2%), MO30/60

          — слегка. Это починка, не регрессия.

  fix:    replay печатает строку-подписку -> sum7 видит символ

          (матрицы больше не с "?").



v2.15 over v2.14 — REPLAY + границы реализма для тонких пар:

  R (replay): MM_REPLAY=<raw> MM_REPLAY_SPEED=<0|1> — прогон записанной

          ленты (MM_RAW_LOG) через ТОТ ЖЕ путь, что живой WS. Сим-часы:

          в replay nowSecs/nowMsSteady = время ЗАПИСИ (steady-домен

          исходной сессии) -> fill-гейт, ghost-окна, бакет, escape,

          markout, TFI воспроизводятся в исходных временах автоматически;

          сон при SPEED>0 — чистый pacing, на корректность не влияет.

          SPEED=0 валиден: вся ночь прогоняется за секунды (матрицы).

          Прайминг по первому кадру: duration в summary = длительность

          ЗАПИСАННОЙ сессии. Replay до EOF; runSecs игнорируется.

  MM_SEED: детерминизм (латентности ордеров — из tr.rng). Два прогона

          одного raw при одном seed -> идентичные SUMMARY. Смена

          стратегии меняет порядок сэмплов даже при одном seed —

          сравнивай распределения итогов, не построчные логи.

  ФИКС 20 (queue bounding): MM_QUEUE_MODE — 0=frac (как v2.14),

          1=pessimistic (чужие отмены НЕ двигают нашу очередь),

          2=optimistic (убирают очередь полностью). Матрицы TAG/ZIL

          показали: при requote ~10с очередь эпизодическая, frac не

          меняет поток филлов — ось закрыта, флаг остаётся страховкой.

  ФИКС 21 (self-in-book): MM_SELF_IN_BOOK=1 — сигнальный имбаланс

          включает наш LIVE-ордер. ТОЛЬКО для сигналов; очередь/

          levelQty — по чужой книге. Дефолт OFF (feedback-loop риск).

  ФИКС 22 (escape stress): MM_ESCAPE_SKIP_LEVELS=N — escape-walk с

          уровня N+1 (withdrawal ближней ликвидности). expAvg при

          РЕШЕНИИ считается по живой книге -> escSlip меряет стоимость

          withdrawal. В сделку идёт фактически заполненное fq.



v2.14 over v2.13.1 — ФИКС 17 (ramp entry MM_INIT_MODE=1);

  ФИКС 18 (MM_ESCAPE_COOLDOWN_SEC); ФИКС 19 (MM_CUTOFF_BPS/SEC);

  MM_START_USDT.

v2.13.1 — ФИКС 16 (dual clock): fill-гейт в часах СДЕЛОК

  (exchOffDealMs), сортировка с компенсацией skew, карантин

  future-сделок (MM_QUAR_MS).

v2.13 — ФИКС 11..15: latency dist; mo-распределения; escape

  slippage; fees maker/taker; MM_RAW_LOG.

v2.12 — event-time книга (MM_REORDER_MS); gap certain/possible;

  MM_CANCEL_LAT_MS; fill reasons + $-вклад.

v2.11 — раздельные знаменатели mo/pxmo (+txmo); очередь: сброс на

  PLACE + MM_CANCEL_AHEAD_FRAC; flow по биржевому времени; escape с

  латентностью; санитайзинг книги.

v2.10 — вычитка INIT. v2.9.1 — FIX INIT. v2.9 — FIX SELL-SIZING.

v2.8 — PROXY-MARKOUT. v2.7 — честная модель исполнения, flow, P&L-разбивка.



Usage:

  MM_LOG=run.log ./spred TAGUSDT 3600 20 150

Replay:

  MM_REPLAY=TAG_n9.raw MM_REPLAY_SPEED=0 MM_SEED=42 MM_LOG=rp.log \

    <env той же ночи> ./spred TAGUSDT 7200 20 90

Env:

  MM_LOG(off) MM_LOG_DEALS(0) MM_TICK MM_QTY_STEP

  MM_MAKER_BPS(0) MM_TAKER_BPS(5) MM_ORDER_USDT(50) MM_MAX_POS(150)

  MM_MAX_SPREAD_BPS(500) MM_REQUOTE_FRAC(0.11) MM_MIN_REQUOTE_SEC(2)

  MM_RATE_RPS(10) MM_RTT_MS(23) MM_SWEEP_COOLDOWN_SEC(15)

  MM_SKEW_IMB(1) MM_SKEW_TFI(1) MM_IMB_SKEW_FRAC(0.10)

  MM_TFI_SKEW_FRAC(0.10) MM_INV_SKEW_FRAC(0.30)

  MM_TFI_WINDOW(60) MM_TFI_THRESH(0.40) MM_RUN_STOP(3)

  MM_RUN_DECAY(20) MM_MIN_DEALS(5)

  MM_MAX_LOSS(25) MM_STALE_SEC(30) MM_ESCAPE_SECS(30) MM_ALLOW_SHORT(0)

  MM_MIN_INIT_FRAC(0) MM_CANCEL_AHEAD_FRAC(0.5)

  MM_REORDER_MS(50) MM_GAP_FILL_MODE(0) MM_CANCEL_LAT_MS(=order)

  MM_LAT_DIST(0) MM_LAT_P90_MULT(1.6) MM_LAT_P99_MULT(2.5)

  MM_LAT_MIN_MULT(0.3)   [v2.13]

  MM_RAW_LOG(off)        [v2.13] бинарный дамп фреймов для replay

  MM_QUAR_MS(2000)       [v2.13.1] карантин сделок «из будущего»

  MM_START_USDT(10000)   [v2.14] стартовый виртуальный баланс

  MM_INIT_MODE(1)        [v2.14/v3.1] 0=classic taker pre-buy, 1=ramp bid-only

  MM_ESCAPE_COOLDOWN_SEC(90) [v2.14] пауза стороны после escape

  MM_CUTOFF_BPS(90) MM_CUTOFF_SEC(120) [v2.14] cutoff при пробитии уровня

  MM_PRINT_FILTER(1) MM_PRINT_FILTER_MS(1500) [v3.1.8/F47] фильтр внутренних принтов

  MM_PRINT_FILL_P(0)     [v3.1.10/F49] вероятность филла от внутреннего принта

  MM_LIVE_SYNC(1)        [v3.1.10/F50] реальные филлы -> shadow (cooldown/sync)

  MM_SELF_EXCL(1) MM_SELF_EXCL_SEC(2)  [v3.1.10/F52] live: книга без наших ордеров

  MM_LIVE_MAX_FILLS_MIN(6) MM_LIVE_BREAKER_SEC(600) [v3.1.10/F53] breaker

  MM_FEE_LOOKUP(1)       [v3.1.10/F54] комиссии через myTrades

  MM_REPLAY(off) MM_REPLAY_SPEED(1.0) MM_SEED(0)      [v2.15] replay

  MM_QUEUE_MODE(0) MM_ESCAPE_SKIP_LEVELS(0)           [v2.15] Ф20/Ф22

  MM_SELF_IN_BOOK(0)                                   [v2.15] Ф21

  MM_SELL_WALLET_BASE(0) MM_QTY_DRIFT_USDT(0)          [v3.1.1] см. changelog

  MM_LIVE(0) MM_LIVE_CONFIRM("") MM_API_KEY MM_API_SECRET   [v3.0] LIVE

  MM_RECV_WINDOW_MS(10000) MM_TIME_SYNC_SEC(60)

  MM_RECON_SEC(15) MM_BALANCE_SEC(30) MM_LIVE_CLEAN_START(1)

  MM_KILL_FILE(off) MM_KEEP_ORDERS_ON_EXIT(0) MM_RECON_ADOPT(0)

  MM_API_HOST(api.mexc.com) MM_TLS_NOVERIFY(0) MM_LIVE_ALLOW_REPLAY(0)

  MM_SYMBOLS(off)            [v3.1] пары через запятую (= argv[1])

  MM_QUOTE_SHARE(1/N)        [v3.1] доля свободных USDT кошелька на пару

  MM_<ПАРАМ>@<SYMBOL>        [v3.1] per-symbol override (INV_SKEW_FRAC,

                            CUTOFF_BPS/SEC, ORDER_USDT, MAX_POS, MAX_LOSS,

                            MAX_SPREAD_BPS, REQUOTE_FRAC, MIN_REQUOTE_SEC,

                            ESCAPE_COOLDOWN_SEC, START_USDT, TICK, QTY_STEP,

                            QUOTE_SHARE, KILL_FILE)

============================================================================
