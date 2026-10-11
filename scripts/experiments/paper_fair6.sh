#!/bin/bash
# paper F55 #6 (v3.2.1+F59): пары со стабильным маркаутом 2 дня (05/06.10), trend 60/900, тейкер по парам
# ./paper_fair6.sh [SEC]   по умолчанию — до 13:45 завтра (МСК, часы VDS)
cd "$(dirname "$0")/../.."
SEC=${1:-$(( $(date -d 'tomorrow 13:45' +%s) - $(date +%s) ))}
echo "длительность $SEC с, конец $(date -d "+$SEC sec" '+%d.%m %H:%M')"
C="MM_FAIR=bybit MM_FAIR_STALE_MS=4000 MM_INIT_MODE=1 MM_ORDER_USDT=10 MM_MAX_POS=50 MM_MAX_LOSS=3 MM_RUN_STOP=0 MM_LOSS_BASIS=1 MM_HALT_FLAT=1 MM_TREND_BPS=60 MM_TREND_SEC=900 MM_MAKER_BPS=0 MM_TAKER_BPS=6.4"
env $C MM_FAIR_D_BPS@RVNUSDT=20 MM_FAIR_D_BPS@BILLUSDT=15 MM_FAIR_D_BPS@BONKUSDT=5 \
    MM_FAIR_D_BPS@ZIGUSDT=30 MM_FAIR_D_BPS@INITUSDT=30 MM_FAIR_D_BPS@SANDUSDT=10 \
    MM_TAKER_BPS@RVNUSDT=4 \
    MM_LOG=data/paper_fair6a.log \
    setsid nohup ./spred RVNUSDT,BILLUSDT,BONKUSDT,ZIGUSDT,INITUSDT,SANDUSDT $SEC > data/paper_fair6a.out 2>&1 &
env $C MM_FAIR_D_BPS@RUNEUSDT=10 MM_FAIR_D_BPS@ETCUSDT=15 MM_FAIR_D_BPS@FLOCKUSDT=10 \
    MM_FAIR_D_BPS@KAIAUSDT=10 MM_FAIR_D_BPS@SKRUSDT=15 MM_FAIR_D_BPS@CYBERUSDT=15 \
    MM_TAKER_BPS@RUNEUSDT=4 MM_TAKER_BPS@ETCUSDT=4 MM_TAKER_BPS@KAIAUSDT=4 MM_TAKER_BPS@CYBERUSDT=4 \
    MM_LOG=data/paper_fair6b.log \
    setsid nohup ./spred RUNEUSDT,ETCUSDT,FLOCKUSDT,KAIAUSDT,SKRUSDT,CYBERUSDT $SEC > data/paper_fair6b.out 2>&1 &
sleep 2; pgrep -af "spred .*$SEC"
