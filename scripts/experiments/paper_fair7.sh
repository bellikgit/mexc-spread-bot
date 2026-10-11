#!/bin/bash
# paper F55 #7 (v3.3): A/B на одних и тех же 11 парах (paper не влияет на рынок)
#   A = контроль: v3.3 по умолчанию (=v3.2.1) + реальные комиссии, тренд-фильтр 60/900
#   B = новое: мейкерский выход F60, Avellaneda–Stoikov F63 вместо тренда,
#       микроцена Bybit + дисбаланс MEXC F62, общий лимитер F61 (MM_SR_PAPER)
# ./paper_fair7.sh [SEC]   по умолчанию — до 13:45 завтра (МСК, часы VDS)
cd "$(dirname "$0")/../.."
BIN=${BIN:-./spred}
SEC=${1:-$(( $(date -d 'tomorrow 13:45' +%s) - $(date +%s) ))}
echo "бинарник $BIN, длительность $SEC с, конец $(date -d "+$SEC sec" '+%d.%m %H:%M')"
C="MM_FAIR=bybit MM_FAIR_STALE_MS=4000 MM_INIT_MODE=1 MM_ORDER_USDT=10 MM_MAX_POS=50 MM_MAX_LOSS=3 MM_RUN_STOP=0 MM_LOSS_BASIS=1 MM_HALT_FLAT=1 MM_MAKER_BPS=0 MM_TAKER_BPS=8"
A="MM_TREND_BPS=60 MM_TREND_SEC=900"
B="MM_TREND_BPS=0 MM_ESC_MAKER=1 MM_ESC_MAKER_SEC=180 MM_AS_GAMMA=0.1 MM_AS_TAU=60 MM_FAIR_MICRO=1 MM_FAIR_IMB_BPS=3 MM_SR_PAPER=1"
P1="BILLUSDT,BONKUSDT,ZIGUSDT,INITUSDT,SANDUSDT"
E1="MM_FAIR_D_BPS@BILLUSDT=15 MM_FAIR_D_BPS@BONKUSDT=5 MM_FAIR_D_BPS@ZIGUSDT=30 MM_FAIR_D_BPS@INITUSDT=30 MM_FAIR_D_BPS@SANDUSDT=10"
P2="RUNEUSDT,ETCUSDT,FLOCKUSDT,KAIAUSDT,SKRUSDT,CYBERUSDT"
E2="MM_FAIR_D_BPS@RUNEUSDT=10 MM_FAIR_D_BPS@ETCUSDT=15 MM_FAIR_D_BPS@FLOCKUSDT=10 MM_FAIR_D_BPS@KAIAUSDT=10 MM_FAIR_D_BPS@SKRUSDT=15 MM_FAIR_D_BPS@CYBERUSDT=15 MM_TAKER_BPS@RUNEUSDT=5 MM_TAKER_BPS@ETCUSDT=5 MM_TAKER_BPS@KAIAUSDT=5 MM_TAKER_BPS@CYBERUSDT=5"
for V in A B; do
  eval X=\$$V
  env $C $X $E1 MM_LOG=data/paper_fair7${V}1.log \
      setsid nohup $BIN $P1 $SEC > data/paper_fair7${V}1.out 2>&1 &
  env $C $X $E2 MM_LOG=data/paper_fair7${V}2.log \
      setsid nohup $BIN $P2 $SEC > data/paper_fair7${V}2.out 2>&1 &
done
sleep 2; pgrep -af "spred.* $SEC"
