#!/usr/bin/env python3
"""feecheck.py — комиссии MEXC по ключу (tradeFee) для списка пар.
  K=... S=... python3 feecheck.py data/xu/xrec_pairs.json   # пары из отбора xrec
  K=... S=... python3 feecheck.py BTCUSDC,SOLUSDC                 # или списком
Печатает maker/taker в bps и строку MM_TAKER_BPS@SYM=... для скрипта paper."""
import hashlib, hmac, json, os, sys, time, urllib.request
K, S = os.environ["K"], os.environ["S"]
arg = sys.argv[1]
syms = [r["sym"] for r in json.load(open(arg))["pick"]] if arg.endswith(".json") else arg.split(",")
env = []
for sym in syms:
    q = "symbol=%s&timestamp=%d" % (sym, time.time() * 1000)
    sig = hmac.new(S.encode(), q.encode(), hashlib.sha256).hexdigest()
    req = urllib.request.Request("https://api.mexc.com/api/v3/tradeFee?%s&signature=%s" % (q, sig),
                                 headers={"X-MEXC-APIKEY": K})
    try:
        d = json.load(urllib.request.urlopen(req, timeout=10)).get("data", {})
        mk, tk = float(d.get("makerCommission", 0)) * 1e4, float(d.get("takerCommission", 0)) * 1e4
        print(f"{sym:<14} maker {mk:5.1f} bps  taker {tk:5.1f} bps")
        env.append(f"MM_TAKER_BPS@{sym}={tk:g}")
    except Exception as e:
        print(f"{sym:<14} ошибка {e}")
    time.sleep(0.25)
print(" ".join(env))
