#!/usr/bin/env python3
# pickdays.py v1.0 — отбор пар по СТАБИЛЬНОМУ маркауту модели Q за несколько дней (xscan по дням).
# Вывод 06.10: P&L с инвентарём между днями не коррелирует (шум), маркаут — коррелирует 0.4-0.5.
# python3 pickdays.py OUT.sh SEC N PER rep/days/xscan_20261005.csv rep/days/xscan_20261006.csv ...
#   env: PICK_EXCLUDE=BRUSDT,MINAUSDT  PICK_DMAX=40  PICK_MINFILLS=1  PICK_TAKER="RVNUSDT=4,ETCUSDT=4"
import sys, os, pandas as pd
out, sec, N, per = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), min(10, int(sys.argv[4]))
files = sys.argv[5:]
if len(files) < 2: sys.exit("нужно >= 2 файлов xscan по дням")
excl = set(x.strip().upper() for x in os.environ.get("PICK_EXCLUDE", "BRUSDT,MINAUSDT,MONUSDT,HNTUSDT,CPOOLUSDT").split(",") if x.strip())
dmax = float(os.environ.get("PICK_DMAX", 40)); mf = float(os.environ.get("PICK_MINFILLS", 1))
tk = dict(x.split("=") for x in os.environ.get("PICK_TAKER", "").split(",") if "=" in x)
M = None
for k, f in enumerate(files):
    g = pd.read_csv(f); g = g[(g.model == "Q") & (g.d <= dmax)][["sym", "d", "fills_h", "pnl_h", "inv_pnl_h"]]
    g = g.rename(columns={c: f"{c}{k}" for c in ("fills_h", "pnl_h", "inv_pnl_h")})
    M = g if M is None else M.merge(g, on=["sym", "d"])
K = range(len(files))
M["mo_min"] = M[[f"pnl_h{k}" for k in K]].min(axis=1)
M["mo_avg"] = M[[f"pnl_h{k}" for k in K]].mean(axis=1)
M["f_min"] = M[[f"fills_h{k}" for k in K]].min(axis=1)
M = M[(~M.sym.isin(excl)) & (M.f_min >= mf)]
b = M.loc[M.groupby("sym").mo_min.idxmax()]
b = b[b.mo_min > 0].sort_values("mo_min", ascending=False).head(N)
print(b.round(3).to_string(index=False))
if b.empty: sys.exit("нет пар")
print(f"\nсреднее mo_min по {len(b)} парам: {b.mo_min.mean():.3f} $/ч/пару (модель Q, реально ~ в 3 раза меньше)")
rows = list(b.itertuples()); sh = "#!/bin/bash\n# сгенерировано pickdays.py v1.0 (spred v3.2.1+F59)\ncd \"$(dirname \"$0\")\"\nSEC=${1:-%d}\n" % sec
C = ("MM_FAIR=bybit MM_FAIR_STALE_MS=4000 MM_INIT_MODE=1 MM_ORDER_USDT=10 MM_MAX_POS=50 MM_MAX_LOSS=3 MM_RUN_STOP=0 "
     "MM_LOSS_BASIS=1 MM_HALT_FLAT=1 MM_TREND_BPS=60 MM_TREND_SEC=900 MM_MAKER_BPS=0 MM_TAKER_BPS=6.4")
for i in range(0, len(rows), per):
    part = rows[i:i + per]; n = chr(ord("a") + i // per); syms = ",".join(r.sym for r in part)
    ds = " ".join(f"MM_FAIR_D_BPS@{r.sym}={r.d:g}" for r in part)
    ts = " ".join(f"MM_TAKER_BPS@{r.sym}={tk[r.sym]}" for r in part if r.sym in tk)
    sh += f"""env {C} \\
    {ds} {ts} \\
    MM_LOG=data/paper_pick{n}.log \\
    setsid nohup ./spred {syms} $SEC > data/paper_pick{n}.out 2>&1 &
"""
sh += 'sleep 2; pgrep -af "spred .*$SEC"\n'
open(out, "w").write(sh); os.chmod(out, 0o755); print("скрипт:", out)
