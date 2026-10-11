# pick.py v1.2 — выбор пар для paper MM_FAIR=bybit по xscan_grid.csv (модель Q) -> скрипт запуска
# python3 pick.py GRID OUT.sh HOURS [N] [PER_PROC]
# v1.2: d по 1-й половине, отбор — плюс в обеих половинах И с инвентарём (inv_pnl_h > 0),
#       плюсовых часов >= 60%, mo60 > 0; сортировка по сумме половин (не по pnl_h2);
#       PICK_EXCLUDE=BRUSDT,MINAUSDT — ручной стоп-лист; запуск под spred v3.2.1
#       (MM_INIT_MODE=1, MM_MAX_LOSS=3 по mm, MM_HALT_FLAT=1, MM_TREND_BPS=60/900 с).
import pandas as pd, sys, os
g = pd.read_csv(sys.argv[1]); out = sys.argv[2]
hours = float(sys.argv[3]) if len(sys.argv) > 3 else 5
N = int(sys.argv[4]) if len(sys.argv) > 4 else 8
per = min(10, int(sys.argv[5]) if len(sys.argv) > 5 else 8)
excl = set(x.strip().upper() for x in os.environ.get("PICK_EXCLUDE", "").split(",") if x.strip())
g = g[(g.model == "Q") & (g.Q == g.Q.min()) & (~g.sym.isin(excl))]
if g.empty: print("в сетке нет модели Q — нужен xscan v1.2+"); sys.exit(1)
b = g.loc[g.groupby("sym").pnl_h1.idxmax()].copy()
ph = b.pos_hours.astype(str).str.split("/", expand=True).astype(float)
b["ph_frac"] = ph[0] / ph[1].where(ph[1] > 0)
b = b[(b.pnl_h1 > 0) & (b.pnl_h2 > 0) & (b.inv_pnl_h > 0) & (b.mo60 > 0) &
      (b.ph_frac >= 0.6) & (b.fills_h >= 2)]
b = b.assign(tot=b.pnl_h1 + b.pnl_h2).sort_values("tot", ascending=False).head(N)
print(b[["sym", "d", "fills_h", "mo60", "pnl_h1", "pnl_h2", "inv_pnl_h", "pos_hours"]].to_string(index=False))
if b.empty: print("нет подходящих пар"); sys.exit(1)
rows = list(b.itertuples())
sh = "#!/bin/bash\n# paper MM_FAIR=bybit, сгенерировано pick.py v1.2 (spred v3.2.1)\ncd \"$(dirname \"$0\")\"\n"
for k in range(0, len(rows), per):
    part = rows[k:k + per]; n = k // per + 1
    syms = ",".join(r.sym for r in part); ds = " ".join(f"MM_FAIR_D_BPS@{r.sym}={r.d:g}" for r in part)
    sh += f"""rm -f data/paper_fair{n}.log
env MM_FAIR=bybit MM_FAIR_STALE_MS=4000 MM_INIT_MODE=1 MM_ORDER_USDT=10 MM_MAX_POS=50 MM_MAX_LOSS=3 MM_RUN_STOP=0 \\
    MM_LOSS_BASIS=1 MM_HALT_FLAT=1 MM_TREND_BPS=60 MM_TREND_SEC=900 \\
    MM_MAKER_BPS=0 MM_TAKER_BPS=16 {ds} \\
    MM_LOG=data/paper_fair{n}.log \\
    setsid nohup ./spred {syms} {int(hours * 3600)} > data/paper_fair{n}.out 2>&1 &
echo "процесс {n}: {syms}"
"""
open(out, "w").write(sh)
print(f"\nскрипт: {out} ({(len(rows) + per - 1) // per} процесс.)")
