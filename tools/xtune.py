#!/usr/bin/env python3
# xtune.py v1.1 (+ --trend: сетка фильтра тренда bps, --trend-s окно) — подбор d / сдвига от позиции / порога реквота по модели Q (xscan v1.3)
import argparse, glob, os, sys, time, itertools
import numpy as np, pandas as pd
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import xscan

def run(ctx, a, d, skm, rqm, trb=0):
    c = ctx
    F = xscan.qsim(a, d, c["tick"], c["tY"], c["ymid"], c["bas_t"], c["br"], c["tM"], c["mbid"], c["mask"],
                   c["mbq"], c["maq"], c["tt"], c["px"], c["qq"], c["sd"], c["real"], c["t0"], c["t1"], None,
                   skew=skm * d, rq_bps=max(2.0, rqm * d), trend_bps=trb, trend_s=a.trend_s)
    t0, t1 = c["t0"], c["t1"]; tm = (t0 + t1) / 2; H = c["hours"]
    if not F:
        return dict(fills_h=0, mo=0.0, mk_h1=0, mk_h2=0, mtm_h1=0, mtm_h2=0, maxpos=0)
    ft = np.array([x[0] for x in F]); fs = np.array([x[1] for x in F]); fp = np.array([x[2] for x in F])
    fu = np.array([x[3] for x in F]); sg = np.where(fs == 1, 1.0, -1.0); q = sg * fu / fp
    fy = c["fair_y"]
    mo = sg * (fy(ft + 60000 + a.lat_ms) - fp) / fp * 1e4; ok = np.isfinite(mo)
    pf = np.where(ok, mo, 0) * fu / 1e4 - fu * a.fee_bps / 1e4
    def mtm(T):   # cash + pos*fair на момент T, по филлам до T
        m = ft <= T
        f = fy(np.array([T], dtype=np.int64))[0]
        return float(-(q[m] * fp[m]).sum() - (fu[m] * a.fee_bps / 1e4).sum() + q[m].sum() * f)
    v1 = mtm(tm); v2 = mtm(t1)
    pos = np.cumsum(q) * fp
    return dict(fills_h=len(F) / H, mo=float(np.average(mo[ok], weights=fu[ok])) if ok.sum() else 0.0,
                mk_h1=pf[ft < tm].sum() / (H / 2), mk_h2=pf[ft >= tm].sum() / (H / 2),
                mtm_h1=v1 / (H / 2), mtm_h2=(v2 - v1) / (H / 2), maxpos=float(np.abs(pos).max()))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dir"); ap.add_argument("--glob", default="xr_*.csv.gz")
    ap.add_argument("--last-hours", type=int, default=0); ap.add_argument("--syms", default="")
    ap.add_argument("--d", default="5,10,15,20,30,40"); ap.add_argument("--skew", default="0,0.5,1,2",
                    help="сдвиг от позиции в долях d (MM_FAIR_SKEW_BPS = k*d)")
    ap.add_argument("--rq", default="0.2,0.5,1", help="порог реквота в долях d (MM_FAIR_RQ_BPS = max(2, k*d))")
    ap.add_argument("--q", default="10"); ap.add_argument("--max-pos-usd", type=float, default=50)
    ap.add_argument("--fee-bps", type=float, default=0); ap.add_argument("--csv", default="xtune.csv")
    ap.add_argument("--trend", default="0", help="[v1.1] сетка MM_TREND_BPS (0=выкл), напр. 0,50,100")
    ap.add_argument("--trend-s", type=float, default=900, help="[v1.1] окно тренда, с (MM_TREND_SEC)")
    ap.add_argument("--metric", default="mtm", help="mtm (с инвентарём) или mk (маркаут 60 с)")
    a0 = ap.parse_args()
    # параметры xscan по умолчанию + наши
    sys.argv = ["xscan", a0.dir]
    p = xscan.__dict__
    a = argparse.Namespace(lat_ms=150, basis_min=10, in_win_ms=1500, move_bps=5, fee_bps=a0.fee_bps,
                           max_pos_usd=a0.max_pos_usd, d=[15.0], q=[float(x) for x in a0.q.split(",")],
                           horizons=[5, 30, 60], q_grid_ms=250, q_same_frac=0.5, q_behind_frac=0.0,
                           q_inside=0, q_skew=0.0, q_rq_mult=0.2, sweep_cool_s=15, ctx_only=True, trend_s=a0.trend_s)
    files = sorted(glob.glob(os.path.join(a0.dir, a0.glob)))
    if a0.last_hours: files = files[-a0.last_hours:]
    syms = set(s.strip().upper() for s in a0.syms.split(",") if s.strip())
    store = xscan.load(files, syms)
    D = [float(x) for x in a0.d.split(",")]; SK = [float(x) for x in a0.skew.split(",")]
    RQ = [float(x) for x in a0.rq.split(",")]; TR = [float(x) for x in a0.trend.split(",")]
    R = []
    for sym in sorted(store):
        t = time.time()
        ctx, _ = xscan.analyze(sym, xscan.frame(store.pop(sym)), a)
        if not ctx: continue
        for d, sk, rq, trb in itertools.product(D, SK, RQ, TR):
            r = run(ctx, a, d, sk, rq, trb); r.update(sym=sym, d=d, skew=sk, rq=rq, trend=trb, hours=ctx["hours"],
                                                 mexc_bps=ctx["mexc_bps"]); R.append(r)
        xscan.log(f"{sym} {time.time() - t:.1f}s")
    G = pd.DataFrame(R); G.to_csv(a0.csv, index=False)
    m = a0.metric
    G["tot"] = (G[f"{m}_h1"] + G[f"{m}_h2"]) / 2
    pd.set_option("display.width", 220)
    # 1) одна общая настройка (skew, rq) для всех пар при их лучшем d по 1-й половине
    print(f"# метрика {m}: $/ч; выбор d по 1-й половине, результат на 2-й (вне выборки)")
    out = []
    for (sk, rq), g in G.groupby(["skew", "rq"]):
        b = g.loc[g.groupby("sym")[f"{m}_h1"].idxmax()]
        sel = b[b[f"{m}_h1"] > 0]
        out.append(dict(skew=sk, rq=rq, pairs_pos_h1=len(sel), oos_h2=sel[f"{m}_h2"].sum(),
                        oos_pos=int((sel[f"{m}_h2"] > 0).sum()), fills_h=sel.fills_h.sum(),
                        maxpos_med=sel.maxpos.median()))
    O = pd.DataFrame(out).sort_values("oos_h2", ascending=False)
    print(O.round(3).to_string(index=False))
    # 2) фиксированный d=15 для всех: влияние skew/rq на полную выборку
    print("\n# d=15 для всех пар: сумма по парам, $/ч (обе половины)")
    f = G[G.d == 15].groupby(["skew", "rq"]).agg(tot=("tot", "sum"), h1=(f"{m}_h1", "sum"), h2=(f"{m}_h2", "sum"),
                                                  fills_h=("fills_h", "sum"), maxpos_med=("maxpos", "median"))
    print(f.round(3).sort_values("tot", ascending=False).to_string())
    print(f"\nсетка: {a0.csv}")

if __name__ == "__main__":
    main()
