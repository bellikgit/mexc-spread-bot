#!/usr/bin/env python3
# lagtest.py — отстаёт ли MEXC от Bybit и можно ли на этом заработать.
# MEXC bookTicker @10ms + Bybit orderbook.1 (10 мс), общие локальные часы.
# python3 lagtest.py --min 15 --syms BTCUSDT,ETHUSDT,...   (demon2.py и xrec.py рядом)
import argparse, json, math, os, socket, ssl, sys, threading, time
import numpy as np
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import demon2 as d2, xrec

DEF = ("BTCUSDT,ETHUSDT,SOLUSDT,XRPUSDT,DOGEUSDT,ADAUSDT,LINKUSDT,AVAXUSDT,SUIUSDT,TONUSDT,"
       "PEPEUSDT,WIFUSDT,ENAUSDT,ARBUSDT,OPUSDT,NEARUSDT,APTUSDT,TIAUSDT,SEIUSDT,INJUSDT")

def rtt(host):
    ts = []
    for _ in range(5):
        t = time.perf_counter(); s = socket.create_connection((host, 443), timeout=5); ts.append((time.perf_counter() - t) * 1000); s.close()
    return float(np.median(ts))

class Rec:
    def __init__(s): s.M = {}; s.Y = {}; s.lock = threading.Lock(); s.ybook = {}; s.lastM = {}; s.lastY = {}
    def on_mexc(s, payload, mono):
        now = time.time() * 1000
        sym = ""; body = b""; st = 0
        for f, w, v in d2.pb_fields(payload):
            if f == 3 and w == 2: sym = v.decode()
            elif f == 6 and w == 0: st = v
            elif f == 315 and w == 2: body = v
        if not body: return
        b = a = 0.0
        for f, w, v in d2.pb_fields(body):
            if f == 1: b = float(v)
            elif f == 3: a = float(v)
        if b > 0 and a > b and s.lastM.get(sym) != (b, a):   # только смена цены: память VDS
            s.lastM[sym] = (b, a)
            with s.lock: s.M.setdefault(sym, []).append((now, b, a, st))
    def on_bybit(s, m, now):
        tp = m.get("topic", "")
        if not tp.startswith("orderbook.1."): return
        d = m.get("data") or {}; sym = d.get("s"); bk = s.ybook.setdefault(sym, [0.0, 0.0])
        if d.get("b"): bk[0] = float(d["b"][0][0]) if float(d["b"][0][1]) > 0 else 0.0
        if d.get("a"): bk[1] = float(d["a"][0][0]) if float(d["a"][0][1]) > 0 else 0.0
        if bk[0] > 0 and bk[1] > bk[0] and s.lastY.get(sym) != (bk[0], bk[1]):
            s.lastY[sym] = (bk[0], bk[1])
            with s.lock: s.Y.setdefault(sym, []).append((now * 1000, bk[0], bk[1], m.get("cts") or m.get("ts") or 0))

def asof(t, v, q):
    i = np.searchsorted(t, q, side="right") - 1
    out = np.where(i >= 0, v[np.clip(i, 0, None)], np.nan); return out

def analyze(sym, M, Y, fee_bps, lats):
    M = np.array(M); Y = np.array(Y)
    if len(M) < 20 or len(Y) < 20: return None
    tM, mm = M[:, 0], (M[:, 1] + M[:, 2]) / 2; tY, ym = Y[:, 0], (Y[:, 1] + Y[:, 2]) / 2
    t0, t1 = max(tM[0], tY[0]) + 2000, min(tM[-1], tY[-1]) - 5000
    if t1 - t0 < 120000: return None
    g = np.arange(t0, t1, 10.0)
    lm, ly = np.log(asof(tM, mm, g)), np.log(asof(tY, ym, g))
    k = 20                                     # приращения за 200 мс
    best, cors = (0, -1), {}
    for L in range(-30, 101, 2):               # шаг 20 мс, L>0: Bybit впереди
        i = np.arange(k + max(L, 0), len(g) - max(-L, 0))
        dy = ly[i - L] - ly[i - L - k]; dm = lm[i] - lm[i - k]
        ok = (dy != 0) | (dm != 0)
        if ok.sum() < 200: continue
        c = float(np.corrcoef(dy[ok], dm[ok])[0, 1]); cors[L * 10] = c
        if c > best[1]: best = (L * 10, c)
    # то же по БИРЖЕВЫМ меткам (MEXC sendTime vs Bybit cts) — не зависит от нашей сети
    bestx = (0, -1)
    if (M[:, 3] > 0).mean() > 0.9 and (Y[:, 3] > 0).mean() > 0.9:
        oM = np.argsort(M[:, 3], kind="stable"); oY = np.argsort(Y[:, 3], kind="stable")
        gx = g - tM[0] + M[0, 3]
        lmx = np.log(asof(M[oM, 3], mm[oM], gx)); lyx = np.log(asof(Y[oY, 3], ym[oY], gx))
        for L in range(-30, 101, 2):
            i = np.arange(k + max(L, 0), len(g) - max(-L, 0))
            dy = lyx[i - L] - lyx[i - L - k]; dm = lmx[i] - lmx[i - k]
            ok = np.isfinite(dy) & np.isfinite(dm) & ((dy != 0) | (dm != 0))
            if ok.sum() < 200: continue
            c = float(np.corrcoef(dy[ok], dm[ok])[0, 1])
            if c > bestx[1]: bestx = (L * 10, c)
    # базис: скользящая медиана по прошлому (60 с)
    lb = lm - ly
    step = 100; bmed = np.full(len(g), np.nan)
    for j in range(0, len(g), step):
        lo = max(0, j - 6000); seg = lb[lo:j + 1]
        bmed[j:j + step] = np.nanmedian(seg) if len(seg) > 100 else np.nan
    yspr = float(np.median((Y[:, 2] - Y[:, 1]) / ym * 1e4)); mspr = float(np.median((M[:, 2] - M[:, 1]) / mm * 1e4))
    thr = max(3.0, 1.5 * yspr) / 1e4
    dy100 = ly[10:] - ly[:-10]                 # скачок Bybit за 100 мс
    ev = np.where(np.abs(dy100) > thr)[0] + 10
    keep = []
    for e in ev:
        if (not keep or e - keep[-1] > 100) and e + 300 < len(g) and np.isfinite(bmed[e]): keep.append(e)
    react, edges = [], {L: [] for L in lats}
    for e in keep:
        sgn = np.sign(dy100[e - 10])
        base = lm[e - 10]
        fut = sgn * (lm[e:e + 300] - base)
        hit = np.where(fut > thr / 2)[0]
        react.append(hit[0] * 10 if len(hit) else np.nan)
        fair3 = ly[e + 300] + bmed[e]           # справедливая цена MEXC через 3 с
        for L in lats:
            q = g[e] + L
            b, a = asof(tM, M[:, 1], np.array([q]))[0], asof(tM, M[:, 2], np.array([q]))[0]
            px = a if sgn > 0 else b            # бьём по рынку в сторону движения
            gross = sgn * (fair3 - math.log(px)) * 1e4
            edges[L].append(gross - fee_bps)
    hrs = (t1 - t0) / 3.6e6
    r = dict(sym=sym, min=round((t1 - t0) / 60000, 1), m_upd_s=round(len(M) / ((tM[-1] - tM[0]) / 1000), 1),
             y_upd_s=round(len(Y) / ((tY[-1] - tY[0]) / 1000), 1), mspr=round(mspr, 1), yspr=round(yspr, 1),
             lag_ms=best[0], lagx_ms=bestx[0], corr=round(best[1], 2), corr0=round(cors.get(0, float("nan")), 2),
             ev=len(keep), ev_h=round(len(keep) / hrs, 0),
             react_med=float(np.nanmedian(react)) if react else float("nan"),
             never=round(float(np.mean(np.isnan(react))), 2) if react else float("nan"))
    for L in lats:
        x = np.array(edges[L]); x = x[np.isfinite(x)]
        r[f"edge{L}"] = round(float(x.mean()), 1) if len(x) else float("nan")
        r[f"win{L}"] = round(float((x > 0).mean()), 2) if len(x) else float("nan")
    return r

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min", type=float, default=20); ap.add_argument("--syms", default=DEF)
    ap.add_argument("--fee-bps", type=float, default=5.0, help="тейкер MEXC (0.05%%)")
    ap.add_argument("--lats", default="0,50,100,200", help="наша задержка реакции, мс")
    ap.add_argument("--out", default="lagtest.json")
    ap.add_argument("--pairs-json", default="", help="x/xrec_pairs.json — взять пары рекордера")
    a = ap.parse_args(); syms = a.syms.split(",")
    lats = [int(x) for x in a.lats.split(",")]
    if a.pairs_json:
        j = json.load(open(a.pairs_json))
        if isinstance(j, dict): j = j.get("pairs", j.get("syms", list(j.keys())))
        syms = [x if isinstance(x, str) else (x.get("sym") or x.get("symbol")) for x in j][:20]
    print(f"RTT TCP: MEXC {rtt('wbs-api.mexc.com'):.0f} мс, Bybit {rtt('stream.bybit.com'):.0f} мс", flush=True)
    R = Rec(); gate = d2.Gate(20)
    ch = [f"spot@public.aggre.bookTicker.v3.api.pb@10ms@{s}" for s in syms]
    for i in range(0, len(ch), 30):
        c = d2.WSConn(f"m{i}", R.on_mexc, gate); c.add(ch[i:i + 30]); c.start()
    for i in range(0, len(syms), 10):
        y = xrec.BybitWS(f"y{i}", syms[i:i + 10], R.on_bybit); y.start()
    t_end = time.time() + a.min * 60
    while time.time() < t_end:
        time.sleep(60)
        with R.lock: print(f"[{time.strftime('%H:%M:%S')}] MEXC {sum(map(len, R.M.values()))}, Bybit {sum(map(len, R.Y.values()))}", flush=True)
    d2.STOP.set(); time.sleep(1.5)
    with R.lock: M, Y = dict(R.M), dict(R.Y)
    import pickle; pickle.dump((M, Y), open(a.out.replace(".json", ".pkl"), "wb"))
    rows = [r for s in syms if (r := analyze(s, M.get(s, []), Y.get(s, []), a.fee_bps, lats))]
    hdr = f"{'SYM':<10}{'мин':>5}{'Mсм':>6}{'Yсм':>6}{'сprM':>6}{'спрY':>6}{'лаг':>6}{'лагX':>6}{'corr':>6}{'c0':>6}{'соб/ч':>7}{'реакц':>7}{'нет%':>6}" + "".join(f"{'край'+str(L):>9}{'%+':>5}" for L in lats)
    print("\n" + hdr); print("-" * len(hdr))
    for r in sorted(rows, key=lambda x: -x["lag_ms"]):
        print(f"{r['sym']:<10}{r['min']:>5.0f}{r['m_upd_s']:>6.0f}{r['y_upd_s']:>6.0f}{r['mspr']:>6.1f}{r['yspr']:>6.1f}{r['lag_ms']:>6}{r['lagx_ms']:>6}{r['corr']:>6.2f}{r['corr0']:>6.2f}{r['ev_h']:>7.0f}{r['react_med']:>7.0f}{100*r['never']:>6.0f}"
              + "".join(f"{r[f'edge{L}']:>9.1f}{100*r[f'win{L}']:>5.0f}" for L in lats))
    print("\nлаг — сдвиг максимума корреляции (мс, >0 = Bybit впереди) по нашим часам приёма; лагX — то же\n"
          "по биржевым меткам (MEXC sendTime / Bybit cts), не зависит от сети; c0 — корреляция без сдвига;\n"
          "соб/ч — скачков Bybit ≥max(3 bps, 1.5 спреда Bybit) за 100 мс; реакц — медиана мс до сдвига MEXC\n"
          "на половину скачка; нет% — MEXC не догнал за 3 с; крайL — средний результат (bps) удара по рынку MEXC\n"
          f"через L мс после скачка, выход по справедливой цене через 3 с, минус тейкер {a.fee_bps} bps; %+ — доля плюсовых.")
    json.dump(rows, open(a.out, "w"), indent=1)

if __name__ == "__main__":
    main()
