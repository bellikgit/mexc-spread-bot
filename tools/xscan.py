#!/usr/bin/env python3
# xscan.py v1.4 (+ --trend-bps/--trend-s: фильтр тренда как F58) — анализ записей xrec.py: базис MEXC/Bybit, lead-lag, модель котирования от Bybit
import argparse, glob, gzip, io, math, os, sys, time
import numpy as np, pandas as pd

def log(m): print(m, file=sys.stderr, flush=True)

def read_gz(f):
    # текущий час ещё пишется: читаем, сколько есть, обрезаем до последней целой строки
    buf = io.BytesIO()
    with gzip.open(f, "rb") as g:
        try:
            while True:
                b = g.read(1 << 20)
                if not b: break
                buf.write(b)
        except (EOFError, OSError, gzip.BadGzipFile):
            pass
    data = buf.getvalue()
    cut = data.rfind(b"\n")
    return io.BytesIO(data[:cut + 1] if cut >= 0 else b"")

def load(files, syms):
    # по парам: k,v int8, T int64, a,b,c float32 — ~22 байта на строку
    store = {}
    for f in files:
        n = 0
        try:
            src = read_gz(f)
            it = pd.read_csv(src, header=None, names=list("kTvsabcd"), usecols=range(8),
                             dtype={"k": "category", "v": "category", "s": "category", "T": "int64",
                                    "a": "float32", "b": "float32", "c": "float32", "d": "float32"},
                             on_bad_lines="skip", engine="c", chunksize=300000)
            for df in it:
                if syms: df = df[df.s.isin(syms)]
                k = (df.k == "T").values.astype(np.int8); v = (df.v == "Y").values.astype(np.int8)
                codes = df.s.cat.codes.values; cats = df.s.cat.categories
                T = df["T"].values; A = df.a.values; B = df.b.values; C = df.c.values; Dd = df.d.values
                order = np.argsort(codes, kind="stable"); cs = codes[order]
                bounds = np.flatnonzero(np.diff(cs)) + 1
                for idx in np.split(order, bounds):
                    if not len(idx): continue
                    sym = cats[codes[idx[0]]]
                    store.setdefault(sym, []).append((k[idx], v[idx], T[idx], A[idx], B[idx], C[idx], Dd[idx]))
                n += len(df)
        except Exception as e:
            log(f"skip {f}: {e!r}"); continue
        log(f"read {os.path.basename(f)}: {n}")
    return store

def frame(pieces):
    cols = [np.concatenate([p[i] for p in pieces]) for i in range(7)]
    return pd.DataFrame({"k": cols[0], "v": cols[1], "T": cols[2], "a": cols[3].astype(np.float64),
                         "b": cols[4].astype(np.float64), "c": cols[5].astype(np.float64),
                         "d": cols[6].astype(np.float64)})

def asof(t_src, v_src, t_q):
    i = np.searchsorted(t_src, t_q, side="right") - 1
    out = np.full(len(t_q), np.nan)
    ok = i >= 0
    out[ok] = v_src[i[ok]]
    return out

def analyze(sym, g, a):
    mb = g[(g.k == 0) & (g.v == 0)].sort_values("T", kind="stable")
    yb = g[(g.k == 0) & (g.v == 1)].sort_values("T", kind="stable")
    mt = g[(g.k == 1) & (g.v == 0)].sort_values("T", kind="stable")
    if len(mb) < 50 or len(yb) < 50 or len(mt) < 5:
        return None, []
    tM = mb["T"].values.astype(np.int64); mbid = mb.a.values.astype(float); mask = mb.c.values.astype(float)
    tY = yb["T"].values.astype(np.int64); ybid = yb.a.values.astype(float); yask = yb.c.values.astype(float)
    mmid = (mbid + mask) / 2; ymid = (ybid + yask) / 2
    t0 = max(tM[0], tY[0]); t1 = min(tM[-1], tY[-1])
    hours = (t1 - t0) / 3.6e6
    if hours < 0.05: return None, []
    # тик MEXC
    pr = np.unique(np.concatenate([mbid, mask]))
    df_ = np.diff(pr); df_ = df_[df_ > 0]
    tick = float(df_.min()) if len(df_) else 0.0
    # сетка 100 мс
    grid = np.arange(t0, t1, 100, dtype=np.int64)
    gm = asof(tM, mmid, grid); gy = asof(tY, ymid, grid)
    gms = asof(tM, (mask - mbid) / mmid * 1e4, grid); gys = asof(tY, (yask - ybid) / ymid * 1e4, grid)
    lb = np.log(gm / gy)
    # базис: скользящая медиана только по прошлому (окно a.basis_min мин, шаг 1 с)
    s1 = pd.Series(lb[::10])
    br = s1.rolling(int(a.basis_min * 60), min_periods=30).median().shift(1).bfill().values
    bas_t = grid[::10]
    basis_med = float(np.nanmedian(lb) * 1e4); basis_sd = float(np.nanstd(lb - np.repeat(br, 10)[:len(lb)]) * 1e4)
    # lead-lag по 1-с приращениям
    ly, lm = np.log(gy), np.log(gm)
    best = (0, -9.0); cors = {}
    for L in range(-10, 31, 1):   # x100 мс; L>0: Bybit впереди
        k = 10
        n = len(grid)
        i = np.arange(max(k + max(L, 0), k), n - max(-L, 0))
        dy = ly[i - L] - ly[i - L - k]; dm = lm[i] - lm[i - k]
        okk = np.isfinite(dy) & np.isfinite(dm) & (dy != 0)
        if okk.sum() < 100: continue
        c = float(np.corrcoef(dy[okk], dm[okk])[0, 1]); cors[L] = c
        if c > best[1]: best = (L, c)
    # реакция: доля сильных 1-с движений Bybit, за которыми MEXC сдвинулся туда же за 3 с
    k = 10; dy = ly[k:] - ly[:-k]
    thr = max(a.move_bps, 2 * np.nanmedian(gys)) / 1e4
    ev = np.where(np.abs(dy) > thr)[0] + k
    ev = ev[(ev + 30 < len(grid))]
    if len(ev):
        keep = [ev[0]]
        for e in ev[1:]:
            if e - keep[-1] > 30: keep.append(e)
        ev = np.array(keep)
        fol = np.sign(lm[ev + 30] - lm[ev]) == np.sign(ly[ev] - ly[ev - k])
        follow = float(np.mean(fol)); n_ev = len(ev)
    else:
        follow, n_ev = float("nan"), 0
    # сделки MEXC
    tt = mt["T"].values.astype(np.int64); px = mt.a.values.astype(float); qq = mt.b.values.astype(float)
    sd = mt.c.values.astype(int)
    okt = (tt >= t0) & (tt <= t1)
    tt, px, qq, sd = tt[okt], px[okt], qq[okt], sd[okt]
    usd = px * qq
    # fair Bybit с задержкой и базисом
    def fair_y(tq):
        y = asof(tY, ymid, tq - a.lat_ms)
        b = asof(bas_t, br, tq)
        return y * np.exp(b)
    def fair_m(tq):
        return asof(tM, mmid, tq - a.lat_ms)
    mb_q = lambda tq: asof(tM, mbid, tq)
    ma_q = lambda tq: asof(tM, mask, tq)
    # фильтр внутренних принтов: продажа агрессора считается "настоящей", если цена <= мин. bid MEXC за окно
    w = a.in_win_ms
    def bbo_extreme(tq, arr, fn):
        # мин/макс значения arr на [tq-w, tq] через последний известный + все обновления в окне
        res = asof(tM, arr, tq - w)
        i0 = np.searchsorted(tM, tq - w, side="right"); i1 = np.searchsorted(tM, tq, side="right")
        out = res.copy()
        for j in range(len(tq)):
            if i1[j] > i0[j]:
                seg = arr[i0[j]:i1[j]]
                out[j] = fn(np.append(seg, res[j])) if np.isfinite(res[j]) else fn(seg)
        return out
    # настоящая продажа агрессора: цена <= какого-то bid, бывшего в окне; иначе — принт внутри спреда
    maxbid = bbo_extreme(tt, mbid, np.max); minask = bbo_extreme(tt, mask, np.min)
    real = np.where(sd == 2, px <= maxbid * (1 + 1e-9), px >= minask * (1 - 1e-9))
    in_pct = float(100 * (1 - real.mean())) if len(real) else 0.0
    fy_t = fair_y(tt); fm_t = fair_m(tt); mbt = mb_q(tt); mat = ma_q(tt)
    if getattr(a, "ctx_only", False):   # [v1.3] для подбора параметров (xtune.py)
        tq_ = float(f"{tick:.2g}") if tick > 0 else tick
        sn = lambda x: np.round(x / tq_) * tq_
        return dict(sym=sym, tick=tq_, tY=tY, ymid=ymid, bas_t=bas_t, br=br, tM=tM, mbid=sn(mbid), mask=sn(mask),
                    mbq=mb.b.values.astype(float), maq=mb.d.values.astype(float), tt=tt, px=sn(px), qq=qq, sd=sd,
                    real=real, t0=t0, t1=t1, hours=hours, fair_y=fair_y, mexc_bps=float(np.nanmedian(gms)),
                    in_pct=in_pct), []
    H = a.horizons
    fy_h = {h: fair_y(tt + h * 1000 + a.lat_ms) for h in H}   # без задержки в будущем: используем истинную fair
    summ = dict(sym=sym, hours=round(hours, 2), mexc_bps=round(float(np.nanmedian(gms)), 1),
                bybit_bps=round(float(np.nanmedian(gys)), 1), basis=round(basis_med, 1), basis_sd=round(basis_sd, 1),
                lag_ms=best[0] * 100, corr=round(best[1], 2), corr0=round(cors.get(0, float("nan")), 2),
                follow=round(follow, 2), n_ev=n_ev, tr_h=round(len(tt) / hours, 0),
                usd_h=round(usd.sum() / hours, 0), in_pct=round(in_pct, 0))
    rows = []
    for model, fr in (("Y", fy_t), ("M", fm_t)):
        for d in a.d:
            bid = fr * (1 - d / 1e4); ask = fr * (1 + d / 1e4)
            # post-only: не пересекать книгу MEXC
            bid = np.minimum(bid, mat - tick); ask = np.maximum(ask, mbt + tick)
            for flt in ("all", "real"):
                hitb = (sd == 2) & (px <= bid) & np.isfinite(bid)
                hita = (sd == 1) & (px >= ask) & np.isfinite(ask)
                if flt == "real": hitb &= real; hita &= real
                for Q in a.q:
                    fq = np.minimum(Q, usd)
                    res = dict(sym=sym, model=model, d=d, flt=flt, Q=Q)
                    nb, na = int(hitb.sum()), int(hita.sum())
                    res["fills_h"] = round((nb + na) / hours, 1)
                    res["usd_fill_h"] = round((fq[hitb].sum() + fq[hita].sum()) / hours, 0)
                    edge = np.concatenate([(fy_t[hitb] - bid[hitb]) / bid[hitb], (ask[hita] - fy_t[hita]) / ask[hita]]) * 1e4
                    res["edge"] = round(float(np.nanmean(edge)), 1) if len(edge) else float("nan")
                    w_ = np.concatenate([fq[hitb], fq[hita]])
                    for h in H:
                        mo = np.concatenate([(fy_h[h][hitb] - bid[hitb]) / bid[hitb],
                                             (ask[hita] - fy_h[h][hita]) / ask[hita]]) * 1e4
                        okm = np.isfinite(mo)
                        res[f"mo{h}"] = round(float(np.average(mo[okm], weights=w_[okm])), 1) if okm.sum() else float("nan")
                        if h == H[-1]:
                            res["pnl_h"] = round(float(np.sum(mo[okm] * w_[okm]) / 1e4 / hours) - (w_.sum() * a.fee_bps / 1e4 / hours), 3)
                            # устойчивость: половины записи и доля плюсовых часов
                            tf = np.concatenate([tt[hitb], tt[hita]])
                            pf = np.where(okm, mo, 0.0) * w_ / 1e4 - w_ * a.fee_bps / 1e4
                            tm = (t0 + t1) / 2
                            h1 = (tm - t0) / 3.6e6; h2 = (t1 - tm) / 3.6e6
                            res["pnl_h1"] = round(float(pf[tf < tm].sum() / max(h1, 1e-9)), 3)
                            res["pnl_h2"] = round(float(pf[tf >= tm].sum() / max(h2, 1e-9)), 3)
                            nb_ = max(1, int(round(hours)))
                            hb_ = np.minimum(((tf - t0) / 3.6e6).astype(int), nb_ - 1)
                            ph = np.bincount(hb_, weights=pf, minlength=nb_) if len(tf) else np.zeros(nb_)
                            res["pos_hours"] = f"{int((ph > 0).sum())}/{nb_}"
                    # инвентарь с лимитом
                    res["inv_pnl_h"] = round(inv_sim(tt, hitb, hita, bid, ask, fq, fair_y, t1, a.max_pos_usd, a.fee_bps) / hours, 3)
                    rows.append(res)
    # [v1.2] модель Q (как spred) по реальным сделкам
    mbq = mb.b.values.astype(float); maq = mb.d.values.astype(float)
    # шаг цены: 2 значащие цифры, цены — на сетку шага (float32 при загрузке)
    tick_q = float(f"{tick:.2g}") if tick > 0 else tick
    snap = lambda x: np.round(x / tick_q) * tick_q
    mbid_q, mask_q, px_q = snap(mbid), snap(mask), snap(px)
    tm_ = (t0 + t1) / 2
    for d in a.d:
        F = qsim(a, d, tick_q, tY, ymid, bas_t, br, tM, mbid_q, mask_q, mbq, maq, tt, px_q, qq, sd, real, t0, t1, None)
        res = dict(sym=sym, model="Q", d=d, flt="real", Q=a.q[0])
        if F:
            ft = np.array([x[0] for x in F]); fs = np.array([x[1] for x in F]); fp = np.array([x[2] for x in F])
            fu = np.array([x[3] for x in F])
            sg = np.where(fs == 1, 1.0, -1.0)
            res["fills_h"] = round(len(F) / hours, 1); res["usd_fill_h"] = round(fu.sum() / hours, 0)
            res["edge"] = round(float(np.nanmean(sg * (fair_y(ft) - fp) / fp * 1e4)), 1)
            for h in a.horizons:
                mo = sg * (fair_y(ft + h * 1000 + a.lat_ms) - fp) / fp * 1e4
                ok = np.isfinite(mo)
                res[f"mo{h}"] = round(float(np.average(mo[ok], weights=fu[ok])), 1) if ok.sum() else float("nan")
                if h == a.horizons[-1]:
                    pf = np.where(ok, mo, 0.0) * fu / 1e4 - fu * a.fee_bps / 1e4
                    res["pnl_h"] = round(float(pf.sum() / hours), 3)
                    h1 = (tm_ - t0) / 3.6e6; h2 = (t1 - tm_) / 3.6e6
                    res["pnl_h1"] = round(float(pf[ft < tm_].sum() / h1), 3)
                    res["pnl_h2"] = round(float(pf[ft >= tm_].sum() / h2), 3)
                    nb_ = max(1, int(round(hours)))
                    ph = np.bincount(np.minimum(((ft - t0) / 3.6e6).astype(int), nb_ - 1), weights=pf, minlength=nb_)
                    res["pos_hours"] = f"{int((ph > 0).sum())}/{nb_}"
            q_ = sg * fu / fp
            fe = fair_y(np.array([t1], dtype=np.int64))[0]
            res["inv_pnl_h"] = round(float((-(q_ * fp).sum() * 1 + q_.sum() * fe) / hours), 3)
        else:
            res.update(fills_h=0.0, usd_fill_h=0, pnl_h=0.0, pnl_h1=0.0, pnl_h2=0.0, inv_pnl_h=0.0,
                       pos_hours=f"0/{max(1, int(round(hours)))}")
        rows.append(res)
    return summ, rows

def qsim(a, d, tick, tY, ymid, bas_t, br, tM, mbid, mask, mbq, maq, tt, px, qq, sd, real, t0, t1, fy_end,
         skew=None, rq_bps=None, trend_bps=None, trend_s=None):
    """[v1.2] реалистичная модель как в spred: тик-округление, порог реквота, очередь,
    лимит позиции, пауза после sweep. Возвращает список филлов (t, side, price, usd)."""
    lat = a.lat_ms
    grid = np.arange(t0, t1, a.q_grid_ms, dtype=np.int64)
    fair = asof(tY, ymid, grid - lat) * np.exp(asof(bas_t, br, grid))
    gb = asof(tM, mbid, grid); ga = asof(tM, mask, grid)
    db = np.floor(fair * (1 - d / 1e4) / tick + 1e-9) * tick
    da = np.ceil(fair * (1 + d / 1e4) / tick - 1e-9) * tick
    db = np.minimum(db, ga - tick); da = np.maximum(da, gb + tick)
    skew = a.q_skew if skew is None else skew            # [v1.3] bps при |pos| = MAX_POS, как MM_FAIR_SKEW_BPS
    rq = (max(2.0, a.q_rq_mult * d) if rq_bps is None else rq_bps) / 1e4   # как MM_FAIR_RQ_BPS
    # [v1.4] фильтр тренда (как MM_TREND_BPS/MM_TREND_SEC в spred F58): не наращивать позицию
    # по направлению против движения fair за окно trend_s
    tb = getattr(a, "trend_bps", 0) if trend_bps is None else trend_bps
    ts = getattr(a, "trend_s", 900) if trend_s is None else trend_s
    if tb and tb > 0:
        k = max(1, int(ts * 1000 / a.q_grid_ms))
        trd = np.zeros(len(fair)); trd[k:] = (fair[k:] / fair[:-k] - 1) * 1e4
        trd = np.nan_to_num(trd)
    else:
        trd = None
    qmed = float(np.nanmedian(np.concatenate([mbq, maq]))) if len(mbq) else 0.0
    # события: смены желаемой цены (после округления) + сделки
    ch = np.flatnonzero((np.diff(db) != 0) | (np.diff(da) != 0)) + 1
    evt = np.concatenate([grid[ch], tt]); kind = np.concatenate([np.zeros(len(ch), int), np.ones(len(tt), int)])
    idx = np.concatenate([ch, np.arange(len(tt))])
    o = np.lexsort((kind, evt))
    Q = a.q[0]; maxpos = a.max_pos_usd; cool = a.sweep_cool_s * 1000
    ordp = [0.0, 0.0]; ahead = [0.0, 0.0]; rem = [0.0, 0.0]   # 0 = bid, 1 = ask
    pos = 0.0; pause = [0, 0]; fills = []
    def place(sdx, p, t):
        ordp[sdx] = p; rem[sdx] = Q / p
        bb = asof(tM, mbid, np.array([t]))[0] if sdx == 0 else asof(tM, mask, np.array([t]))[0]
        bq = asof(tM, mbq if sdx == 0 else maq, np.array([t]))[0]
        better = (p > bb + tick / 2) if sdx == 0 else (p < bb - tick / 2)
        same = abs(p - bb) < tick / 2
        ahead[sdx] = 0.0 if better else (a.q_same_frac * bq if same else a.q_behind_frac * qmed)
    for j in o:
        t = evt[j]; i = idx[j]
        gi = min(np.searchsorted(grid, t, side="right") - 1, len(grid) - 1)
        if gi < 0 or not np.isfinite(fair[gi]): continue
        mid = fair[gi]
        posv = pos * mid
        if skew:
            sh = -(posv / maxpos) * skew
            wb = min(np.floor(mid * (1 + (-d + sh) / 1e4) / tick + 1e-9) * tick, ga[gi] - tick)
            wa = max(np.ceil(mid * (1 + (d + sh) / 1e4) / tick - 1e-9) * tick, gb[gi] + tick)
            want = (wb, wa)
        else:
            want = (db[gi], da[gi])
        allow = (posv < maxpos and t >= pause[0], posv > -maxpos and t >= pause[1])
        if trd is not None:
            if trd[gi] < -tb and posv >= -Q / 2: allow = (False, allow[1])
            if trd[gi] > tb and posv <= Q / 2: allow = (allow[0], False)
        for s_ in (0, 1):
            if not allow[s_] or not np.isfinite(want[s_]) or want[s_] <= 0:
                ordp[s_] = 0.0
            elif ordp[s_] == 0.0 or abs(want[s_] - ordp[s_]) >= max(1.5 * tick, rq * mid):
                place(s_, want[s_], t)
        if kind[j] == 1 and (real[i] or a.q_inside):
            s_ = 0 if sd[i] == 2 else 1
            p = ordp[s_]
            if p <= 0: continue
            tp = px[i]; tq = qq[i]
            through = tp < p - tick / 2 if s_ == 0 else tp > p + tick / 2
            at = abs(tp - p) < tick / 2
            if not real[i] and through:   # как spred MM_PRINT_FILTER=1: внутренний принт не sweep,
                through = False; at = True  # а обычный филл на нашей цене через очередь
            if through:
                f = rem[s_]; pause[s_] = t + cool
            elif at:
                f = max(0.0, tq - ahead[s_]); ahead[s_] = max(0.0, ahead[s_] - tq); f = min(f, rem[s_])
            else:
                continue
            if f <= 0: continue
            rem[s_] -= f
            pos += f if s_ == 0 else -f
            fills.append((t, 1 if s_ == 0 else 2, p, f * p))
            if rem[s_] <= 1e-12: ordp[s_] = 0.0
    return fills

def inv_sim(tt, hb, ha, bid, ask, fq, fair_fn, t_end, maxpos, fee):
    pos = 0.0; cash = 0.0
    idx = np.where(hb | ha)[0]
    for j in idx:
        if hb[j] and pos * bid[j] < maxpos:
            q = fq[j] / bid[j]; pos += q; cash -= q * bid[j] * (1 + fee / 1e4)
        elif ha[j] and -pos * ask[j] < maxpos:
            q = fq[j] / ask[j]; pos -= q; cash += q * ask[j] * (1 - fee / 1e4)
    fe = fair_fn(np.array([t_end], dtype=np.int64))[0]
    return cash + pos * (fe if np.isfinite(fe) else 0.0)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("dir"); ap.add_argument("--glob", default="xr_*.csv.gz")
    ap.add_argument("--last-hours", type=int, default=0, help="взять только N последних файлов")
    ap.add_argument("--syms", default="")
    ap.add_argument("--lat-ms", type=int, default=150)
    ap.add_argument("--basis-min", type=float, default=10)
    ap.add_argument("--in-win-ms", type=int, default=1500)
    ap.add_argument("--move-bps", type=float, default=5)
    ap.add_argument("--fee-bps", type=float, default=0.0)
    ap.add_argument("--max-pos-usd", type=float, default=50)
    ap.add_argument("--d", default="5,10,15,20,30,40,60,80,100")
    ap.add_argument("--q", default="10")
    ap.add_argument("--fixed", default="15,30", help="фиксированные d для сводки")
    ap.add_argument("--q-grid-ms", type=int, default=250, help="[v1.2] шаг пересчёта котировки в модели Q")
    ap.add_argument("--q-same-frac", type=float, default=0.5, help="[v1.2] очередь при цене = лучшей: доля объёма уровня")
    ap.add_argument("--q-behind-frac", type=float, default=0.0, help="[v1.2] очередь за лучшей ценой: доля медианного уровня")
    ap.add_argument("--q-inside", type=int, default=0, help="[v1.2] 1 = внутренние принты дают филл через очередь (оптимистично)")
    ap.add_argument("--q-skew", type=float, default=0, help="[v1.3] сдвиг от позиции, bps при |pos|=MAX_POS (MM_FAIR_SKEW_BPS)")
    ap.add_argument("--q-rq-mult", type=float, default=0.2, help="[v1.3] порог реквота = max(2, mult*d) bps (MM_FAIR_RQ_BPS)")
    ap.add_argument("--trend-bps", type=float, default=0, help="[v1.4] фильтр тренда, bps за окно (MM_TREND_BPS), 0=выкл")
    ap.add_argument("--trend-s", type=float, default=900, help="[v1.4] окно тренда, с (MM_TREND_SEC)")
    ap.add_argument("--sweep-cool-s", type=float, default=15, help="[v1.2] пауза стороны после sweep")
    ap.add_argument("--model", default="Q", help="модель для сводки и oos: Q (как spred) или Y (идеал)")
    ap.add_argument("--horizons", default="5,30,60")
    ap.add_argument("--csv", default="xscan_grid.csv")
    a = ap.parse_args()
    a.d = [float(x) for x in a.d.split(",")]; a.q = [float(x) for x in a.q.split(",")]
    a.horizons = [int(x) for x in a.horizons.split(",")]
    a.fixed = [float(x) for x in a.fixed.split(",")]
    files = sorted(glob.glob(os.path.join(a.dir, a.glob)))
    if a.last_hours: files = files[-a.last_hours:]
    syms = set(s.strip().upper() for s in a.syms.split(",") if s.strip())
    store = load(files, syms)
    if not store: log("нет данных"); return
    S, R = [], []
    for sym in sorted(store):
        g = frame(store.pop(sym))
        t = time.time()
        try:
            s, r = analyze(sym, g, a)
        except Exception as e:
            log(f"{sym}: {e!r}"); continue
        if s: S.append(s); R += r
        del g
        log(f"{sym} {time.time()-t:.1f}s")
    if not S: log("мало данных"); return
    G = pd.DataFrame(R); G.to_csv(a.csv, index=False)
    T = pd.DataFrame(S)
    Q0 = a.q[0]
    for model, flt, nm in (("Q", "real", "bestQ"), ("Y", "real", "bestY_real"), ("Y", "all", "bestY_all"), ("M", "real", "bestM_real")):
        sub = G[(G.model == model) & (G.flt == flt) & (G.Q == Q0)]
        b = sub.loc[sub.groupby("sym").pnl_h.idxmax()].set_index("sym")
        T[nm + "_d"] = T.sym.map(b.d); T[nm + "_$h"] = T.sym.map(b.pnl_h)
        if flt == "real" and model == "Y":
            T["Yr_fills_h"] = T.sym.map(b.fills_h); T["Yr_mo60"] = T.sym.map(b.mo60 if "mo60" in b else b.iloc[:, -2])
            T["Yr_inv$h"] = T.sym.map(b.inv_pnl_h)
    # вне выборки: d выбран по 1-й половине, результат на 2-й (модель --model, real)
    sub = G[(G.model == a.model) & (G.flt == "real") & (G.Q == Q0)]
    b1 = sub.loc[sub.groupby("sym").pnl_h1.idxmax()].set_index("sym")
    T["oos_d"] = T.sym.map(b1.d); T["oos_h1"] = T.sym.map(b1.pnl_h1); T["oos_h2"] = T.sym.map(b1.pnl_h2)
    for dfix in a.fixed:
        f_ = sub[sub.d == dfix].set_index("sym")
        T[f"d{dfix:g}_$h"] = T.sym.map(f_.pnl_h); T[f"d{dfix:g}_inv"] = T.sym.map(f_.inv_pnl_h)
        T[f"d{dfix:g}_ph"] = T.sym.map(f_.pos_hours)
    T = T.sort_values("oos_h2", ascending=False)
    pd.set_option("display.width", 250); pd.set_option("display.max_columns", 50); pd.set_option("display.max_rows", 500)
    print(f"# lat {a.lat_ms} мс, Q {Q0}$, fee {a.fee_bps} bps, маркаут {a.horizons[-1]} с; $h = $/час по маркауту; inv = с лимитом позиции {a.max_pos_usd}$")
    print(T.to_string(index=False))
    print(f"\nсетка: {a.csv}")
    tot = T["bestY_real_$h"].clip(lower=0).sum()
    print(f"сумма плюсовых bestY_real (оптимистично): {tot:.3f} $/ч на {int((T['bestY_real_$h']>0).sum())} парах")
    ok = T[(T.oos_h1 > 0) & (T.oos_h2 > 0)]
    tq = T["bestQ_$h"].clip(lower=0).sum()
    print(f"сумма плюсовых bestQ (модель как spred, лучший d — всё ещё подгонка): {tq:.3f} $/ч на {int((T['bestQ_$h']>0).sum())} парах")
    print(f"[модель {a.model}] вне выборки (d по 1-й половине, плюс в обеих): {len(ok)} пар, 2-я половина {ok.oos_h2.sum():.3f} $/ч: {', '.join(ok.sym)}")
    for dfix in a.fixed:
        c = f"d{dfix:g}_inv"
        print(f"[модель {a.model}] фикс. d={dfix:g}: с лимитом позиции сумма {T[c].sum():.3f} $/ч, в плюсе {int((T[c] > 0).sum())}/{len(T)}")

if __name__ == "__main__":
    main()
