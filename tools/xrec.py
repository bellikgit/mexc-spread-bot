#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
xrec.py v1.1 — двухбиржевой рекордер: MEXC (где торгуем) + Bybit (справедливая цена).
Python 3.8+, без зависимостей; MEXC-часть берёт WS-клиент и парсеры из demon2.py
(положить рядом). Для xscan.py: lead-lag, базис, λ(d)/m(d) от Bybit-мида.

Пишет почасовые x/xr_YYYYMMDD_HH.csv.gz (локальное время t_ms — ОБЩИЕ часы для
обеих бирж, это важно для lead-lag):
  B,t_ms,V,SYM,bid,bq,ask,aq,t_exch     V: M=MEXC, Y=Bybit (BBO: при смене цены,
                                        смена объёма — не чаще 1 раза/с)
  T,t_ms,V,SYM,price,qty,side,t_exch    side 1=buy 2=sell (агрессор)

Пары: общие USDT-споты MEXC и Bybit, отбор при старте (--auto N):
  Bybit: спред <= --ref-max-bps, оборот 24ч >= --ref-min-vol;
  MEXC : спред >= --min-bps и >= --ratio x спреда Bybit, объём >= --min-vol;
  цены совпадают в пределах 2% (защита от одноимённых разных токенов).
  + --force SYM1,SYM2. Список и метрики отбора -> x/xrec_pairs.json.

  nohup python3 xrec.py --out data/x --auto 40 > xrec.log 2>&1 &

v1.1: --quote USDC — пары MEXC BASEUSDC, справедливая цена с Bybit BASEUSDT;
  строки Y пишутся под символом MEXC (BASEUSDC), xscan сопоставляет по SYM.
  nohup python3 xrec.py --quote USDC --out data/xu --auto 40 > xrecu.log 2>&1 &
"""
import argparse, base64, glob, gzip, json, math, os, socket, ssl, struct, sys
import threading, time, urllib.request
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import demon2 as d2   # WSConn, Gate, parse_wrapper, pb_fields, parse_aggre_deals, STOP

STOP = d2.STOP
UA = "xrec/1.1"
BYBIT_WS = ("stream.bybit.com", "/v5/public/spot")


def log(msg):
    print(f"[xr] {datetime.now():%m-%d %H:%M:%S} {msg}", flush=True)


def http_json(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def fnum(x):
    try:
        v = float(x)
        return v if math.isfinite(v) else 0.0
    except (TypeError, ValueError):
        return 0.0


# ------------------------------------------------------------ writer
class HourGz:
    def __init__(self, outdir, keep_days):
        self.dir = outdir
        self.keep = keep_days
        os.makedirs(outdir, exist_ok=True)
        self.key = None
        self.fh = None
        self.lock = threading.Lock()
        self.last_flush = time.time()
        self.lines = 0

    def write(self, line):
        with self.lock:
            k = datetime.now(timezone.utc).strftime("%Y%m%d_%H")
            if k != self.key:
                if self.fh:
                    self.fh.close()
                self.fh = gzip.open(os.path.join(self.dir, f"xr_{k}.csv.gz"), "at",
                                    encoding="utf-8", compresslevel=6)
                self.key = k
                self.cleanup()
            self.fh.write(line)
            self.lines += 1
            now = time.time()
            if now - self.last_flush > 5:
                self.fh.flush()
                self.last_flush = now

    def cleanup(self):
        lim = time.time() - self.keep * 86400
        for p in glob.glob(os.path.join(self.dir, "xr_*.csv.gz")):
            try:
                if os.path.getmtime(p) < lim:
                    os.remove(p)
            except OSError:
                pass

    def close(self):
        with self.lock:
            if self.fh:
                self.fh.close()
                self.fh = None


class BboDedup:
    """Пишем BBO при смене цены; смена только объёма — не чаще раза в 1 с."""
    def __init__(self):
        self.last = {}

    def need(self, key, b, a, bq, aq, now):
        l = self.last.get(key)
        if l is None or l[0] != b or l[1] != a:
            self.last[key] = (b, a, now)
            return True
        if now - l[2] >= 1.0:
            self.last[key] = (b, a, now)
            return True
        return False


# ------------------------------------------------------------ Bybit WS (JSON)
class BybitWS(threading.Thread):
    def __init__(self, name, syms, on_msg):
        super().__init__(daemon=True, name=name)
        self.syms = syms
        self.on_msg = on_msg
        self.sock = None
        self.frames = self.reconnects = 0

    def _send(self, text, op=1):
        p = text.encode() if isinstance(text, str) else text
        m = os.urandom(4)
        n = len(p)
        if n < 126:
            h = bytes([0x80 | op, 0x80 | n])
        elif n < 65536:
            h = bytes([0x80 | op, 0x80 | 126]) + struct.pack(">H", n)
        else:
            h = bytes([0x80 | op, 0x80 | 127]) + struct.pack(">Q", n)
        mm = (m * (n // 4 + 1))[:n]
        self.sock.sendall(h + m + bytes(a ^ b for a, b in zip(p, mm)))

    def _connect(self):
        host, path = BYBIT_WS
        raw = socket.create_connection((host, 443), timeout=10)
        s = ssl.create_default_context().wrap_socket(raw, server_hostname=host)
        key = base64.b64encode(os.urandom(16)).decode()
        s.sendall((f"GET {path} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\n"
                   f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n"
                   f"Sec-WebSocket-Version: 13\r\nUser-Agent: {UA}\r\n\r\n").encode())
        buf = b""
        while b"\r\n\r\n" not in buf:
            d = s.recv(4096)
            if not d:
                raise ConnectionError("handshake EOF")
            buf += d
        head, _, rest = buf.partition(b"\r\n\r\n")
        if b" 101 " not in head.split(b"\r\n")[0]:
            raise ConnectionError(head.split(b"\r\n")[0].decode("latin1"))
        s.settimeout(1.0)
        self.sock = s
        return rest

    def _recv(self):
        while True:
            try:
                d = self.sock.recv(65536)
            except socket.timeout:
                if STOP.is_set():
                    raise ConnectionError("stop")
                return None
            if not d:
                raise ConnectionError("EOF")
            return d

    def run(self):
        backoff = 1.0
        while not STOP.is_set():
            try:
                buf = self._connect()
                args = []
                for s in self.syms:
                    args += [f"orderbook.1.{s}", f"publicTrade.{s}"]
                for i in range(0, len(args), 10):    # spot: <=10 args на запрос
                    self._send(json.dumps({"op": "subscribe", "args": args[i:i + 10]}))
                    time.sleep(0.05)
                last_ping = last_rx = time.time()
                backoff = 1.0
                frag = b""
                while not STOP.is_set():
                    now = time.time()
                    if now - last_ping > 20:
                        self._send('{"op":"ping"}')
                        last_ping = now
                    if now - last_rx > 60:
                        raise ConnectionError("тишина 60 с")
                    while len(buf) < 2:
                        d = self._recv()
                        if d is None:
                            break
                        buf += d
                    if len(buf) < 2:
                        continue
                    fin = buf[0] & 0x80
                    op = buf[0] & 15
                    n = buf[1] & 127
                    hl = 2 + (2 if n == 126 else 8 if n == 127 else 0)
                    while len(buf) < hl:
                        d = self._recv()
                        if d:
                            buf += d
                    if n == 126:
                        n = struct.unpack(">H", buf[2:4])[0]
                    elif n == 127:
                        n = struct.unpack(">Q", buf[2:10])[0]
                    while len(buf) < hl + n:
                        d = self._recv()
                        if d:
                            buf += d
                    payload = buf[hl:hl + n]
                    buf = buf[hl + n:]
                    last_rx = time.time()
                    if op in (1, 0):
                        frag += payload
                        if not fin:
                            continue
                        msg, frag = frag, b""
                        self.frames += 1
                        try:
                            self.on_msg(json.loads(msg), last_rx)
                        except Exception as e:
                            if self.frames < 5:
                                log(f"{self.name}: parse {e!r}")
                    elif op == 9:
                        self._send(payload, 10)
                    elif op == 8:
                        raise ConnectionError("close frame")
            except Exception as e:
                try:
                    if self.sock:
                        self.sock.close()
                except Exception:
                    pass
                self.sock = None
                if STOP.is_set():
                    break
                self.reconnects += 1
                log(f"{self.name}: {e!r} — реконнект через {backoff:.0f} с")
                STOP.wait(backoff)
                backoff = min(backoff * 2, 60.0)


# ------------------------------------------------------------ отбор пар
def select_pairs(a):
    info = http_json(d2.API + "/api/v3/exchangeInfo", 30)
    ok = {}
    for s in info.get("symbols", []):
        if s.get("quoteAsset") != a.quote:
            continue
        base = s.get("baseAsset", "").upper()
        if base in d2.STABLES or d2.LEVERAGED.search(base):
            continue
        if str(s.get("status")) in ("1", "ENABLED") and s.get("isSpotTradingAllowed", True):
            ok[s["symbol"]] = s
    mb = {x["symbol"]: x for x in http_json(d2.API + "/api/v3/ticker/bookTicker", 30)}
    mv = {x["symbol"]: fnum(x.get("quoteVolume")) for x in http_json(d2.API + "/api/v3/ticker/24hr", 30)}
    yb = http_json("https://api.bybit.com/v5/market/tickers?category=spot", 30)
    yl = {x["symbol"]: x for x in yb.get("result", {}).get("list", [])}
    rows = []
    for ysym, yt in yl.items():
        if not ysym.endswith("USDT"):
            continue
        sym = ysym[:-4] + a.quote          # символ MEXC (BASEUSDT или BASEUSDC)
        if sym not in ok or sym not in mb:
            continue
        yb_, ya_ = fnum(yt.get("bid1Price")), fnum(yt.get("ask1Price"))
        mb_, ma_ = fnum(mb[sym].get("bidPrice")), fnum(mb[sym].get("askPrice"))
        if not (yb_ > 0 and ya_ > yb_ and mb_ > 0 and ma_ > mb_):
            continue
        ym, mm = (yb_ + ya_) / 2, (mb_ + ma_) / 2
        if abs(mm / ym - 1) > 0.02:
            continue
        ys = (ya_ - yb_) / ym * 1e4
        ms = (ma_ - mb_) / mm * 1e4
        yv = fnum(yt.get("turnover24h"))
        qv = mv.get(sym, 0.0)
        rows.append(dict(sym=sym, ref=ysym, mexc_bps=round(ms, 1), bybit_bps=round(ys, 1),
                         mexc_vol=round(qv), bybit_vol=round(yv),
                         basis_bps=round((mm / ym - 1) * 1e4, 1)))
    force = [s.strip().upper() for s in a.force.split(",") if s.strip()]
    cand = [r for r in rows if r["bybit_bps"] <= a.ref_max_bps and r["bybit_vol"] >= a.ref_min_vol
            and r["mexc_bps"] >= a.min_bps and r["mexc_bps"] >= a.ratio * max(r["bybit_bps"], 0.1)
            and r["mexc_vol"] >= a.min_vol and r["sym"] not in force]
    # потенциал: лишний спред MEXC x корень из объёма MEXC
    for r in cand:
        r["score"] = round((r["mexc_bps"] - 2 * r["bybit_bps"]) * math.sqrt(r["mexc_vol"]), 0)
    cand.sort(key=lambda r: -r["score"])
    pick = [r for r in rows if r["sym"] in force] + cand[:max(0, a.auto)]
    log(f"[pairs] общих MEXC∩Bybit: {len(rows)}, прошли фильтр: {len(cand)}, беру {len(pick)}")
    return pick, rows


# ------------------------------------------------------------ рекордер
class XRec:
    def __init__(self, a, syms, refs=None):
        self.a = a
        self.syms = syms
        self.refs = refs or list(syms)                 # символы Bybit
        self.ref2m = dict(zip(self.refs, self.syms))   # Bybit -> MEXC
        self.out = HourGz(a.out, a.retention_days)
        self.dd = BboDedup()
        self.cnt = {"MB": 0, "MT": 0, "YB": 0, "YT": 0}
        self.ybook = {}

    def on_mexc(self, payload, mono_ms):
        now = time.time()
        try:
            ch, sym, bf, body = d2.parse_wrapper(payload)
        except Exception:
            return
        tms = int(now * 1000)
        if bf == 315:
            b = bq = a = aq = 0.0
            for f, w, v in d2.pb_fields(body):
                if w != 2:
                    continue
                if f == 1: b = fnum(v.decode())
                elif f == 2: bq = fnum(v.decode())
                elif f == 3: a = fnum(v.decode())
                elif f == 4: aq = fnum(v.decode())
            send = 0
            for f, w, v in d2.pb_fields(payload):
                if f == 6 and w == 0:
                    send = v
            if b > 0 and a > b and self.dd.need(("M", sym), b, a, bq, aq, now):
                self.out.write(f"B,{tms},M,{sym},{b:.10g},{bq:.10g},{a:.10g},{aq:.10g},{send}\n")
                self.cnt["MB"] += 1
        elif bf == 314:
            for t, px, q, side in d2.parse_aggre_deals(body):
                self.out.write(f"T,{tms},M,{sym},{px},{q},{side},{t}\n")
                self.cnt["MT"] += 1

    def on_bybit(self, m, now):
        topic = m.get("topic", "")
        if not topic:
            return
        tms = int(now * 1000)
        d = m.get("data")
        if topic.startswith("orderbook.1."):
            sym = d.get("s") or topic.split(".")[-1]
            sym = self.ref2m.get(sym, sym)
            book = self.ybook.setdefault(sym, [0.0, 0.0, 0.0, 0.0])
            snap = m.get("type") == "snapshot"
            for side, i in (("b", 0), ("a", 2)):
                lv = d.get(side) or []
                if lv:
                    p, q = fnum(lv[0][0]), fnum(lv[0][1])
                    if q > 0:
                        book[i], book[i + 1] = p, q
                    elif not snap:
                        book[i], book[i + 1] = 0.0, 0.0
                elif snap:
                    book[i], book[i + 1] = 0.0, 0.0
            b, bq, a, aq = book
            if b > 0 and a > b and self.dd.need(("Y", sym), b, a, bq, aq, now):
                self.out.write(f"B,{tms},Y,{sym},{b:.10g},{bq:.10g},{a:.10g},{aq:.10g},{m.get('ts', 0)}\n")
                self.cnt["YB"] += 1
        elif topic.startswith("publicTrade."):
            for t in d or []:
                side = 1 if t.get("S") == "Buy" else 2
                self.out.write(f"T,{tms},Y,{self.ref2m.get(t.get('s'), t.get('s'))},{t.get('p')},{t.get('v')},{side},{t.get('T', 0)}\n")
                self.cnt["YT"] += 1

    def run(self):
        gate = d2.Gate(20)
        conns = []
        chans = []
        for s in self.syms:
            chans += [f"spot@public.aggre.bookTicker.v3.api.pb@100ms@{s}",
                      f"spot@public.aggre.deals.v3.api.pb@100ms@{s}"]
        for i in range(0, len(chans), 30):           # MEXC: <=30 подписок на коннект
            c = d2.WSConn(f"mexc{i // 30 + 1}", self.on_mexc, gate)
            c.kind = "x"
            c.add(chans[i:i + 30])
            c.start()
            conns.append(c)
        ys = []
        for i in range(0, len(self.refs), 25):
            y = BybitWS(f"bybit{i // 25 + 1}", self.refs[i:i + 25], self.on_bybit)
            y.start()
            ys.append(y)
        log(f"[run] {len(self.syms)} пар: MEXC {len(conns)} коннект., Bybit {len(ys)} коннект.")
        t0 = time.time()
        while not STOP.is_set():
            STOP.wait(60)
            el = max(time.time() - t0, 1)
            log("[stat] " + " ".join(f"{k} {v} ({v / el:.1f}/с)" for k, v in self.cnt.items()) +
                f" | mexc reconn {sum(c.reconnects for c in conns)}, bybit reconn "
                f"{sum(y.reconnects for y in ys)}")
        self.out.close()


def main():
    ap = argparse.ArgumentParser(description="MEXC+Bybit рекордер для xscan.py")
    ap.add_argument("--out", default="x")
    ap.add_argument("--quote", default="USDT", type=str.upper, help="котируемая MEXC: USDT или USDC (Bybit всегда USDT)")
    ap.add_argument("--auto", type=int, default=40, help="сколько пар взять по отбору")
    ap.add_argument("--force", default="", help="SYM1,SYM2 — писать всегда")
    ap.add_argument("--min-bps", type=float, default=15.0, help="мин. спред MEXC, bps")
    ap.add_argument("--ratio", type=float, default=2.5, help="спред MEXC / спред Bybit, не меньше")
    ap.add_argument("--min-vol", type=float, default=20000, help="мин. объём MEXC 24ч, USDT")
    ap.add_argument("--ref-max-bps", type=float, default=15.0, help="макс. спред Bybit, bps")
    ap.add_argument("--ref-min-vol", type=float, default=300000, help="мин. оборот Bybit 24ч, USDT")
    ap.add_argument("--retention-days", type=float, default=5)
    ap.add_argument("--dry", action="store_true", help="только показать отбор")
    a = ap.parse_args()
    pick, rows = select_pairs(a)
    os.makedirs(a.out, exist_ok=True)
    with open(os.path.join(a.out, "xrec_pairs.json"), "w", encoding="utf-8") as fh:
        json.dump({"t": datetime.now().isoformat(timespec="seconds"), "args": vars(a),
                   "pick": pick, "all": rows}, fh, ensure_ascii=False, indent=1)
    print(f"{'SYM':<14}{'MEXC bps':>9}{'Bybit bps':>10}{'MEXC vol':>11}{'Bybit vol':>13}{'basis':>7}")
    for r in pick:
        print(f"{r['sym']:<14}{r['mexc_bps']:>9}{r['bybit_bps']:>10}{r['mexc_vol']:>11}"
              f"{r['bybit_vol']:>13}{r['basis_bps']:>7}")
    if a.dry or not pick:
        return
    import signal
    signal.signal(signal.SIGTERM, lambda *_: STOP.set())
    try:
        XRec(a, [r["sym"] for r in pick], [r.get("ref", r["sym"]) for r in pick]).run()
    except KeyboardInterrupt:
        STOP.set()


if __name__ == "__main__":
    main()
