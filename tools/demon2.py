#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
demon2.py v1.0 — рекордер MEXC для scan10 / sim10 / spred. Python 3.8+, БЕЗ зависимостей.

Заменяет demon1 (bbo_rec v3.1). Три потока данных:

  1) BBO всей вселенной (REST bookTicker, по умолчанию раз в 5 с, а не 15):
       bbo_YYYYMMDD.csv.gz   — ТОТ ЖЕ формат, что у demon1 ("t,s,b,a,bq,aq",
       heartbeat "t,#"), scan9/scan10 читают без изменений.
  2) ВСЕ сделки вселенной (WS spot@public.aggre.deals, push-поток, без лимита
     «последние 1000» у REST /trades):
       trades_YYYYMMDD.csv.gz — "t_exch,s,price,qty,side"  (side 1=buy 2=sell
       тейкера). На этом scan10 считает филлы/маркауты по ПОЛНОЙ ленте.
  3) Полная лента бота для кандидатов (depth(20) + bookTicker + deals — ровно
     те каналы, что у spred) в ФОРМАТЕ MM_RAW_LOG:
       raw/SYM_YYYYMMDD_HH.raw  (почасовые; закрытые часы сжимаются в .raw.gz)
     sim10.py прогоняет на них сам spred (MM_REPLAY) — честный paper по каждой
     паре без запуска бота на каждую.

  Кандидаты для (3): --force (через запятую) + --watch scan_watchlist.json
  (перечитывается каждые 5 мин) + топ --raw-auto по последнему scan10-CSV.
  Не больше --raw-max пар (по умолчанию 20: ~3-4 КБ/с на пару, ~300 МБ/сут/пару
  в .raw, ~15-25 МБ после gzip закрытого часа — замер 04.10). --retention-days удаляет старые файлы.

  Лимиты MEXC: <=30 подписок на соединение (тут 30 deals-пар или 10 raw-пар на
  коннект), соединение живёт <=24 ч (переподключаемся сами раз в 23 ч),
  WS-запросы <=100/с (подписки пачками с паузой), REST вес IP 300/10 с.

  python3 demon2.py --out data --force TAGUSDT,GMEUSDT
  nohup python3 demon2.py --out data --force TAGUSDT,GMEUSDT > demon2.log 2>&1 &
"""
import argparse, base64, csv, glob, gzip, json, os, re, signal, socket, ssl
import struct, sys, threading, time, urllib.request
from datetime import datetime, timezone

API = "https://api.mexc.com"
WS_HOST, WS_PATH = "wbs-api.mexc.com", "/ws"
UA = "demon2/1.0"
STABLES = {
    "USDC", "FDUSD", "TUSD", "DAI", "USDE", "USDD", "PYUSD", "USD1", "USDP", "EUR", "EURC",
    "EURT", "BUSD", "USDS", "RLUSD", "XAUT", "PAXG", "USDTB", "GUSD", "FRAX", "LUSD", "CUSD",
    "USDJ", "USTC", "AEUR", "EURI", "BFUSD", "USDQ", "USDR", "USDX", "SUSD", "CRVUSD", "GHO",
}
LEVERAGED = re.compile(r"\d+[LS]$")
STOP = threading.Event()


def log(msg):
    print(f"[d2] {datetime.now():%m-%d %H:%M:%S} {msg}", flush=True)


def http_json(url, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept-Encoding": "gzip"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        data = r.read()
        if r.headers.get("Content-Encoding") == "gzip":
            data = gzip.decompress(data)
        return json.loads(data)


def day_utc(t=None):
    return datetime.fromtimestamp(t or time.time(), timezone.utc).strftime("%Y%m%d")


# ------------------------------------------------------------ protobuf-lite
def _varint(b, i):
    r = s = 0
    while True:
        c = b[i]; i += 1
        r |= (c & 0x7F) << s
        if c < 0x80:
            return r, i
        s += 7


def pb_fields(b):
    """Генератор (field, wire, value) — value: int для 0, bytes для 2."""
    i, n = 0, len(b)
    while i < n:
        tag, i = _varint(b, i)
        f, w = tag >> 3, tag & 7
        if w == 0:
            v, i = _varint(b, i)
        elif w == 2:
            ln, i = _varint(b, i)
            v = b[i:i + ln]; i += ln
        elif w == 1:
            v = b[i:i + 8]; i += 8
        elif w == 5:
            v = b[i:i + 4]; i += 4
        else:
            return
        yield f, w, v


def parse_wrapper(b):
    """-> (channel, symbol, body_field, body_bytes)."""
    ch = sym = ""
    bf, body = 0, b""
    for f, w, v in pb_fields(b):
        if f == 1 and w == 2:
            ch = v.decode("utf-8", "replace")
        elif f == 3 and w == 2:
            sym = v.decode("utf-8", "replace")
        elif f >= 300 and w == 2:
            bf, body = f, v
    if not sym and "@" in ch:
        sym = ch.rsplit("@", 1)[-1] if not ch.split("@")[-1].isdigit() else ch.split("@")[-2]
    return ch, sym, bf, body


def parse_aggre_deals(body):
    out = []
    for f, w, v in pb_fields(body):
        if f == 1 and w == 2:
            px = q = ""
            side = t = 0
            for f2, w2, v2 in pb_fields(v):
                if f2 == 1: px = v2.decode()
                elif f2 == 2: q = v2.decode()
                elif f2 == 3: side = v2
                elif f2 == 4: t = v2
            out.append((t, px, q, side))
    return out


# ------------------------------------------------------------ websocket
class WSConn(threading.Thread):
    """Одно WS-соединение: <=30 каналов, авто-реконнект, PING, ротация 23 ч."""
    MAX_SUBS = 30
    LIFETIME = 23 * 3600

    def __init__(self, name, on_bin, sub_gate):
        super().__init__(daemon=True, name=name)
        self.on_bin = on_bin
        self.gate = sub_gate
        self.lock = threading.Lock()
        self.channels = set()
        self.pending = []            # (op, [channels]) к отправке
        self.sock = None
        self.frames = self.bytes = self.reconnects = self.sub_fail = 0
        self.connected = False

    # -- API для менеджера
    def free(self):
        with self.lock:
            return self.MAX_SUBS - len(self.channels)

    def add(self, chans):
        with self.lock:
            new = [c for c in chans if c not in self.channels]
            self.channels.update(new)
            if new and self.connected:
                self.pending.append(("SUBSCRIPTION", new))

    def remove(self, chans):
        with self.lock:
            old = [c for c in chans if c in self.channels]
            self.channels.difference_update(old)
            if old and self.connected:
                self.pending.append(("UNSUBSCRIPTION", old))

    # -- низкий уровень
    def _send(self, payload, op=1):
        p = payload.encode() if isinstance(payload, str) else payload
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
        raw = socket.create_connection((WS_HOST, 443), timeout=10)
        s = ssl.create_default_context().wrap_socket(raw, server_hostname=WS_HOST)
        key = base64.b64encode(os.urandom(16)).decode()
        s.sendall((f"GET {WS_PATH} HTTP/1.1\r\nHost: {WS_HOST}\r\nUpgrade: websocket\r\n"
                   f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n"
                   f"Sec-WebSocket-Version: 13\r\nUser-Agent: {UA}\r\n\r\n").encode())
        buf = b""
        while b"\r\n\r\n" not in buf:
            d = s.recv(4096)
            if not d:
                raise ConnectionError("handshake EOF")
            buf += d
            if len(buf) > 65536:
                raise ConnectionError("handshake too long")
        head, _, rest = buf.partition(b"\r\n\r\n")
        if b" 101 " not in head.split(b"\r\n")[0]:
            raise ConnectionError(head.split(b"\r\n")[0].decode("latin1"))
        s.settimeout(1.0)
        self.sock = s
        return rest

    def _subscribe(self, op, chans):
        # пачки по 10 каналов в одном сообщении; глобальный гейт <=~20 сообщений/с
        for i in range(0, len(chans), 10):
            self.gate.wait()
            self._send(json.dumps({"method": op, "params": chans[i:i + 10]}))

    def run(self):
        backoff = 1.0
        while not STOP.is_set():
            try:
                buf = self._connect()
                with self.lock:
                    chans = sorted(self.channels)
                    self.pending.clear()
                    self.connected = True
                self._subscribe("SUBSCRIPTION", chans)
                t_open = last_ping = last_rx = time.time()
                backoff = 1.0
                while not STOP.is_set():
                    now = time.time()
                    if now - t_open > self.LIFETIME:
                        raise ConnectionError("плановая ротация 23 ч")
                    if now - last_ping > 20:
                        self._send('{"method":"PING"}')
                        last_ping = now
                    if now - last_rx > 75:
                        raise ConnectionError("тишина 75 с")
                    with self.lock:
                        todo, self.pending = self.pending, []
                    for op, ch in todo:
                        self._subscribe(op, ch)
                    # чтение кадра
                    while len(buf) < 2:
                        try:
                            d = self.sock.recv(65536)
                        except socket.timeout:
                            d = None
                            break
                        if not d:
                            raise ConnectionError("EOF")
                        buf += d
                    if len(buf) < 2:
                        continue
                    op = buf[0] & 15
                    n = buf[1] & 127
                    hl = 2
                    if n == 126:
                        hl = 4
                    elif n == 127:
                        hl = 10
                    while len(buf) < hl:
                        d = self._recv_block()
                        buf += d
                    if n == 126:
                        n = struct.unpack(">H", buf[2:4])[0]
                    elif n == 127:
                        n = struct.unpack(">Q", buf[2:10])[0]
                    while len(buf) < hl + n:
                        buf += self._recv_block()
                    payload = buf[hl:hl + n]
                    buf = buf[hl + n:]
                    last_rx = time.time()
                    if op == 2:
                        self.frames += 1
                        self.bytes += n
                        self.on_bin(payload, time.monotonic() * 1000.0)
                    elif op == 1:
                        if b"Not Subscribed" in payload or b'"code":0' not in payload \
                                and b"PONG" not in payload:
                            self.sub_fail += 1
                            if self.sub_fail <= 5:
                                log(f"{self.name}: {payload[:160].decode('utf-8','replace')}")
                    elif op == 9:
                        self._send(payload, 10)
                    elif op == 8:
                        raise ConnectionError("close frame")
            except Exception as e:
                with self.lock:
                    self.connected = False
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

    def _recv_block(self):
        while True:
            try:
                d = self.sock.recv(65536)
            except socket.timeout:
                if STOP.is_set():
                    raise ConnectionError("stop")
                continue
            if not d:
                raise ConnectionError("EOF")
            return d


class Gate:
    """Глобальный лимит исходящих WS-сообщений (MEXC: 100/с на IP)."""
    def __init__(self, per_sec=20):
        self.dt = 1.0 / per_sec
        self.t = 0.0
        self.lock = threading.Lock()

    def wait(self):
        with self.lock:
            now = time.time()
            if self.t > now:
                time.sleep(self.t - now)
            self.t = max(now, self.t) + self.dt


# ------------------------------------------------------------ writers
class DayGz:
    """gzip-файл с суточной ротацией (UTC) и периодическим flush (читается на лету)."""
    def __init__(self, outdir, prefix):
        self.outdir, self.prefix = outdir, prefix
        self.day = None
        self.fh = None
        self.lock = threading.Lock()
        self.last_flush = 0.0
        self.lines = 0

    def write(self, text):
        with self.lock:
            d = day_utc()
            if d != self.day:
                if self.fh:
                    self.fh.close()
                self.day = d
                path = os.path.join(self.outdir, f"{self.prefix}_{d}.csv.gz")
                self.fh = gzip.open(path, "at", encoding="utf-8", compresslevel=6)
            self.fh.write(text)
            self.lines += text.count("\n")
            if time.time() - self.last_flush > 10:
                self.fh.flush()
                self.last_flush = time.time()

    def close(self):
        with self.lock:
            if self.fh:
                self.fh.close()
                self.fh = None


class RawWriter:
    """Почасовые raw-файлы формата MM_RAW_LOG: [double recvMs][uint32 len][payload]."""
    def __init__(self, rawdir):
        self.dir = rawdir
        os.makedirs(rawdir, exist_ok=True)
        self.files = {}              # sym -> (hour_key, fh)
        self.lock = threading.Lock()
        self.closed_q = []
        self.bytes = 0

    def write(self, sym, mono_ms, payload):
        hk = datetime.now(timezone.utc).strftime("%Y%m%d_%H")
        with self.lock:
            cur = self.files.get(sym)
            if not cur or cur[0] != hk:
                if cur:
                    cur[1].close()
                    self.closed_q.append(cur[1].name)
                fh = open(os.path.join(self.dir, f"{sym}_{hk}.raw"), "ab")
                self.files[sym] = cur = (hk, fh)
            cur[1].write(struct.pack("<dI", mono_ms, len(payload)) + payload)
            self.bytes += len(payload) + 12

    def drop(self, sym):
        with self.lock:
            cur = self.files.pop(sym, None)
            if cur:
                cur[1].close()
                self.closed_q.append(cur[1].name)

    def flush_all(self):
        with self.lock:
            for _, fh in self.files.values():
                fh.flush()
            q, self.closed_q = self.closed_q, []
        for path in q:                       # сжатие закрытых часов
            try:
                with open(path, "rb") as src, gzip.open(path + ".gz", "wb", 6) as dst:
                    while True:
                        b = src.read(1 << 20)
                        if not b:
                            break
                        dst.write(b)
                os.remove(path)
            except Exception as e:
                log(f"[raw] сжатие {path}: {e!r}")

    def close(self):
        with self.lock:
            for _, fh in self.files.values():
                self.closed_q.append(fh.name)
                fh.close()
            self.files.clear()


# ------------------------------------------------------------ рекордер
class Recorder:
    def __init__(self, a):
        self.a = a
        os.makedirs(a.out, exist_ok=True)
        self.gate = Gate(20)
        self.trades = DayGz(a.out, "trades")
        self.bbo = DayGz(a.out, "bbo")
        self.raw = RawWriter(os.path.join(a.out, "raw"))
        self.conns = []              # WSConn
        self.deal_owner = {}         # sym -> conn (deals-вселенная)
        self.raw_owner = {}          # sym -> conn (полная лента)
        self.universe = set()
        self.raw_set = set()
        self.force = {s.strip().upper() for s in a.force.split(",") if s.strip()}
        self.n_deals = 0
        self.last_bbo = {}
        self.bbo_polls = 0
        self.lock = threading.Lock()

    # --- входящие кадры
    def on_bin(self, payload, mono_ms):
        try:
            ch, sym, bf, body = parse_wrapper(payload)
        except Exception:
            return
        if sym in self.raw_set:
            self.raw.write(sym, mono_ms, payload)
        if bf == 314:                        # aggre.deals
            try:
                ds = parse_aggre_deals(body)
            except Exception:
                return
            if ds:
                self.n_deals += len(ds)
                self.trades.write("".join(f"{t},{sym},{p},{q},{s}\n" for t, p, q, s in ds))

    def _conn_with_space(self, need, pool):
        for c in self.conns:
            if c.kind == pool and c.free() >= need:
                return c
        c = WSConn(f"{pool}{len([x for x in self.conns if x.kind == pool]) + 1}",
                   self.on_bin, self.gate)
        c.kind = pool
        self.conns.append(c)
        c.start()
        return c

    @staticmethod
    def deal_ch(s):
        return f"spot@public.aggre.deals.v3.api.pb@100ms@{s}"

    @staticmethod
    def raw_chs(s):
        return [f"spot@public.limit.depth.v3.api.pb@{s}@20",
                f"spot@public.aggre.bookTicker.v3.api.pb@100ms@{s}",
                f"spot@public.aggre.deals.v3.api.pb@100ms@{s}"]

    def apply_sets(self, deal_syms, raw_syms):
        # raw-пары получают сделки через свой коннект — из deals-пула их убираем
        deal_syms = set(deal_syms) - set(raw_syms)
        for s in list(self.deal_owner):
            if s not in deal_syms:
                self.deal_owner.pop(s).remove([self.deal_ch(s)])
        for s in list(self.raw_owner):
            if s not in raw_syms:
                self.raw_owner.pop(s).remove(self.raw_chs(s))
                self.raw_set.discard(s)
                self.raw.drop(s)
        for s in sorted(raw_syms):
            if s not in self.raw_owner:
                c = self._conn_with_space(3, "raw")
                c.add(self.raw_chs(s))
                self.raw_owner[s] = c
                self.raw_set.add(s)
        for s in sorted(deal_syms):
            if s not in self.deal_owner:
                c = self._conn_with_space(1, "deals")
                c.add([self.deal_ch(s)])
                self.deal_owner[s] = c

    # --- вселенная
    def refresh_universe(self):
        try:
            info = http_json(API + "/api/v3/exchangeInfo", 30)
            t24 = http_json(API + "/api/v3/ticker/24hr", 30)
        except Exception as e:
            log(f"[universe] сбой: {e!r} — оставляю прежнюю ({len(self.universe)})")
            return
        ok = set()
        for s in info.get("symbols", []):
            if s.get("quoteAsset") != "USDT":
                continue
            base = s.get("baseAsset", "").upper()
            if base in STABLES or LEVERAGED.search(base):
                continue
            if str(s.get("status")) in ("1", "ENABLED") and s.get("isSpotTradingAllowed", True):
                ok.add(s["symbol"])
        lo, hi = self.a.min_vol, self.a.max_vol
        new = set()
        for t in t24 if isinstance(t24, list) else []:
            sym = t.get("symbol")
            if sym not in ok:
                continue
            try:
                qv = float(t.get("quoteVolume") or 0)
            except (TypeError, ValueError):
                continue
            # гистерезис: уже записываемую пару держим в полосе 0.7..1.3
            if sym in self.universe:
                if 0.7 * lo <= qv <= 1.3 * hi:
                    new.add(sym)
            elif lo <= qv <= hi:
                new.add(sym)
        new |= self.force | {"BTCUSDT", "ETHUSDT"}
        if len(new) > 5:
            add, drop = len(new - self.universe), len(self.universe - new)
            self.universe = new
            log(f"[universe] {len(new)} пар (+{add}/-{drop})")

    def pick_raw(self):
        want = list(self.force)
        for path in self.a.watch:
            for p in sorted(glob.glob(path)):
                try:
                    j = json.load(open(p, encoding="utf-8"))
                    for x in j if isinstance(j, list) else j.get("pairs", []):
                        s = x["symbol"] if isinstance(x, dict) else str(x)
                        if s not in want:
                            want.append(s)
                except Exception:
                    try:
                        for s in open(p, encoding="utf-8").read().replace(",", " ").split():
                            if s.upper() not in want:
                                want.append(s.upper())
                    except Exception:
                        pass
        if self.a.raw_auto > 0:
            csvs = sorted(glob.glob(os.path.join(self.a.scan_dir, "scan*_2*.csv")),
                          key=os.path.getmtime)
            if csvs:
                try:
                    with open(csvs[-1], encoding="utf-8-sig") as fh:
                        rows = list(csv.DictReader(fh))
                    k = 0
                    for r in rows:
                        s = r.get("symbol")
                        if s and s not in want and r.get("flags", "").find("NEG") < 0:
                            want.append(s)
                            k += 1
                            if k >= self.a.raw_auto:
                                break
                except Exception:
                    pass
        return set(want[: self.a.raw_max])

    # --- BBO REST
    def poll_bbo(self, full):
        d = http_json(API + "/api/v3/ticker/bookTicker", 15)
        if not isinstance(d, list):
            return
        tms = int(time.time() * 1000)
        out = [f"{tms},#\n"]
        u = self.universe
        for x in d:
            s = x.get("symbol", "")
            if s not in u:
                continue
            try:
                b = float(x.get("bidPrice") or 0)
                a = float(x.get("askPrice") or 0)
            except ValueError:
                continue
            if b <= 0 or a <= b:
                continue
            cur = (b, a, x.get("bidQty"), x.get("askQty"))
            if full or self.last_bbo.get(s) != cur:
                self.last_bbo[s] = cur
                out.append(f"{tms},{s},{b!r},{a!r},{cur[2]},{cur[3]}\n")
        self.bbo.write("".join(out))
        self.bbo_polls += 1

    def cleanup(self):
        if self.a.retention_days <= 0:
            return
        cut = time.time() - self.a.retention_days * 86400
        n = 0
        for p in glob.glob(os.path.join(self.a.out, "raw", "*.raw.gz")):
            try:
                if os.path.getmtime(p) < cut:
                    os.remove(p); n += 1
            except OSError:
                pass
        if n:
            log(f"[clean] удалено {n} старых raw-часов")

    def run(self):
        a = self.a
        log(f"старт: out={os.path.abspath(a.out)}, BBO каждые {a.bbo_every:g} с, "
            f"raw-max {a.raw_max}, force {sorted(self.force)}")
        self.refresh_universe()
        raw = self.pick_raw()
        self.apply_sets(self.universe, raw)
        log(f"[ws] deals-пар {len(self.deal_owner)}, raw-пар {len(self.raw_owner)} "
            f"({sorted(self.raw_owner)}), коннектов {len(self.conns)}")
        t_univ = t_raw = t_stat = t_flush = t_clean = time.time()
        t_full = 0.0
        next_bbo = time.time()
        while not STOP.is_set():
            now = time.time()
            if now >= next_bbo:
                next_bbo = now + a.bbo_every
                try:
                    full = now - t_full >= 60
                    if full:
                        t_full = now
                    self.poll_bbo(full)
                except Exception as e:
                    log(f"[bbo] {e!r}")
            if now - t_univ >= a.universe_refresh:
                t_univ = now
                self.refresh_universe()
                self.apply_sets(self.universe, self.raw_set)
            if now - t_raw >= 300:
                t_raw = now
                raw = self.pick_raw()
                if raw != self.raw_set:
                    log(f"[raw] набор: +{sorted(raw - self.raw_set)} -{sorted(self.raw_set - raw)}")
                    self.apply_sets(self.universe, raw)
            if now - t_flush >= 10:
                t_flush = now
                self.raw.flush_all()
            if now - t_clean >= 3600:
                t_clean = now
                self.cleanup()
            if now - t_stat >= a.stat_every:
                t_stat = now
                fr = sum(c.frames for c in self.conns)
                rc = sum(c.reconnects for c in self.conns)
                live = sum(1 for c in self.conns if c.connected)
                log(f"[stat] коннектов {live}/{len(self.conns)} (реконнектов {rc}), "
                    f"кадров {fr}, сделок {self.n_deals}, BBO-опросов {self.bbo_polls}, "
                    f"raw {self.raw.bytes / 1e6:.1f} МБ, вселенная {len(self.universe)}")
            STOP.wait(0.2)
        log("остановка: закрываю файлы")
        self.trades.close()
        self.bbo.close()
        self.raw.close()
        self.raw.flush_all()


def main():
    ap = argparse.ArgumentParser(description="demon2 — рекордер MEXC (BBO + все сделки + raw кандидатов)")
    ap.add_argument("--out", default="data")
    ap.add_argument("--bbo-every", type=float, default=5.0)
    ap.add_argument("--min-vol", type=float, default=50_000)
    ap.add_argument("--max-vol", type=float, default=5_000_000)
    ap.add_argument("--universe-refresh", type=float, default=600)
    ap.add_argument("--force", default="", help="пары с полной лентой всегда, через запятую")
    ap.add_argument("--watch", nargs="*", default=["scan_watchlist.json"],
                    help="watchlist.json / txt со списком пар для полной ленты")
    ap.add_argument("--scan-dir", default=".", help="где искать последний scan*_*.csv")
    ap.add_argument("--raw-auto", type=int, default=10, help="+топ-N из последнего scan-CSV")
    ap.add_argument("--raw-max", type=int, default=20)
    ap.add_argument("--retention-days", type=float, default=4)
    ap.add_argument("--stat-every", type=float, default=300)
    ap.add_argument("--duration", type=float, default=0, help="сек (0 = бесконечно)")
    a = ap.parse_args()
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(sig, lambda *_: STOP.set())
    if a.duration > 0:
        threading.Timer(a.duration, STOP.set).start()
    Recorder(a).run()


if __name__ == "__main__":
    main()
