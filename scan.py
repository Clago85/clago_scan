#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SCAN TRADING GIORNALIERO — funnel 48 -> max 2 trade/giorno
L1 Regime (BTC D1/H4, BTC.D ex-stables, TOTAL alts ex-stables) -> bias o flat
L2 Eventi (promemoria manuale) | L3 Selezione (RS, RVOL, struttura, ATR)
L4 Derivati (prezzo+OI+funding, CVD proxy) | L5 Esecuzione (VWAP, livelli, 2:1)
Dati: Binance perp (fallback OKX -> Bitget), CoinGecko (global, stables, PAXG oro).
Output: Telegram + console. Include "oggi zero trade". Non è consiglio finanziario.
"""

import os, sys, json, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
try:
    from zoneinfo import ZoneInfo
    TZ = ZoneInfo("Europe/Rome")
except Exception:
    TZ = timezone.utc

import requests

TG_TOKEN = os.environ.get("TG_TOKEN", "").strip()   # secret GitHub: TG_TOKEN
TG_CHAT  = os.environ.get("TG_CHAT", "").strip()     # secret GitHub: TG_CHAT

FAPI = "https://fapi.binance.com"
OKX  = "https://www.okx.com"
BGET = "https://api.bitget.com"
CG   = "https://api.coingecko.com/api/v3"

STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scan_state.json")

MAX_TRADES = 2
RVOL_MIN = 1.3
ATR_CONSUMED_MAX = 0.70
LEVEL_PROX = 0.015
MIN_RR = 2.0
FUNDING_CROWDED = 0.0005
WINDOW = (9, 23)   # run ammessi dalle 9:00 alle 22:59

UNIVERSE = {
    "BTC": "MAJ", "ETH": "MAJ",
    "SOL": "L1", "SUI": "L1", "NEAR": "L1", "APT": "L1", "SEI": "L1", "TIA": "L1",
    "TRX": "L1", "ADA": "L1", "AVAX": "L1", "DOT": "L1", "ICP": "L1", "HBAR": "L1",
    "ALGO": "L1", "LTC": "L1", "XRP": "L1", "XLM": "L1", "KAS": "L1", "XPL": "L1",
    "INJ": "L1",
    "POL": "L2", "STRK": "L2",
    "LINK": "DEFI", "AAVE": "DEFI", "UNI": "DEFI", "ONDO": "DEFI", "ENA": "DEFI",
    "JUP": "DEFI", "CRV": "DEFI", "RAY": "DEFI", "EIGEN": "DEFI",
    "TAO": "AI", "FET": "AI", "RENDER": "AI", "WLD": "AI", "VIRTUAL": "AI",
    "AR": "AI", "ROSE": "AI",
    "DOGE": "MEME", "PEPE": "MEME", "BONK": "MEME", "FARTCOIN": "MEME", "PENGU": "MEME",
    "HYPE": "ALONE", "ZEC": "ALONE", "BGB": "ALONE",
}
GOLD_ID = "pax-gold"

TIER_A = 400e6
TIER_B = 100e6

S = requests.Session()
S.headers.update({"User-Agent": "clago-scan/1.2"})

EXCH = {"BIN": True, "OKX": True, "BGET": True}


def probe_exchanges():
    global EXCH
    for name, url in (("BIN", f"{FAPI}/fapi/v1/ping"),
                      ("OKX", f"{OKX}/api/v5/public/time"),
                      ("BGET", f"{BGET}/api/v2/public/time")):
        try:
            get(url, timeout=8, retries=1)
            EXCH[name] = True
        except Exception:
            EXCH[name] = False
    if os.environ.get("NO_BIN"):
        EXCH["BIN"] = False  # per test fallback
    print(f"[probe] Binance={'OK' if EXCH['BIN'] else 'BLOCCATA'} · OKX={'OK' if EXCH['OKX'] else 'NO'} · Bitget={'OK' if EXCH['BGET'] else 'NO'}")


def kl_btc(interval, limit):
    if EXCH["BIN"]: return kl_binance("BTCUSDT", interval, limit)
    if EXCH["OKX"]: return kl_okx("BTC-USDT-SWAP", interval, limit)
    return kl_bitget("BTCUSDT", interval, limit)


def cvd_okx(real, bars=24):
    try:
        base = real.split("-")[0]
        d = get(f"{OKX}/api/v5/rubik/stat/taker-volume",
                {"ccy": base, "instType": "CONTRACTS", "period": "1H"})["data"]
        d = sorted(d, key=lambda r: int(r[0]))[-bars:]
        buy = sum(float(r[2]) for r in d); sell = sum(float(r[1]) for r in d)
        tot = buy + sell
        return (buy - sell) / tot if tot > 0 else 0.0
    except Exception:
        return 0.0


CLZ = "https://api.coinalyze.net/v1"
CLZ_KEY = os.environ.get("COINALYZE_API_KEY", "").strip()
KRAKEN = "https://api.kraken.com"


def clz_symbol(a):
    if a in ("BONK", "PEPE", "FLOKI", "SHIB"):
        return f"1000{a}USDT_PERP.A"
    return f"{a}USDT_PERP.A"


def coinalyze_batch(assets):
    """OI 24h % e funding AGGREGATI multi-exchange (Binance inclusa)."""
    out = {}
    if not CLZ_KEY or not assets:
        return out
    try:
        m = {clz_symbol(a): a for a in assets}
        csv = ",".join(m)
        h = {"api_key": CLZ_KEY}
        now = int(time.time())
        r = S.get(f"{CLZ}/funding-rate", params={"symbols": csv}, headers=h, timeout=25)
        if r.status_code == 200:
            for it in r.json() or []:
                sym = it.get("symbol"); v = it.get("value")
                if sym in m and v is not None:
                    # Coinalyze restituisce % per 8h -> converti in frazione
                    out.setdefault(m[sym], {})["funding"] = float(v) / 100.0
        time.sleep(3)
        r = S.get(f"{CLZ}/open-interest-history", headers=h, timeout=25,
                  params={"symbols": csv, "interval": "1hour",
                          "from": now - 25 * 3600, "to": now, "convert_to_usd": "true"})
        if r.status_code == 200:
            for it in r.json() or []:
                sym = it.get("symbol"); hist = it.get("history") or []
                if sym in m and len(hist) >= 2:
                    hist = sorted(hist, key=lambda x: x.get("t", 0))
                    a0, b0 = float(hist[0]["c"]), float(hist[-1]["c"])
                    if a0:
                        out.setdefault(m[sym], {})["oi_24h"] = (b0 / a0 - 1) * 100
    except Exception as e:
        print(f"[warn] coinalyze: {e}")
    return out


def cross_prices(a, src, real, px):
    """Confronta il prezzo con altre fonti (OKX, Bitget, Kraken). -> (n_fonti, max_dev_%)"""
    if real.startswith("1000"):
        px = px / 1000.0
    prices = []
    if src != "OKX" and EXCH["OKX"]:
        try:
            t = get(f"{OKX}/api/v5/market/ticker", {"instId": f"{a}-USDT-SWAP"})["data"]
            if t: prices.append(float(t[0]["last"]))
        except Exception: pass
    if src != "BGET" and EXCH["BGET"]:
        try:
            t = get(f"{BGET}/api/v2/mix/market/ticker",
                    {"symbol": a + "USDT", "productType": "USDT-FUTURES"})["data"]
            if t: prices.append(float(t[0]["lastPr"]))
        except Exception: pass
    try:
        r = get(f"{KRAKEN}/0/public/Ticker", {"pair": a + "USD"})
        for v in (r.get("result") or {}).values():
            prices.append(float(v["c"][0])); break
    except Exception: pass
    devs = [abs(p / px - 1) * 100 for p in prices if p > 0]
    return (1 + len(devs), max(devs) if devs else 0.0)


def get(url, params=None, timeout=20, retries=3):
    for i in range(retries):
        try:
            r = S.get(url, params=params, timeout=timeout)
            if r.status_code == 429:
                time.sleep(4 * (i + 1)); continue
            r.raise_for_status()
            return r.json()
        except Exception:
            if i == retries - 1: raise
            time.sleep(2)


def load_state():
    try:
        with open(STATE_FILE) as f: return json.load(f)
    except Exception:
        return {}


def save_state(st):
    with open(STATE_FILE, "w") as f: json.dump(st, f, indent=1)


def kl_binance(sym, interval, limit):
    return get(f"{FAPI}/fapi/v1/klines", {"symbol": sym, "interval": interval, "limit": limit})


OKX_BAR = {"30m": "30m", "1h": "1H", "4h": "4H", "1d": "1Dutc"}
def kl_okx(inst, interval, limit):
    raw = get(f"{OKX}/api/v5/market/candles",
              {"instId": inst, "bar": OKX_BAR[interval], "limit": str(min(limit, 100))})
    rows = list(reversed(raw.get("data", [])))
    return [[int(r[0]), r[1], r[2], r[3], r[4], r[5], 0, 0, 0, None] for r in rows]


BGET_GRAN = {"30m": "30m", "1h": "1H", "4h": "4H", "1d": "1D"}
def kl_bitget(sym, interval, limit):
    raw = get(f"{BGET}/api/v2/mix/market/candles",
              {"symbol": sym, "productType": "USDT-FUTURES",
               "granularity": BGET_GRAN[interval], "limit": str(min(limit, 200))})
    rows = raw.get("data", [])
    return [[int(r[0]), r[1], r[2], r[3], r[4], r[5], 0, 0, 0, None] for r in rows]


def klines(src, real, interval, limit):
    if src == "BIN": return kl_binance(real, interval, limit)
    if src == "OKX": return kl_okx(real, interval, limit)
    return kl_bitget(real, interval, limit)


def resolve_universe():
    perps, tick = set(), {}
    if EXCH["BIN"]:
        info = get(f"{FAPI}/fapi/v1/exchangeInfo")
        perps = {s["symbol"] for s in info["symbols"]
                 if s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING"
                 and s.get("quoteAsset") == "USDT"}
        tick = {t["symbol"]: t for t in get(f"{FAPI}/fapi/v1/ticker/24hr")}
    okx_ticks = None
    res, tk = {}, {}
    for a in UNIVERSE:
        cands = [a + "USDT", "1000" + a + "USDT"]
        hit = next((c for c in cands if c in perps), None)
        if hit:
            res[a] = ("BIN", hit)
            t = tick[hit]
            tk[a] = (float(t["priceChangePercent"]), float(t["quoteVolume"]))
            continue
        if okx_ticks is None:
            okx_ticks = {}
            if EXCH["OKX"]:
                try:
                    data = get(f"{OKX}/api/v5/market/tickers", {"instType": "SWAP"})["data"]
                    okx_ticks = {d["instId"]: d for d in data}
                except Exception:
                    okx_ticks = {}
        inst = f"{a}-USDT-SWAP"
        if inst in okx_ticks:
            res[a] = ("OKX", inst)
            try:
                t = okx_ticks[inst]
                last, o24 = float(t["last"]), float(t["open24h"])
                chg = (last / o24 - 1) * 100 if o24 else 0
                qv = float(t.get("volCcy24h", 0)) * last
                tk[a] = (chg, qv)
            except Exception:
                tk[a] = (0.0, 0.0)
            continue
        try:
            t = get(f"{BGET}/api/v2/mix/market/ticker",
                    {"symbol": a + "USDT", "productType": "USDT-FUTURES"})["data"][0]
            res[a] = ("BGET", a + "USDT")
            tk[a] = (float(t.get("change24h", 0)) * 100, float(t.get("usdtVolume", 0)))
        except Exception:
            print(f"[warn] {a}: non trovato su Binance/OKX/Bitget — saltato")
    return res, tk


def funding_of(src, real):
    try:
        if src == "BIN":
            return float(get(f"{FAPI}/fapi/v1/premiumIndex", {"symbol": real})["lastFundingRate"])
        if src == "OKX":
            return float(get(f"{OKX}/api/v5/public/funding-rate", {"instId": real})["data"][0]["fundingRate"])
        return float(get(f"{BGET}/api/v2/mix/market/current-fund-rate",
                         {"symbol": real, "productType": "USDT-FUTURES"})["data"][0]["fundingRate"])
    except Exception:
        return None


def oi_24h_of(src, real):
    try:
        if src == "BIN":
            oi = get(f"{FAPI}/futures/data/openInterestHist",
                     {"symbol": real, "period": "1h", "limit": 25})
            if oi and len(oi) >= 2:
                a, b = float(oi[0]["sumOpenInterestValue"]), float(oi[-1]["sumOpenInterestValue"])
                return (b / a - 1) * 100 if a else None
        if src == "OKX":
            base = real.split("-")[0]
            d = get(f"{OKX}/api/v5/rubik/stat/contracts/open-interest-volume",
                    {"ccy": base, "period": "1H"})["data"]
            if d and len(d) >= 2:
                d = sorted(d, key=lambda r: int(r[0]))[-25:]
                a, b = float(d[0][1]), float(d[-1][1])
                return (b / a - 1) * 100 if a else None
    except Exception:
        pass
    return None


def atr(kl, period=14):
    if len(kl) < period + 1: return None
    trs = []
    for i in range(1, len(kl)):
        h, l, pc = float(kl[i][2]), float(kl[i][3]), float(kl[i - 1][4])
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    a = sum(trs[:period]) / period
    for tr in trs[period:]:
        a = (a * (period - 1) + tr) / period
    return a


def rsi(closes, period=14):
    if len(closes) < period + 1: return None
    g, lo = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        g.append(max(d, 0)); lo.append(max(-d, 0))
    ag, al = sum(g[:period]) / period, sum(lo[:period]) / period
    for i in range(period, len(g)):
        ag = (ag * (period - 1) + g[i]) / period
        al = (al * (period - 1) + lo[i]) / period
    return 100.0 if al == 0 else 100 - 100 / (1 + ag / al)


def swing_structure(kl, lookback=30):
    if len(kl) < lookback: return "ND"
    hs = [float(k[2]) for k in kl[-lookback:]]
    ls = [float(k[3]) for k in kl[-lookback:]]
    mid = lookback // 2
    hh = max(hs[mid:]) > max(hs[:mid]); hl = min(ls[mid:]) > min(ls[:mid])
    if hh and hl: return "UP"
    if (not hh) and (not hl): return "DOWN"
    return "RANGE"


def session_vwap(kl_h1):
    today0 = int(datetime.now(timezone.utc).replace(hour=0, minute=0, second=0,
                                                    microsecond=0).timestamp() * 1000)
    pv = vv = 0.0
    for k in kl_h1:
        if int(k[0]) >= today0:
            tp = (float(k[2]) + float(k[3]) + float(k[4])) / 3
            v = float(k[5]); pv += tp * v; vv += v
    return pv / vv if vv > 0 else None


def cvd_proxy(kl_h1, bars=24):
    tail = kl_h1[-bars:]
    delta = tot = 0.0
    for k in tail:
        if k[9] is None: return 0.0
        v = float(k[5]); tb = float(k[9])
        delta += 2 * tb - v; tot += v
    return delta / tot if tot > 0 else 0.0


def regime(state):
    out = {"notes": []}
    kd = kl_btc("1d", 60)
    kh4 = kl_btc("4h", 60)
    closes_d = [float(k[4]) for k in kd]
    px = closes_d[-1]
    hi20 = max(float(k[2]) for k in kd[-21:-1]); lo20 = min(float(k[3]) for k in kd[-21:-1])
    d1 = "UP" if px > hi20 * 0.99 else ("DOWN" if px < lo20 * 1.01 else "RANGE")
    h4 = swing_structure(kh4, 30)
    out["btc_d1"], out["btc_h4"], out["btc_px"] = d1, h4, px
    out["btc_24h"] = (px / float(kd[-2][4]) - 1) * 100
    try:
        g = get(f"{CG}/global")["data"]
        total = g["total_market_cap"]["usd"]
        mcp = g["market_cap_percentage"]
        btc_d, eth_d = mcp.get("btc", 0), mcp.get("eth", 0)
        st = get(f"{CG}/simple/price",
                 {"ids": "tether,usd-coin,dai", "vs_currencies": "usd",
                  "include_market_cap": "true"})
        stables = sum(v.get("usd_market_cap", 0) for v in st.values())
        stables_pct = stables / total * 100
        btc_s = btc_d - stables_pct
        total_alts = (total * (1 - (btc_d + eth_d) / 100) - stables) / 1e9
        out.update(btc_s=btc_s, stables_pct=stables_pct, total_alts=total_alts, btc_dom=btc_d)
        prev = state.get("cg_snapshot")
        if prev:
            out["d_btc_s"] = btc_s - prev.get("btc_s", btc_s)
            out["d_stables"] = stables_pct - prev.get("stables_pct", stables_pct)
            out["d_total_alts"] = (total_alts / prev["total_alts"] - 1) * 100 if prev.get("total_alts") else 0
        state["cg_snapshot"] = {"btc_s": btc_s, "stables_pct": stables_pct,
                                "total_alts": total_alts, "ts": int(time.time())}
    except Exception as e:
        out["notes"].append(f"CoinGecko non disponibile: {e}")
    score = 0
    if d1 == "UP": score += 2
    elif d1 == "DOWN": score -= 2
    if h4 == "UP": score += 1
    elif h4 == "DOWN": score -= 1
    ds = out.get("d_stables")
    if ds is not None:
        if ds < -0.05: score += 1
        elif ds > 0.05: score -= 1
    dta = out.get("d_total_alts")
    if dta is not None:
        if dta > 0.5: score += 1
        elif dta < -0.5: score -= 1
    out["score"] = score
    if score >= 2: out["bias"] = "LONG"
    elif score <= -2: out["bias"] = "SHORT"
    elif d1 == "RANGE" and h4 == "RANGE": out["bias"] = "FLAT"
    else: out["bias"] = "NEUTRAL"
    return out


def tier_of(qvol):
    if qvol >= TIER_A: return "A"
    if qvol >= TIER_B: return "B"
    return "C"


def analyze_symbol(a, src, real, chg24, qvol, btc24):
    kh1 = klines(src, real, "1h", 60)
    kh4 = klines(src, real, "4h", 60)
    kd  = klines(src, real, "1d", 20)
    k30 = klines(src, real, "30m", 60)
    if not kh1 or len(kh1) < 25 or not kd or len(kd) < 15: return None
    px = float(kh1[-1][4])
    # RVOL su candele 30m chiuse: ogni run da 30 min vede informazione nuova
    v30 = [float(k[5]) for k in (k30[:-1] if k30 and len(k30) > 22 else kh1[:-1])]
    base = sum(v30[-21:-1]) / 20
    rvol = v30[-1] / base if base > 0 else 0
    a14 = atr(kd[:-1], 14)
    today_range = float(kd[-1][2]) - float(kd[-1][3])
    atr_used = today_range / a14 if a14 else 1.0
    atr_pct = (a14 / px) if (a14 and px) else 0.02
    prox_thr = min(0.03, max(0.008, 0.25 * atr_pct))  # 25% dell'ATR giornaliero, tra 0.8% e 3%
    pd_h, pd_l = float(kd[-2][2]), float(kd[-2][3])
    wk_open = float(kd[-8][1]) if len(kd) >= 8 else float(kd[0][1])
    levels = {"PDH": pd_h, "PDL": pd_l, "WO": wk_open}
    near_name, near_px = min(levels.items(), key=lambda kv: abs(px - kv[1]) / px)
    near_dist = abs(px - near_px) / px
    pv = vv = 0.0
    for k in (kh4[-42:] if kh4 and len(kh4) >= 20 else []):
        tp = (float(k[2]) + float(k[3]) + float(k[4])) / 3
        v = float(k[5]); pv += tp * v; vv += v
    ext_w = ((px / (pv / vv)) - 1) * 100 if vv > 0 else 0.0
    return {"sym": a, "src": src, "real": real, "px": px, "qvol": qvol, "ext_w": ext_w,
            "tier": tier_of(qvol), "cluster": UNIVERSE[a], "rvol": rvol,
            "atr_d": a14, "atr_used": atr_used, "prox_thr": prox_thr,
            "p24": chg24, "rs": chg24 - btc24,
            "struct": swing_structure(kh4, 30), "struct_h1": swing_structure(kh1, 30),
            "vwap": session_vwap(kh1),
            "rsi_h1": rsi([float(k[4]) for k in kh1], 14),
            "near": near_name, "near_px": near_px, "near_dist": near_dist,
            "pdh": pd_h, "pdl": pd_l,
            "hi20": max(float(k[2]) for k in kd[:-1]),
            "lo20": min(float(k[3]) for k in kd[:-1]), "kh1": kh1}


def prefilter_ok(d):
    if d["sym"] in ("BTC", "ETH"): return True
    checks = 0
    if d["rvol"] >= RVOL_MIN: checks += 1
    if d["near_dist"] <= d["prox_thr"]: checks += 1
    if d["atr_used"] <= ATR_CONSUMED_MAX: checks += 1
    return checks >= 2


def crowded_thr(cluster):
    return {"MAJ": 0.0003, "MEME": 0.001}.get(cluster, FUNDING_CROWDED)


def l4_read(px24, oi24, funding, cvd, cluster=""):
    # soglie per tipo di coin: majors si muovono meno, meme molto di più
    oi_thr = {"MAJ": 1.5, "MEME": 4.0}.get(cluster, 2.0)
    px_thr = {"MAJ": 0.5, "MEME": 2.0}.get(cluster, 1.0)
    fthr = crowded_thr(cluster)
    s = 0
    if oi24 is not None:
        if px24 > px_thr and oi24 > oi_thr: s += 1
        elif px24 < -px_thr and oi24 > oi_thr: s -= 1
        elif px24 < -px_thr and oi24 < -oi_thr: s += 0.5
    if cvd > 0.03: s += 0.5
    elif cvd < -0.03: s -= 0.5
    if funding is not None:
        if funding > fthr: s -= 0.5
        elif funding < -fthr: s += 0.5
    return s


def build_setup(d, direction):
    px, a14 = d["px"], d["atr_d"]
    if not a14: return None
    if direction == "LONG":
        trigger = max(d["pdh"], px) * 1.001 if d["struct"] == "UP" else (d["vwap"] or px)
        stop = trigger - a14 * 0.28
        target = trigger + (trigger - stop) * 2.2
        stype = "breakout" if trigger > px else "pullback"
    else:
        trigger = min(d["pdl"], px) * 0.999 if d["struct"] == "DOWN" else (d["vwap"] or px)
        stop = trigger + a14 * 0.28
        target = trigger - (stop - trigger) * 2.2
        stype = "breakdown" if trigger < px else "pullback"
    risk = abs(trigger - stop)
    rr = abs(target - trigger) / risk if risk > 0 else 0
    if d["tier"] == "C" and stype == "pullback": return None
    return {"type": stype, "entry": trigger, "stop": stop, "target": target, "rr": rr}


def fmt(p):
    if p is None: return "-"
    if p >= 1000: return f"{p:,.0f}"
    if p >= 1: return f"{p:.3f}"
    if p >= 0.01: return f"{p:.4f}"
    return f"{p:.7f}"


def send_tg(text):
    for chunk in [text[i:i + 3900] for i in range(0, len(text), 3900)]:
        requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                      data={"chat_id": TG_CHAT, "text": chunk, "parse_mode": "HTML",
                            "disable_web_page_preview": True}, timeout=20)


def main():
    force = "--force" in sys.argv
    now = datetime.now(TZ)
    print(f"=== SCAN {now.strftime('%d/%m/%Y %H:%M')} (Europe/Rome) ===")
    if not (WINDOW[0] <= now.hour < WINDOW[1]) and not force:
        print("Fuori finestra 10-22. Usa --force per test."); return
    state = load_state()
    today = now.strftime("%Y-%m-%d")
    day = state.get("day", {})
    if day.get("date") != today:
        day = {"date": today, "proposed": [], "ran": False}
    already = day["proposed"]
    first_run = not day.get("ran", False)
    probe_exchanges()
    reg = regime(state)
    print(f"L1: bias={reg['bias']} (score {reg['score']}) | BTC D1={reg['btc_d1']} H4={reg['btc_h4']} {reg['btc_24h']:+.2f}%")
    gold_line = "🥇 Oro: dati non disponibili"
    try:
        gp = get(f"{CG}/simple/price", {"ids": GOLD_ID, "vs_currencies": "usd",
                                        "include_24hr_change": "true"})[GOLD_ID]
        gold_line = f"🥇 Oro (PAXG): ${gp['usd']:,.0f} ({gp.get('usd_24h_change', 0):+.2f}% 24h) — check DXY/calendario manuale"
    except Exception:
        pass
    symmap, tk = resolve_universe()
    btc24 = tk["BTC"][0]
    rows = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(analyze_symbol, a, src, real, tk[a][0], tk[a][1], btc24): a
                for a, (src, real) in symmap.items()}
        for fu in as_completed(futs):
            try:
                d = fu.result()
                if d: rows.append(d)
            except Exception as e:
                print(f"[warn] {futs[fu]}: {e}")
    passed = [d for d in rows if prefilter_ok(d)]
    print(f"Funnel: {len(rows)} analizzati -> {len(passed)} pre-filtro")
    for d in passed:
        s = min(d["rvol"], 4) * 0.8
        s += max(min(abs(d["rs"]) / 2, 2), 0)
        if d["struct"] in ("UP", "DOWN"): s += 1
        elif d.get("struct_h1") in ("UP", "DOWN"): s += 0.5   # struttura veloce H1 (anticipa l'H4)
        if d["near_dist"] <= d["prox_thr"]: s += 1
        if d["atr_used"] <= ATR_CONSUMED_MAX: s += 0.5
        d["l3"] = s
        st_eff = d["struct"] if d["struct"] != "RANGE" else d.get("struct_h1", "RANGE")
        d["dir0"] = "LONG" if (d["rs"] > 0 and st_eff != "DOWN") else \
                    ("SHORT" if (d["rs"] < 0 and st_eff != "UP") else None)
    shortlist = sorted([d for d in passed if d["dir0"]], key=lambda x: -x["l3"])[:8]

    def _der(d):
        return {"oi_24h": oi_24h_of(d["src"], d["real"]),
                "funding": funding_of(d["src"], d["real"]),
                "cvd": cvd_proxy(d["kh1"], 24) if d["src"] == "BIN"
                       else cvd_okx(d["real"]) if d["src"] == "OKX" else 0.0}
    with ThreadPoolExecutor(max_workers=4) as ex:
        ders = list(ex.map(_der, shortlist))
    clz = coinalyze_batch([d["sym"] for d in shortlist])
    for d, der in zip(shortlist, ders):
        d.update(der)
        c = clz.get(d["sym"], {})
        if c.get("oi_24h") is not None:
            d["oi_x"] = d.get("oi_24h"); d["oi_24h"] = c["oi_24h"]; d["oi_aggr"] = True
        if c.get("funding") is not None:
            d["fund_x"] = d.get("funding"); d["funding"] = c["funding"]; d["fund_aggr"] = True
        d["l4"] = l4_read(d["p24"], d.get("oi_24h"), d.get("funding"), der["cvd"], d["cluster"])
        if d["dir0"] == "LONG" and d["l4"] < -0.5: d["dir0"] = None
        if d["dir0"] == "SHORT" and d["l4"] > 0.5: d["dir0"] = None
        d["score"] = d["l3"] + (d["l4"] if d["dir0"] == "LONG"
                                else -d["l4"] if d["dir0"] == "SHORT" else 0)
        # penalità estensione: non inseguire movimenti già corsi sulla scala settimanale
        ext = d.get("ext_w", 0.0); pen = 0.0
        if d["dir0"] == "LONG" and ext > 8:
            pen = min(2.0, (ext - 8) * 0.25)
        elif d["dir0"] == "SHORT" and ext < -8:
            pen = min(2.0, (-ext - 8) * 0.25)
        d["ext_pen"] = pen
        d["score"] -= pen
    cands = sorted([d for d in shortlist if d["dir0"]], key=lambda x: -x["score"])
    final = []
    for d in cands:
        if any(p["sym"] == d["sym"] and p["dir"] == d["dir0"] for p in already): continue
        # BTC decide l'asticella: allineato basso, contro-regime alto
        aligned = (reg["bias"] == "LONG" and d["dir0"] == "LONG") or \
                  (reg["bias"] == "SHORT" and d["dir0"] == "SHORT")
        if reg["bias"] == "FLAT":
            if d["rvol"] < 2.0: continue
            thr = 6.5
        elif aligned:
            thr = 5.5
        elif reg["bias"] == "NEUTRAL":
            thr = 6.0
        else:
            thr = 7.0   # contro-regime: solo eccezionale
        if d["score"] < thr: continue
        f = d.get("funding")
        if f is not None:
            fthr = crowded_thr(d["cluster"])
            if d["dir0"] == "LONG" and f > fthr: continue
            if d["dir0"] == "SHORT" and f < -fthr: continue
        setup = build_setup(d, d["dir0"])
        if not setup: continue
        if any(x["cluster"] == d["cluster"] and x["dir0"] == d["dir0"] for x in final): continue
        if any(p.get("cluster") == d["cluster"] and p["dir"] == d["dir0"] for p in already): continue
        d["setup"] = setup
        final.append(d)
    for d in final:
        already.append({"sym": d["sym"], "dir": d["dir0"], "cluster": d["cluster"],
                        "entry": d["setup"]["entry"], "score": round(d["score"], 1),
                        "rvol": round(d["rvol"], 2), "hhmm": now.strftime("%H:%M")})
    day["ran"] = True
    state["day"] = day

    L = [f"📡 <b>SCAN GIORNALIERO</b> — {now.strftime('%a %d/%m %H:%M')}"]
    L.append(f"<b>L1 Regime:</b> {reg['bias']} (score {reg['score']:+d}) · BTC {fmt(reg['btc_px'])} {reg['btc_24h']:+.2f}% · D1 {reg['btc_d1']} / H4 {reg['btc_h4']}")
    if reg.get("btc_s") is not None:
        db = reg.get("d_btc_s"); ds = reg.get("d_stables"); dt = reg.get("d_total_alts")
        L.append(f"BTC.S (ex-stables) {reg['btc_s']:.1f}%{f' ({db:+.2f})' if db is not None else ''} · Stables.D {reg['stables_pct']:.2f}%{f' ({ds:+.2f})' if ds is not None else ''} · TOTAL alts ${reg['total_alts']:.0f}B{f' ({dt:+.1f}%)' if dt is not None else ''}")
    if not EXCH["BIN"]:
        L.append("ℹ️ prezzi via OKX (Binance non raggiungibile dal server)")
    if CLZ_KEY:
        L.append("ℹ️ OI/funding: Coinalyze aggregato multi-exchange (Binance inclusa)")
    L.append(gold_line)
    L.append("⚠️ <b>L2 eventi:</b> check manuale macro USA / unlock / listing prima di eseguire")
    L.append(f"\n<b>Funnel:</b> {len(rows)} → {len(passed)} pre-filtro → {len(shortlist)} scoring → {len(final)} trade · <b>proposti oggi: {len(already)}</b>")
    if not final:
        L.append("\n🚫 <b>OGGI ZERO TRADE.</b> Nessun setup supera i filtri (geometria 2:1, derivati, regime). Il no-trade è una posizione.")
    for i, d in enumerate(final, 1):
        s = d["setup"]
        emoji = "🟢" if d["dir0"] == "LONG" else "🔴"
        L.append(f"\n{emoji} <b>TRADE {i}: {d['dir0']} {d['sym']}</b> · score {d['score']:.1f} ({s['type']}, Tier {d['tier']}, {d['cluster']}, {d['src']})")
        L.append(f"Prezzo {fmt(d['px'])} · Entry <b>{fmt(s['entry'])}</b> ({'stop sopra' if s['type']=='breakout' else 'stop sotto' if s['type']=='breakdown' else 'limit'})")
        above = [x for x in (d['pdh'], d['hi20']) if x > s['entry'] * 1.002]
        below = [x for x in (d['pdl'], d['lo20']) if x < s['entry'] * 0.998]
        resx = min(above) if above else None
        supx = max(below) if below else None
        lv = []
        if resx: lv.append(f"resistenza {fmt(resx)} ({(resx/s['entry']-1)*100:+.1f}%)")
        if supx: lv.append(f"supporto {fmt(supx)} ({(supx/s['entry']-1)*100:+.1f}%)")
        L.append("<b>Livelli:</b> " + (" · ".join(lv) if lv else "nessun livello vicino nei 20g"))
        L.append(f"<b>SL/TP li decidi tu</b> — riferimenti vol.: SL {fmt(s['stop'])} (0.28 ATR) · 2.2R = {fmt(s['target'])}")
        oi = d.get("oi_24h"); fu = d.get("funding")
        oi_tag = " (aggr)" if d.get("oi_aggr") else ""
        fu_tag = " (aggr)" if d.get("fund_aggr") else ""
        ext_tag = f" · est.7g {d.get('ext_w',0):+.1f}%" + (f" (pen -{d['ext_pen']:.1f})" if d.get("ext_pen") else "")
        L.append(f"RS vs BTC {d['rs']:+.1f}% · RVOL {d['rvol']:.1f}x{ext_tag} · OI 24h {f'{oi:+.1f}%{oi_tag}' if oi is not None else 'n/d'} · funding {f'{fu*100:.3f}%{fu_tag}' if fu is not None else 'n/d'} · CVD {d.get('cvd',0):+.2f}")
        ncr, dcr = cross_prices(d["sym"], d["src"], d["real"], d["px"])
        L.append(f"🔎 Cross-check prezzo: {ncr} fonti · Δmax {dcr:.2f}%" + (" ⚠️ VERIFICA PRIMA DI ESEGUIRE" if dcr > 0.7 else " ✅"))
        L.append("Invalidazione: chiusura H1 oltre SL · BE a +1R · time-stop 22:00 se mai +1R"
                 + (" · ⛔️ no overnight, size ridotta (Tier C)" if d["tier"] == "C" else ""))
    watch = [f"{d['sym']} ({d['dir0']}, {d['score']:.1f})" for d in cands if d not in final][:5]
    if watch: L.append(f"\n👀 <b>Watchlist:</b> {', '.join(watch)}")
    L.append("\n<i>Rischio: max 2-2.5% aggregato · funding 18:00 · non è consiglio finanziario</i>")
    if final or first_run:
        msg = "\n".join(L)
    else:
        parts = [f"🔎 <b>Scan {now.strftime('%H:%M')}</b> — niente di nuovo · regime {reg['bias']} · proposti oggi {len(already)}"]
        if already:
            parts.append("proposti oggi: " + ", ".join(p["dir"] + " " + p["sym"] for p in already))
        if watch:
            parts.append("👀 " + ", ".join(watch))
        msg = "\n".join(parts)
    print("\n" + msg.replace("<b>", "").replace("</b>", "").replace("<i>", "").replace("</i>", ""))
    send_tg(msg)
    save_state(state)
    print("\n[OK] inviato a Telegram")


if __name__ == "__main__":
    main()
