import os
import sqlite3
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field


DB_PATH = os.getenv("DB_PATH", "/data/stock_prices.db")
API_TOKEN = os.getenv("API_TOKEN", "").strip()

app = FastAPI(title="stock-market-data-service", version="1.0.0")


class SnapshotsRequest(BaseModel):
    symbols: list[str] = Field(default_factory=list)
    lookback: int = 60


def guess_currency(symbol: str) -> str:
    return "KRW" if symbol.endswith(".KS") or symbol.endswith(".KQ") else "USD"


def _auth_or_401(authorization: str | None):
    if not API_TOKEN:
        return
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    token = authorization.split(" ", 1)[1].strip()
    if token != API_TOKEN:
        raise HTTPException(status_code=401, detail="invalid token")


def db_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def calc_rsi(closes: pd.Series, period: int = 14) -> float | None:
    if closes.size < period + 1:
        return None
    delta = closes.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    avg_gain = up.rolling(period).mean()
    avg_loss = down.rolling(period).mean()
    g = avg_gain.iloc[-1]
    l = avg_loss.iloc[-1]
    if pd.isna(g) or pd.isna(l):
        return None
    if l == 0:
        return 100.0
    rs = g / l
    return float(100.0 - (100.0 / (1.0 + rs)))


def calc_stoch_rsi(closes: pd.Series, rsi_period: int = 14, stoch_period: int = 14) -> float | None:
    if closes.size < rsi_period + stoch_period:
        return None
    delta = closes.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    avg_gain = up.rolling(rsi_period).mean()
    avg_loss = down.rolling(rsi_period).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    rsi = rsi.dropna()
    if rsi.empty or rsi.size < stoch_period:
        return None
    tail = rsi.iloc[-stoch_period:]
    lo = float(tail.min())
    hi = float(tail.max())
    if hi <= lo:
        return None
    return float((float(rsi.iloc[-1]) - lo) / (hi - lo) * 100.0)


def calc_macd(closes: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> tuple[float | None, float | None]:
    if closes.size < slow + signal:
        return None, None
    ema_fast = closes.ewm(span=fast, adjust=False).mean()
    ema_slow = closes.ewm(span=slow, adjust=False).mean()
    macd = ema_fast - ema_slow
    sig = macd.ewm(span=signal, adjust=False).mean()
    m = macd.iloc[-1]
    s = sig.iloc[-1]
    if pd.isna(m) or pd.isna(s):
        return None, None
    return float(m), float(s)


def calc_bollinger(closes: pd.Series, period: int = 20, k: float = 2.0):
    if closes.size < period:
        return {"middle": None, "upper": None, "lower": None}
    w = closes.iloc[-period:]
    m = float(w.mean())
    std = float(w.std(ddof=0))
    return {"middle": m, "upper": m + k * std, "lower": m - k * std}


def calc_adx(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> float | None:
    if high.size < period + 1 or low.size < period + 1 or close.size < period + 1:
        return None
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(period).mean()
    plus_di = 100 * (plus_dm.rolling(period).mean() / atr.replace(0, pd.NA))
    minus_di = 100 * (minus_dm.rolling(period).mean() / atr.replace(0, pd.NA))
    dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, pd.NA)) * 100
    adx = dx.rolling(period).mean()
    last = adx.iloc[-1]
    if pd.isna(last):
        return None
    return float(last)


def load_daily_bars(conn: sqlite3.Connection, symbol: str) -> pd.DataFrame:
    rows = conn.execute(
        """
        SELECT trade_date_local, close, high, low
        FROM price_bars
        WHERE symbol = ? AND interval = '1d'
        ORDER BY trade_date_local ASC
        """,
        (symbol,),
    ).fetchall()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows, columns=["trade_date_local", "close", "high", "low"])


def load_quote(conn: sqlite3.Connection, symbol: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT symbol, price, prev_close, currency, market_state, market_time_utc, fetched_at_utc
        FROM latest_quotes
        WHERE symbol = ?
        """,
        (symbol,),
    ).fetchone()
    return dict(row) if row else None


def build_snapshot(conn: sqlite3.Connection, symbol: str, lookback: int) -> dict[str, Any] | None:
    bars = load_daily_bars(conn, symbol)
    if bars.empty or bars.shape[0] < 3:
        return None

    close = bars["close"].astype(float)
    high = bars["high"].astype(float)
    low = bars["low"].astype(float)

    close1 = float(close.iloc[-1])
    prev_close = float(close.iloc[-2])
    prev_close_chg_pct = (close1 / prev_close - 1.0) if prev_close else None
    asof_date = str(bars["trade_date_local"].iloc[-1])

    high_n = float(high.iloc[-lookback:].max())
    high_drawdown_pct = (close1 / high_n - 1.0) if high_n else None

    rsi14 = calc_rsi(close, 14)
    stoch_rsi14 = calc_stoch_rsi(close, 14, 14)
    macd, macd_signal = calc_macd(close, 12, 26, 9)
    bollinger20 = calc_bollinger(close, 20, 2.0)
    adx14 = calc_adx(high, low, close, 14)

    high52w = float(high.iloc[-252:].max())
    high52w_drawdown_pct = (close1 / high52w - 1.0) if high52w else None

    q = load_quote(conn, symbol) or {}
    cur_price = q.get("price")
    if cur_price is not None:
        cur_price = float(cur_price)
    day_base = prev_close if prev_close else close1
    cur_chg_pct = (cur_price / day_base - 1.0) if cur_price is not None and day_base else None

    market_time = None
    raw_mt = q.get("market_time_utc")
    if raw_mt:
        try:
            market_time = datetime.fromisoformat(str(raw_mt).replace("Z", "+00:00"))
        except Exception:
            market_time = None

    return {
        "ccy": q.get("currency") or guess_currency(symbol),
        "asofDate": asof_date,
        "close1": close1,
        "prevClose": prev_close,
        "prevCloseChgPct": prev_close_chg_pct,
        "curPrice": cur_price,
        "curChgPct": cur_chg_pct,
        "highN": high_n,
        "highDrawdownPct": high_drawdown_pct,
        "rsi14": rsi14,
        "stochRsi14": stoch_rsi14,
        "macd": macd,
        "macdSignal": macd_signal,
        "bollinger20": bollinger20,
        "adx14": adx14,
        "high52w": high52w,
        "high52wDrawdownPct": high52w_drawdown_pct,
        "marketState": q.get("market_state") or "",
        "marketTimeKST": market_time,
    }


@app.get("/health")
def health():
    now_utc = datetime.now(tz=timezone.utc).isoformat()
    return {"ok": True, "db_path": DB_PATH, "now_utc": now_utc}


@app.get("/v1/quotes")
def quotes(symbols: str, authorization: str | None = Header(default=None)):
    _auth_or_401(authorization)
    syms = [x.strip().upper() for x in symbols.split(",") if x.strip()]
    if not syms:
        return {"ok": True, "quotes": {}}
    out = {}
    with db_conn() as conn:
        for sym in syms:
            q = load_quote(conn, sym)
            if not q:
                continue
            out[sym] = {
                "price": q.get("price"),
                "prevClose": q.get("prev_close"),
                "currency": q.get("currency") or guess_currency(sym),
                "marketState": q.get("market_state"),
                "marketTimeUtc": q.get("market_time_utc"),
                "fetchedAtUtc": q.get("fetched_at_utc"),
            }
    return {"ok": True, "quotes": out}


@app.post("/v1/snapshots")
def snapshots(req: SnapshotsRequest, authorization: str | None = Header(default=None)):
    _auth_or_401(authorization)
    lookback = max(10, min(250, int(req.lookback or 60)))
    syms = [str(s).strip().upper() for s in req.symbols if str(s).strip()]
    out = {}
    with db_conn() as conn:
        for sym in syms:
            snap = build_snapshot(conn, sym, lookback)
            if snap:
                out[sym] = snap
    return {"ok": True, "snapshots": out}
