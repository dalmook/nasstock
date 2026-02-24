import argparse
import datetime as dt
import json
import logging
import re
import sqlite3
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf

LOG = logging.getLogger("price_ingest")
SYMBOL_REGEX = re.compile(r'symbol\s*:\s*"([A-Z0-9][A-Z0-9.\-]{0,19})"')
CORE_MARKET_SYMBOLS = [
    "^KS11",      # 코스피
    "^KQ11",      # 코스닥
    "^N225",      # 니케이
    "^DJI",       # 다우
    "^IXIC",      # 나스닥
    "^GSPC",      # S&P500
    "000001.SS",  # 상해종합
    "^HSI",       # 항셍
    "BTC-USD",    # 비트코인
    "GC=F",       # 국제 금
    "CL=F",       # WTI
    "KRW=X",      # USD/KRW
    "^TNX",       # 미국채 10년
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Yahoo market data ingestor (daily + intraday).")
    parser.add_argument("--symbols-file", default="/app/chat-configs.js", help="Path to chat-configs.js")
    parser.add_argument("--db-path", default="/data/stock_prices.db", help="SQLite DB file path")
    parser.add_argument("--mode", choices=["daily", "intraday", "both"], default="daily")
    parser.add_argument("--daily-period-days", type=int, default=420, help="Daily lookback days to store (1d bars)")
    parser.add_argument("--intraday-period", default="10d", help="Intraday lookback period (ex. 7d, 10d)")
    parser.add_argument("--intraday-interval", default="30m", help="Intraday interval (ex. 30m, 60m)")
    parser.add_argument("--lookback-days", type=int, default=60, help="Indicator lookback for highN")
    return parser.parse_args()


def extract_symbols_from_overrides(db_path: Path) -> list[str]:
    if not db_path.exists():
        return []
    try:
        conn = sqlite3.connect(db_path)
    except Exception:
        return []
    out = set()
    try:
        rows = conn.execute("SELECT override_json FROM chat_config_overrides").fetchall()
        for (raw,) in rows:
            try:
                obj = json.loads(raw)
            except Exception:
                continue
            if not isinstance(obj, dict):
                continue
            tickers = obj.get("tickers")
            if not isinstance(tickers, list):
                continue
            for t in tickers:
                if not isinstance(t, dict):
                    continue
                sym = str(t.get("symbol", "")).strip().upper()
                if re.fullmatch(r"[A-Z0-9][A-Z0-9.\-]{0,19}", sym):
                    out.add(sym)
    except Exception:
        return []
    finally:
        conn.close()
    return sorted(out)


def extract_symbols(symbols_file: Path, db_path: Path | None = None) -> list[str]:
    raw = symbols_file.read_text(encoding="utf-8", errors="ignore")
    symbols = set(SYMBOL_REGEX.findall(raw))
    symbols.update(CORE_MARKET_SYMBOLS)
    if db_path is not None:
        for sym in extract_symbols_from_overrides(db_path):
            symbols.add(sym)
    symbols = sorted(symbols)
    if not symbols:
        raise RuntimeError(f"No symbols found in {symbols_file}")
    return symbols

def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS price_bars (
            symbol TEXT NOT NULL,
            interval TEXT NOT NULL,
            ts_utc TEXT NOT NULL,
            trade_date_local TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            source TEXT NOT NULL DEFAULT 'yahoo',
            fetched_at_utc TEXT NOT NULL,
            PRIMARY KEY (symbol, interval, ts_utc)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_price_bars_symbol_interval_date
        ON price_bars(symbol, interval, trade_date_local DESC)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_indicators (
            symbol TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            currency TEXT NOT NULL,
            close1 REAL,
            prev_close REAL,
            prev_close_chg_pct REAL,
            high_n REAL,
            high_drawdown_pct REAL,
            rsi14 REAL,
            stoch_rsi14 REAL,
            macd REAL,
            macd_signal REAL,
            bb_middle REAL,
            bb_upper REAL,
            bb_lower REAL,
            adx14 REAL,
            high52w REAL,
            high52w_drawdown_pct REAL,
            updated_at_utc TEXT NOT NULL,
            PRIMARY KEY (symbol, trade_date)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS latest_quotes (
            symbol TEXT PRIMARY KEY,
            price REAL,
            prev_close REAL,
            currency TEXT,
            market_state TEXT,
            market_time_utc TEXT,
            source TEXT NOT NULL DEFAULT 'yahoo',
            fetched_at_utc TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ingest_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mode TEXT NOT NULL,
            started_at_utc TEXT NOT NULL,
            finished_at_utc TEXT NOT NULL,
            symbols_total INTEGER NOT NULL,
            symbols_ok INTEGER NOT NULL,
            symbols_failed INTEGER NOT NULL,
            note TEXT
        )
        """
    )
    conn.commit()


def guess_currency(symbol: str) -> str:
    return "KRW" if symbol.endswith(".KS") or symbol.endswith(".KQ") else "USD"


def market_tz(symbol: str) -> ZoneInfo:
    return ZoneInfo("Asia/Seoul") if symbol.endswith(".KS") or symbol.endswith(".KQ") else ZoneInfo("America/New_York")


def _ensure_dt_index(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    out = out.dropna(subset=["Close"])
    if out.empty:
        return pd.DataFrame()
    if not isinstance(out.index, pd.DatetimeIndex):
        out.index = pd.to_datetime(out.index, utc=True)
    elif out.index.tz is None:
        out.index = out.index.tz_localize("UTC")
    return out


def _to_iso_utc(ts: pd.Timestamp) -> str:
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    ts_utc = ts.tz_convert("UTC")
    return ts_utc.isoformat().replace("+00:00", "Z")


def calc_rsi(closes: pd.Series, period: int = 14) -> float | None:
    if closes.size < period + 1:
        return None
    delta = closes.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    avg_gain = up.rolling(period).mean()
    avg_loss = down.rolling(period).mean()
    last_gain = avg_gain.iloc[-1]
    last_loss = avg_loss.iloc[-1]
    if pd.isna(last_gain) or pd.isna(last_loss):
        return None
    if last_loss == 0:
        return 100.0
    rs = last_gain / last_loss
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
    macd_last = macd.iloc[-1]
    sig_last = sig.iloc[-1]
    if pd.isna(macd_last) or pd.isna(sig_last):
        return None, None
    return float(macd_last), float(sig_last)


def calc_bollinger(closes: pd.Series, period: int = 20, k: float = 2.0) -> tuple[float | None, float | None, float | None]:
    if closes.size < period:
        return None, None, None
    window = closes.iloc[-period:]
    mean = float(window.mean())
    std = float(window.std(ddof=0))
    return mean, mean + k * std, mean - k * std


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


def pick_prev_close_from_daily(history: pd.DataFrame, symbol: str) -> tuple[str, float, float] | None:
    if history.empty or history.shape[0] < 2:
        return None
    local_today = dt.datetime.now(tz=market_tz(symbol)).date()

    rows = history[["Close"]].dropna().copy()
    rows["trade_date_local"] = rows.index.tz_convert(market_tz(symbol)).date
    rows = rows.sort_values("trade_date_local")
    if rows.shape[0] < 2:
        return None

    valid = rows[rows["trade_date_local"] < local_today]
    if valid.shape[0] >= 2:
        close1 = float(valid.iloc[-1]["Close"])
        close0 = float(valid.iloc[-2]["Close"])
        asof = valid.iloc[-1]["trade_date_local"].isoformat()
        return asof, close1, close0

    close1 = float(rows.iloc[-1]["Close"])
    close0 = float(rows.iloc[-2]["Close"])
    asof = rows.iloc[-1]["trade_date_local"].isoformat()
    return asof, close1, close0


def upsert_price_bars(conn: sqlite3.Connection, symbol: str, interval: str, history: pd.DataFrame, tz_local: ZoneInfo) -> int:
    if history.empty:
        return 0
    now_utc = dt.datetime.now(tz=dt.timezone.utc).replace(microsecond=0).isoformat()
    rows = []
    for ts, row in history.iterrows():
        ts_utc = _to_iso_utc(pd.Timestamp(ts))
        trade_date_local = pd.Timestamp(ts).tz_convert(tz_local).date().isoformat()
        rows.append(
            (
                symbol,
                interval,
                ts_utc,
                trade_date_local,
                _nan_to_none(row.get("Open")),
                _nan_to_none(row.get("High")),
                _nan_to_none(row.get("Low")),
                _nan_to_none(row.get("Close")),
                _nan_to_none(row.get("Volume")),
                now_utc,
            )
        )
    with conn:
        conn.executemany(
            """
            INSERT INTO price_bars(
                symbol, interval, ts_utc, trade_date_local,
                open, high, low, close, volume, source, fetched_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'yahoo', ?)
            ON CONFLICT(symbol, interval, ts_utc) DO UPDATE SET
                open=excluded.open,
                high=excluded.high,
                low=excluded.low,
                close=excluded.close,
                volume=excluded.volume,
                source='yahoo',
                fetched_at_utc=excluded.fetched_at_utc
            """,
            rows,
        )
    return len(rows)


def _nan_to_none(v):
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    return float(v) if isinstance(v, (int, float)) else v


def count_daily_bars(conn: sqlite3.Connection, symbol: str) -> int:
    try:
        row = conn.execute("SELECT COUNT(*) FROM price_bars WHERE symbol = ? AND interval = '1d'", (symbol,)).fetchone()
        return int(row[0]) if row else 0
    except Exception:
        return 0


def load_daily_history_from_db(conn: sqlite3.Connection, symbol: str, limit: int = 520) -> pd.DataFrame:
    rows = conn.execute(
        """
        SELECT ts_utc, open, high, low, close, volume
        FROM price_bars
        WHERE symbol = ? AND interval = '1d'
        ORDER BY ts_utc DESC
        LIMIT ?
        """,
        (symbol, int(limit)),
    ).fetchall()
    if not rows:
        return pd.DataFrame()
    recs = []
    for r in rows:
        ts = pd.to_datetime(r[0], utc=True, errors="coerce")
        if pd.isna(ts):
            continue
        recs.append(
            {
                "ts": ts,
                "Open": r[1],
                "High": r[2],
                "Low": r[3],
                "Close": r[4],
                "Volume": r[5],
            }
        )
    if not recs:
        return pd.DataFrame()
    df = pd.DataFrame.from_records(recs).dropna(subset=["ts"]).sort_values("ts")
    df = df.set_index("ts")
    return _ensure_dt_index(df)


def upsert_daily_indicator(conn: sqlite3.Connection, symbol: str, currency: str, daily_history: pd.DataFrame, lookback_days: int) -> bool:
    pick = pick_prev_close_from_daily(daily_history, symbol)
    if pick is None:
        return False
    asof, close1, prev_close = pick
    prev_close_chg_pct = (close1 / prev_close - 1.0) if prev_close else None

    work = daily_history[["High", "Low", "Close"]].dropna().copy()
    if work.empty:
        return False

    closes = work["Close"]
    highs = work["High"]
    lows = work["Low"]

    recent = work.iloc[-lookback_days:] if work.shape[0] >= 2 else work
    high_n = float(recent["High"].max()) if not recent.empty else None
    high_drawdown_pct = (close1 / high_n - 1.0) if high_n else None

    rsi14 = calc_rsi(closes, 14)
    stoch_rsi14 = calc_stoch_rsi(closes, 14, 14)
    macd, macd_signal = calc_macd(closes, 12, 26, 9)
    bb_middle, bb_upper, bb_lower = calc_bollinger(closes, 20, 2.0)
    adx14 = calc_adx(highs, lows, closes, 14)

    high52w = float(highs.iloc[-252:].max()) if highs.size else None
    high52w_drawdown_pct = (close1 / high52w - 1.0) if high52w else None

    now_utc = dt.datetime.now(tz=dt.timezone.utc).replace(microsecond=0).isoformat()
    with conn:
        conn.execute(
            """
            INSERT INTO daily_indicators(
                symbol, trade_date, currency,
                close1, prev_close, prev_close_chg_pct,
                high_n, high_drawdown_pct,
                rsi14, stoch_rsi14,
                macd, macd_signal,
                bb_middle, bb_upper, bb_lower,
                adx14, high52w, high52w_drawdown_pct,
                updated_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, trade_date) DO UPDATE SET
                currency=excluded.currency,
                close1=excluded.close1,
                prev_close=excluded.prev_close,
                prev_close_chg_pct=excluded.prev_close_chg_pct,
                high_n=excluded.high_n,
                high_drawdown_pct=excluded.high_drawdown_pct,
                rsi14=excluded.rsi14,
                stoch_rsi14=excluded.stoch_rsi14,
                macd=excluded.macd,
                macd_signal=excluded.macd_signal,
                bb_middle=excluded.bb_middle,
                bb_upper=excluded.bb_upper,
                bb_lower=excluded.bb_lower,
                adx14=excluded.adx14,
                high52w=excluded.high52w,
                high52w_drawdown_pct=excluded.high52w_drawdown_pct,
                updated_at_utc=excluded.updated_at_utc
            """,
            (
                symbol,
                asof,
                currency,
                close1,
                prev_close,
                prev_close_chg_pct,
                high_n,
                high_drawdown_pct,
                rsi14,
                stoch_rsi14,
                macd,
                macd_signal,
                bb_middle,
                bb_upper,
                bb_lower,
                adx14,
                high52w,
                high52w_drawdown_pct,
                now_utc,
            ),
        )
    return True


def upsert_latest_quote(conn: sqlite3.Connection, symbol: str, ticker: yf.Ticker) -> bool:
    price = None
    prev_close = None
    currency = guess_currency(symbol)
    market_state = None

    try:
        fi = ticker.fast_info
    except Exception:
        fi = None

    if fi:
        try:
            price = float(fi.get("lastPrice")) if fi.get("lastPrice") is not None else None
        except Exception:
            price = None
        try:
            prev_close = float(fi.get("previousClose")) if fi.get("previousClose") is not None else None
        except Exception:
            prev_close = None
        if fi.get("currency"):
            currency = str(fi.get("currency"))

    if price is None:
        try:
            h = _ensure_dt_index(ticker.history(period="3d", interval="1d", auto_adjust=False, actions=False))
            if not h.empty:
                price = float(h["Close"].dropna().iloc[-1])
                if h["Close"].dropna().shape[0] >= 2:
                    prev_close = float(h["Close"].dropna().iloc[-2])
        except Exception:
            pass

    if price is None and prev_close is None:
        return False

    now_utc = dt.datetime.now(tz=dt.timezone.utc).replace(microsecond=0).isoformat()
    with conn:
        conn.execute(
            """
            INSERT INTO latest_quotes(symbol, price, prev_close, currency, market_state, market_time_utc, source, fetched_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, 'yahoo', ?)
            ON CONFLICT(symbol) DO UPDATE SET
                price=excluded.price,
                prev_close=excluded.prev_close,
                currency=excluded.currency,
                market_state=excluded.market_state,
                market_time_utc=excluded.market_time_utc,
                source='yahoo',
                fetched_at_utc=excluded.fetched_at_utc
            """,
            (symbol, price, prev_close, currency, market_state, now_utc, now_utc),
        )
    return True


def fetch_history(ticker: yf.Ticker, period: str, interval: str) -> pd.DataFrame:
    hist = ticker.history(period=period, interval=interval, auto_adjust=False, actions=False, prepost=False)
    return _ensure_dt_index(hist)


def run(mode: str, symbols: Iterable[str], db_path: Path, daily_period_days: int, intraday_period: str, intraday_interval: str, lookback_days: int) -> None:
    started = dt.datetime.now(tz=dt.timezone.utc)

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    ok = 0
    failed = 0

    try:
        ensure_schema(conn)
        for symbol in symbols:
            try:
                ticker = yf.Ticker(symbol)
                tz_local = market_tz(symbol)

                if mode in {"daily", "both"}:
                    existing_daily_count = count_daily_bars(conn, symbol)
                    # Existing symbols fetch only a short window, then indicators are recalculated from DB history.
                    # If DB history is still shallow (new/just-added symbol), do a full bootstrap fetch.
                    use_full_bootstrap = existing_daily_count < max(300, lookback_days + 80)
                    daily_fetch_days = int(daily_period_days) if use_full_bootstrap else min(10, int(daily_period_days))
                    daily = fetch_history(ticker, period=f"{daily_fetch_days}d", interval="1d")
                    bars_written = upsert_price_bars(conn, symbol, "1d", daily, tz_local)
                    daily_for_ind = load_daily_history_from_db(conn, symbol, limit=520)
                    ind_ok = upsert_daily_indicator(conn, symbol, guess_currency(symbol), daily_for_ind, lookback_days)
                    quote_ok = upsert_latest_quote(conn, symbol, ticker)
                    LOG.info(
                        "%s daily fetch=%sd bars=%s db_daily=%s indicator=%s quote=%s",
                        symbol,
                        daily_fetch_days,
                        bars_written,
                        count_daily_bars(conn, symbol),
                        ind_ok,
                        quote_ok,
                    )

                if mode in {"intraday", "both"}:
                    intraday = fetch_history(ticker, period=intraday_period, interval=intraday_interval)
                    bars_written = upsert_price_bars(conn, symbol, intraday_interval, intraday, tz_local)
                    quote_ok = upsert_latest_quote(conn, symbol, ticker)
                    LOG.info("%s intraday(%s) bars=%s quote=%s", symbol, intraday_interval, bars_written, quote_ok)

                ok += 1
            except Exception as exc:
                failed += 1
                LOG.warning("symbol failed: %s -> %s", symbol, exc)

        finished = dt.datetime.now(tz=dt.timezone.utc)
        with conn:
            conn.execute(
                """
                INSERT INTO ingest_runs(mode, started_at_utc, finished_at_utc, symbols_total, symbols_ok, symbols_failed, note)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    mode,
                    started.replace(microsecond=0).isoformat(),
                    finished.replace(microsecond=0).isoformat(),
                    ok + failed,
                    ok,
                    failed,
                    None,
                ),
            )
    finally:
        conn.close()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    symbols = extract_symbols(Path(args.symbols_file), Path(args.db_path))
    LOG.info("Loaded %d symbols", len(symbols))

    run(
        mode=args.mode,
        symbols=symbols,
        db_path=Path(args.db_path),
        daily_period_days=max(90, int(args.daily_period_days)),
        intraday_period=args.intraday_period,
        intraday_interval=args.intraday_interval,
        lookback_days=max(10, int(args.lookback_days)),
    )


if __name__ == "__main__":
    main()



