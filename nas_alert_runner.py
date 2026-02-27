import argparse
import datetime as dt
import hashlib
import io
import json
import logging
import os
import re
import secrets
import sqlite3
import time
from dataclasses import dataclass
from html import escape, unescape
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode, urljoin
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from pypdf import PdfReader


LOG = logging.getLogger("nas_alert_runner")
KST = ZoneInfo("Asia/Seoul")
DEFAULT_OVERSIZE_DROP_PCT = -0.05
DEFAULT_OVERSIZE_BUY_SHARES = 1
GLOBAL_BRIEF_CHAT_ID = "__MARKET_BRIEF__"
MARKET_BRIEF_TEMPLATE_VERSION = "v2"
ECONOMY_BRIEF_TEMPLATE_VERSION = "v3"
DONATE_URL = "https://qr.kakaopay.com/FL40EvAYX"

SCORE_MAX_INDICATORS = 8
BUY_SCORE_RATIO_MIN = 0.5
BUY_RSI_MAX = 30.0
BUY_STOCH_MAX = 20.0
SELL_RSI_STRONG = 70.0
SELL_STOCH_STRONG = 80.0
SELL_RSI_BB = 65.0
SELL_SCORE_RATIO_FLOOR = 0.2
SELL_BB_UPPER_PROXIMITY_PCT = 0.005
LLM_CANDIDATE_TOP_N = 5
UNIVERSE_TOP_PER_GROUP = 2
UNIVERSE_STRONG_BUY_RATIO = 1.0
UNIVERSE_REASON_TAG_MAX = 5

UNIVERSE_DOW30_SYMBOLS = [
    "AAPL", "AMGN", "AXP", "BA", "CAT", "CRM", "CSCO", "CVX", "DIS", "GS",
    "HD", "HON", "IBM", "INTC", "JNJ", "JPM", "KO", "MCD", "MMM", "MRK",
    "MSFT", "NKE", "PG", "SHW", "TRV", "UNH", "V", "VZ", "WBA", "WMT",
]
UNIVERSE_NASDAQ_MAJOR_SYMBOLS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "GOOG", "TSLA", "AVGO", "COST",
    "NFLX", "AMD", "ADBE", "PEP", "CSCO", "TMUS", "CMCSA", "INTC", "INTU", "QCOM",
    "AMGN", "TXN", "HON", "AMAT", "BKNG", "ISRG", "ADP", "GILD", "LRCX", "MU",
    "PANW", "SNPS", "KLAC", "MELI", "CDNS", "MAR", "CTAS", "FTNT", "ORLY", "ABNB",
    "PYPL", "ADSK", "CRWD", "MRVL", "WDAY", "NXPI", "REGN", "MNST", "AEP", "KDP",
]

NEWS_BUCKET_KEYWORDS: dict[str, tuple[str, ...]] = {
    "macro": (
        "연준",
        "fomc",
        "cpi",
        "pce",
        "고용",
        "실업",
        "금리",
        "기준금리",
        "국채",
        "재정",
        "관세",
        "fed",
        "ecb",
        "boj",
    ),
    "us": (
        "나스닥",
        "s&p",
        "다우",
        "뉴욕증시",
        "미국증시",
        "테슬라",
        "엔비디아",
        "애플",
        "마이크로소프트",
        "아마존",
        "메타",
        "알파벳",
        "nasdaq",
        "wall street",
        "earnings",
        "us stock",
    ),
    "kr": (
        "코스피",
        "코스닥",
        "국내증시",
        "한국증시",
        "상한가",
        "하한가",
        "삼성전자",
        "sk하이닉스",
        "외국인",
        "기관",
        "개인",
        "공매도",
        "krx",
    ),
    "fx": (
        "환율",
        "원달러",
        "달러",
        "달러인덱스",
        "dxy",
        "유가",
        "wti",
        "브렌트",
        "금값",
        "구리",
        "천연가스",
        "중동",
        "전쟁",
        "지정학",
        "원자재",
    ),
}

MORNING_CARD_ITEMS = [
    {"symbol": "^KS11", "name": "코스피", "digits": 2},
    {"symbol": "^KQ11", "name": "코스닥", "digits": 2},
    {"symbol": "^N225", "name": "니케이", "digits": 2},
    {"symbol": "^DJI", "name": "다우", "digits": 2},
    {"symbol": "^IXIC", "name": "나스닥", "digits": 2},
    {"symbol": "^GSPC", "name": "S&P500", "digits": 2},
    {"symbol": "000001.SS", "name": "상해종합", "digits": 2},
    {"symbol": "^HSI", "name": "항셍", "digits": 2},
    {"symbol": "BTC-USD", "name": "비트코인", "digits": 2},
    {"symbol": "GC=F", "name": "국제 금", "digits": 2},
    {"symbol": "CL=F", "name": "WTI", "digits": 2},
    {"symbol": "KRW=X", "name": "달러화(USD/KRW)", "digits": 2},
    {"symbol": "^TNX", "name": "미국채(10년)", "digits": 3, "transform": "tnx"},
]


@dataclass
class RuntimeConfig:
    monthly_budget_krw: float = 1_000_000
    dca_ratio: float = 0.7
    trigger_ratio: float = 0.3
    daily_dca_krw: float = 35_000
    lookback: int = 60
    oversize_buy_shares: int = DEFAULT_OVERSIZE_BUY_SHARES


class TelegramClient:
    def __init__(self, token: str):
        self.token = token.strip()
        if not self.token:
            raise ValueError("Missing TELEGRAM_BOT_TOKEN")

    def _post(self, method: str, payload: dict[str, Any]) -> dict[str, Any]:
        data = urlencode(payload).encode("utf-8")
        req = Request(
            f"https://api.telegram.org/bot{self.token}/{method}",
            data=data,
            method="POST",
            headers={"content-type": "application/x-www-form-urlencoded"},
        )
        with urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
        obj = json.loads(raw)
        if not obj.get("ok"):
            raise RuntimeError(f"Telegram API failed: {obj}")
        return obj

    def send_text(self, chat_id: str, text: str):
        chunks = split_text(text, 3500)
        for c in chunks:
            self._post("sendMessage", {"chat_id": chat_id, "text": c, "disable_web_page_preview": "true"})

    def send_text_with_buttons(self, chat_id: str, text: str, buttons: list[dict[str, str]]):
        rows = build_inline_keyboard_rows(buttons)
        payload = {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": "true",
            "reply_markup": json.dumps({"inline_keyboard": rows}, ensure_ascii=False),
        }
        self._post("sendMessage", payload)


def build_inline_keyboard_rows(buttons: list[dict[str, str]]) -> list[list[dict[str, str]]]:
    return [[{"text": b["text"], "url": b["url"]}] for b in buttons if b.get("text") and b.get("url")]


def build_daily_report_buttons(
    market_brief_url: str = "",
    economy_brief_url: str = "",
    report_url: str = "",
    manage_url: str = "",
) -> list[dict[str, str]]:
    buttons: list[dict[str, str]] = []
    if market_brief_url:
        buttons.append({"text": "📰 마켓 브리프", "url": market_brief_url})
    if economy_brief_url:
        buttons.append({"text": "📚 경제분석 리포트", "url": economy_brief_url})
    if report_url:
        buttons.append({"text": "📊 내 종목 현황", "url": report_url})
    if manage_url:
        buttons.append({"text": "⚙️ 내 종목 관리", "url": manage_url})
        buttons.append({"text": "💖 후원하기(카카오)", "url": DONATE_URL})
    return buttons


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="NAS-only alert runner")
    p.add_argument("--db-path", default="/data/stock_prices.db")
    p.add_argument("--symbols-file", default="/app/chat-configs.js")
    p.add_argument("--task", choices=["daily_report", "morning_card", "envelope_watch", "universe_scan"], required=True)
    p.add_argument("--telegram-token", default="")
    p.add_argument("--telegram-chat-ids-json", default="")
    p.add_argument("--telegram-chat-id", default="")
    p.add_argument("--manage-page-url", default="")
    p.add_argument("--web-base-url", default="")
    p.add_argument("--envelope-period", type=int, default=20)
    p.add_argument("--envelope-pct", type=float, default=0.2)
    p.add_argument("--obs-log-enabled", action="store_true")
    p.add_argument("--prepare-economy-only", action="store_true", help="Prepare economy brief only (no telegram send)")
    return p.parse_args()


def now_kst() -> dt.datetime:
    return dt.datetime.now(tz=KST)


def now_utc_iso() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).replace(microsecond=0).isoformat()


def fmt_date_kst(d: dt.datetime) -> str:
    return d.strftime("%Y-%m-%d")


def fmt_time_kst(d: dt.datetime) -> str:
    return d.strftime("%H:%M")


def fmt_ym(d: dt.datetime) -> str:
    return d.strftime("%Y-%m")


def is_krw_symbol(symbol: str) -> bool:
    return symbol.endswith(".KS") or symbol.endswith(".KQ")


def currency_of(symbol: str) -> str:
    return "KRW" if is_krw_symbol(symbol) else "USD"


def round_price(price: Any, ccy: str) -> float | None:
    try:
        n = float(price)
    except Exception:
        return None
    if ccy == "KRW":
        return float(round(n))
    return round(n, 2)


def fmt_money(amount: Any, ccy: str) -> str:
    try:
        n = float(amount)
    except Exception:
        return "N/A"
    if ccy == "KRW":
        return f"{int(round(n)):,}원"
    return f"${n:,.2f}"


def fmt_pct_signed(x: Any, digits: int = 1) -> str:
    try:
        n = float(x)
    except Exception:
        return "N/A"
    v = f"{n * 100:.{digits}f}%"
    return f"+{v}" if n >= 0 else v


def split_text(text: str, chunk_size: int) -> list[str]:
    if len(text) <= chunk_size:
        return [text]
    out = []
    cur = []
    cur_len = 0
    for line in text.split("\n"):
        ll = len(line) + 1
        if cur_len + ll > chunk_size and cur:
            out.append("\n".join(cur))
            cur = [line]
            cur_len = ll
        else:
            cur.append(line)
            cur_len += ll
    if cur:
        out.append("\n".join(cur))
    return out


def js_object_to_json_text(js: str) -> str:
    body = re.sub(r"//.*", "", js)
    body = re.sub(r"\bexport\s+default\s+chatConfigs\s*;", "", body)
    body = re.sub(r"\bconst\s+chatConfigs\s*=", "", body)
    body = body.strip().rstrip(";")

    def qk(m: re.Match) -> str:
        pre = m.group(1)
        key = m.group(2)
        return f'{pre}"{key}":'

    body = re.sub(r"([\{,]\s*)([A-Za-z_][A-Za-z0-9_]*)\s*:", qk, body)
    body = re.sub(r",\s*([}\]])", r"\1", body)
    return body


def load_chat_configs(path: Path) -> dict[str, Any]:
    raw = path.read_text(encoding="utf-8", errors="ignore")
    json_text = js_object_to_json_text(raw)
    obj = json.loads(json_text)
    if not isinstance(obj, dict):
        raise RuntimeError("chat-configs.js parse failed")
    return obj


def load_chat_ids_from_db(db_path: str) -> list[str]:
    try:
        conn = sqlite3.connect(db_path)
    except Exception:
        return []
    try:
        rows = conn.execute("SELECT chat_id FROM manage_users WHERE enabled = 1").fetchall()
        out = []
        for (cid,) in rows:
            s = str(cid or "").strip()
            if s and s not in out:
                out.append(s)
        return out
    except Exception:
        return []
    finally:
        conn.close()


def load_chat_ids(args: argparse.Namespace, cfg: dict[str, Any]) -> list[str]:
    if args.telegram_chat_ids_json:
        try:
            arr = json.loads(args.telegram_chat_ids_json)
            if isinstance(arr, list):
                return [str(x) for x in arr]
        except Exception:
            pass
    if args.telegram_chat_id:
        return [str(args.telegram_chat_id)]

    by_db = load_chat_ids_from_db(args.db_path)
    if by_db:
        return by_db

    out = []
    for k in cfg.keys():
        if k in {"meta", "default"}:
            continue
        if re.fullmatch(r"\d+", str(k)):
            out.append(str(k))
    return sorted(set(out))


def default_runtime_config() -> RuntimeConfig:
    return RuntimeConfig()


def load_chat_config(cfg: dict[str, Any], chat_id: str) -> dict[str, Any]:
    dft = cfg.get("default") if isinstance(cfg.get("default"), dict) else {}
    by_chat = cfg.get(chat_id) if isinstance(cfg.get(chat_id), dict) else {}

    tickers = by_chat.get("tickers") if isinstance(by_chat.get("tickers"), list) and by_chat.get("tickers") else dft.get("tickers", [])
    oversize = by_chat.get("oversize_drop_pct") if isinstance(by_chat.get("oversize_drop_pct"), dict) else dft.get("oversize_drop_pct", {})
    news_keywords = by_chat.get("news_keywords") if isinstance(by_chat.get("news_keywords"), list) else (
        dft.get("news_keywords", []) if isinstance(dft.get("news_keywords"), list) else []
    )
    return {"tickers": tickers, "oversize_drop_pct": oversize, "news_keywords": news_keywords}


def load_chat_config_override(conn: sqlite3.Connection, chat_id: str) -> dict[str, Any] | None:
    row = conn.execute("SELECT override_json FROM chat_config_overrides WHERE chat_id = ?", (chat_id,)).fetchone()
    if not row:
        return None
    try:
        obj = json.loads(row[0])
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def merge_chat_config(base: dict[str, Any], override: dict[str, Any] | None) -> dict[str, Any]:
    out = {
        "tickers": list(base.get("tickers", [])),
        "oversize_drop_pct": dict(base.get("oversize_drop_pct", {})),
        "news_keywords": list(base.get("news_keywords", [])) if isinstance(base.get("news_keywords"), list) else [],
    }
    if not isinstance(override, dict):
        return out
    if isinstance(override.get("tickers"), list):
        out["tickers"] = override["tickers"]
    if isinstance(override.get("oversize_drop_pct"), dict):
        out["oversize_drop_pct"] = override["oversize_drop_pct"]
    if isinstance(override.get("news_keywords"), list):
        out["news_keywords"] = override["news_keywords"]
    return out


def ensure_alert_schema(conn: sqlite3.Connection):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS chat_state (
            chat_id TEXT PRIMARY KEY,
            state_json TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS manage_users (
            name TEXT PRIMARY KEY,
            pin TEXT NOT NULL,
            chat_id TEXT NOT NULL,
            is_admin INTEGER NOT NULL DEFAULT 0,
            enabled INTEGER NOT NULL DEFAULT 1,
            updated_at_utc TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS chat_config_overrides (
            chat_id TEXT PRIMARY KEY,
            override_json TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS chat_reports (
            chat_id TEXT NOT NULL,
            report_token TEXT PRIMARY KEY,
            html_content TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS candidate_brief_cache (
            cache_date TEXT NOT NULL,
            cache_key TEXT NOT NULL,
            brief_json TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL,
            PRIMARY KEY (cache_date, cache_key)
        )
        """
    )
    conn.commit()


def empty_state() -> dict[str, Any]:
    return {
        "v": 1,
        "symbols": {},
        "envelope_watch": {},
        "morning_card_date": None,
        "news_digest_date": None,
        "candidate_brief_date": None,
        "candidate_brief_json": None,
        "report_token": None,
        "updated_at": None,
    }


def load_state(conn: sqlite3.Connection, chat_id: str) -> dict[str, Any]:
    row = conn.execute("SELECT state_json FROM chat_state WHERE chat_id = ?", (chat_id,)).fetchone()
    if not row:
        return empty_state()
    try:
        doc = json.loads(row[0])
        if not isinstance(doc, dict):
            return empty_state()
        if not isinstance(doc.get("symbols"), dict):
            doc["symbols"] = {}
        if not isinstance(doc.get("envelope_watch"), dict):
            doc["envelope_watch"] = {}
        if doc.get("news_digest_date") is not None and not isinstance(doc.get("news_digest_date"), str):
            doc["news_digest_date"] = None
        if doc.get("candidate_brief_date") is not None and not isinstance(doc.get("candidate_brief_date"), str):
            doc["candidate_brief_date"] = None
        if doc.get("candidate_brief_json") is not None and not isinstance(doc.get("candidate_brief_json"), dict):
            doc["candidate_brief_json"] = None
        if doc.get("report_token") is not None and not isinstance(doc.get("report_token"), str):
            doc["report_token"] = None
        return doc
    except Exception:
        return empty_state()


def save_state(conn: sqlite3.Connection, chat_id: str, doc: dict[str, Any]):
    now_utc = dt.datetime.now(tz=dt.timezone.utc).replace(microsecond=0).isoformat()
    doc["updated_at"] = now_utc
    with conn:
        conn.execute(
            """
            INSERT INTO chat_state(chat_id, state_json, updated_at_utc)
            VALUES (?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
              state_json=excluded.state_json,
              updated_at_utc=excluded.updated_at_utc
            """,
            (chat_id, json.dumps(doc, ensure_ascii=False), now_utc),
        )


def ensure_report_token(state: dict[str, Any]) -> str:
    tok = state.get("report_token")
    if isinstance(tok, str) and tok.strip():
        return tok.strip()
    tok = secrets.token_urlsafe(24)
    state["report_token"] = tok
    return tok


def save_chat_report(conn: sqlite3.Connection, chat_id: str, token: str, html: str):
    now_utc = now_utc_iso()
    with conn:
        conn.execute(
            """
            INSERT INTO chat_reports(chat_id, report_token, html_content, updated_at_utc)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(report_token) DO UPDATE SET
              chat_id=excluded.chat_id,
              html_content=excluded.html_content,
              updated_at_utc=excluded.updated_at_utc
            """,
            (chat_id, token, html, now_utc),
        )


def load_chat_report_html(conn: sqlite3.Connection, token: str) -> str | None:
    row = conn.execute("SELECT html_content FROM chat_reports WHERE report_token = ?", (token,)).fetchone()
    if not row:
        return None
    return str(row[0] or "")


def load_candidate_brief_cache(conn: sqlite3.Connection, cache_date: str, cache_key: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT brief_json FROM candidate_brief_cache WHERE cache_date = ? AND cache_key = ?",
        (cache_date, cache_key),
    ).fetchone()
    if not row:
        return None
    try:
        parsed = json.loads(str(row[0] or ""))
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


def save_candidate_brief_cache(conn: sqlite3.Connection, cache_date: str, cache_key: str, brief: dict[str, Any]):
    now_utc = now_utc_iso()
    with conn:
        conn.execute(
            """
            INSERT INTO candidate_brief_cache(cache_date, cache_key, brief_json, updated_at_utc)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(cache_date, cache_key) DO UPDATE SET
              brief_json=excluded.brief_json,
              updated_at_utc=excluded.updated_at_utc
            """,
            (cache_date, cache_key, json.dumps(brief, ensure_ascii=False), now_utc),
        )


def build_portfolio_coach_token(date_kst: str) -> str:
    return f"portfolio_coach:{date_kst}"


def _clamp_int(v: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, int(v)))


def compute_portfolio_diagnosis(stock_rows: list[dict[str, Any]]) -> dict[str, Any]:
    rows = [r for r in stock_rows if isinstance(r, dict)]
    drawdown20_count = 0
    vol_surge_count = 0
    ma_up_count = 0
    ma_down_count = 0
    atr_vals: list[float] = []
    atr_high_count = 0
    oversold_count = 0
    risk_points_items: list[tuple[str, float]] = []

    for r in rows:
        sym = str(r.get("symbol", "")).strip()
        name = str(r.get("name", sym)).strip()
        label = f"{name}({sym})" if sym else name
        rp = 0.0

        dd60 = r.get("highDrawdownPct")
        if dd60 is not None:
            try:
                ddn = float(dd60)
                if ddn <= -0.30:
                    rp += 3
                elif ddn <= -0.20:
                    rp += 2
                if ddn <= -0.20:
                    drawdown20_count += 1
            except Exception:
                pass

        rsi = r.get("rsi14")
        if rsi is not None:
            try:
                rsin = float(rsi)
                if rsin < 25:
                    rp += 3
                elif rsin < 30:
                    rp += 2
                if rsin < 30:
                    oversold_count += 1
            except Exception:
                pass

        vr = r.get("volumeRatio20")
        if vr is not None:
            try:
                vrn = float(vr)
                if vrn >= 3.0:
                    rp += 2
                elif vrn >= 2.0:
                    rp += 1
                if vrn >= 2.0:
                    vol_surge_count += 1
            except Exception:
                pass

        ma20 = r.get("ma20")
        ma60 = r.get("ma60")
        if ma20 is not None and ma60 is not None:
            try:
                m20 = float(ma20)
                m60 = float(ma60)
                if m20 > m60:
                    ma_up_count += 1
                elif m20 < m60:
                    ma_down_count += 1
                    rp += 1
            except Exception:
                pass

        atrp = r.get("atr14Pct")
        if atrp is not None:
            try:
                an = float(atrp)
                atr_vals.append(an)
                if an >= 0.07:
                    rp += 2
                elif an >= 0.05:
                    rp += 1
                if an >= 0.05:
                    atr_high_count += 1
            except Exception:
                pass

        if rp > 0:
            risk_points_items.append((label, rp))

    risk_points_items.sort(key=lambda x: x[1], reverse=True)
    total_risk_points = sum(v for _, v in risk_points_items)
    top2_risk_points = sum(v for _, v in risk_points_items[:2])
    risk_concentration_pct = (top2_risk_points / max(total_risk_points, 1.0)) * 100.0 if risk_points_items else 0.0

    row_count = max(len(rows), 1)
    downtrend_ratio = (ma_down_count / row_count) if row_count else 0.0
    atr_avg = (sum(atr_vals) / len(atr_vals)) if atr_vals else None

    score = 0
    score += min(drawdown20_count, 4)
    score += min(oversold_count, 3)
    score += min(vol_surge_count, 2)
    if downtrend_ratio >= 0.7:
        score += 3
    elif downtrend_ratio >= 0.5:
        score += 2
    if atr_avg is not None:
        if atr_avg >= 0.06:
            score += 2
        elif atr_avg >= 0.04:
            score += 1
    if risk_concentration_pct >= 80:
        score += 2
    elif risk_concentration_pct >= 60:
        score += 1
    risk_score_10 = _clamp_int(score, 0, 10)

    return {
        "riskConcentrationPct": risk_concentration_pct,
        "drawdown20Count": drawdown20_count,
        "volSurgeCount": vol_surge_count,
        "maUpCount": ma_up_count,
        "maDownCount": ma_down_count,
        "atrAvgPct": (atr_avg * 100.0) if atr_avg is not None else None,
        "atrHighCount": atr_high_count,
        "riskScore10": risk_score_10,
        "riskPointsTotal": total_risk_points,
        "riskTop2": risk_points_items[:2],
        "rowCount": len(rows),
    }


def _portfolio_diag_fallback_comment(diag: dict[str, Any]) -> str:
    s = int(diag.get("riskScore10") or 0)
    conc = float(diag.get("riskConcentrationPct") or 0.0)
    atr = diag.get("atrAvgPct")
    if s >= 8:
        return "리스크 신호가 다수 누적되어 있고 일부 종목에 위험이 집중될 수 있어 변동성 확대 구간 점검이 필요합니다."
    if conc >= 80:
        return "전체 위험 신호가 소수 종목에 크게 집중된 모습이라 특정 종목 이벤트 영향도를 유의해서 보시면 좋겠습니다."
    if atr is not None and float(atr) >= 5.0:
        return "평균 변동성이 높은 편이라 종목별 움직임 차이가 커질 수 있는 구간으로 보입니다."
    return "위험 신호는 일부 존재하지만 전반적으로 분산된 편이며 지표 변화 방향을 함께 추적하면 좋겠습니다."


def get_or_create_portfolio_coach_line(conn: sqlite3.Connection, today_kst: str, diag: dict[str, Any]) -> str:
    token = build_portfolio_coach_token(today_kst)
    cached = (load_chat_report_html(conn, token) or "").strip()
    if cached:
        return cached

    api_key = (os.getenv("OPENAI_API_KEY", "") or "").strip()
    if not api_key:
        line = _portfolio_diag_fallback_comment(diag)
        save_chat_report(conn, GLOBAL_BRIEF_CHAT_ID, token, line)
        return line

    payload = {
        "risk_score_10": int(diag.get("riskScore10") or 0),
        "row_count": int(diag.get("rowCount") or 0),
        "risk_concentration_pct": round(float(diag.get("riskConcentrationPct") or 0.0), 1),
        "drawdown_20_count": int(diag.get("drawdown20Count") or 0),
        "vol_surge_count": int(diag.get("volSurgeCount") or 0),
        "ma_up_count": int(diag.get("maUpCount") or 0),
        "ma_down_count": int(diag.get("maDownCount") or 0),
        "atr_avg_pct": round(float(diag.get("atrAvgPct") or 0.0), 2) if diag.get("atrAvgPct") is not None else None,
        "atr_high_count": int(diag.get("atrHighCount") or 0),
        "top2_risk_items": [
            {"name": str(n), "risk_points": float(v)} for n, v in (diag.get("riskTop2") or [])[:2]
        ],
    }
    req_body = {
        "model": "gpt-5-mini",
        "reasoning": {"effort": "low"},
        "input": [
            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": "너는 포트폴리오 상태를 리스크 관점으로 1문장 요약한다. 투자 조언 금지(매수/매도 지시 금지), 중립 톤. 한국어 1문장, 이모지 1개 이내. 제공된 vol_surge_count, ma_up_count/ma_down_count, atr_avg_pct/atr_high_count를 가능한 경우 문장 판단 근거에 반영해라.",
                    }
                ],
            },
            {"role": "user", "content": [{"type": "input_text", "text": json.dumps(payload, ensure_ascii=False)}]},
        ],
        "truncation": "auto",
        "max_output_tokens": 100,
    }
    line = ""
    try:
        req = Request(
            "https://api.openai.com/v1/responses",
            data=json.dumps(req_body, ensure_ascii=False).encode("utf-8"),
            method="POST",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        )
        with urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
        obj = json.loads(raw)
        line = re.sub(r"\s+", " ", _openai_extract_json_text(obj)).strip()
        if line:
            line = line.splitlines()[0].strip()
        if len(line) > 140:
            line = line[:139].rstrip() + "…"
    except Exception as e:
        LOG.warning("portfolio coach line failed: %s", e)
        line = ""

    if not line:
        line = _portfolio_diag_fallback_comment(diag)
    save_chat_report(conn, GLOBAL_BRIEF_CHAT_ID, token, line)
    return line


def build_market_brief_token(date_kst: str) -> str:
    return f"market-brief-{MARKET_BRIEF_TEMPLATE_VERSION}-{date_kst}"


def _openai_extract_json_text(obj: Any) -> str:
    if not isinstance(obj, dict):
        return ""
    # Responses API often exposes output_text directly.
    if isinstance(obj.get("output_text"), str):
        return str(obj["output_text"])
    # Fallback for nested output format.
    out = obj.get("output")
    if isinstance(out, list):
        texts: list[str] = []
        for part in out:
            if not isinstance(part, dict):
                continue
            content = part.get("content")
            if not isinstance(content, list):
                continue
            for c in content:
                if isinstance(c, dict) and c.get("type") in {"output_text", "text"} and isinstance(c.get("text"), str):
                    texts.append(str(c["text"]))
        if texts:
            return "\n".join(texts)
    # Fallback for chat completions style.
    choices = obj.get("choices")
    if isinstance(choices, list) and choices:
        msg = choices[0].get("message") if isinstance(choices[0], dict) else None
        if isinstance(msg, dict):
            content = msg.get("content")
            if isinstance(content, str):
                return content
    return ""


def _openai_usage_summary(obj: Any) -> dict[str, int | None]:
    usage = obj.get("usage") if isinstance(obj, dict) else None
    if not isinstance(usage, dict):
        return {"input_tokens": None, "output_tokens": None, "total_tokens": None}

    def _as_int(v: Any) -> int | None:
        try:
            return int(v) if v is not None else None
        except Exception:
            return None

    # Responses API usually uses input/output/total_tokens.
    input_tokens = _as_int(usage.get("input_tokens"))
    output_tokens = _as_int(usage.get("output_tokens"))
    total_tokens = _as_int(usage.get("total_tokens"))

    # Some schemas may use prompt/completion tokens.
    if input_tokens is None:
        input_tokens = _as_int(usage.get("prompt_tokens"))
    if output_tokens is None:
        output_tokens = _as_int(usage.get("completion_tokens"))
    if total_tokens is None:
        total_tokens = _as_int(usage.get("total"))

    return {"input_tokens": input_tokens, "output_tokens": output_tokens, "total_tokens": total_tokens}


def _log_openai_usage(tag: str, obj: Any, extra: str = ""):
    u = _openai_usage_summary(obj)
    if all(v is None for v in u.values()):
        LOG.info("openai %s success usage_unavailable%s", tag, f" {extra}" if extra else "")
        return
    LOG.info(
        "openai %s success input_tokens=%s output_tokens=%s total_tokens=%s%s",
        tag,
        u["input_tokens"],
        u["output_tokens"],
        u["total_tokens"],
        f" {extra}" if extra else "",
    )
    _log_openai_cost_estimate(tag, u, extra=extra)


def _env_float(name: str, default: float | None = None) -> float | None:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except Exception:
        return default


def _log_openai_cost_estimate(tag: str, usage: dict[str, int | None], extra: str = ""):
    in_tok = usage.get("input_tokens")
    out_tok = usage.get("output_tokens")
    if in_tok is None and out_tok is None:
        return

    # Pricing is intentionally env-driven because vendor pricing can change.
    # Unit: USD per 1M tokens.
    in_per_1m = _env_float("OPENAI_PRICE_INPUT_PER_1M_USD")
    out_per_1m = _env_float("OPENAI_PRICE_OUTPUT_PER_1M_USD")
    if in_per_1m is None and out_per_1m is None:
        return

    cost_in = ((in_tok or 0) / 1_000_000.0) * float(in_per_1m or 0.0)
    cost_out = ((out_tok or 0) / 1_000_000.0) * float(out_per_1m or 0.0)
    total = cost_in + cost_out
    LOG.info(
        "openai %s estimated_cost_usd=%.6f (input=%.6f output=%.6f price_in_per_1m=%s price_out_per_1m=%s)%s",
        tag,
        total,
        cost_in,
        cost_out,
        in_per_1m,
        out_per_1m,
        f" {extra}" if extra else "",
    )


def openai_market_brief_json(headlines_payload: dict[str, Any], indicators_payload: list[dict[str, Any]]) -> dict[str, Any] | None:
    api_key = (os.getenv("OPENAI_API_KEY", "") or "").strip()
    if not api_key:
        LOG.info("openai market_brief skipped: missing OPENAI_API_KEY")
        return None

    model = (os.getenv("OPENAI_MODEL", "gpt-5-mini") or "gpt-5-mini").strip()

    system_prompt = (
        "한국어 금융 브리프 편집자. 기사 본문 금지. 입력 헤드라인/설명/지표만 사용. "
        "중복 제거·압축하고 JSON만 출력. 카테고리 혼입 금지."
    )
    schema_hint = {
        "tldr": ["문장", "문장", "문장"],
        "scores": {"risk_on": 0, "vol": 0, "rates": 0, "usd": 0},
        "playbook": {"pullback": "", "breakout": "", "breakdown": ""},
        "dash": {"us": "", "kr": ""},
        "indicators": [{"name": "", "value": "", "comment": ""}],
        "news_buckets": {
            "macro": [{"point": "", "links": [""]}],
            "us": [{"point": "", "links": [""]}],
            "kr": [{"point": "", "links": [""]}],
            "fx": [{"point": "", "links": [""]}],
        },
        "execution_note": "",
    }
    user_payload = {
        "date_kst": fmt_date_kst(now_kst()),
        "indicators": indicators_payload,
        "news_headlines": headlines_payload,
        "output_schema_example": schema_hint,
        "rules": [
            "JSON object only",
            "no markdown",
            "bucket당 2~4개 포인트",
            "카테고리 혼입 금지(미국/한국/거시/환율·원자재 엄격 분리)",
            "링크는 대표 1~3개",
            "점수는 0~10 정수",
            "불확실하면 보수적 표현",
        ],
    }

    req_body = {
        "model": model,
        "reasoning": {"effort": "low"},
        "input": [
            {"role": "system", "content": [{"type": "input_text", "text": system_prompt}]},
            {"role": "user", "content": [{"type": "input_text", "text": json.dumps(user_payload, ensure_ascii=False)}]},
        ],
        "text": {"format": {"type": "json_object"}},
        "truncation": "auto",
        "max_output_tokens": 2200,
    }

    last_err: Exception | None = None
    for attempt in range(4):
        try:
            LOG.info(
                "openai market_brief call attempt=%s model=%s headlines=%s indicators=%s max_output_tokens=%s",
                attempt + 1,
                model,
                sum(len(v) for v in headlines_payload.values()) if isinstance(headlines_payload, dict) else 0,
                len(indicators_payload) if isinstance(indicators_payload, list) else 0,
                req_body.get("max_output_tokens"),
            )
            req = Request(
                "https://api.openai.com/v1/responses",
                data=json.dumps(req_body, ensure_ascii=False).encode("utf-8"),
                method="POST",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
            )
            with urlopen(req, timeout=70) as resp:
                raw = resp.read().decode("utf-8", errors="ignore")

            obj = json.loads(raw)
            _log_openai_usage("market_brief", obj)

            txt = _openai_extract_json_text(obj).strip()
            if not txt:
                raise RuntimeError("empty OpenAI JSON text")

            parsed = json.loads(txt)
            return parsed if isinstance(parsed, dict) else None

        except HTTPError as e:
            last_err = e
            detail = ""
            try:
                detail = e.read().decode("utf-8", errors="ignore")
            except Exception:
                pass
            LOG.warning(
                "openai market_brief HTTPError attempt=%s status=%s detail=%s",
                attempt + 1,
                getattr(e, "code", "?"),
                (detail or str(e))[:1200],
            )
            if getattr(e, "code", 0) in (400, 401, 403, 404):
                break
            time.sleep(min(8.0, (2**attempt) + 0.3))
            continue

        except (URLError, TimeoutError, json.JSONDecodeError, RuntimeError, OSError) as e:
            last_err = e
            LOG.warning("openai market_brief error attempt=%s err=%s", attempt + 1, e)
            time.sleep(min(8.0, (2**attempt) + 0.3))
            continue

    if last_err:
        LOG.warning("openai market brief failed: %s", last_err)
    return None


def _bucket_score(text: str, bucket: str) -> int:
    keywords = NEWS_BUCKET_KEYWORDS.get(bucket, ())
    if not keywords:
        return 0
    low = text.lower()
    return sum(1 for kw in keywords if kw in low)


def _guess_news_bucket(title: str, summary: str) -> str | None:
    text = f"{title} {summary}".strip()
    if not text:
        return None
    scores = {b: _bucket_score(text, b) for b in NEWS_BUCKET_KEYWORDS.keys()}
    best_bucket = max(scores, key=lambda b: scores[b])
    return best_bucket if scores[best_bucket] > 0 else None


def _news_bucket_queries() -> dict[str, list[str]]:
    return {
        "macro": ["연준 금리 CPI 고용 미국채", "FOMC inflation bond yield"],
        "us": ["나스닥 S&P500 빅테크 실적 AI", "NASDAQ market stocks AI earnings"],
        "kr": ["코스피 코스닥 반도체 외국인 환율", "삼성전자 SK하이닉스 코스피"],
        "fx": ["달러 환율 원달러 유가 금 미국채10년", "WTI gold dollar index treasury yield"],
    }


def collect_market_brief_headlines(
    naver_client_id: str, naver_client_secret: str, per_bucket_limit: int = 20
) -> dict[str, list[dict[str, str]]]:
    buckets: dict[str, list[dict[str, str]]] = {k: [] for k in _news_bucket_queries().keys()}
    if not naver_client_id or not naver_client_secret:
        return buckets

    per_bucket_links: dict[str, set[str]] = {k: set() for k in buckets.keys()}
    for bucket, queries in _news_bucket_queries().items():
        for q in queries:
            try:
                rows = fetch_naver_news(naver_client_id, naver_client_secret, q, display=20)
            except Exception as e:
                LOG.warning("naver brief fetch failed bucket=%s q=%s err=%s", bucket, q, e)
                continue
            for r in rows:
                link = str(r.get("link", "")).strip()
                title = str(r.get("title", "")).strip()
                summary = _clean_news_text(r.get("summary", ""), limit=100)
                if not link or not title:
                    continue
                if _guess_news_bucket(title, summary) != bucket:
                    continue
                if link in per_bucket_links[bucket]:
                    continue
                per_bucket_links[bucket].add(link)
                buckets[bucket].append(
                    {
                        "title": title,
                        "description": summary,
                        "link": link,
                        "pubDate": str(r.get("pub", "")),
                    }
                )
                if len(buckets[bucket]) >= per_bucket_limit:
                    break
            if len(buckets[bucket]) >= per_bucket_limit:
                break
    return buckets


def build_market_brief_indicators_payload(market_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in market_rows:
        out.append(
            {
                "name": str(r.get("name", "")),
                "symbol": str(r.get("symbol", "")),
                "value": fmt_money(r.get("price"), "KRW" if str(r.get("symbol", "")).endswith(".KS") else "USD")
                if str(r.get("symbol", "")) not in {"^KS11", "^KQ11", "^N225", "^DJI", "^IXIC", "^GSPC", "000001.SS", "^HSI", "^TNX"}
                else f"{float(r.get('price')):,.{int(r.get('digits', 2))}f}" if r.get("price") is not None else "N/A",
                "change_pct": fmt_report_pct(r.get("change_pct")) if r.get("change_pct") is not None else "N/A",
            }
        )
    return out


def fallback_market_brief_json(headlines: dict[str, list[dict[str, str]]], indicators_payload: list[dict[str, Any]]) -> dict[str, Any]:
    def bucket_points(items: list[dict[str, str]], n: int = 3) -> list[dict[str, Any]]:
        out = []
        for it in items[:n]:
            out.append({"point": str(it.get("title", "")), "links": [str(it.get("link", ""))]})
        return out

    return {
        "tldr": [
            "오늘 시장은 금리·달러 방향성 확인이 우선입니다.",
            "뉴스보다 지수/환율/금리 가격 반응을 먼저 확인하세요.",
            "추격 매수보다 눌림/재확인 구간에서 분할 접근이 유리합니다.",
        ],
        "scores": {"risk_on": 5, "vol": 5, "rates": 5, "usd": 5},
        "playbook": {
            "pullback": "강한 섹터만 분할 접근, 평균단가 통제",
            "breakout": "돌파 추격보다 재확인 이후 소량 접근",
            "breakdown": "원인 분석보다 고베타 비중 축소 우선",
        },
        "dash": {"us": "미국 증시 방향과 금리 반응을 함께 확인", "kr": "한국은 환율/수급이 장 초 변동성 핵심"},
        "indicators": indicators_payload[:8],
        "news_buckets": {
            "macro": bucket_points(headlines.get("macro", [])),
            "us": bucket_points(headlines.get("us", [])),
            "kr": bucket_points(headlines.get("kr", [])),
            "fx": bucket_points(headlines.get("fx", [])),
        },
        "execution_note": "자동 요약 실패 시 fallback 브리프입니다. 원문 링크와 가격 반응을 우선 확인하세요.",
    }


def _safe_int_0_10(v: Any, default: int = 5) -> int:
    try:
        n = int(v)
    except Exception:
        return default
    return max(0, min(10, n))


def _score_bar(label: str, val: int, color: str) -> str:
    w = max(0, min(100, int(val * 10)))
    return (
        "<div style='display:flex;align-items:center;gap:10px;margin-top:10px;'>"
        f"<div style='width:110px;font-size:13px;font-weight:800;'>{escape(label)}</div>"
        "<div style='flex:1;height:10px;background:#eef2f7;border-radius:999px;overflow:hidden;border:1px solid #e5e7eb;'>"
        f"<div style='width:{w}%;height:100%;background:{color};'></div>"
        "</div>"
        f"<div style='width:36px;text-align:right;font-size:13px;font-weight:900;'>{val}</div>"
        "</div>"
    )


def render_market_brief_html(
    today: str,
    now_time: str,
    brief: dict[str, Any],
    market_rows: list[dict[str, Any]],
) -> str:
    tldr = [str(x) for x in (brief.get("tldr") or []) if str(x).strip()][:3]
    scores = brief.get("scores") if isinstance(brief.get("scores"), dict) else {}
    playbook = brief.get("playbook") if isinstance(brief.get("playbook"), dict) else {}
    dash = brief.get("dash") if isinstance(brief.get("dash"), dict) else {}
    buckets = brief.get("news_buckets") if isinstance(brief.get("news_buckets"), dict) else {}
    exec_note = str(brief.get("execution_note") or "").strip()

    by_sym = {str(r.get("symbol", "")): r for r in market_rows}
    def _chg_color(chg_pct: Any) -> str:
        try:
            n = float(chg_pct)
        except Exception:
            return "#6b7280"
        if n > 0:
            return "#c81e1e"
        if n < 0:
            return "#1d4ed8"
        return "#6b7280"

    def _fmt_change_span(chg_pct: Any) -> str:
        c = fmt_report_pct(chg_pct) if chg_pct is not None else "N/A"
        color = _chg_color(chg_pct)
        return f"<span style='color:{color};font-weight:700;'>({escape(c)})</span>"

    def line_for(sym: str) -> str:
        r = by_sym.get(sym)
        if not r:
            return "N/A"
        v = f"{float(r.get('price')):,.{int(r.get('digits', 2))}f}" if r.get("price") is not None else "N/A"
        c = fmt_report_pct(r.get("change_pct")) if r.get("change_pct") is not None else "N/A"
        return f"{v} ({c})"

    def line_for_html(sym: str) -> str:
        r = by_sym.get(sym)
        if not r:
            return "N/A"
        v = f"{float(r.get('price')):,.{int(r.get('digits', 2))}f}" if r.get("price") is not None else "N/A"
        return f"{escape(v)} {_fmt_change_span(r.get('change_pct'))}"

    def bucket_html(title: str, key: str) -> str:
        items = buckets.get(key) if isinstance(buckets.get(key), list) else []
        lis = []
        for it in items[:4]:
            if not isinstance(it, dict):
                continue
            point = str(it.get("point", "")).strip()
            links = [str(x).strip() for x in (it.get("links") or []) if str(x).strip()]
            if not point:
                continue
            link_html = ""
            if links:
                link_buttons = []
                for i, u in enumerate(links[:3], start=1):
                    link_buttons.append(
                        f"<a href='{escape(u)}' "
                        "style='display:inline-block;padding:3px 8px;border-radius:999px;"
                        "background:#eff6ff;border:1px solid #bfdbfe;color:#1d4ed8;"
                        "text-decoration:none;font-weight:600;margin-right:6px;margin-top:4px;' "
                        "target='_blank' rel='noopener noreferrer'>"
                        f"&#50896;&#47928; &#48372;&#44592; {i}</a>"
                    )
                link_html = (
                    "<div style='margin-top:6px;font-size:12px;color:#6b7280;'>"
                    "&#45824;&#54364; &#47553;&#53356;: "
                    + "".join(link_buttons)
                    + "</div>"
                )
            lis.append(f"<li style='margin-bottom:8px;'>{escape(point)}{link_html}</li>")
        if not lis:
            lis = ["<li>요약 데이터 없음</li>"]
        return (
            "<div style='flex:1;min-width:320px;padding:12px;border-radius:14px;background:#f9fafb;border:1px solid #eef2f7;'>"
            f"<div style='font-size:13px;font-weight:900;margin-bottom:8px;'>{escape(title)}</div>"
            f"<ul style='margin:0;padding-left:18px;font-size:13px;line-height:1.65;'>{''.join(lis)}</ul>"
            "</div>"
        )

    us_dash = str(dash.get("us") or "미국 시장 코멘트 데이터 없음")
    kr_dash = str(dash.get("kr") or "한국 시장 코멘트 데이터 없음")
    tl_dr_html = "<br/>".join([f"{i+1}. {escape(x)}" for i, x in enumerate(tldr)]) if tldr else "요약 데이터 없음"
    pb_pull = escape(str(playbook.get("pullback") or "데이터 없음"))
    pb_breakout = escape(str(playbook.get("breakout") or "데이터 없음"))
    pb_breakdown = escape(str(playbook.get("breakdown") or "데이터 없음"))
    indicator_lines = []
    for r in market_rows:
        sym = str(r.get("symbol", ""))
        if sym not in {"KRW=X", "^TNX", "CL=F", "GC=F"}:
            continue
        v = f"{float(r.get('price')):,.{int(r.get('digits', 2))}f}" if r.get("price") is not None else "N/A"
        indicator_lines.append(
            f"<td style='padding:6px 0;'><b>{escape(str(r.get('name','')))}</b> {escape(v)} {_fmt_change_span(r.get('change_pct'))}</td>"
        )
    if len(indicator_lines) < 4:
        indicator_lines += ["<td style='padding:6px 0;'>데이터 없음</td>"] * (4 - len(indicator_lines))

    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>아침 올인원 마켓 브리프</title>
  <style>
    * {{ box-sizing: border-box; }}
    html, body {{ max-width: 100%; overflow-x: hidden; }}
    .mb-shell {{ width: 720px; max-width: 100%; }}
    .mb-title {{ font-size: 28px; }}
    .mb-flex {{ display:flex; gap:12px; flex-wrap:wrap; }}
    .mb-col {{ flex:1; min-width:320px; }}
    .mb-news-grid {{ display:flex; gap:10px; flex-wrap:wrap; }}
    .mb-dash-table {{ border-collapse:separate; border-spacing:0 10px; }}
    .mb-dash-col {{ width:50%; vertical-align:top; }}
    .mb-pad-r {{ padding-right:8px; }}
    .mb-pad-l {{ padding-left:8px; }}
    a {{ word-break: break-all; }}
    td, div {{ word-break: keep-all; }}
    @media (max-width: 640px) {{
      body {{ padding: 0 !important; }}
      .mb-wrap {{ padding: 14px 8px !important; }}
      .mb-title {{ font-size: 22px !important; }}
      table[width="720"] {{ width: 100% !important; }}
      td[style*="width:50%"] {{ width:100% !important; }}
      .mb-flex {{ display:block !important; }}
      .mb-col {{ min-width: 0 !important; width:100% !important; margin-top:10px; }}
      .mb-news-grid {{ display:block !important; }}
      .mb-news-grid > div {{ min-width: 0 !important; width:100% !important; margin-top:10px; }}
      .mb-dash-table, .mb-dash-table tbody, .mb-dash-table tr, .mb-dash-col {{
        display:block !important; width:100% !important;
      }}
      .mb-pad-r, .mb-pad-l {{ padding-right:0 !important; padding-left:0 !important; }}
      .mb-dash-col + .mb-dash-col {{ margin-top:10px; }}
      td, div {{ word-break: break-word; }}
    }}
  </style>
</head>
<body style="margin:0;padding:0;background:#f6f7fb;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Noto Sans KR','Apple SD Gothic Neo','Malgun Gothic',Arial,sans-serif;color:#111827;">
  <div style="display:none;max-height:0;overflow:hidden;opacity:0;">한국/미국 증시 + 지표 + 주요 경제뉴스 압축 브리프</div>
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" class="mb-wrap" style="background:#f6f7fb;padding:24px 12px;">
    <tr><td align="center">
      <table role="presentation" width="720" cellpadding="0" cellspacing="0" class="mb-shell" style="width:720px;max-width:100%;">
        <tr><td style="padding:0 0 12px 0;">
          <div style="font-size:13px;color:#6b7280;">📮 Morning Market Brief · All-in-One</div>
          <div class="mb-title" style="font-size:28px;font-weight:900;line-height:1.2;margin:6px 0 0 0;">{escape(today)} {escape(now_time)} KST</div>
          <div style="font-size:13px;color:#6b7280;margin-top:6px;">범위: 전일 미국 마감 + 금일 한국 개장 전 · 공통 1회 생성</div>
        </td></tr>
        <tr><td style="background:#111827;border-radius:18px;padding:18px;border:1px solid #0b1220;color:#ffffff;">
          <div style="font-size:12px;letter-spacing:0.08em;text-transform:uppercase;color:#c7d2fe;">TL;DR (1 Minute)</div>
          <div style="font-size:18px;font-weight:900;line-height:1.45;margin-top:8px;">{tl_dr_html}</div>
          <div style="margin-top:12px;font-size:12px;color:#9ca3af;">* 투자 조언이 아닌 정보 제공 목적입니다.</div>
        </td></tr>
        <tr><td style="height:12px;"></td></tr>
        <tr><td style="background:#ffffff;border-radius:18px;padding:18px;border:1px solid #e5e7eb;">
          <div class="mb-flex" style="display:flex;gap:12px;flex-wrap:wrap;">
            <div class="mb-col" style="flex:1;min-width:320px;">
              <div style="font-size:14px;font-weight:900;margin-bottom:10px;">컨디션 스코어 (0~10)</div>
              {_score_bar("Risk-on", _safe_int_0_10(scores.get("risk_on")), "#22c55e")}
              {_score_bar("Vol", _safe_int_0_10(scores.get("vol")), "#f59e0b")}
              {_score_bar("Rates", _safe_int_0_10(scores.get("rates")), "#ef4444")}
              {_score_bar("USD", _safe_int_0_10(scores.get("usd")), "#60a5fa")}
              <div style="margin-top:10px;font-size:12px;color:#6b7280;line-height:1.6;">해석: 0=극단적 리스크오프 · 10=강한 리스크온</div>
              <div style="margin-top:10px;padding:10px;border-radius:12px;background:#f8fafc;border:1px solid #e2e8f0;font-size:12px;line-height:1.7;color:#334155;">
                <div><b>Risk-on</b>: 위험자산(주식/성장주) 선호 강도</div>
                <div><b>Vol</b>: 변동성/흔들림 강도</div>
                <div><b>Rates</b>: 금리(특히 미국채) 이슈 영향도</div>
                <div><b>USD</b>: 달러/환율 영향도 (한국장 민감도 포함)</div>
              </div>
            </div>
            <div class="mb-col" style="flex:1;min-width:320px;">
              <div style="font-size:14px;font-weight:900;margin-bottom:10px;">오늘의 플레이북 (3 시나리오)</div>
              <div style="padding:12px;border-radius:14px;background:#f9fafb;border:1px solid #eef2f7;font-size:13px;line-height:1.75;">
                <b>눌림:</b> {pb_pull}<br/>
                <b>돌파:</b> {pb_breakout}<br/>
                <b>이탈:</b> {pb_breakdown}
              </div>
              <div style="margin-top:12px;padding:12px;border-radius:14px;background:#fff7ed;border:1px solid #fed7aa;">
                <div style="font-size:13px;font-weight:900;margin-bottom:6px;">⚠️ 오늘의 리스크 트리거</div>
                <div style="font-size:13px;line-height:1.7;">금리·달러 재가속, 이벤트 직후 급반전, 장 초 환율 급변에 유의하세요.</div>
              </div>
            </div>
          </div>
        </td></tr>
        <tr><td style="height:12px;"></td></tr>
        <tr><td style="background:#ffffff;border-radius:18px;padding:18px;border:1px solid #e5e7eb;">
          <div style="font-size:14px;font-weight:900;margin-bottom:12px;">마켓 대시보드 (한/미 + 주요 지표)</div>
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" class="mb-dash-table" style="border-collapse:separate;border-spacing:0 10px;">
            <tr>
              <td class="mb-dash-col mb-pad-r" style="width:50%;vertical-align:top;padding-right:8px;">
                <div style="padding:12px;border-radius:14px;background:#f9fafb;border:1px solid #eef2f7;">
                  <div style="font-size:13px;font-weight:900;margin-bottom:8px;">🇺🇸 미국 (전일 마감)</div>
                  <div style="font-size:13px;line-height:1.7;">
                    S&P 500: <b>{line_for_html("^GSPC")}</b><br/>
                    NASDAQ: <b>{line_for_html("^IXIC")}</b><br/>
                    DOW: <b>{line_for_html("^DJI")}</b><br/>
                    <b>한 줄 코멘트:</b> {escape(us_dash)}
                  </div>
                </div>
              </td>
              <td class="mb-dash-col mb-pad-l" style="width:50%;vertical-align:top;padding-left:8px;">
                <div style="padding:12px;border-radius:14px;background:#f9fafb;border:1px solid #eef2f7;">
                  <div style="font-size:13px;font-weight:900;margin-bottom:8px;">🇰🇷 한국 (개장 전 프리뷰)</div>
                  <div style="font-size:13px;line-height:1.7;">
                    KOSPI: <b>{line_for_html("^KS11")}</b><br/>
                    KOSDAQ: <b>{line_for_html("^KQ11")}</b><br/>
                    원/달러: <b>{line_for_html("KRW=X")}</b><br/>
                    <b>한 줄 코멘트:</b> {escape(kr_dash)}
                  </div>
                </div>
              </td>
            </tr>
          </table>
          <div style="padding:12px;border-radius:14px;background:#f9fafb;border:1px solid #eef2f7;">
            <div style="font-size:13px;font-weight:900;margin-bottom:8px;">📌 핵심 지표</div>
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="font-size:13px;line-height:1.7;">
              <tr>{indicator_lines[0]}{indicator_lines[1]}</tr>
              <tr>{indicator_lines[2]}{indicator_lines[3]}</tr>
            </table>
          </div>
        </td></tr>
        <tr><td style="height:12px;"></td></tr>
        <tr><td style="background:#ffffff;border-radius:18px;padding:18px;border:1px solid #e5e7eb;">
          <div style="font-size:14px;font-weight:900;margin-bottom:10px;">전일 주요 뉴스 요약 (헤드라인 압축)</div>
          <div style="font-size:12px;color:#6b7280;margin-bottom:12px;">Naver Search News 헤드라인/설명/링크 기반 압축 요약</div>
          <div class="mb-news-grid" style="display:flex;gap:10px;flex-wrap:wrap;">
            {bucket_html("🧠 매크로/정책", "macro")}
            {bucket_html("🇺🇸 미국 주식 뉴스", "us")}
            {bucket_html("🇰🇷 한국 주식 뉴스", "kr")}
            {bucket_html("🌍 환율/원자재/지정학", "fx")}
          </div>
          <div style="margin-top:12px;padding:12px;border-radius:14px;background:#ecfeff;border:1px solid #cffafe;">
            <div style="font-size:13px;font-weight:900;margin-bottom:6px;">✅ 오늘 한 줄 실행 메모</div>
            <div style="font-size:13px;line-height:1.7;">{escape(exec_note or '뉴스는 참고, 금리·달러·지수의 가격 반응을 먼저 확인하세요.')}</div>
          </div>
        </td></tr>
        <tr><td style="height:14px;"></td></tr>
        <tr><td style="padding:0 6px;">
          <div style="font-size:12px;color:#6b7280;line-height:1.6;">자동 생성 요약은 오류/누락이 있을 수 있습니다. 원문/지표는 공식 출처를 확인하세요.</div>
          <div style="margin-top:10px;font-size:11px;color:#9ca3af;">© 2026 Morning Market Brief · Generated on NAS</div>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""


def get_or_create_market_brief_report(
    conn: sqlite3.Connection,
    today: str,
    now_time: str,
    market_rows: list[dict[str, Any]],
    naver_client_id: str,
    naver_client_secret: str,
) -> str:
    token = build_market_brief_token(today)
    existing = load_chat_report_html(conn, token)
    if existing:
        return token

    headlines = collect_market_brief_headlines(naver_client_id, naver_client_secret, per_bucket_limit=20)
    indicators_payload = build_market_brief_indicators_payload(market_rows)
    brief_json = openai_market_brief_json(headlines, indicators_payload) or fallback_market_brief_json(headlines, indicators_payload)
    html = render_market_brief_html(today, now_time, brief_json, market_rows)
    save_chat_report(conn, GLOBAL_BRIEF_CHAT_ID, token, html)
    return token


def build_economy_brief_token(date_kst: str) -> str:
    return f"economy-brief-{ECONOMY_BRIEF_TEMPLATE_VERSION}-{date_kst}"


def _is_naver_report_date_today(report_date: str, today_ymd: str) -> bool:
    s = str(report_date or "").strip()
    if not s or not today_ymd:
        return False
    try:
        if re.fullmatch(r"\d{2}\.\d{2}\.\d{2}", s):
            yy, mm, dd = [int(x) for x in s.split(".")]
            target = dt.date(2000 + yy, mm, dd)
            return target.strftime("%Y-%m-%d") == today_ymd
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
            return s == today_ymd
    except Exception:
        return False
    return False


def _fallback_economy_report_digest(reports: list[dict[str, str]]) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    topic_hints: list[str] = []
    for r in reports[:8]:
        title = str(r.get("title") or "").strip()
        source = str(r.get("source") or "").strip()
        date = str(r.get("date") or "").strip()
        excerpt = str(r.get("pdf_excerpt") or "").strip()
        pdf_link = str(r.get("pdf_link") or "").strip()
        detail_link = str(r.get("detail_link") or "").strip()

        # First 1-2 sentences worth of text for a deterministic local summary fallback.
        cleaned = re.sub(r"\s+", " ", excerpt).strip()
        summary = cleaned[:220].rstrip()
        if len(cleaned) > 220:
            summary += "…"
        if not summary:
            summary = "PDF 본문 추출이 비어 제목/기관 기준으로만 정리했습니다."

        study_points: list[str] = []
        for kw in ["관세", "금리", "물가", "고용", "환율", "성장", "GDP", "CPI", "PCE", "연준", "한국은행"]:
            if kw.lower() in title.lower() or kw.lower() in cleaned.lower():
                study_points.append(f"{kw} 관련 흐름 확인")
            if len(study_points) >= 2:
                break
        if not study_points:
            study_points = ["핵심 주장과 근거 분리", "정책/지표 영향 경로 확인"]

        items.append(
            {
                "title": title,
                "source": source,
                "date": date,
                "summary": summary,
                "study_points": study_points,
                "pdf_link": pdf_link,
                "detail_link": detail_link,
                "link": pdf_link or detail_link,
            }
        )
        if title:
            topic_hints.append(title)

    overview = ""
    if items:
        srcs = sorted({str(it.get("source") or "").strip() for it in items if str(it.get("source") or "").strip()})
        overview = f"오늘 등록 리포트 {len(items)}건을 제목/PDF 발췌 기준으로 정리했습니다."
        if srcs:
            overview += f" 주요 발행기관: {', '.join(srcs[:5])}"
        overview += " 공통 주제와 숫자/정책 키워드를 중심으로 원문 확인을 권장합니다."

    return {"overview": overview, "items": items, "study_guide": ["제목 → 핵심 주장 → 근거지표 순서로 읽기", "중복 주제는 기관별 시각 차이 비교", "수치/정책 일정은 원문 PDF에서 재확인"]}


def render_economy_brief_html(today: str, now_time: str, digest: dict[str, Any], reports: list[dict[str, str]]) -> str:
    overview = escape(str(digest.get("overview", "")).strip()) if isinstance(digest, dict) else ""
    items = digest.get("items") if isinstance(digest, dict) and isinstance(digest.get("items"), list) else []
    lis = []
    for i, it in enumerate(items[:10], start=1):
        if not isinstance(it, dict):
            continue
        title = escape(str(it.get("title") or "(제목 없음)").strip())
        source = escape(str(it.get("source") or "").strip())
        date = escape(str(it.get("date") or "").strip())
        summary = escape(str(it.get("summary") or "").strip())
        link = str(it.get("pdf_link") or it.get("detail_link") or it.get("link") or "").strip()
        meta = " · ".join([x for x in [source, date] if x])
        h = f"<li><b>{i}. {title}</b>"
        if meta:
            h += f" <span style='color:#6b7280;'>({meta})</span>"
        if summary:
            h += f"<div style='margin-top:4px;'>{summary}</div>"
        if link:
            h += f"<div style='margin-top:4px;'><a href='{escape(link)}' target='_blank' rel='noopener noreferrer'>원문 링크</a></div>"
        h += "</li>"
        lis.append(h)
    if not lis:
        for i, r in enumerate(reports[:10], start=1):
            title = escape(str(r.get("title", "")))
            source = escape(str(r.get("source", "")))
            date = escape(str(r.get("date", "")))
            link = str(r.get("pdf_link") or r.get("detail_link") or "").strip()
            lis.append(f"<li><b>{i}. {title}</b> <span style='color:#6b7280;'>({source} · {date})</span>" + (f"<div><a href='{escape(link)}' target='_blank' rel='noopener noreferrer'>원문 링크</a></div>" if link else "") + "</li>")
    if not lis:
        lis.append("<li>금일 등록된 경제분석 리포트가 아직 없습니다.</li>")

    return f"""<!doctype html><html lang='ko'><head><meta charset='utf-8'/><meta name='viewport' content='width=device-width,initial-scale=1'/><title>경제분석 리포트 브리프</title></head>
<body style='margin:0;padding:20px;background:#f6f7fb;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Noto Sans KR,sans-serif;color:#111827;'>
<div style='max-width:780px;margin:0 auto;background:#fff;border:1px solid #e5e7eb;border-radius:16px;padding:18px;'>
<div style='font-size:13px;color:#6b7280;'>📘 Economy Report Brief</div>
<div style='font-size:26px;font-weight:900;margin-top:6px;'>{escape(today)} {escape(now_time)} KST</div>
<div style='font-size:13px;color:#6b7280;margin-top:4px;'>생성시각: 15시 이후 1회 · 발송시각: 16시 이후 1회</div>
<div style='margin-top:14px;padding:12px;border-radius:12px;background:#f8fafc;border:1px solid #e2e8f0;line-height:1.7;'>{overview or '요약 없음'}</div>
<div style='margin-top:14px;font-weight:900;'>리포트 목록 (최대 10개)</div>
<ul style='line-height:1.7;padding-left:18px;'>{''.join(lis)}</ul>
</div></body></html>"""


def get_or_create_economy_brief_report(conn: sqlite3.Connection, today: str, now_time: str) -> str:
    token = build_economy_brief_token(today)
    existing = load_chat_report_html(conn, token)
    if existing:
        return token
    reports = fetch_naver_economy_reports(limit=5)
    reports = [r for r in reports if _is_naver_report_date_today(str(r.get("date") or ""), today)]
    if not reports:
        LOG.info("economy brief skipped: no reports for %s", today)
        return ""
    digest = openai_economy_report_digest(reports) if reports else {}
    has_items = isinstance(digest, dict) and isinstance(digest.get("items"), list) and len(digest.get("items") or []) > 0
    has_overview = isinstance(digest, dict) and bool(str(digest.get("overview") or "").strip())
    if not (has_overview or has_items):
        digest = _fallback_economy_report_digest(reports)
    html = render_economy_brief_html(today, now_time, digest if isinstance(digest, dict) else {}, reports)
    save_chat_report(conn, GLOBAL_BRIEF_CHAT_ID, token, html)
    return token


def _candidate_payload_rows(rows: list[dict[str, Any]], limit: int = 6) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in rows[:limit]:
        ma20 = r.get("ma20")
        ma60 = r.get("ma60")
        ma_trend = ""
        try:
            if ma20 is not None and ma60 is not None:
                m20 = float(ma20)
                m60 = float(ma60)
                if m20 > m60:
                    ma_trend = "up"
                elif m20 < m60:
                    ma_trend = "down"
                else:
                    ma_trend = "flat"
        except Exception:
            ma_trend = ""
        out.append(
            {
                "symbol": str(r.get("symbol", "")),
                "name": str(r.get("name", "")),
                "ccy": str(r.get("ccy", "")),
                "price": r.get("price"),
                "changePct": r.get("changePct"),
                "rsi14": r.get("rsi14"),
                "stochRsi14": r.get("stochRsi14"),
                "macd": r.get("macd"),
                "macdSignal": r.get("macdSignal"),
                "adx14": r.get("adx14"),
                "volumeRatio20": r.get("volumeRatio20"),
                "ma20": ma20,
                "ma60": ma60,
                "ma20DiffPct": r.get("ma20DiffPct"),
                "ma60DiffPct": r.get("ma60DiffPct"),
                "maTrend": ma_trend,
                "atr14Pct": r.get("atr14Pct"),
                "high52wDrawdownPct": r.get("high52wDrawdownPct"),
                "bbText": str(r.get("bbText", "")),
                "score": int(r.get("score", 0)),
                "max": int(r.get("max", SCORE_MAX_INDICATORS)),
                "candidateClass": str(r.get("candidateClass", "NONE")),
            }
        )
    return out


def _candidate_brief_market_rows(market_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in market_rows:
        sym = str(r.get("symbol", ""))
        if sym not in {"^KS11", "^KQ11", "^IXIC", "^GSPC", "^DJI", "KRW=X", "^TNX", "CL=F", "GC=F"}:
            continue
        out.append(
            {
                "symbol": sym,
                "name": str(r.get("name", "")),
                "price": r.get("price"),
                "changePct": r.get("change_pct"),
            }
        )
    return sorted(out, key=lambda x: str(x.get("symbol", "")))


def _candidate_brief_cache_key(
    market_rows: list[dict[str, Any]],
    buy_candidates: list[dict[str, Any]],
    sell_candidates: list[dict[str, Any]],
    watch_candidates: list[dict[str, Any]],
) -> str:
    key_payload = {
        "market": _candidate_brief_market_rows(market_rows),
        "buy": _candidate_payload_rows(
            sorted(buy_candidates, key=lambda r: (str(r.get("symbol", "")), str(r.get("name", "")))),
            LLM_CANDIDATE_TOP_N,
        ),
        "sell": _candidate_payload_rows(
            sorted(sell_candidates, key=lambda r: (str(r.get("symbol", "")), str(r.get("name", "")))),
            LLM_CANDIDATE_TOP_N,
        ),
        "watch": _candidate_payload_rows(
            sorted(watch_candidates, key=lambda r: (str(r.get("symbol", "")), str(r.get("name", "")))),
            LLM_CANDIDATE_TOP_N,
        ),
    }
    normalized = json.dumps(key_payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def openai_candidate_brief_json(
    market_rows: list[dict[str, Any]],
    buy_candidates: list[dict[str, Any]],
    sell_candidates: list[dict[str, Any]],
    watch_candidates: list[dict[str, Any]],
) -> dict[str, Any] | None:
    api_key = (os.getenv("OPENAI_API_KEY", "") or "").strip()
    if not api_key:
        LOG.info("openai candidate_brief skipped: missing OPENAI_API_KEY")
        return None
    if not buy_candidates and not sell_candidates and not watch_candidates:
        return {"summary": "", "buy": [], "sell": [], "watch": []}

    model = (os.getenv("OPENAI_MODEL", "gpt-5-mini") or "gpt-5-mini").strip()

    market_compact = _candidate_brief_market_rows(market_rows)

    payload = {
        "market": market_compact,
        "candidates": {
            "buy": _candidate_payload_rows(buy_candidates, LLM_CANDIDATE_TOP_N),
            "sell": _candidate_payload_rows(sell_candidates, LLM_CANDIDATE_TOP_N),
            "watch": _candidate_payload_rows(watch_candidates, LLM_CANDIDATE_TOP_N),
        },
        "classification_definition": {
            "BUY": "신규 분할매수 관찰/진입 후보",
            "SELL": "강한 신호, 익절/리스크 관리 고려",
            "WATCH": "과열 주의, 매도 단정 금지",
        },
        "format": {
            "summary": "6~10줄 이내 간결 요약",
            "buy": [{"symbol": "", "name": "", "brief": "", "risk": "", "plan": ""}],
            "sell": [{"symbol": "", "name": "", "brief": "", "risk": "", "plan": ""}],
            "watch": [{"symbol": "", "name": "", "brief": "", "risk": "", "plan": ""}],
        },
        "rules": [
            "Korean",
            "JSON only",
            "Keep summary concise in 6-10 short lines",
            "Mention top candidates only (up to 5 per class)",
            "Each candidate should reflect indicators directly (2-3 short sentences total across brief/risk/plan)",
            "No price prediction, no certainty claims",
            "Use volumeRatio20, MA20/MA60(or maTrend), and atr14Pct when present",
            "Do not invent missing values",
            "Never phrase WATCH as sell recommendation",
        ],
    }

    req_body = {
        "model": model,
        "reasoning": {"effort": "low"},
        "input": [
            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": "Stock daily brief writer. Use only provided data and output JSON only. Classification is fixed: SELL=strong signal for profit-taking/risk management, WATCH=caution only (not a sell call), BUY=observation/entry candidate. Reflect RSI/StochRSI/MACD/MA20-60/Bollinger/score plus volumeRatio20 and atr14Pct when available. Write Korean text values.",
                    }
                ],
            },
            {"role": "user", "content": [{"type": "input_text", "text": json.dumps(payload, ensure_ascii=False)}]},
        ],
        "text": {"format": {"type": "json_object"}},
        "truncation": "auto",
        "max_output_tokens": 1600,
    }

    last_err: Exception | None = None
    for attempt in range(3):
        try:
            LOG.info(
                "openai candidate_brief call attempt=%s model=%s buy_candidates=%s sell_candidates=%s watch_candidates=%s max_output_tokens=%s",
                attempt + 1,
                model,
                len(buy_candidates),
                len(sell_candidates),
                len(watch_candidates),
                req_body.get("max_output_tokens"),
            )
            req = Request(
                "https://api.openai.com/v1/responses",
                data=json.dumps(req_body, ensure_ascii=False).encode("utf-8"),
                method="POST",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            )
            with urlopen(req, timeout=60) as resp:
                raw = resp.read().decode("utf-8", errors="ignore")

            obj = json.loads(raw)
            _log_openai_usage("candidate_brief", obj)

            txt = _openai_extract_json_text(obj).strip()
            if not txt:
                raise RuntimeError("empty OpenAI JSON text")

            parsed = json.loads(txt)
            return parsed if isinstance(parsed, dict) else None

        except HTTPError as e:
            last_err = e
            detail = ""
            try:
                detail = e.read().decode("utf-8", errors="ignore")
            except Exception:
                pass
            LOG.warning(
                "openai candidate_brief HTTPError attempt=%s status=%s detail=%s",
                attempt + 1,
                getattr(e, "code", "?"),
                (detail or str(e))[:1200],
            )
            if getattr(e, "code", 0) in (400, 401, 403, 404):
                break
            time.sleep(min(6.0, (2**attempt) + 0.2))
            continue

        except (URLError, TimeoutError, json.JSONDecodeError, RuntimeError, OSError) as e:
            last_err = e
            LOG.warning("openai candidate_brief error attempt=%s err=%s", attempt + 1, e)
            time.sleep(min(6.0, (2**attempt) + 0.2))
            continue

    if last_err:
        LOG.warning("openai candidate brief failed: %s", last_err)
    return None



def fallback_candidate_brief_json(
    buy_candidates: list[dict[str, Any]],
    sell_candidates: list[dict[str, Any]],
    watch_candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    def _fmt_num(v: Any, digits: int = 1) -> str:
        try:
            if v is None:
                return "N/A"
            return f"{float(v):.{digits}f}"
        except Exception:
            return "N/A"

    def _fmt_pct(v: Any, digits: int = 1) -> str:
        try:
            if v is None:
                return "N/A"
            return f"{float(v) * 100:.{digits}f}%"
        except Exception:
            return "N/A"

    def one_line(r: dict[str, Any], side: str) -> dict[str, str]:
        name = str(r.get("name", ""))
        sym = str(r.get("symbol", ""))
        rsi = r.get("rsi14")
        adx = r.get("adx14")
        macd = r.get("macd")
        sig = r.get("macdSignal")
        score = int(r.get("score", 0))
        maxv = int(r.get("max", 8))
        bb = str(r.get("bbText", ""))
        vol20 = r.get("volumeRatio20")
        ma20 = r.get("ma20")
        ma60 = r.get("ma60")
        atrp = r.get("atr14Pct")
        trend = "?? ??" if (macd is not None and sig is not None and macd > sig) else "?? ??"
        ma_bias = "MA N/A"
        try:
            if ma20 is not None and ma60 is not None:
                if float(ma20) > float(ma60):
                    ma_bias = "MA20>MA60"
                elif float(ma20) < float(ma60):
                    ma_bias = "MA20<MA60"
                else:
                    ma_bias = "MA20=MA60"
        except Exception:
            ma_bias = "MA N/A"
        brief = (
            f"?? {score}/{maxv}, RSI {f'{float(rsi):.1f}' if rsi is not None else 'N/A'}, ADX {f'{float(adx):.1f}' if adx is not None else 'N/A'}, "
            f"Vol(20D) {_fmt_num(vol20, 2)}x, {ma_bias}, ATR {_fmt_pct(atrp, 2)} ???? {trend} ?? ??."
        )
        if side == "buy":
            risk = f"변동성(ATR {_fmt_pct(atrp, 2)})와 거래량({_fmt_num(vol20, 2)}x) 과열 여부를 같이 확인"
            plan = f"신규 분할매수 관찰/진입 후보로 관리 (Vol/ATR 체크) | {bb}"
        elif side == "watch":
            risk = f"과열 구간 신호일 수 있으나 단일 지표일 가능성도 있어 추격 진입은 보수적으로"
            plan = f"매도 단정 대신 과열 완화/추세 확인 중심으로 관찰 | {bb}"
        else:
            risk = f"모멘텀 약화 또는 추세 약세 동반 여부를 우선 점검 ({ma_bias})"
            plan = f"익절/리스크 관리 관점에서 비중 점검 또는 기준 이탈 관리 | {bb}"
        return {"symbol": sym, "name": name, "brief": brief, "risk": risk, "plan": plan}

    return {
        "summary": "?? ??? RSI/MACD/ADX? ?? Vol(20D), MA20/60, ATR%? ?? ??? ?????? ??? ?? ???.",
        "buy": [one_line(r, "buy") for r in buy_candidates[:6]],
        "sell": [one_line(r, "sell") for r in sell_candidates[:6]],
        "watch": [one_line(r, "watch") for r in watch_candidates[:6]],
    }



def _fmt_brief_num(v: Any, digits: int = 1) -> str:
    try:
        if v is None:
            return "N/A"
        return f"{float(v):.{digits}f}"
    except Exception:
        return "N/A"


def _fmt_brief_pct(v: Any, digits: int = 1) -> str:
    try:
        if v is None:
            return "N/A"
        return f"{float(v) * 100:.{digits}f}%"
    except Exception:
        return "N/A"


def _fallback_symbol_brief_item(r: dict[str, Any], side: str) -> dict[str, str]:
    name = str(r.get("name", ""))
    sym = str(r.get("symbol", ""))
    rsi = r.get("rsi14")
    adx = r.get("adx14")
    macd = r.get("macd")
    sig = r.get("macdSignal")
    score = int(r.get("score", 0))
    maxv = int(r.get("max", SCORE_MAX_INDICATORS))
    bb = str(r.get("bbText", ""))
    vol20 = r.get("volumeRatio20")
    ma20 = r.get("ma20")
    ma60 = r.get("ma60")
    atrp = r.get("atr14Pct")
    trend = "상승 우위" if (macd is not None and sig is not None and macd > sig) else "하락 우위"
    ma_bias = "MA N/A"
    try:
        if ma20 is not None and ma60 is not None:
            if float(ma20) > float(ma60):
                ma_bias = "MA20>MA60"
            elif float(ma20) < float(ma60):
                ma_bias = "MA20<MA60"
            else:
                ma_bias = "MA20=MA60"
    except Exception:
        ma_bias = "MA N/A"
    brief = (
        f"점수 {score}/{maxv}, RSI {f'{float(rsi):.1f}' if rsi is not None else 'N/A'}, ADX {f'{float(adx):.1f}' if adx is not None else 'N/A'}, "
        f"Vol(20D) {_fmt_brief_num(vol20, 2)}x, {ma_bias}, ATR {_fmt_brief_pct(atrp, 2)} 기준으로 {trend} 신호입니다."
    )
    if side == "buy":
        risk = f"변동성(ATR {_fmt_brief_pct(atrp, 2)})과 거래량({_fmt_brief_num(vol20, 2)}x) 과열 여부를 같이 확인"
        plan = f"신규 분할매수 관찰/진입 후보로 관리 (Vol/ATR 체크) | {bb}"
    elif side == "watch":
        risk = "과열 구간 신호일 수 있으나 단일 지표일 가능성도 있어 추격 진입은 보수적으로"
        plan = f"매도 단정 대신 과열 완화/추세 확인 중심으로 관찰 | {bb}"
    else:
        risk = f"모멘텀 약화 또는 추세 약세 동반 여부를 우선 점검 ({ma_bias})"
        plan = f"익절/리스크 관리 관점에서 비중 점검 또는 기준 이탈 관리 | {bb}"
    return {"symbol": sym, "name": name, "brief": brief, "risk": risk, "plan": plan}


def _symbol_brief_cache_key(market_rows: list[dict[str, Any]], row: dict[str, Any], side: str) -> str:
    payload = {
        "market": _candidate_brief_market_rows(market_rows),
        "side": side,
        "row": _candidate_payload_rows([row], 1)[0] if row else {},
    }
    normalized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return "sym:v1:" + hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def openai_symbol_brief_json(market_rows: list[dict[str, Any]], row: dict[str, Any], side: str) -> dict[str, str] | None:
    api_key = (os.getenv("OPENAI_API_KEY", "") or "").strip()
    if not api_key:
        return None
    model = (os.getenv("OPENAI_MODEL", "gpt-5-mini") or "gpt-5-mini").strip()
    market_compact = _candidate_brief_market_rows(market_rows)
    candidate_payload = _candidate_payload_rows([row], 1)
    if not candidate_payload:
        return None
    payload = {
        "market": market_compact,
        "candidate_class": side.upper(),
        "candidate": candidate_payload[0],
        "classification_definition": {
            "BUY": "신규 분할매수 관찰/진입 후보",
            "SELL": "강한 신호, 익절/리스크 관리 고려",
            "WATCH": "과열 주의, 매도 단정 금지",
        },
        "format": {"brief": "", "risk": "", "plan": ""},
        "rules": [
            "Korean",
            "JSON only",
            "2-3 short sentences total",
            "No certainty claims, no price prediction",
            "Never phrase WATCH as sell recommendation",
        ],
    }
    req_body = {
        "model": model,
        "reasoning": {"effort": "low"},
        "input": [
            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": "Stock candidate brief writer. Use only provided data. SELL means risk-management signal, WATCH means caution only and must not be described as sell call, BUY means observation/entry candidate. Output JSON only with brief/risk/plan in Korean.",
                    }
                ],
            },
            {"role": "user", "content": [{"type": "input_text", "text": json.dumps(payload, ensure_ascii=False)}]},
        ],
        "text": {"format": {"type": "json_object"}},
        "truncation": "auto",
        "max_output_tokens": 300,
    }
    try:
        req = Request(
            "https://api.openai.com/v1/responses",
            data=json.dumps(req_body, ensure_ascii=False).encode("utf-8"),
            method="POST",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        )
        with urlopen(req, timeout=40) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
        obj = json.loads(raw)
        _log_openai_usage("symbol_brief", obj, extra=f"symbol={row.get('symbol')} class={side}")
        txt = _openai_extract_json_text(obj).strip()
        if not txt:
            return None
        parsed = json.loads(txt)
        if not isinstance(parsed, dict):
            return None
        return {
            "symbol": str(row.get("symbol", "")),
            "name": str(row.get("name", "")),
            "brief": str(parsed.get("brief", "")).strip(),
            "risk": str(parsed.get("risk", "")).strip(),
            "plan": str(parsed.get("plan", "")).strip(),
        }
    except Exception as e:
        LOG.warning("openai symbol_brief failed symbol=%s class=%s err=%s", row.get("symbol"), side, e)
        return None


def get_or_build_symbol_brief_item(
    conn: sqlite3.Connection | None,
    today: str,
    market_rows: list[dict[str, Any]],
    row: dict[str, Any],
    side: str,
    shared_cache: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    key = _symbol_brief_cache_key(market_rows, row, side)
    if shared_cache is not None and key in shared_cache:
        return shared_cache[key]
    if conn is not None:
        cached = load_candidate_brief_cache(conn, today, key)
        if isinstance(cached, dict):
            if shared_cache is not None:
                shared_cache[key] = cached
            return cached
    item = openai_symbol_brief_json(market_rows, row, side) or _fallback_symbol_brief_item(row, side)
    if conn is not None:
        save_candidate_brief_cache(conn, today, key, item)
    if shared_cache is not None:
        shared_cache[key] = item
    return item


def get_or_build_candidate_brief_for_chat(
    state: dict[str, Any],
    today: str,
    market_rows: list[dict[str, Any]],
    report_stock_rows: list[dict[str, Any]],
    conn: sqlite3.Connection | None = None,
    shared_cache: dict[str, dict[str, Any]] | None = None,
) -> tuple[dict[str, Any], bool]:
    cached = state.get("candidate_brief_json")
    if state.get("candidate_brief_date") == today and isinstance(cached, dict):
        return cached, False

    buy_candidates = [r for r in report_stock_rows if bool(r.get("buyCandidate"))][:LLM_CANDIDATE_TOP_N]
    sell_candidates = [r for r in report_stock_rows if bool(r.get("sellCandidate"))][:LLM_CANDIDATE_TOP_N]
    watch_candidates = [r for r in report_stock_rows if bool(r.get("watchCandidate"))][:LLM_CANDIDATE_TOP_N]

    buy_items = [get_or_build_symbol_brief_item(conn, today, market_rows, r, "buy", shared_cache) for r in buy_candidates]
    sell_items = [get_or_build_symbol_brief_item(conn, today, market_rows, r, "sell", shared_cache) for r in sell_candidates]
    watch_items = [get_or_build_symbol_brief_item(conn, today, market_rows, r, "watch", shared_cache) for r in watch_candidates]

    summary_lines = [
        f"BUY {len(buy_candidates)}개 / SELL {len(sell_candidates)}개 / WATCH {len(watch_candidates)}개",
        "SELL은 익절/리스크 관리 관점, WATCH는 과열 주의(매도 아님) 기준으로 정리했습니다.",
    ]
    if buy_items:
        summary_lines.append("BUY 상위: " + ", ".join([str(x.get("name") or x.get("symbol") or "") for x in buy_items[:3]]))
    if sell_items:
        summary_lines.append("SELL 상위: " + ", ".join([str(x.get("name") or x.get("symbol") or "") for x in sell_items[:3]]))
    if watch_items:
        summary_lines.append("WATCH 상위: " + ", ".join([str(x.get("name") or x.get("symbol") or "") for x in watch_items[:3]]))

    brief = {
        "summary": "\n".join(summary_lines),
        "buy": buy_items,
        "sell": sell_items,
        "watch": watch_items,
    }
    state["candidate_brief_date"] = today
    state["candidate_brief_json"] = brief
    return brief, True


def fmt_report_pct(v: float | None) -> str:
    if v is None:
        return "N/A"
    return f"{v * 100:+.2f}%"


def load_market_snapshot_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for it in MORNING_CARD_ITEMS:
        sym = it["symbol"]
        r = conn.execute("SELECT price, prev_close FROM latest_quotes WHERE symbol = ?", (sym,)).fetchone()
        if not r or r[0] is None:
            continue
        price = float(r[0])
        prev = float(r[1]) if r[1] is not None else None
        if it.get("transform") == "tnx":
            price = price / 10.0
            prev = prev / 10.0 if prev is not None else None
        chg = (price - prev) if prev is not None else None
        chg_pct = (price / prev - 1.0) if prev not in (None, 0) else None
        rows.append(
            {
                "symbol": sym,
                "name": it["name"],
                "price": price,
                "change": chg,
                "change_pct": chg_pct,
                "digits": int(it["digits"]),
            }
        )
    return rows


def render_chat_report_html(
    today: str,
    now_time: str,
    buy_list: list[str],
    sell_list: list[str],
    watch_list: list[str],
    universe_suggestions: dict[str, Any] | None,
    stock_rows: list[dict[str, Any]],
    manage_url: str,
    market_rows: list[dict[str, Any]] | None = None,
    candidate_brief: dict[str, Any] | None = None,
    portfolio_diag: dict[str, Any] | None = None,
):
    def fmt_opt(v: Any, digits: int) -> str:
        if v is None:
            return "N/A"
        return f"{float(v):.{digits}f}"

    def fmt_num(v: Any, digits: int = 2) -> str:
        if v is None:
            return "N/A"
        try:
            return f"{float(v):,.{digits}f}"
        except Exception:
            return "N/A"

    def tradingview_symbol(sym: str) -> str:
        s = str(sym or "").strip().upper()
        m = re.fullmatch(r"(\d{6})\.(KS|KQ)", s)
        if m:
            return f"KRX:{m.group(1)}"
        return s

    def tradingview_chart_url(sym: str) -> str:
        return "https://www.tradingview.com/chart/?symbol=" + quote(tradingview_symbol(sym), safe="")

    def badge(label: str, tone: str) -> str:
        return f"<span class='pill {tone}'>{escape(label)}</span>"

    def metric_value(text: str, badge_html: str) -> str:
        return f"<div class='metric'><span class='metric-main'>{escape(text)}</span>{badge_html}</div>"

    def change_cell(v: Any) -> str:
        try:
            n = float(v)
        except Exception:
            return "<span class='chg-flat'>N/A</span>"
        cls = "chg-up" if n > 0 else "chg-down" if n < 0 else "chg-flat"
        return f"<span class='{cls}'>{escape(fmt_report_pct(n))}</span>"

    def rsi_badge(v: Any) -> str:
        try:
            n = float(v)
        except Exception:
            return badge("N/A", "p-neutral")
        if n <= 30:
            return badge("매수", "p-buy")
        if n >= 70:
            return badge("매도", "p-sell")
        return badge("중립", "p-neutral")

    def stoch_badge(v: Any) -> str:
        try:
            n = float(v)
        except Exception:
            return badge("N/A", "p-neutral")
        if n < 20:
            return badge("매수", "p-buy")
        if n > 80:
            return badge("과열주의", "p-watch")
        return badge("중립", "p-neutral")

    def macd_badge(macd_v: Any, sig_v: Any) -> str:
        try:
            m = float(macd_v)
            s = float(sig_v)
        except Exception:
            return badge("N/A", "p-neutral")
        return badge("상승" if m > s else "하락", "p-buy" if m > s else "p-sell")

    def adx_badge(v: Any) -> str:
        try:
            n = float(v)
        except Exception:
            return badge("N/A", "p-neutral")
        if n >= 25:
            return badge("추세강", "p-trend-strong")
        if n >= 20:
            return badge("추세형성", "p-trend-build")
        return badge("추세약", "p-trend-weak")

    def high52_badge(v: Any) -> str:
        try:
            n = float(v)
        except Exception:
            return badge("N/A", "p-neutral")
        if n <= -0.2:
            return badge("저평가", "p-buy")
        if n >= -0.05:
            return badge("고점권", "p-sell")
        return badge("중립", "p-neutral")

    def volume_badge(v: Any) -> str:
        try:
            n = float(v)
        except Exception:
            return badge("N/A", "p-neutral")
        if n >= 2.0:
            return badge("급증", "p-trend-strong")
        if n >= 1.2:
            return badge("증가", "p-trend-build")
        if n < 0.8:
            return badge("약함", "p-trend-weak")
        return badge("보통", "p-neutral")

    def ma_badge(price_v: Any, ma20_v: Any, ma60_v: Any) -> str:
        try:
            p = float(price_v)
            m20 = float(ma20_v)
            m60 = float(ma60_v)
        except Exception:
            return badge("N/A", "p-neutral")
        if p >= m20 and m20 >= m60:
            return badge("상승", "p-buy")
        if p < m20 and m20 < m60:
            return badge("하락", "p-sell")
        return badge("혼조", "p-neutral")

    def atr_badge(v: Any) -> str:
        try:
            n = float(v)
        except Exception:
            return badge("N/A", "p-neutral")
        if n <= 0.03:
            return badge("안정", "p-buy")
        if n <= 0.06:
            return badge("보통", "p-neutral")
        return badge("변동성↑", "p-sell")

    def stock_table_rows(rows_in: list[dict[str, Any]]) -> str:
        html_rows = []
        for r in rows_in:
            if bool(r.get("sellCandidate")):
                tone = "sell"
            elif bool(r.get("buyCandidate")):
                tone = "buy"
            elif bool(r.get("watchCandidate")):
                tone = "watch"
            else:
                score = int(r.get("score", 0))
                maxv = max(1, int(r.get("max", SCORE_MAX_INDICATORS)))
                ratio = score / maxv
                tone = "buy" if ratio >= 0.75 else "mid" if ratio >= 0.35 else "sell"
            sym_txt = str(r.get("symbol", ""))
            name_txt = str(r.get("name", ""))
            chart_url = tradingview_chart_url(sym_txt)
            ccy = str(r.get("ccy", "USD"))
            chg_pct = r.get("changePct")
            macd_txt = f"{fmt_opt(r.get('macd'), 2)} / {fmt_opt(r.get('macdSignal'), 2)}"
            vol_ratio_txt = f"{float(r.get('volumeRatio20')):.2f}x" if r.get("volumeRatio20") is not None else "N/A"
            ma_txt = f"{fmt_opt(r.get('ma20'), 1)} / {fmt_opt(r.get('ma60'), 1)}"
            atr_txt = fmt_report_pct(r.get("atr14Pct")) if r.get("atr14Pct") is not None else "N/A"
            html_rows.append(
                f"<tr class='row-{tone}'>"
                f"<td class='sticky-col'><a class='ticker-link' href='{escape(chart_url)}' target='_blank' rel='noopener noreferrer'>{escape(name_txt)}</a><div class='subsym'>{escape(sym_txt)}</div></td>"
                f"<td>{escape(fmt_money(r.get('price'), ccy))}</td>"
                f"<td>{escape(fmt_money(r.get('prevClose'), ccy))}</td>"
                f"<td>{change_cell(chg_pct)}</td>"
                f"<td>{metric_value(fmt_opt(r.get('rsi14'), 1), rsi_badge(r.get('rsi14')))}</td>"
                f"<td>{metric_value(fmt_opt(r.get('stochRsi14'), 3), stoch_badge(r.get('stochRsi14')))}</td>"
                f"<td>{metric_value(macd_txt, macd_badge(r.get('macd'), r.get('macdSignal')))}</td>"
                f"<td>{metric_value(fmt_opt(r.get('adx14'), 1), adx_badge(r.get('adx14')))}</td>"
                f"<td>{metric_value(vol_ratio_txt, volume_badge(r.get('volumeRatio20')))}</td>"
                f"<td>{metric_value(ma_txt, ma_badge(r.get('price'), r.get('ma20'), r.get('ma60')))}</td>"
                f"<td>{metric_value(atr_txt, atr_badge(r.get('atr14Pct')))}</td>"
                f"<td>{metric_value(fmt_report_pct(r.get('high52wDrawdownPct')), high52_badge(r.get('high52wDrawdownPct')))}</td>"
                f"<td class='bb-cell'>{escape(str(r.get('bbText', '')))}</td>"
                f"<td><span class='score-chip'>{int(r.get('score', 0))}/{int(r.get('max', 8))}</span></td>"
                "</tr>"
            )
        return "".join(html_rows)

    market_rows = market_rows or []
    candidate_brief = candidate_brief or {}
    portfolio_diag = portfolio_diag or {}
    groups = [
        ("📈 주요 지수", {"^KS11", "^KQ11", "^N225", "^DJI", "^IXIC", "^GSPC", "000001.SS", "^HSI"}),
        ("🪙 원자재/암호", {"BTC-USD", "GC=F", "CL=F"}),
        ("💱 환율", {"KRW=X"}),
        ("🏦 금리", {"^TNX"}),
    ]
    market_cards: list[str] = []
    for title, syms in groups:
        rows_html = []
        for r in market_rows:
            if str(r.get("symbol")) not in syms:
                continue
            cp = r.get("change_pct")
            tone = "up" if (cp or 0) > 0 else "down" if (cp or 0) < 0 else "flat"
            rows_html.append(
                "<div class='mrow'>"
                f"<div class='mrow-name'>{escape(str(r.get('name', '')))}</div>"
                f"<div class='mrow-val'>{escape(fmt_num(r.get('price'), int(r.get('digits', 2))))}</div>"
                f"<div class='mrow-chg {tone}'>{escape(fmt_report_pct(cp) if cp is not None else 'N/A')}</div>"
                "</div>"
            )
        if not rows_html:
            rows_html = ["<div class='mempty'>데이터 없음</div>"]
        market_cards.append(f"<section class='panel metric-panel'><h3>{escape(title)}</h3><div class='mlist'>{''.join(rows_html)}</div></section>")

    kr_rows = [r for r in stock_rows if str(r.get("ccy", "")) == "KRW"]
    us_rows = [r for r in stock_rows if str(r.get("ccy", "")) != "KRW"]
    manage_html = f"<a class='btn' href='{escape(manage_url)}' target='_blank'>⚙️ 내 종목 관리</a>" if manage_url else ""
    cb_summary = str(candidate_brief.get("summary", "") or "").strip()
    cb_buy = candidate_brief.get("buy") if isinstance(candidate_brief.get("buy"), list) else []
    cb_sell = candidate_brief.get("sell") if isinstance(candidate_brief.get("sell"), list) else []
    cb_watch = candidate_brief.get("watch") if isinstance(candidate_brief.get("watch"), list) else []
    coach_line = str(portfolio_diag.get("coachLine", "") or "").strip()
    risk_top2 = portfolio_diag.get("riskTop2") if isinstance(portfolio_diag.get("riskTop2"), list) else []
    top2_meta_items: list[str] = []
    for it in risk_top2[:2]:
        if not (isinstance(it, (list, tuple)) and len(it) >= 2):
            continue
        nm = str(it[0]).strip()
        try:
            pts = float(it[1])
        except Exception:
            continue
        if nm:
            top2_meta_items.append(f"{nm} ({pts:.0f}p)")
    top2_meta = " / ".join(top2_meta_items)
    atr_avg_pct_val = portfolio_diag.get("atrAvgPct")
    atr_avg_pct_text = "N/A"
    if atr_avg_pct_val is not None:
        try:
            atr_avg_pct_text = f"{float(atr_avg_pct_val):.1f}%"
        except Exception:
            atr_avg_pct_text = "N/A"
    diag_card_html = (
        "<div class='ai-block'><div class='ai-title'>📊 포트폴리오 진단</div><div class='diag-card'>"
        + f"<div class='diag-line'><b>🎯 리스크 집중도</b> 상위2 리스크 집중도: {float(portfolio_diag.get('riskConcentrationPct', 0.0)):.1f}%</div>"
        + f"<div class='diag-line'><b>📉 고점대비 -20% 이상 종목</b> {int(portfolio_diag.get('drawdown20Count', 0))}개 (60일 고점 기준)</div>"
        + f"<div class='diag-line'><b>📈 거래량 급증 종목</b> {int(portfolio_diag.get('volSurgeCount', 0))}개 (≥2.0x)</div>"
        + f"<div class='diag-line'><b>📏 MA20/MA60</b> MA20&gt;MA60 {int(portfolio_diag.get('maUpCount', 0))}개 / MA20&lt;MA60 {int(portfolio_diag.get('maDownCount', 0))}개</div>"
        + f"<div class='diag-line'><b>🌪 ATR% 평균</b> {atr_avg_pct_text} / ATR% 높음(≥5%) {int(portfolio_diag.get('atrHighCount', 0))}개</div>"
        + f"<div class='diag-line'><b>⚠ 위험 점수</b> {int(portfolio_diag.get('riskScore10', 0))}/10</div>"
        + (f"<div class='diag-line'><b>🧩 상위 리스크 기여</b> {escape(top2_meta)}</div>" if top2_meta else "")
        + f"<div class='diag-line'><b>🧠 AI 한 줄 코멘트</b> {escape(coach_line or '코멘트 없음')}</div>"
        + "</div></div>"
    )

    indicator_guide_html = """
<details class='panel guide-panel'>
  <summary class='guide-summary'>📘 지표 읽는 법 (클릭)</summary>
  <div class='guide-grid'>
    <div class='guide-item'><b>RSI</b> 단기 과열/과매도 지표입니다. 보통 70 이상은 과열, 30 이하는 과매도로 참고합니다.</div>
    <div class='guide-item'><b>StochRSI</b> RSI의 민감한 버전입니다. 80 이상 과열, 20 이하 과매도 구간으로 자주 봅니다.</div>
    <div class='guide-item'><b>MACD/Sig</b> 추세 방향 지표입니다. MACD가 Signal 위면 상승 우위, 아래면 하락 우위로 봅니다.</div>
    <div class='guide-item'><b>ADX</b> 추세의 강도를 보여줍니다. 보통 25 이상이면 추세가 강한 편입니다.</div>
    <div class='guide-item'><b>Vol(20D)</b> 오늘 거래량이 최근 20일 평균 대비 몇 배인지입니다. 2.0x 이상이면 거래량 급증입니다.</div>
    <div class='guide-item'><b>MA20/60</b> 20일/60일 이동평균입니다. MA20&gt;MA60이면 상승 흐름, 반대면 약세 흐름 참고값입니다.</div>
    <div class='guide-item'><b>ATR%</b> ATR(14)을 현재가 대비 %로 본 값입니다. 높을수록 변동성이 큽니다.</div>
    <div class='guide-item'><b>52주 고점비</b> 52주 최고점 대비 현재 위치입니다. 낙폭이 클수록 저점권 가능성이 있지만 약세 지속도 함께 봐야 합니다.</div>
    <div class='guide-item'><b>Bollinger</b> 가격의 밴드 위치입니다. 상단 근처는 과열 경계, 하단 근처는 과매도 구간으로 참고합니다.</div>
    <div class='guide-item'><b>Score</b> 여러 지표를 합친 참고 점수입니다. 단독보다 MACD/거래량/추세와 함께 보세요.</div>
  </div>
  <div class='guide-note'>※ 지표는 참고용입니다. 여러 지표가 같은 방향일 때 신뢰도가 상대적으로 높아집니다.</div>
</details>"""

    def brief_cards(items: list[Any], title: str, tone: str) -> str:
        cards = []
        for it in items[:6]:
            if not isinstance(it, dict):
                continue
            nm = str(it.get("name", "")).strip() or str(it.get("symbol", "")).strip()
            sym = str(it.get("symbol", "")).strip()
            brief = str(it.get("brief", "")).strip()
            risk = str(it.get("risk", "")).strip()
            plan = str(it.get("plan", "")).strip()
            cards.append(
                f"<div class='ai-card {tone}'>"
                f"<div class='ai-head'><b>{escape(nm)}</b> <span>{escape(sym)}</span></div>"
                f"<div class='ai-line'><b>브리프</b> {escape(brief or '데이터 없음')}</div>"
                f"<div class='ai-line'><b>리스크</b> {escape(risk or '데이터 없음')}</div>"
                f"<div class='ai-line'><b>플랜</b> {escape(plan or '데이터 없음')}</div>"
                "</div>"
            )
        if not cards:
            return ""
        return f"<div class='ai-block'><div class='ai-title'>{escape(title)}</div><div class='ai-grid'>{''.join(cards)}</div></div>"

    def universe_cards_html() -> str:
        uni = universe_suggestions if isinstance(universe_suggestions, dict) else {}
        rows_map = uni.get("rows") if isinstance(uni.get("rows"), dict) else {}
        summary_map = uni.get("summary") if isinstance(uni.get("summary"), dict) else {}
        title_map = {
            "KOSPI200": "KOSPI200",
            "KOSDAQ100": "KOSDAQ100",
            "NASDAQ_MAJOR": "NASDAQ MAJOR",
            "DOW30": "DOW30",
        }
        blocks: list[str] = []
        for key in ("KOSPI200", "KOSDAQ100", "NASDAQ_MAJOR", "DOW30"):
            lst = rows_map.get(key) if isinstance(rows_map.get(key), list) else []
            if not lst:
                continue
            summary_text = str(summary_map.get(key) or "").strip() or "추천 이유 요약 없음"
            lines: list[str] = []
            for r in lst[:UNIVERSE_TOP_PER_GROUP]:
                sym = str(r.get("symbol", ""))
                nm = str(r.get("name", "") or sym)
                ccy = str(r.get("ccy", "USD"))
                price = fmt_money(r.get("price"), ccy) if r.get("price") is not None else "N/A"
                score_txt = f"{int(r.get('score', 0))}/{int(r.get('max', SCORE_MAX_INDICATORS))}"
                url = tradingview_chart_url(sym) if sym else ""
                name_html = (
                    f"<a class='ticker-link' href='{escape(url)}' target='_blank' rel='noopener noreferrer'>{escape(nm)}</a>"
                    if url
                    else escape(nm)
                )
                lines.append(
                    "<div class='u-item'>"
                    f"<div class='u-name'>{name_html} <span class='u-sym'>{escape(sym)}</span></div>"
                    f"<div class='u-meta'>가격 {escape(price)} · 점수 {escape(score_txt)}</div>"
                    "</div>"
                )
            body = "".join(lines)
            blocks.append(
                "<section class='panel universe-panel'>"
                f"<h3>📌 {escape(title_map[key])}</h3>"
                f"<div class='u-summary'>{escape(summary_text)}</div>"
                f"<div class='u-list'>{body}</div>"
                "</section>"
            )
        if not blocks:
            return (
                "<div class='section-title'>🤖 AI 종목 제안</div>"
                "<section class='panel universe-panel'><div class='u-none'>추천 종목 없음</div></section>"
            )
        return "<div class='section-title'>🤖 AI 종목 제안</div><div class='u-grid'>" + "".join(blocks) + "</div>"
    return f"""<!doctype html>
<html><head><meta charset='utf-8'><title>내 종목 현황</title>
<meta name='viewport' content='width=device-width, initial-scale=1'>
<style>
body{{font-family:Segoe UI,Apple SD Gothic Neo,Malgun Gothic,sans-serif;margin:0;background:linear-gradient(180deg,#f8fbff 0%,#eef4fb 100%);color:#0f172a}}
.wrap{{max-width:1500px;margin:20px auto;padding:0 14px 20px}}
.top{{display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap}}
.title{{font-size:24px;font-weight:700}}
.meta{{color:#475569;font-size:14px}}
.cards{{display:grid;grid-template-columns:repeat(3,minmax(220px,1fr));gap:10px;margin:14px 0}}
.card,.panel{{background:#fff;border:1px solid #dbe3ef;border-radius:14px;padding:12px;box-shadow:0 4px 16px rgba(15,23,42,.04)}}
.card .label{{font-size:12px;color:#64748b}}
.card .val{{font-size:14px;font-weight:700;margin-top:6px;line-height:1.5}}
.btn{{display:inline-block;background:#0f172a;color:#fff;padding:10px 12px;border-radius:10px;text-decoration:none}}
.panel h3{{margin:0 0 10px 0;font-size:15px}}
.metric-panel{{padding:14px}}
.mgrid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:10px}}
.u-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:10px;margin-bottom:12px}}
.universe-panel h3{{margin:0 0 8px 0}}
.u-summary{{font-size:12px;line-height:1.5;color:#334155;background:#f8fafc;border:1px solid #e2e8f0;border-radius:10px;padding:8px}}
.u-list{{margin-top:8px;display:flex;flex-direction:column;gap:8px}}
.u-item{{border:1px solid #e2e8f0;border-radius:10px;padding:8px;background:#fff}}
.u-name{{font-size:13px;font-weight:700;color:#0f172a}}
.u-sym{{font-size:11px;color:#64748b;margin-left:4px}}
.u-meta{{font-size:12px;color:#475569;margin-top:4px}}
.u-none{{font-size:12px;color:#64748b}}
.mlist{{display:flex;flex-direction:column;gap:4px}}
.mrow{{display:grid;grid-template-columns:minmax(0,1.2fr) auto auto;gap:10px;align-items:center;padding:6px 0;border-bottom:1px solid #eef2f7}}
.mrow:last-child{{border-bottom:0}}
.mrow-name{{font-size:13px;color:#334155}}
.mrow-val{{font-size:13px;font-weight:700;color:#111827}}
.mrow-chg{{font-size:12px;font-weight:700;text-align:right}}
.mrow-chg.up{{color:#c81e1e}}
.mrow-chg.down{{color:#1d4ed8}}
.mrow-chg.flat{{color:#64748b}}
.mempty{{font-size:14px;color:#64748b;padding:8px 0}}
.section-title{{font-size:16px;font-weight:700;margin:16px 2px 8px}}
table{{width:100%;border-collapse:collapse;background:#fff;border:1px solid #dbe3ef}}
th,td{{border:1px solid #e2e8f0;padding:8px;font-size:13px;vertical-align:middle;white-space:nowrap}}
th{{background:#f8fafc;position:sticky;top:0;z-index:1}}
.sticky-col{{position:sticky;left:0;z-index:2;background:inherit}}
th.sticky-col{{z-index:3;background:#f8fafc}}
.ticker-link{{color:#0f172a;text-decoration:none;font-weight:600}}
.ticker-link:hover{{text-decoration:underline;color:#0b5bd3}}
.subsym{{font-size:11px;color:#64748b;margin-top:3px}}
.metric{{display:flex;align-items:center;gap:6px;justify-content:flex-start}}
.metric-main{{font-variant-numeric:tabular-nums}}
.pill{{display:inline-flex;align-items:center;padding:2px 8px;border-radius:999px;font-size:11px;font-weight:700;border:1px solid transparent}}
.p-buy{{background:#dcfce7;color:#166534;border-color:#86efac}}
.p-sell{{background:#fee2e2;color:#b91c1c;border-color:#fca5a5}}
.p-watch{{background:#fef3c7;color:#92400e;border-color:#fcd34d}}
.p-neutral{{background:#dbeafe;color:#1d4ed8;border-color:#93c5fd}}
.p-trend-strong{{background:#ede9fe;color:#6d28d9;border-color:#c4b5fd}}
.p-trend-build{{background:#dbeafe;color:#1d4ed8;border-color:#93c5fd}}
.p-trend-weak{{background:#e5e7eb;color:#374151;border-color:#d1d5db}}
.chg-up{{color:#c81e1e;font-weight:700}}
.chg-down{{color:#1d4ed8;font-weight:700}}
.chg-flat{{color:#64748b;font-weight:700}}
.bb-cell{{max-width:260px;white-space:normal;line-height:1.35}}
.score-chip{{display:inline-block;padding:3px 8px;border-radius:999px;background:#f1f5f9;color:#0f172a;font-weight:700}}
.ai-panel{{margin-top:12px}}
.ai-summary{{font-size:13px;line-height:1.6;color:#334155;background:#f8fafc;border:1px solid #e2e8f0;border-radius:12px;padding:10px}}
.ai-block{{margin-top:10px}}
.ai-title{{font-size:13px;font-weight:800;margin-bottom:8px}}
.ai-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:8px}}
.ai-card{{border:1px solid #e2e8f0;border-radius:12px;padding:10px;background:#fff}}
.ai-card.buytone{{background:#f0fdf4;border-color:#bbf7d0}}
.ai-card.selltone{{background:#fef2f2;border-color:#fecaca}}
.ai-card.watchtone{{background:#fffbeb;border-color:#fde68a}}
.diag-card{{border:1px solid #dbeafe;border-radius:12px;padding:10px;background:linear-gradient(180deg,#f8fbff 0%,#eef6ff 100%)}}
.diag-line{{font-size:12px;line-height:1.55;color:#334155;margin-top:4px}}
.diag-line:first-child{{margin-top:0}}
.diag-line b{{color:#0f172a}}
.guide-panel{{margin-top:10px}}
.guide-summary{{cursor:pointer;font-weight:700;font-size:14px;color:#0f172a;list-style:none}}
.guide-summary::-webkit-details-marker{{display:none}}
.guide-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:8px}}
.guide-item{{font-size:12px;line-height:1.55;color:#334155;border:1px solid #e2e8f0;border-radius:10px;padding:9px;background:#fbfdff}}
.guide-item b{{color:#0f172a}}
.guide-note{{margin-top:8px;font-size:12px;color:#64748b}}
.ai-head{{font-size:13px;margin-bottom:6px}}
.ai-head span{{color:#64748b;font-size:12px;margin-left:4px}}
.ai-line{{font-size:12px;line-height:1.55;color:#334155;margin-top:4px}}
.ai-line b{{color:#0f172a}}
.row-buy{{background:#ecfdf5}}
.row-mid{{background:#fff}}
.row-sell{{background:#fef2f2}}
.row-watch{{background:#fffbeb}}
.scroll{{overflow:auto;border-radius:12px}}
@media (max-width: 768px) {{
  .title{{font-size:20px}}
  .cards{{grid-template-columns:1fr}}
  .card .val{{font-size:13px}}
  th,td{{font-size:12px;padding:6px}}
  .metric{{flex-direction:column;align-items:flex-start;gap:4px}}
}}
</style></head>
<body><div class='wrap'>
<div class='top'>
  <div>
    <div class='title'>📊 내 종목 현황</div>
    <div class='meta'>{escape(today)} {escape(now_time)}</div>
  </div>
  {manage_html}
</div>
{universe_cards_html()}
<div class='cards'>
  <div class='card'><div class='label'>매수 후보 리스트</div><div class='val'>{escape(', '.join(buy_list) if buy_list else '없음')}</div></div>
  <div class='card'><div class='label'>매도 후보 리스트</div><div class='val'>{escape(', '.join(sell_list) if sell_list else '없음')}</div></div>
  <div class='card'><div class='label'>과열 주의(WATCH) 리스트</div><div class='val'>{escape(', '.join(watch_list) if watch_list else '없음')}</div></div>
</div>
<div class='panel ai-panel'>
  <h3>🤖 후보 브리핑</h3>
  <div class='ai-summary'>{escape(cb_summary or '후보 종목이 없거나 요약 데이터가 없습니다. 지표 기반 표를 우선 확인하세요.')}</div>
  {diag_card_html}
  {brief_cards(cb_buy, "🟢 매수 후보 브리핑", "buytone")}
  {brief_cards(cb_sell, "🔴 매도 후보 브리핑", "selltone")}
  {brief_cards(cb_watch, "🟡 과열 주의(WATCH) 브리핑", "watchtone")}
</div>
<div class='section-title'>🇰🇷 한국 주식</div>
<div class='scroll'>
<table>
<tr><th class='sticky-col'>종목</th><th>현재가</th><th>전일종가</th><th>등락률</th><th>RSI</th><th>StochRSI</th><th>MACD/Sig</th><th>ADX</th><th>Vol(20D)</th><th>MA20/60</th><th>ATR%</th><th>52주고점비</th><th>Bollinger</th><th>Score</th></tr>
{stock_table_rows(kr_rows) if kr_rows else "<tr><td colspan='14'>표시할 종목이 없습니다.</td></tr>"}
</table>
</div>
<div class='section-title'>🇺🇸 미국 주식</div>
<div class='scroll'>
<table>
<tr><th class='sticky-col'>종목</th><th>현재가</th><th>전일종가</th><th>등락률</th><th>RSI</th><th>StochRSI</th><th>MACD/Sig</th><th>ADX</th><th>Vol(20D)</th><th>MA20/60</th><th>ATR%</th><th>52주고점비</th><th>Bollinger</th><th>Score</th></tr>
{stock_table_rows(us_rows) if us_rows else "<tr><td colspan='14'>표시할 종목이 없습니다.</td></tr>"}
</table>
</div>
{indicator_guide_html}
<div class='mgrid'>{''.join(market_cards)}</div>
</div></body></html>"""


def fetch_usdkrw() -> float | None:
    urls = [
        "https://api.exchangerate.host/latest?base=USD&symbols=KRW",
        "https://open.er-api.com/v6/latest/USD",
    ]
    for u in urls:
        try:
            req = Request(u, headers={"accept": "application/json"})
            with urlopen(req, timeout=20) as resp:
                data = json.loads(resp.read().decode("utf-8", errors="ignore"))
            if "rates" in data and "KRW" in data["rates"]:
                return float(data["rates"]["KRW"])
        except Exception:
            continue
    return None


def _clean_news_text(s: Any, limit: int = 140) -> str:
    text = unescape(str(s or ""))
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) <= limit:
        return text
    return text[: max(1, limit - 1)].rstrip() + "…"


def _parse_naver_pub_date(pub_date: Any) -> str:
    s = str(pub_date or "").strip()
    if not s:
        return ""
    try:
        d = dt.datetime.strptime(s, "%a, %d %b %Y %H:%M:%S %z")
        return d.astimezone(KST).strftime("%m-%d %H:%M")
    except Exception:
        return s


def fetch_naver_news(client_id: str, client_secret: str, query: str, display: int = 5) -> list[dict[str, str]]:
    q = str(query or "").strip()
    if not q or not client_id or not client_secret:
        return []
    url = (
        "https://openapi.naver.com/v1/search/news.json?"
        + urlencode({"query": q, "display": max(1, min(int(display), 20)), "sort": "date"})
    )
    req = Request(
        url,
        headers={
            "X-Naver-Client-Id": client_id,
            "X-Naver-Client-Secret": client_secret,
            "user-agent": "Mozilla/5.0",
            "accept": "application/json",
        },
    )
    with urlopen(req, timeout=20) as resp:
        raw = resp.read().decode("utf-8", errors="ignore")
    obj = json.loads(raw)
    items = obj.get("items", [])
    if not isinstance(items, list):
        return []
    out: list[dict[str, str]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        link = str(it.get("originallink") or it.get("link") or "").strip()
        title = _clean_news_text(it.get("title"), limit=90)
        summary = _clean_news_text(it.get("description"), limit=140)
        if not link or not title:
            continue
        out.append({"title": title, "summary": summary, "link": link, "pub": _parse_naver_pub_date(it.get("pubDate"))})
    return out


def _extract_pdf_text_excerpt(pdf_bytes: bytes, max_pages: int = 8, max_chars: int = 3800) -> str:
    if not pdf_bytes:
        return ""
    text_chunks: list[str] = []
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        for page in reader.pages[: max(1, max_pages)]:
            page_text = str(page.extract_text() or "").strip()
            if page_text:
                text_chunks.append(page_text)
            if sum(len(x) for x in text_chunks) >= max_chars:
                break
    except Exception:
        return ""
    text = "\n".join(text_chunks)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_chars]


def _naver_finance_abs_url(url: str) -> str:
    s = str(url or "").strip()
    if not s:
        return ""
    if s.startswith("http://") or s.startswith("https://"):
        return s
    return urljoin("https://finance.naver.com/research/", s)


def _fetch_naver_report_pdf_info(detail_link: str) -> tuple[str, str]:
    detail_link = _naver_finance_abs_url(detail_link)
    if not detail_link:
        return "", ""
    try:
        req = Request(detail_link, headers={"user-agent": "Mozilla/5.0", "accept": "text/html"})
        with urlopen(req, timeout=25) as resp:
            html = resp.read().decode("euc-kr", errors="ignore")
    except Exception:
        return "", ""

    m = re.search(r'href="([^"]+\.pdf(?:\?[^"]*)?)"', html, flags=re.IGNORECASE)
    if not m:
        return "", ""
    pdf_link = _naver_finance_abs_url(str(m.group(1)).strip())
    if not pdf_link.startswith("http"):
        return "", ""

    try:
        req_pdf = Request(pdf_link, headers={"user-agent": "Mozilla/5.0", "accept": "application/pdf"})
        with urlopen(req_pdf, timeout=35) as resp:
            pdf_bytes = resp.read()
    except Exception:
        return pdf_link, ""
    return pdf_link, _extract_pdf_text_excerpt(pdf_bytes)


def fetch_naver_economy_reports(limit: int = 10) -> list[dict[str, str]]:
    count = max(1, min(int(limit), 10))
    url = "https://finance.naver.com/research/economy_list.naver"
    req = Request(
        url,
        headers={"user-agent": "Mozilla/5.0", "accept": "text/html,application/xhtml+xml"},
    )
    with urlopen(req, timeout=25) as resp:
        html = resp.read().decode("euc-kr", errors="ignore")

    # 네이버 리서치 테이블 구조: 제목 | 증권사 | 첨부 | 작성일 | 조회수
    tr_blocks = re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=re.IGNORECASE | re.DOTALL)
    out: list[dict[str, str]] = []
    for tr in tr_blocks:
        if "<a" not in tr or "pdf" not in tr.lower():
            continue

        title_m = re.search(r'<a[^>]*href="(?P<href>[^"]+)"[^>]*>(?P<title>.*?)</a>', tr, flags=re.IGNORECASE | re.DOTALL)
        if not title_m:
            continue
        detail_link = _naver_finance_abs_url(_clean_news_text(title_m.group("href"), limit=300))
        title = _clean_news_text(title_m.group("title"), limit=120)

        tds = re.findall(r"<td[^>]*>(.*?)</td>", tr, flags=re.IGNORECASE | re.DOTALL)
        cols = [_clean_news_text(td, limit=120) for td in tds]
        source = cols[1] if len(cols) >= 2 else ""
        # 작성일이 보통 4번째 열(예: 26.02.24)
        date = cols[3] if len(cols) >= 4 else ""
        if not re.fullmatch(r"\d{2}\.\d{2}\.\d{2}", date or ""):
            cand = next((c for c in cols if re.fullmatch(r"\d{2}\.\d{2}\.\d{2}", c)), "")
            date = cand

        pdf_link, pdf_excerpt = _fetch_naver_report_pdf_info(detail_link)
        out.append(
            {
                "date": date,
                "source": source,
                "title": title,
                "detail_link": detail_link,
                "pdf_link": pdf_link,
                "pdf_excerpt": _clean_news_text(pdf_excerpt, limit=3500),
            }
        )
        if len(out) >= count:
            break

    return out


def openai_economy_report_digest(reports: list[dict[str, str]]) -> dict[str, Any] | None:
    api_key = (os.getenv("OPENAI_API_KEY", "") or "").strip()
    if not api_key or not reports:
        if not api_key:
            LOG.info("openai economy_digest skipped: missing OPENAI_API_KEY")
        elif not reports:
            LOG.info("openai economy_digest skipped: no reports")
        return None
    llm_reports: list[dict[str, str]] = []
    for r in reports[:8]:
        if not isinstance(r, dict):
            continue
        excerpt = str(r.get("pdf_excerpt") or "").strip()
        excerpt = re.sub(r"\s+", " ", excerpt)
        # Keep only the high-signal front portion to reduce prompt tokens while preserving key context.
        if len(excerpt) > 900:
            excerpt = excerpt[:900].rstrip() + "…"
        llm_reports.append(
            {
                "date": str(r.get("date") or "").strip(),
                "source": str(r.get("source") or "").strip(),
                "title": str(r.get("title") or "").strip(),
                "detail_link": str(r.get("detail_link") or "").strip(),
                "pdf_link": str(r.get("pdf_link") or "").strip(),
                "pdf_excerpt": excerpt,
            }
        )
    payload = {
        "date_kst": fmt_date_kst(now_kst()),
        "reports": llm_reports,
        "output_format": {
            "overview": "오늘의 경제분석 리포트 흐름 요약 3~5문장",
            "items": [{"title": "", "source": "", "date": "", "summary": "", "study_points": ["", "", ""], "link": ""}],
            "study_guide": ["", "", ""],
        },
        "rules": [
            "한국어",
            "JSON object only",
            "items는 최대 8개",
            "각 summary는 2~3문장, 핵심 근거 중심",
            "study_points는 학습 포인트 2개 위주(중복 금지)",
            "입력 목록 외 내용 추정 금지",
            "pdf_excerpt가 비어있으면 제목/기관/날짜만 보수적으로 요약",
            "중요 숫자/정책/지표 키워드가 보이면 우선 반영",
        ],
    }
    req_body = {
        "model": "gpt-5-mini",
        "reasoning": {"effort": "low"},
        "input": [
            {"role": "system", "content": [{"type": "input_text", "text": "거시경제 리서치 튜터. PDF 발췌문 중심으로 학습용 요약을 만들고 JSON만 출력."}]},
            {"role": "user", "content": [{"type": "input_text", "text": json.dumps(payload, ensure_ascii=False)}]},
        ],
        "text": {"format": {"type": "json_object"}},
        "truncation": "auto",
        "max_output_tokens": 1800,
    }
    try:
        total_excerpt_chars = sum(len(str(r.get("pdf_excerpt") or "")) for r in llm_reports)
        LOG.info(
            "openai economy_digest call reports=%s excerpt_chars=%s max_output_tokens=%s",
            len(llm_reports),
            total_excerpt_chars,
            req_body.get("max_output_tokens"),
        )
        req = Request(
            "https://api.openai.com/v1/responses",
            data=json.dumps(req_body, ensure_ascii=False).encode("utf-8"),
            method="POST",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        )
        with urlopen(req, timeout=70) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
        obj = json.loads(raw)
        _log_openai_usage("economy_digest", obj, extra=f"reports={len(llm_reports)}")
        parsed = json.loads(_openai_extract_json_text(obj).strip())
        return parsed if isinstance(parsed, dict) else None
    except Exception as e:
        LOG.warning("openai economy digest failed: %s", e)
        return None


def build_economy_report_digest_message(today: str, now_time: str, reports: list[dict[str, str]]) -> str:
    digest = openai_economy_report_digest(reports)
    lines = [f"📘 네이버 경제분석 리포트 요약 ({today} {now_time})", "━━━━━━━━━━━━━━━"]
    if isinstance(digest, dict):
        overview = str(digest.get("overview", "")).strip()
        if overview:
            lines.extend(["[오늘의 흐름]", overview, "━━━━━━━━━━━━━━━"])
        items = digest.get("items") if isinstance(digest.get("items"), list) else []
        for idx, it in enumerate(items[:10], start=1):
            if not isinstance(it, dict):
                continue
            title = str(it.get("title") or "(제목 없음)").strip()
            source = str(it.get("source") or "").strip()
            date = str(it.get("date") or "").strip()
            summary = str(it.get("summary") or "").strip()
            link = str(it.get("pdf_link") or it.get("detail_link") or "").strip()
            pts = [str(x).strip() for x in (it.get("study_points") or []) if str(x).strip()]
            meta = " · ".join([x for x in [source, date] if x])
            lines.append(f"{idx}. {title}" + (f" ({meta})" if meta else ""))
            if summary:
                lines.append(f"요약: {summary}")
            if pts:
                lines.append("학습포인트: " + " / ".join(pts[:3]))
            if link:
                lines.append(f"링크: {link}")
            lines.append("━━━━━━━━━━━━━━━")
        guide = digest.get("study_guide") if isinstance(digest.get("study_guide"), list) else []
        guides = [str(x).strip() for x in guide if str(x).strip()]
        if guides:
            lines.append("[학습 가이드]")
            for i, g in enumerate(guides[:5], start=1):
                lines.append(f"- {i}) {g}")
            lines.append("━━━━━━━━━━━━━━━")

    if len(lines) <= 2:
        for i, r in enumerate(reports[:10], start=1):
            lines.append(f"{i}. {r.get('title','')} ({r.get('source','')} · {r.get('date','')})")
            lines.append(f"링크: {r.get('pdf_link','') or r.get('detail_link','')}")
            lines.append("━━━━━━━━━━━━━━━")

    return "\n".join(lines[:-1] if lines and lines[-1] == "━━━━━━━━━━━━━━━" else lines)


def build_news_digest_message(today: str, now_time: str, keywords: list[str], items: list[dict[str, str]]) -> str:
    lines = [f"📰 키워드 뉴스 요약 ({today} {now_time})", f"🔎 키워드: {', '.join(keywords)}", "━━━━━━━━━━━━━━━"]
    for i, it in enumerate(items, start=1):
        head = f"{i}. {it.get('title', '')}"
        if it.get("pub"):
            head += f" ({it['pub']})"
        lines.append(head)
        if it.get("summary"):
            lines.append(f"요약: {it['summary']}")
        lines.append(f"링크: {it.get('link', '')}")
        lines.append("━━━━━━━━━━━━━━━")
    return "\n".join(lines[:-1] if len(lines) > 3 else lines)


def send_keyword_news_digest(
    conn: sqlite3.Connection,
    tg: TelegramClient,
    chat_id: str,
    state: dict[str, Any],
    chat_cfg: dict[str, Any],
    today: str,
    now_time: str,
    naver_client_id: str,
    naver_client_secret: str,
) -> bool:
    if not naver_client_id or not naver_client_secret:
        return False
    if state.get("news_digest_date") == today:
        return False

    raw_keywords = chat_cfg.get("news_keywords", [])
    if not isinstance(raw_keywords, list):
        return False

    keywords: list[str] = []
    for x in raw_keywords:
        s = str(x or "").strip()
        if s and s not in keywords:
            keywords.append(s)
    keywords = keywords[:5]
    if not keywords:
        return False

    merged: list[dict[str, str]] = []
    seen_links: set[str] = set()
    for kw in keywords:
        try:
            rows = fetch_naver_news(naver_client_id, naver_client_secret, kw, display=4)
        except Exception as e:
            LOG.warning("naver news fetch failed chat_id=%s keyword=%s err=%s", chat_id, kw, e)
            continue
        for r in rows:
            link = str(r.get("link", "")).strip()
            if not link or link in seen_links:
                continue
            seen_links.add(link)
            merged.append(r)
            if len(merged) >= 8:
                break
        if len(merged) >= 8:
            break

    if not merged:
        return False

    tg.send_text(chat_id, build_news_digest_message(today, now_time, keywords, merged))
    state["news_digest_date"] = today
    save_state(conn, chat_id, state)
    return True


def rsi_signal(v: float | None) -> tuple[str, str]:
    if v is None:
        return "⚪", "RSI 데이터 부족"
    if v <= 30:
        return "🟢", "매수 추천(<=30)"
    if v >= 70:
        return "🔴", "매도 추천(>=70)"
    return "🟡", "중립(30~70)"


def stoch_rsi_signal(v: float | None) -> tuple[str, str]:
    if v is None:
        return "⚪", "데이터 부족"
    if v < 20:
        return "🟢", "매수 후보(<20)"
    if v > 80:
        return "🟡", "과열 주의(>80)"
    return "⚪", "중립(20~80)"


def macd_signal(macd: float | None, signal: float | None) -> tuple[str, str]:
    if macd is None or signal is None:
        return "⚪", "데이터 부족"
    if macd > signal:
        return "🟢", "상승 추세(MACD > Signal)"
    return "🔴", "하락 추세(MACD < Signal)"


def adx_signal(adx: float | None) -> tuple[str, str]:
    if adx is None:
        return "⚪", "데이터 부족"
    if adx >= 25:
        return "🟢", "강한 추세(>=25)"
    if adx >= 20:
        return "🟡", "추세 형성(20~25)"
    return "⚪", "약한 추세(<20)"


def high52w_signal(drawdown: float | None) -> tuple[str, str]:
    if drawdown is None:
        return "⚪", "데이터 부족"
    if drawdown <= -0.2:
        return "🟢", "장기 심리 위축(역발상 후보)"
    if drawdown >= -0.05:
        return "🔴", "52주 고점 근접(과열 주의)"
    return "🟡", "중립 구간"


def bollinger_signal(price: float | None, middle: float | None, upper: float | None, lower: float | None) -> str:
    if price is None or upper is None or lower is None or middle is None:
        return "⚪ Bollinger: 데이터 부족"
    if price <= lower:
        return "🟢 하단 밴드 터치(단기 반등 후보)"
    if price >= upper:
        return "🟡 상단 밴드 터치(과열 주의)"
    return "⚪ 밴드 내부(중립)"


def total_score(
    rsi: float | None,
    stoch: float | None,
    macd: float | None,
    macd_sig: float | None,
    adx: float | None,
    high52w_dd: float | None,
    volume_ratio20: float | None,
    ma20: float | None,
    ma60: float | None,
    price_now: float | None,
    atr14_pct: float | None,
) -> tuple[int, int]:
    s = 0
    if rsi is not None and rsi <= 30:
        s += 1
    if stoch is not None and stoch < 20:
        s += 1
    if macd is not None and macd_sig is not None and macd > macd_sig:
        s += 1
    if adx is not None and adx >= 25:
        s += 1
    if high52w_dd is not None and high52w_dd <= -0.2:
        s += 1
    if volume_ratio20 is not None and volume_ratio20 >= 1.8:
        s += 1
    if (
        ma20 is not None
        and ma60 is not None
        and price_now is not None
        and price_now >= ma20
        and ma20 >= ma60
    ):
        s += 1
    if atr14_pct is not None and 0.0 < atr14_pct <= 0.05:
        s += 1
    return s, SCORE_MAX_INDICATORS


def is_bollinger_upper_touch_or_near(price: float | None, upper: float | None) -> bool:
    if price is None or upper is None:
        return False
    try:
        p = float(price)
        u = float(upper)
    except Exception:
        return False
    if u <= 0:
        return p >= u
    return p >= (u * (1.0 - SELL_BB_UPPER_PROXIMITY_PCT))


def classify_candidates(
    score_ratio: float,
    rsi: float | None,
    stoch: float | None,
    macd: float | None,
    macd_sig: float | None,
    ma20: float | None,
    ma60: float | None,
    is_bb_upper_near: bool,
) -> tuple[bool, bool, bool, str]:
    macd_up = macd is not None and macd_sig is not None and macd > macd_sig
    macd_down = macd is not None and macd_sig is not None and macd < macd_sig
    ma_down = ma20 is not None and ma60 is not None and ma20 < ma60

    is_buy = score_ratio >= BUY_SCORE_RATIO_MIN and (
        macd_up
        or (rsi is not None and rsi <= BUY_RSI_MAX)
        or (stoch is not None and stoch < BUY_STOCH_MAX)
    )

    sell_rule_1 = (rsi is not None and rsi >= SELL_RSI_STRONG) and (stoch is not None and stoch > SELL_STOCH_STRONG)
    sell_rule_2 = is_bb_upper_near and (rsi is not None and rsi >= SELL_RSI_BB)
    sell_rule_3 = (stoch is not None and stoch > SELL_STOCH_STRONG) and macd_down and ma_down
    sell_rule_4 = (score_ratio <= SELL_SCORE_RATIO_FLOOR) and macd_down
    is_sell = sell_rule_1 or sell_rule_2 or sell_rule_3 or sell_rule_4

    watch_single_overheat = (
        (stoch is not None and stoch > SELL_STOCH_STRONG)
        or is_bb_upper_near
        or (rsi is not None and rsi >= SELL_RSI_STRONG)
    )
    is_watch = (not is_sell) and watch_single_overheat

    candidate_class = "SELL" if is_sell else "BUY" if is_buy else "WATCH" if is_watch else "NONE"
    return is_buy, is_sell, is_watch, candidate_class


def score_label(score: int, maxv: int) -> tuple[str, str]:
    ratio = score / maxv if maxv else 0
    if ratio >= 0.75:
        return "🟢", "강매수 구간"
    if ratio >= 0.5:
        return "🟢", "매수 우위"
    if ratio >= 0.25:
        return "🟡", "중립/관망"
    return "🔴", "보수적 접근"


def load_trend_volatility_metrics(conn: sqlite3.Connection, symbol: str) -> dict[str, float | None]:
    rows = conn.execute(
        """
        SELECT high, low, close, volume
        FROM price_bars
        WHERE symbol = ? AND interval = '1d'
        ORDER BY trade_date_local DESC
        LIMIT 90
        """,
        (symbol,),
    ).fetchall()
    base = {
        "ma20": None,
        "ma60": None,
        "ma20DiffPct": None,
        "ma60DiffPct": None,
        "atr14Pct": None,
        "volume": None,
        "avgVolume20": None,
        "volumeRatio20": None,
    }
    if not rows:
        return base

    rev = list(reversed(rows))
    highs = [float(r[0]) if r[0] is not None else None for r in rev]
    lows = [float(r[1]) if r[1] is not None else None for r in rev]
    closes_raw = [float(r[2]) if r[2] is not None else None for r in rev]
    vols = [float(r[3]) if r[3] is not None else None for r in rev]
    closes = [c for c in closes_raw if c is not None]
    if not closes:
        return base

    close_last = closes[-1]
    ma20 = (sum(closes[-20:]) / 20.0) if len(closes) >= 20 else None
    ma60 = (sum(closes[-60:]) / 60.0) if len(closes) >= 60 else None
    ma20_diff = (close_last / ma20 - 1.0) if ma20 else None
    ma60_diff = (close_last / ma60 - 1.0) if ma60 else None

    trs: list[float] = []
    for i in range(1, len(rev)):
        h = highs[i]
        l = lows[i]
        c_prev = closes_raw[i - 1]
        c_cur = closes_raw[i]
        if h is None or l is None or c_prev is None or c_cur is None:
            continue
        tr = max(h - l, abs(h - c_prev), abs(l - c_prev))
        trs.append(float(tr))
    atr14_pct = None
    if len(trs) >= 14 and close_last:
        atr14_pct = (sum(trs[-14:]) / 14.0) / close_last

    vol_last = vols[-1] if vols else None
    vol_avg20 = None
    prior_vols = [v for v in vols[-21:-1] if v is not None] if len(vols) >= 21 else []
    if len(prior_vols) >= 10:
        vol_avg20 = sum(prior_vols) / len(prior_vols)
    else:
        tail_vols = [v for v in vols[-20:] if v is not None]
        if len(tail_vols) >= 10:
            vol_avg20 = sum(tail_vols) / len(tail_vols)
    vol_ratio20 = (vol_last / vol_avg20) if (vol_last is not None and vol_avg20 and vol_avg20 > 0) else None

    base.update(
        {
            "ma20": ma20,
            "ma60": ma60,
            "ma20DiffPct": ma20_diff,
            "ma60DiffPct": ma60_diff,
            "atr14Pct": atr14_pct,
            "volume": vol_last,
            "avgVolume20": vol_avg20,
            "volumeRatio20": vol_ratio20,
        }
    )
    return base


def load_latest_snapshots(conn: sqlite3.Connection, symbols: list[str], lookback: int) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for sym in symbols:
        row = conn.execute(
            """
            SELECT d.symbol, d.trade_date, d.currency, d.close1, d.prev_close, d.prev_close_chg_pct,
                   d.high_n, d.high_drawdown_pct, d.rsi14, d.stoch_rsi14, d.macd, d.macd_signal,
                   d.bb_middle, d.bb_upper, d.bb_lower, d.adx14, d.high52w, d.high52w_drawdown_pct,
                   q.price, q.prev_close as q_prev_close, q.fetched_at_utc, q.market_state
            FROM daily_indicators d
            LEFT JOIN latest_quotes q ON q.symbol = d.symbol
            WHERE d.symbol = ?
            ORDER BY d.trade_date DESC
            LIMIT 1
            """,
            (sym,),
        ).fetchone()
        if not row:
            continue
        ccy = row[2] or currency_of(sym)
        close1 = round_price(row[3], ccy)
        prev_close = round_price(row[4], ccy)
        cur_price = round_price(row[18], ccy) if row[18] is not None else close1
        day_base = prev_close if prev_close else close1
        cur_chg = (cur_price / day_base - 1.0) if cur_price and day_base else None
        out[sym] = {
            "ccy": ccy,
            "asofDate": row[1],
            "close1": close1,
            "prevClose": prev_close,
            "prevCloseChgPct": float(row[5]) if row[5] is not None else None,
            "curPrice": cur_price,
            "curChgPct": cur_chg,
            "highN": round_price(row[6], ccy),
            "highDrawdownPct": float(row[7]) if row[7] is not None else None,
            "rsi14": float(row[8]) if row[8] is not None else None,
            "stochRsi14": float(row[9]) if row[9] is not None else None,
            "macd": float(row[10]) if row[10] is not None else None,
            "macdSignal": float(row[11]) if row[11] is not None else None,
            "bollinger20": {
                "middle": float(row[12]) if row[12] is not None else None,
                "upper": float(row[13]) if row[13] is not None else None,
                "lower": float(row[14]) if row[14] is not None else None,
            },
            "adx14": float(row[15]) if row[15] is not None else None,
            "high52w": round_price(row[16], ccy),
            "high52wDrawdownPct": float(row[17]) if row[17] is not None else None,
            "marketState": row[21] or "",
        }
        out[sym].update(load_trend_volatility_metrics(conn, sym))
    return out


def load_symbol_name_map(conn: sqlite3.Connection, symbols: list[str]) -> dict[str, str]:
    if not symbols:
        return {}
    uniq = sorted({str(s).strip().upper() for s in symbols if str(s).strip()})
    if not uniq:
        return {}
    placeholders = ",".join(["?"] * len(uniq))
    rows = conn.execute(f"SELECT symbol, name FROM ticker_master WHERE symbol IN ({placeholders})", uniq).fetchall()
    out: dict[str, str] = {}
    for r in rows:
        sym = str(r[0] or "").strip().upper()
        nm = str(r[1] or "").strip()
        if sym and nm:
            out[sym] = nm
    return out


def _load_universe_symbols_override() -> dict[str, list[str]]:
    path = (os.getenv("UNIVERSE_SYMBOLS_FILE", "/app/universe_symbols.json") or "").strip()
    if not path:
        return {}
    try:
        p = Path(path)
        if not p.exists():
            return {}
        parsed = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(parsed, dict):
            return {}
        out: dict[str, list[str]] = {}
        for k in ("KOSPI200", "KOSDAQ100", "NASDAQ_MAJOR", "DOW30"):
            arr = parsed.get(k)
            if not isinstance(arr, list):
                continue
            syms: list[str] = []
            for s in arr:
                ss = str(s or "").strip().upper()
                if ss:
                    syms.append(ss)
            if syms:
                out[k] = syms
        return out
    except Exception:
        return {}


def _load_kr_universe_from_ticker_master(conn: sqlite3.Connection, market_token: str, limit: int) -> list[str]:
    rows = conn.execute(
        """
        SELECT symbol
        FROM ticker_master
        WHERE upper(market) LIKE ?
        ORDER BY symbol ASC
        LIMIT ?
        """,
        (f"%{market_token.upper()}%", int(limit)),
    ).fetchall()
    out: list[str] = []
    for r in rows:
        sym = str(r[0] or "").strip().upper()
        if sym:
            out.append(sym)
    return out


def load_universe_symbol_map(conn: sqlite3.Connection) -> dict[str, list[str]]:
    override = _load_universe_symbols_override()
    kospi200 = override.get("KOSPI200") or _load_kr_universe_from_ticker_master(conn, "KOSPI", 200)
    kosdaq100 = override.get("KOSDAQ100") or _load_kr_universe_from_ticker_master(conn, "KOSDAQ", 100)
    nasdaq_major = override.get("NASDAQ_MAJOR") or list(UNIVERSE_NASDAQ_MAJOR_SYMBOLS)
    dow30 = override.get("DOW30") or list(UNIVERSE_DOW30_SYMBOLS)
    return {
        "KOSPI200": kospi200,
        "KOSDAQ100": kosdaq100,
        "NASDAQ_MAJOR": nasdaq_major,
        "DOW30": dow30,
    }


def build_universe_reason_tags(row: dict[str, Any]) -> list[str]:
    tags: list[str] = []
    rsi = row.get("rsi14")
    stoch = row.get("stochRsi14")
    macd = row.get("macd")
    sig = row.get("macdSignal")
    adx = row.get("adx14")
    vol = row.get("volumeRatio20")
    ma20 = row.get("ma20")
    ma60 = row.get("ma60")
    atr = row.get("atr14Pct")
    bb_text = str(row.get("bbText", "") or "")
    try:
        if rsi is not None:
            if float(rsi) <= 30:
                tags.append("RSI 과매도")
            elif float(rsi) >= 70:
                tags.append("RSI 과열")
    except Exception:
        pass
    try:
        if stoch is not None:
            if float(stoch) < 20:
                tags.append("StochRSI 과매도")
            elif float(stoch) > 80:
                tags.append("StochRSI 과열")
    except Exception:
        pass
    try:
        if macd is not None and sig is not None:
            tags.append("MACD 상향" if float(macd) > float(sig) else "MACD 하향")
    except Exception:
        pass
    try:
        if adx is not None:
            tags.append("ADX 추세강" if float(adx) >= 25 else "ADX 추세약")
    except Exception:
        pass
    try:
        if vol is not None:
            tags.append(f"거래량 {float(vol):.2f}x")
    except Exception:
        pass
    try:
        if ma20 is not None and ma60 is not None:
            tags.append("MA20>MA60" if float(ma20) > float(ma60) else "MA20<MA60")
    except Exception:
        pass
    try:
        if atr is not None:
            tags.append(f"ATR {float(atr)*100:.1f}%")
    except Exception:
        pass
    if bb_text:
        if "상단" in bb_text:
            tags.append("볼린저 상단")
        elif "하단" in bb_text:
            tags.append("볼린저 하단")
    uniq: list[str] = []
    for t in tags:
        if t not in uniq:
            uniq.append(t)
        if len(uniq) >= UNIVERSE_REASON_TAG_MAX:
            break
    return uniq


def score_universe_candidates(
    symbol_map: dict[str, list[str]],
    snap_map: dict[str, dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {k: [] for k in symbol_map.keys()}
    for uni, symbols in symbol_map.items():
        rows: list[dict[str, Any]] = []
        for sym in symbols:
            s = snap_map.get(sym)
            if not s:
                continue
            price_now = s.get("curPrice") or s.get("close1")
            bb = s.get("bollinger20", {}) if isinstance(s.get("bollinger20"), dict) else {}
            score, maxv = total_score(
                s.get("rsi14"),
                s.get("stochRsi14"),
                s.get("macd"),
                s.get("macdSignal"),
                s.get("adx14"),
                s.get("high52wDrawdownPct"),
                s.get("volumeRatio20"),
                s.get("ma20"),
                s.get("ma60"),
                price_now,
                s.get("atr14Pct"),
            )
            ratio = (score / maxv) if maxv else 0.0
            if ratio < UNIVERSE_STRONG_BUY_RATIO:
                continue
            ccy = str(s.get("ccy") or currency_of(sym))
            rows.append(
                {
                    "symbol": sym,
                    "name": sym,
                    "ccy": ccy,
                    "price": price_now,
                    "changePct": s.get("curChgPct"),
                    "rsi14": s.get("rsi14"),
                    "stochRsi14": s.get("stochRsi14"),
                    "macd": s.get("macd"),
                    "macdSignal": s.get("macdSignal"),
                    "adx14": s.get("adx14"),
                    "volumeRatio20": s.get("volumeRatio20"),
                    "ma20": s.get("ma20"),
                    "ma60": s.get("ma60"),
                    "ma20DiffPct": s.get("ma20DiffPct"),
                    "ma60DiffPct": s.get("ma60DiffPct"),
                    "atr14Pct": s.get("atr14Pct"),
                    "high52wDrawdownPct": s.get("high52wDrawdownPct"),
                    "bbText": bollinger_signal(price_now, bb.get("middle"), bb.get("upper"), bb.get("lower")),
                    "score": score,
                    "max": maxv,
                    "buyScoreRatio": ratio,
                    "reasonTags": build_universe_reason_tags(
                        {
                            "rsi14": s.get("rsi14"),
                            "stochRsi14": s.get("stochRsi14"),
                            "macd": s.get("macd"),
                            "macdSignal": s.get("macdSignal"),
                            "adx14": s.get("adx14"),
                            "volumeRatio20": s.get("volumeRatio20"),
                            "ma20": s.get("ma20"),
                            "ma60": s.get("ma60"),
                            "atr14Pct": s.get("atr14Pct"),
                            "bbText": bollinger_signal(price_now, bb.get("middle"), bb.get("upper"), bb.get("lower")),
                        }
                    ),
                }
            )
        rows.sort(key=lambda r: (float(r.get("buyScoreRatio") or 0.0), float(r.get("volumeRatio20") or 0.0)), reverse=True)
        out[uni] = rows[:UNIVERSE_TOP_PER_GROUP]
    return out


def fallback_universe_summary(universe_rows: dict[str, list[dict[str, Any]]]) -> dict[str, str]:
    out: dict[str, str] = {}
    for uni, rows in universe_rows.items():
        if not rows:
            out[uni] = "추천 없음"
            continue
        reasons: list[str] = []
        for r in rows[:2]:
            nm = str(r.get("name") or r.get("symbol") or "")
            tags = r.get("reasonTags") if isinstance(r.get("reasonTags"), list) else []
            tags_txt = ", ".join([str(x) for x in tags[:3]]) if tags else "지표 우위"
            reasons.append(f"{nm}: {tags_txt}")
        out[uni] = " | ".join(reasons)
    return out


def openai_universe_summary(
    conn: sqlite3.Connection | None,
    today: str,
    universe_rows: dict[str, list[dict[str, Any]]],
    shared_cache: dict[str, dict[str, Any]] | None = None,
) -> dict[str, str]:
    payload = {
        "universes": {
            uni: [
                {
                    "symbol": str(r.get("symbol", "")),
                    "name": str(r.get("name", "")),
                    "buyScoreRatio": r.get("buyScoreRatio"),
                    "reasonTags": r.get("reasonTags", []),
                }
                for r in rows[:UNIVERSE_TOP_PER_GROUP]
            ]
            for uni, rows in universe_rows.items()
        },
        "rules": [
            "분류/선정 금지 (이미 룰 기반 선정 완료)",
            "유니버스별 1~2줄 한국어 이유 요약",
            "전체 8~12줄 이내",
            "없으면 '추천 없음'으로 표시",
        ],
    }
    key_raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    cache_key = "universe_summary:v1:" + hashlib.sha256(key_raw.encode("utf-8")).hexdigest()
    if shared_cache is not None and cache_key in shared_cache:
        cached_obj = shared_cache[cache_key]
        return {k: str(v) for k, v in (cached_obj if isinstance(cached_obj, dict) else {}).items()}
    if conn is not None:
        db_cached = load_candidate_brief_cache(conn, today, cache_key)
        if isinstance(db_cached, dict):
            if shared_cache is not None:
                shared_cache[cache_key] = db_cached
            return {k: str(v) for k, v in db_cached.items()}

    api_key = (os.getenv("OPENAI_API_KEY", "") or "").strip()
    if not api_key:
        out = fallback_universe_summary(universe_rows)
        if conn is not None:
            save_candidate_brief_cache(conn, today, cache_key, out)
        if shared_cache is not None:
            shared_cache[cache_key] = out
        return out

    model = (os.getenv("OPENAI_MODEL", "gpt-5-mini") or "gpt-5-mini").strip()
    req_body = {
        "model": model,
        "reasoning": {"effort": "low"},
        "input": [
            {
                "role": "system",
                "content": [
                    {
                        "type": "input_text",
                        "text": "너는 주식 유니버스 추천 이유 요약기다. 후보 선정은 이미 끝났고 너는 이유만 짧게 작성한다. JSON만 출력한다.",
                    }
                ],
            },
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": json.dumps(
                            {
                                **payload,
                                "output_format": {
                                    "KOSPI200": "1~2줄",
                                    "KOSDAQ100": "1~2줄",
                                    "NASDAQ_MAJOR": "1~2줄",
                                    "DOW30": "1~2줄",
                                },
                            },
                            ensure_ascii=False,
                        ),
                    }
                ],
            },
        ],
        "text": {"format": {"type": "json_object"}},
        "truncation": "auto",
        "max_output_tokens": 500,
    }
    try:
        req = Request(
            "https://api.openai.com/v1/responses",
            data=json.dumps(req_body, ensure_ascii=False).encode("utf-8"),
            method="POST",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        )
        with urlopen(req, timeout=45) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
        obj = json.loads(raw)
        _log_openai_usage("universe_summary", obj)
        txt = _openai_extract_json_text(obj).strip()
        parsed = json.loads(txt) if txt else {}
        out = {
            "KOSPI200": str(parsed.get("KOSPI200", "")).strip() if isinstance(parsed, dict) else "",
            "KOSDAQ100": str(parsed.get("KOSDAQ100", "")).strip() if isinstance(parsed, dict) else "",
            "NASDAQ_MAJOR": str(parsed.get("NASDAQ_MAJOR", "")).strip() if isinstance(parsed, dict) else "",
            "DOW30": str(parsed.get("DOW30", "")).strip() if isinstance(parsed, dict) else "",
        }
        for k in list(out.keys()):
            if not out[k]:
                out[k] = fallback_universe_summary(universe_rows).get(k, "추천 없음")
        if conn is not None:
            save_candidate_brief_cache(conn, today, cache_key, out)
        if shared_cache is not None:
            shared_cache[cache_key] = out
        return out
    except Exception as e:
        LOG.warning("openai universe_summary failed err=%s", e)
        out = fallback_universe_summary(universe_rows)
        if conn is not None:
            save_candidate_brief_cache(conn, today, cache_key, out)
        if shared_cache is not None:
            shared_cache[cache_key] = out
        return out


def shares_from_cash(cash: float, price: float | None) -> int:
    if price is None or price <= 0:
        return 0
    return int(cash // price)


def run_daily_report(
    conn: sqlite3.Connection,
    tg: TelegramClient,
    cfg: dict[str, Any],
    chat_ids: list[str],
    runtime: RuntimeConfig,
    manage_url: str,
    web_base_url: str,
    naver_client_id: str = "",
    naver_client_secret: str = "",
    prepare_economy_only: bool = False,
):
    now = now_kst()
    today = fmt_date_kst(now)
    now_time = fmt_time_kst(now)
    ym_now = fmt_ym(now)

    monthly_budget = runtime.monthly_budget_krw
    dca_target_krw = int(monthly_budget * runtime.dca_ratio)
    trig_target_krw = int(monthly_budget * runtime.trigger_ratio)
    is_weekend = now.weekday() >= 5

    usdkrw = fetch_usdkrw()
    market_rows_for_report = load_market_snapshot_rows(conn)
    market_brief_token = ""
    economy_brief_token = ""
    minute_now = now.hour * 60 + now.minute
    if web_base_url:
        market_brief_token = get_or_create_market_brief_report(
            conn,
            today,
            now_time,
            market_rows_for_report,
            naver_client_id,
            naver_client_secret,
        )
        if minute_now >= 13 * 60:
            economy_brief_token = get_or_create_economy_brief_report(conn, today, now_time)

    if prepare_economy_only:
        return

    chat_cfgs = {}
    for cid in chat_ids:
        base_cfg = load_chat_config(cfg, cid)
        ov = load_chat_config_override(conn, cid)
        chat_cfgs[cid] = merge_chat_config(base_cfg, ov)
    universe_symbol_map = load_universe_symbol_map(conn)
    universe_symbols = sorted({s for arr in universe_symbol_map.values() for s in arr if s})
    all_symbols = sorted(
        {t["symbol"] for c in chat_cfgs.values() for t in c.get("tickers", []) if isinstance(t, dict) and t.get("symbol")}
        | set(universe_symbols)
    )
    snap_map = load_latest_snapshots(conn, all_symbols, runtime.lookback)
    shared_candidate_brief_cache: dict[str, dict[str, Any]] = {}
    symbol_name_map = load_symbol_name_map(conn, all_symbols)
    universe_rows = score_universe_candidates(universe_symbol_map, snap_map)
    for _, rows in universe_rows.items():
        for r in rows:
            sym = str(r.get("symbol", "")).upper()
            if sym and sym in symbol_name_map:
                r["name"] = symbol_name_map[sym]
    universe_summary = openai_universe_summary(conn, today, universe_rows, shared_candidate_brief_cache)
    universe_suggestions = {"rows": universe_rows, "summary": universe_summary}

    for chat_id in chat_ids:
        state = load_state(conn, chat_id)
        had_report_token = bool(state.get("report_token"))
        report_token = ensure_report_token(state)
        sym_states = state.get("symbols", {})
        if not isinstance(sym_states, dict):
            sym_states = {}

        chat_cfg = chat_cfgs[chat_id]
        tickers = chat_cfg.get("tickers", [])
        oversize_rules = chat_cfg.get("oversize_drop_pct", {}) if isinstance(chat_cfg.get("oversize_drop_pct"), dict) else {}

        has_usd = any(currency_of(str(t.get("symbol", ""))) == "USD" for t in tickers if isinstance(t, dict))

        header = [f"📮 투자 알림 ({today} {now_time})"]
        if has_usd:
            header.append(f"💱 환율: 1 USD ≈ {fmt_money(usdkrw, 'KRW')}" if usdkrw else "💱 환율: 조회 실패(USD 종목 일부 계산 제한)")
        header.append("━━━━━━━━━━━━━━━")

        blocks_kr: list[str] = []
        blocks_us: list[str] = []
        buy_list: list[str] = []
        sell_list: list[str] = []
        watch_list: list[str] = []
        report_stock_rows: list[dict[str, Any]] = []
        warn_drop_5: list[str] = []
        warn_high60_dd25: list[str] = []
        warn_rsi25: list[str] = []
        warn_vol3x: list[str] = []

        dirty = not had_report_token

        for t in tickers:
            if not isinstance(t, dict):
                continue
            sym = str(t.get("symbol", "")).strip().upper()
            if not sym:
                continue
            name = str(t.get("name", sym))
            ccy = currency_of(sym)

            s = snap_map.get(sym)
            if not s:
                fail_block = f"🏷️ 종목명 : {name} ({sym})\n❌ DB 데이터 없음 → 적재 후 재시도"
                report_stock_rows.append(
                    {
                        "symbol": sym,
                        "name": name,
                        "ccy": ccy,
                        "price": None,
                        "prevClose": None,
                        "changePct": None,
                        "highN": None,
                        "highDrawdownPct": None,
                        "rsi14": None,
                        "stochRsi14": None,
                        "macd": None,
                        "macdSignal": None,
                        "adx14": None,
                        "high52wDrawdownPct": None,
                        "bbText": "데이터 없음",
                        "score": 0,
                        "max": SCORE_MAX_INDICATORS,
                        "buyCandidate": False,
                        "sellCandidate": False,
                        "watchCandidate": False,
                        "candidateClass": "NONE",
                    }
                )
                if ccy == "KRW":
                    blocks_kr.extend([fail_block, "━━━━━━━━━━━━━━━"])
                else:
                    blocks_us.extend([fail_block, "━━━━━━━━━━━━━━━"])
                continue

            ref_price = s.get("curPrice") or s.get("close1")
            fx_ok = ccy == "KRW" or (usdkrw is not None and usdkrw > 0)

            if ccy == "KRW":
                monthly_budget_local = monthly_budget
                daily_dca_local = runtime.daily_dca_krw
                dca_target_local = dca_target_krw
                trig_target_local = trig_target_krw
            else:
                monthly_budget_local = monthly_budget / usdkrw if usdkrw else 0
                daily_dca_local = runtime.daily_dca_krw / usdkrw if usdkrw else 0
                dca_target_local = dca_target_krw / usdkrw if usdkrw else 0
                trig_target_local = trig_target_krw / usdkrw if usdkrw else 0

            st = sym_states.get(sym, {}) if isinstance(sym_states.get(sym), dict) else {}
            if st.get("ccy") and st.get("ccy") != ccy:
                st = {}

            if st.get("ym") != ym_now and fx_ok:
                cash = float(st.get("cash", 0) or 0) + monthly_budget_local
                dca_buf = float(st.get("dca_buf", 0) or 0)
                trig_buf = float(st.get("trig_buf", 0) or 0)
                reserve = min(trig_target_local, cash)
                cash -= reserve
                trig_buf += reserve
                st = {
                    "ym": ym_now,
                    "ccy": ccy,
                    "cash": cash,
                    "dca_contrib": 0,
                    "dca_buf": dca_buf,
                    "trig_buf": trig_buf,
                }
                sym_states[sym] = st
                dirty = True

            cash = float(st.get("cash", 0) or 0)
            dca_contrib = float(st.get("dca_contrib", 0) or 0)
            dca_buf = float(st.get("dca_buf", 0) or 0)
            trig_buf = float(st.get("trig_buf", 0) or 0)

            if (not is_weekend) and fx_ok and dca_contrib < dca_target_local and cash > 0:
                add = min(daily_dca_local, dca_target_local - dca_contrib, cash)
                dca_contrib += add
                cash -= add
                dca_buf += add

            dca_qty = shares_from_cash(dca_buf, ref_price)
            dca_cost = dca_qty * ref_price if dca_qty and ref_price else 0
            if dca_qty >= 1:
                dca_buf -= dca_cost

            rule = oversize_rules.get(sym)
            drop_th = float(rule) if rule is not None else DEFAULT_OVERSIZE_DROP_PCT
            cur_chg = s.get("curChgPct")
            is_oversize = (cur_chg is not None) and (cur_chg <= drop_th)

            oversize_reason = "조건 미충족"
            oversize_ok = False
            if (not is_weekend) and fx_ok and is_oversize and ref_price:
                need_cost = runtime.oversize_buy_shares * ref_price
                if trig_buf >= need_cost:
                    trig_buf -= need_cost
                    oversize_ok = True
                    oversize_reason = "조건 충족"
                else:
                    oversize_reason = f"예산 부족({fmt_money(trig_buf, ccy)} < {fmt_money(need_cost, ccy)})"

            new_state = {
                "ym": st.get("ym", ym_now if fx_ok else st.get("ym")),
                "ccy": ccy,
                "cash": cash,
                "dca_contrib": dca_contrib,
                "dca_buf": dca_buf,
                "trig_buf": trig_buf,
            }
            if json.dumps(st, sort_keys=True) != json.dumps(new_state, sort_keys=True):
                sym_states[sym] = new_state
                dirty = True

            rsi = s.get("rsi14")
            stoch = s.get("stochRsi14")
            macd = s.get("macd")
            macd_sig = s.get("macdSignal")
            adx = s.get("adx14")
            high52_dd = s.get("high52wDrawdownPct")
            vol_ratio20 = s.get("volumeRatio20")
            ma20 = s.get("ma20")
            ma60 = s.get("ma60")
            atr14_pct = s.get("atr14Pct")

            s_rsi = rsi_signal(rsi)
            s_stoch = stoch_rsi_signal(stoch)
            s_macd = macd_signal(macd, macd_sig)
            s_adx = adx_signal(adx)
            s_high52 = high52w_signal(high52_dd)
            bb = s.get("bollinger20", {})
            price_now = s.get("curPrice") or ref_price
            bb_upper = bb.get("upper")
            s_bb = bollinger_signal(price_now, bb.get("middle"), bb_upper, bb.get("lower"))
            score, maxv = total_score(
                rsi,
                stoch,
                macd,
                macd_sig,
                adx,
                high52_dd,
                vol_ratio20,
                ma20,
                ma60,
                price_now,
                atr14_pct,
            )
            score_text = score_label(score, maxv)

            score_ratio = (score / maxv) if maxv else 0.0
            is_bb_upper_near = is_bollinger_upper_touch_or_near(price_now, bb_upper)
            is_buy, is_sell, is_watch, candidate_class = classify_candidates(
                score_ratio,
                rsi,
                stoch,
                macd,
                macd_sig,
                ma20,
                ma60,
                is_bb_upper_near,
            )
            if is_sell:
                is_buy = False
                is_watch = False
            if is_buy:
                buy_list.append(f"{name}({sym})")
            if is_sell:
                sell_list.append(f"{name}({sym})")
            if is_watch:
                watch_list.append(f"{name}({sym})")
            warn_label = f"{name}({sym})"
            if cur_chg is not None and float(cur_chg) <= -0.05:
                warn_drop_5.append(warn_label)
            high60_dd = s.get("highDrawdownPct")
            if high60_dd is not None and float(high60_dd) <= -0.25:
                warn_high60_dd25.append(warn_label)
            if rsi is not None and float(rsi) <= 25:
                warn_rsi25.append(warn_label)
            if vol_ratio20 is not None and float(vol_ratio20) >= 3.0:
                warn_vol3x.append(warn_label)
            report_stock_rows.append(
                {
                    "symbol": sym,
                    "name": name,
                    "ccy": ccy,
                    "price": s.get("curPrice") or ref_price,
                    "prevClose": s.get("prevClose") or s.get("close1"),
                    "changePct": s.get("curChgPct"),
                    "highN": s.get("highN"),
                    "highDrawdownPct": s.get("highDrawdownPct"),
                    "rsi14": rsi,
                    "stochRsi14": stoch,
                    "macd": macd,
                    "macdSignal": macd_sig,
                    "adx14": adx,
                    "volumeRatio20": vol_ratio20,
                    "ma20": ma20,
                    "ma60": ma60,
                    "ma20DiffPct": s.get("ma20DiffPct"),
                    "ma60DiffPct": s.get("ma60DiffPct"),
                    "atr14Pct": atr14_pct,
                    "high52wDrawdownPct": high52_dd,
                    "bbText": s_bb,
                    "score": score,
                    "max": maxv,
                    "buyCandidate": is_buy,
                    "sellCandidate": is_sell,
                    "watchCandidate": is_watch,
                    "candidateClass": candidate_class,
                }
            )

            lines = [
                f"🏷️ 종목명 : {name} ({sym})",
                f"💰 현재가: {fmt_money(s.get('curPrice') or ref_price, ccy)} ({fmt_pct_signed(s.get('curChgPct'))})",
                f"🧾 전일종가: {fmt_money(s.get('prevClose') or s.get('close1'), ccy)}",
                f"🏔️ 60일고점: {fmt_money(s.get('highN'), ccy)} (고점비 {fmt_pct_signed(s.get('highDrawdownPct'))})",
                f"📉 RSI(14): {f'{rsi:.1f}' if rsi is not None else 'N/A'}  {s_rsi[0]} {s_rsi[1]}",
                f"📊 Stoch RSI(14): {f'{stoch:.3f}' if stoch is not None else 'N/A'}  {s_stoch[0]} {s_stoch[1]}",
                f"📈 MACD: {f'{macd:.4f}' if macd is not None else 'N/A'} / Signal: {f'{macd_sig:.4f}' if macd_sig is not None else 'N/A'}  {s_macd[0]} {s_macd[1]}",
                f"✔ ADX(14): {f'{adx:.1f}' if adx is not None else 'N/A'}  {s_adx[0]} {s_adx[1]}",
                f"📣 거래량(20일 대비): {f'{vol_ratio20:.2f}x' if vol_ratio20 is not None else 'N/A'}",
                f"📐 MA20/MA60: {(fmt_money(ma20, ccy) if ma20 is not None else 'N/A')} / {(fmt_money(ma60, ccy) if ma60 is not None else 'N/A')}",
                f"📉 ATR%(14): {fmt_pct_signed(atr14_pct, 2) if atr14_pct is not None else 'N/A'}",
                f"✔ 52주 고점비: {fmt_pct_signed(high52_dd)}  {s_high52[0]} {s_high52[1]}",
                f"🎯 Bollinger(20,2): {s_bb}",
            ]

            th_text = f"{drop_th * 100:.1f}%"
            if is_weekend:
                lines.append(f"⚡ 과대낙폭(전일비 <= {th_text}): ⛔ 주말 → 참고만")
            elif not fx_ok:
                lines.append(f"⚡ 과대낙폭(전일비 <= {th_text}): ⚠️ 환율 미확인 → 스킵")
            elif not is_oversize:
                lines.append(f"⚡ 과대낙폭(전일비 <= {th_text}): ❌ 미충족")
            else:
                lines.append(f"⚡ 과대낙폭(전일비 <= {th_text}): ✅ 충족")
                lines.append(
                    f"   └ 📌 추천: {runtime.oversize_buy_shares}주 매수 {'✅' if oversize_ok else '⚠️'} (≈{fmt_money(ref_price, ccy)}) · {oversize_reason}"
                )
            lines.append(f"🌟 종합 스코어: {score} / {maxv}  → {score_text[0]} {score_text[1]}")

            if ccy == "KRW":
                blocks_kr.extend(["\n".join(lines), "━━━━━━━━━━━━━━━"])
            else:
                blocks_us.extend(["\n".join(lines), "━━━━━━━━━━━━━━━"])

        if not blocks_kr:
            blocks_kr.extend(["⚠️ 국내주식: 표시할 종목이 없어요.", "━━━━━━━━━━━━━━━"])
        if not blocks_us:
            blocks_us.extend(["⚠️ 미국주식: 표시할 종목이 없어요.", "━━━━━━━━━━━━━━━"])

        report_url = ""
        if web_base_url:
            candidate_brief_json, candidate_brief_dirty = get_or_build_candidate_brief_for_chat(
                state,
                today,
                market_rows_for_report,
                report_stock_rows,
                conn,
                shared_candidate_brief_cache,
            )
            if candidate_brief_dirty:
                dirty = True
            report_url = f"{web_base_url.rstrip('/')}/report/{report_token}"
            portfolio_diag = compute_portfolio_diagnosis(report_stock_rows)
            portfolio_diag["coachLine"] = get_or_create_portfolio_coach_line(conn, today, portfolio_diag)
            html = render_chat_report_html(
                today,
                now_time,
                buy_list,
                sell_list,
                watch_list,
                universe_suggestions,
                report_stock_rows,
                manage_url,
                market_rows_for_report,
                candidate_brief_json,
                portfolio_diag,
            )
            save_chat_report(conn, chat_id, report_token, html)

        market_brief_url = f"{web_base_url.rstrip('/')}/report/{market_brief_token}" if (web_base_url and market_brief_token) else ""
        economy_brief_url = f"{web_base_url.rstrip('/')}/report/{economy_brief_token}" if (web_base_url and economy_brief_token) else ""
        buttons = build_daily_report_buttons(
            market_brief_url=market_brief_url,
            economy_brief_url=economy_brief_url,
            report_url=report_url,
            manage_url=manage_url,
        )
        short_msg_lines = [
            f"📮 투자 알림 ({today} {now_time})",
            f"🟢 매수 후보(신규 분할매수 관찰/진입): {', '.join(buy_list[:6]) if buy_list else '없음'}",
            f"🔴 매도 후보(익절/리스크 관리 고려): {', '.join(sell_list[:6]) if sell_list else '없음'}",
            f"🟡 과열 주의(WATCH, 매도 아님): {', '.join(watch_list[:6]) if watch_list else '없음'}",
        ]
        if len(buy_list) > 6:
            short_msg_lines.append(f"… 매수 후보 추가 {len(buy_list) - 6}개")
        if len(sell_list) > 6:
            short_msg_lines.append(f"… 매도 후보 추가 {len(sell_list) - 6}개")
        if len(watch_list) > 6:
            short_msg_lines.append(f"… 과열 주의 추가 {len(watch_list) - 6}개")
        if has_usd:
            short_msg_lines.append(f"💱 환율: {fmt_money(usdkrw, 'KRW')}" if usdkrw else "💱 환율: 조회 실패")
        warning_rows: list[tuple[str, list[str]]] = [
            ("-5% 급락", warn_drop_5),
            ("60일 고점 대비 -25% 돌파", warn_high60_dd25),
            ("RSI 25 이하", warn_rsi25),
            ("거래량 3배 이상 급증", warn_vol3x),
        ]
        warning_rows = [(label, items) for label, items in warning_rows if items]
        if warning_rows:
            short_msg_lines.append("────────")
            short_msg_lines.append("🚨 Warning 🚨")
            for label, items in warning_rows:
                shown = ", ".join(items[:4])
                more = f" 외 {len(items) - 4}개" if len(items) > 4 else ""
                short_msg_lines.append(f"• {label} : {shown}{more}")
        short_msg_lines.append("────────")
        short_msg_lines.append("도움이 되셨다면 작은 후원으로 운영에 힘을 보태주세요. 💖")
        short_msg_lines.append("────────")
        short_msg = "\n".join(short_msg_lines)
        if buttons:
            tg.send_text_with_buttons(chat_id, short_msg, buttons)
        else:
            tg.send_text(chat_id, short_msg)

        if minute_now >= 14 * 60 and economy_brief_url and state.get("economy_brief_sent_date") != today:
            tg.send_text_with_buttons(
                chat_id,
                f"📘 경제분석 리포트 ({today} {now_time})\n오늘자 리포트가 준비됐어요. 아래 버튼으로 확인하세요.",
                [{"text": "📘 경제분석 리포트", "url": economy_brief_url}],
            )
            state["economy_brief_sent_date"] = today
            save_state(conn, chat_id, state)

        if dirty:
            state["symbols"] = sym_states
            save_state(conn, chat_id, state)

        # Daily report telegram is intentionally kept short; detailed news is moved to the common market brief HTML.
        # Daily report telegram is intentionally kept short; detailed news is moved to the common market brief HTML.
        # 사용자 키워드 뉴스는 별도 텔레그램 메시지로 추가 발송 (개인화 유지)
        send_keyword_news_digest(
            conn,
            tg,
            chat_id,
            state,
            chat_cfg,
            today,
            now_time,
            naver_client_id,
            naver_client_secret,
        )



def run_universe_scan(conn: sqlite3.Connection):
    now = now_kst()
    today = fmt_date_kst(now)
    symbol_map = load_universe_symbol_map(conn)
    symbols = sorted({s for arr in symbol_map.values() for s in arr if s})
    snap_map = load_latest_snapshots(conn, symbols, 60)
    rows = score_universe_candidates(symbol_map, snap_map)
    name_map = load_symbol_name_map(conn, symbols)
    for _, lst in rows.items():
        for r in lst:
            sym = str(r.get("symbol", "")).upper()
            if sym and sym in name_map:
                r["name"] = name_map[sym]
    summary = openai_universe_summary(conn, today, rows, {})
    LOG.info(
        "universe_scan done date=%s kospi200=%s kosdaq100=%s nasdaq_major=%s dow30=%s",
        today,
        len(rows.get("KOSPI200", [])),
        len(rows.get("KOSDAQ100", [])),
        len(rows.get("NASDAQ_MAJOR", [])),
        len(rows.get("DOW30", [])),
    )
    LOG.info("universe_scan summary_keys=%s", ",".join(sorted(summary.keys())))


def run_morning_card(conn: sqlite3.Connection, tg: TelegramClient, chat_ids: list[str]):
    now = now_kst()
    today = fmt_date_kst(now)
    now_time = fmt_time_kst(now)

    rows = []
    missing = []
    for it in MORNING_CARD_ITEMS:
        sym = it["symbol"]
        r = conn.execute("SELECT price, prev_close FROM latest_quotes WHERE symbol = ?", (sym,)).fetchone()
        if not r or r[0] is None:
            missing.append(it["name"])
            continue
        price = float(r[0])
        prev = float(r[1]) if r[1] is not None else None
        if it.get("transform") == "tnx":
            price = price / 10.0
            prev = prev / 10.0 if prev is not None else None
        chg = (price - prev) if prev is not None else None
        chg_pct = (price / prev - 1.0) if prev not in (None, 0) else None
        rows.append({"name": it["name"], "price": price, "change": chg, "change_pct": chg_pct, "digits": it["digits"]})

    if not rows:
        return

    lines = [f"☀️ 모닝 마켓 브리핑 ({today} {now_time})", "━━━━━━━━━━━━━━━"]
    for r in rows:
        up = "🔺" if (r["change"] or 0) > 0 else "🔽" if (r["change"] or 0) < 0 else "⏺"
        p = f"{r['price']:,.{r['digits']}f}"
        c = "N/A" if r["change"] is None else f"{r['change']:+,.{r['digits']}f}"
        pct = "N/A" if r["change_pct"] is None else f"{r['change_pct']*100:+.2f}%"
        lines.append(f"{up} {r['name']}: {p} ({c}, {pct})")

    if missing:
        lines.append("━━━━━━━━━━━━━━━")
        lines.append("⚠️ 데이터 조회 실패")
        for m in missing:
            lines.append(f"• {m}")

    msg = "\n".join(lines)
    for chat_id in chat_ids:
        state = load_state(conn, chat_id)
        if state.get("morning_card_date") == today:
            continue
        tg.send_text(chat_id, msg)
        state["morning_card_date"] = today
        save_state(conn, chat_id, state)


def load_kospi200_symbols() -> list[str]:
    symbols = set()
    for p in range(1, 31):
        url = f"https://finance.naver.com/sise/entryJongmok.naver?&page={p}"
        req = Request(url, headers={"user-agent": "Mozilla/5.0"})
        with urlopen(req, timeout=20) as resp:
            html = resp.read().decode("euc-kr", errors="ignore")
        codes = re.findall(r"code=(\d{6})", html)
        if not codes:
            break
        for c in codes:
            symbols.add(f"{c}.KS")
    return sorted(symbols)


def load_envelope(conn: sqlite3.Connection, symbol: str, period: int, pct: float) -> dict[str, float] | None:
    rows = conn.execute(
        """
        SELECT close FROM price_bars
        WHERE symbol = ? AND interval = '1d'
        ORDER BY trade_date_local DESC
        LIMIT 250
        """,
        (symbol,),
    ).fetchall()
    closes = [float(r[0]) for r in rows if r[0] is not None]
    if len(closes) < period:
        return None
    w = list(reversed(closes[:period]))
    ma = sum(w) / len(w)
    return {"upper": ma * (1 + pct), "lower": ma * (1 - pct)}


def is_krx_market_hours_kst(now: dt.datetime) -> bool:
    if now.weekday() >= 5:
        return False
    minutes = now.hour * 60 + now.minute
    return 9 * 60 <= minutes <= 15 * 60 + 30


def run_envelope_watch(conn: sqlite3.Connection, tg: TelegramClient, chat_ids: list[str], period: int, pct: float, cfg: dict[str, Any]):
    now = now_kst()
    if not is_krx_market_hours_kst(now):
        return
    today = fmt_date_kst(now)
    now_time = fmt_time_kst(now)

    symbols = load_kospi200_symbols()
    if not symbols:
        return

    quotes = {}
    for sym in symbols:
        r = conn.execute("SELECT price FROM latest_quotes WHERE symbol = ?", (sym,)).fetchone()
        if r and r[0] is not None:
            quotes[sym] = float(r[0])

    env_map = {}
    for sym in symbols:
        e = load_envelope(conn, sym, period, pct)
        if e:
            env_map[sym] = e

    symbol_names: dict[str, str] = {}
    dft = cfg.get("default") if isinstance(cfg.get("default"), dict) else {}
    dft_tickers = dft.get("tickers") if isinstance(dft.get("tickers"), list) else []
    for t in dft_tickers:
        if isinstance(t, dict):
            ss = str(t.get("symbol", "")).strip().upper()
            nn = str(t.get("name", "")).strip()
            if ss and nn:
                symbol_names[ss] = nn
    for chat_id in chat_ids:
        chat_cfg = load_chat_config(cfg, chat_id)
        for t in chat_cfg.get("tickers", []):
            if isinstance(t, dict):
                ss = str(t.get("symbol", "")).strip().upper()
                nn = str(t.get("name", "")).strip()
                if ss and nn and ss not in symbol_names:
                    symbol_names[ss] = nn

    for chat_id in chat_ids:
        state = load_state(conn, chat_id)
        watch_state = state.get("envelope_watch", {}) if isinstance(state.get("envelope_watch"), dict) else {}
        dirty = False
        alerts = []

        for sym in symbols:
            envp = env_map.get(sym)
            price = quotes.get(sym)
            if not envp or price is None:
                continue
            side = "LOWER" if price <= envp["lower"] else "UPPER" if price >= envp["upper"] else "NONE"
            prev = watch_state.get(sym, {}).get("side", "NONE") if isinstance(watch_state.get(sym), dict) else "NONE"
            if side != prev:
                watch_state[sym] = {"side": side, "ts": int(now.timestamp()), "date": today}
                dirty = True
                if side != "NONE":
                    title = "🟢 하단 터치" if side == "LOWER" else "🔴 상단 터치"
                    alerts.append(
                        f"{title} {symbol_names.get(sym, sym)} ({sym})\n현재가: {fmt_money(price, 'KRW')}\nEnvelope({period}, {pct*100:.1f}): 상단 {fmt_money(envp['upper'], 'KRW')} / 하단 {fmt_money(envp['lower'], 'KRW')}"
                    )

        if alerts:
            head = f"🚨 KOSPI200 Envelope 터치 ({today} {now_time})"
            tg.send_text(chat_id, "\n".join([head, "━━━━━━━━━━━━━━━"] + alerts))

        if dirty:
            state["envelope_watch"] = watch_state
            save_state(conn, chat_id, state)


def main():
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    cfg = load_chat_configs(Path(args.symbols_file))
    chat_ids: list[str] = []
    token = args.telegram_token or ""
    if args.task != "universe_scan":
        chat_ids = load_chat_ids(args, cfg)
        if not chat_ids:
            raise RuntimeError("No chat ids found. Set --telegram-chat-ids-json or add chat ids in chat-configs.js")
        if not token:
            raise RuntimeError("--telegram-token (or env wrapper) is required")

    conn = sqlite3.connect(args.db_path)
    try:
        ensure_alert_schema(conn)
        tg = TelegramClient(token) if token else None
        runtime = default_runtime_config()
        web_base = (args.web_base_url or "").strip().rstrip("/")
        manage_url = (args.manage_page_url or "").strip()
        if not manage_url and web_base:
            manage_url = f"{web_base}/manage"

        if args.task == "daily_report":
            run_daily_report(
                conn,
                tg if tg is not None else TelegramClient(token),
                cfg,
                chat_ids,
                runtime,
                manage_url,
                web_base,
                (os.getenv("NAVER_CLIENT_ID", "") or "").strip(),
                (os.getenv("NAVER_CLIENT_SECRET", "") or "").strip(),
                prepare_economy_only=bool(args.prepare_economy_only),
            )
        elif args.task == "morning_card":
            run_morning_card(conn, tg if tg is not None else TelegramClient(token), chat_ids)
        elif args.task == "envelope_watch":
            run_envelope_watch(conn, tg if tg is not None else TelegramClient(token), chat_ids, max(2, int(args.envelope_period)), float(args.envelope_pct), cfg)
        elif args.task == "universe_scan":
            run_universe_scan(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
