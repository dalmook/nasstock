import argparse
import datetime as dt
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
from urllib.parse import quote, urlencode
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo


LOG = logging.getLogger("nas_alert_runner")
KST = ZoneInfo("Asia/Seoul")
DEFAULT_OVERSIZE_DROP_PCT = -0.05
DEFAULT_OVERSIZE_BUY_SHARES = 1
GLOBAL_BRIEF_CHAT_ID = "__MARKET_BRIEF__"
MARKET_BRIEF_TEMPLATE_VERSION = "v2"

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
        rows = [[{"text": b["text"], "url": b["url"]}] for b in buttons if b.get("text") and b.get("url")]
        payload = {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": "true",
            "reply_markup": json.dumps({"inline_keyboard": rows}, ensure_ascii=False),
        }
        self._post("sendMessage", payload)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="NAS-only alert runner")
    p.add_argument("--db-path", default="/data/stock_prices.db")
    p.add_argument("--symbols-file", default="/app/chat-configs.js")
    p.add_argument("--task", choices=["daily_report", "morning_card", "envelope_watch"], required=True)
    p.add_argument("--telegram-token", default="")
    p.add_argument("--telegram-chat-ids-json", default="")
    p.add_argument("--telegram-chat-id", default="")
    p.add_argument("--manage-page-url", default="")
    p.add_argument("--web-base-url", default="")
    p.add_argument("--envelope-period", type=int, default=20)
    p.add_argument("--envelope-pct", type=float, default=0.2)
    p.add_argument("--obs-log-enabled", action="store_true")
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


def openai_market_brief_json(headlines_payload: dict[str, Any], indicators_payload: list[dict[str, Any]]) -> dict[str, Any] | None:
    api_key = (os.getenv("OPENAI_API_KEY", "") or "").strip()
    if not api_key:
        return None

    system_prompt = (
        "한국어 금융 브리프 편집자. 기사 본문 금지. 입력 헤드라인/설명/지표만 사용. "
        "중복 제거·압축하고 JSON만 출력."
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
            "링크는 대표 1~3개",
            "점수는 0~10 정수",
            "불확실하면 보수적 표현",
        ],
    }

    req_body = {
        "model": "gpt-5-mini",
        "reasoning": {"effort": "low"},
        "input": [
            {"role": "system", "content": [{"type": "text", "text": system_prompt}]},
            {"role": "user", "content": [{"type": "text", "text": json.dumps(user_payload, ensure_ascii=False)}]},
        ],
        "text": {"format": {"type": "json_object"}},
        "max_output_tokens": 2200,
    }

    last_err: Exception | None = None
    for attempt in range(4):
        try:
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
            txt = _openai_extract_json_text(obj).strip()
            if not txt:
                raise RuntimeError("empty OpenAI JSON text")
            parsed = json.loads(txt)
            return parsed if isinstance(parsed, dict) else None
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, RuntimeError, OSError) as e:
            last_err = e
            sleep_s = min(8.0, (2**attempt) + 0.3)
            time.sleep(sleep_s)
            continue
    if last_err:
        LOG.warning("openai market brief failed: %s", last_err)
    return None


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
    for bucket, queries in _news_bucket_queries().items():
        seen: set[str] = set()
        for q in queries:
            try:
                rows = fetch_naver_news(naver_client_id, naver_client_secret, q, display=20)
            except Exception as e:
                LOG.warning("naver brief fetch failed bucket=%s q=%s err=%s", bucket, q, e)
                continue
            for r in rows:
                link = str(r.get("link", "")).strip()
                if not link or link in seen:
                    continue
                seen.add(link)
                buckets[bucket].append(
                    {
                        "title": str(r.get("title", "")),
                        "description": _clean_news_text(r.get("summary", ""), limit=100),
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
    def line_for(sym: str) -> str:
        r = by_sym.get(sym)
        if not r:
            return "N/A"
        v = f"{float(r.get('price')):,.{int(r.get('digits', 2))}f}" if r.get("price") is not None else "N/A"
        c = fmt_report_pct(r.get("change_pct")) if r.get("change_pct") is not None else "N/A"
        return f"{v} ({c})"

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
                nums = []
                for i, u in enumerate(links[:3], start=1):
                    nums.append(f"<a href='{escape(u)}' style='color:#2563eb;text-decoration:none;' target='_blank' rel='noopener noreferrer'>{i}</a>")
                link_html = f"<div style='margin-top:4px;font-size:12px;color:#6b7280;'>대표링크: {' · '.join(nums)}</div>"
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
        c = fmt_report_pct(r.get("change_pct")) if r.get("change_pct") is not None else "N/A"
        indicator_lines.append(f"<td style='padding:6px 0;'><b>{escape(str(r.get('name','')))}</b> {escape(v)} <span style='color:#6b7280;'>({escape(c)})</span></td>")
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
                    S&P 500: <b>{escape(line_for("^GSPC"))}</b><br/>
                    NASDAQ: <b>{escape(line_for("^IXIC"))}</b><br/>
                    DOW: <b>{escape(line_for("^DJI"))}</b><br/>
                    <b>한 줄 코멘트:</b> {escape(us_dash)}
                  </div>
                </div>
              </td>
              <td class="mb-dash-col mb-pad-l" style="width:50%;vertical-align:top;padding-left:8px;">
                <div style="padding:12px;border-radius:14px;background:#f9fafb;border:1px solid #eef2f7;">
                  <div style="font-size:13px;font-weight:900;margin-bottom:8px;">🇰🇷 한국 (개장 전 프리뷰)</div>
                  <div style="font-size:13px;line-height:1.7;">
                    KOSPI: <b>{escape(line_for("^KS11"))}</b><br/>
                    KOSDAQ: <b>{escape(line_for("^KQ11"))}</b><br/>
                    원/달러: <b>{escape(line_for("KRW=X"))}</b><br/>
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


def _candidate_payload_rows(rows: list[dict[str, Any]], limit: int = 6) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in rows[:limit]:
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
                "high52wDrawdownPct": r.get("high52wDrawdownPct"),
                "bbText": str(r.get("bbText", "")),
                "score": int(r.get("score", 0)),
                "max": int(r.get("max", 5)),
            }
        )
    return out


def openai_candidate_brief_json(
    market_rows: list[dict[str, Any]],
    buy_candidates: list[dict[str, Any]],
    sell_candidates: list[dict[str, Any]],
) -> dict[str, Any] | None:
    api_key = (os.getenv("OPENAI_API_KEY", "") or "").strip()
    if not api_key:
        return None
    if not buy_candidates and not sell_candidates:
        return {"summary": "", "buy": [], "sell": []}

    market_compact = []
    for r in market_rows:
        sym = str(r.get("symbol", ""))
        if sym not in {"^KS11", "^KQ11", "^IXIC", "^GSPC", "^DJI", "KRW=X", "^TNX", "CL=F", "GC=F"}:
            continue
        market_compact.append(
            {
                "symbol": sym,
                "name": str(r.get("name", "")),
                "price": r.get("price"),
                "changePct": r.get("change_pct"),
            }
        )

    payload = {
        "market": market_compact,
        "buy_candidates": _candidate_payload_rows(buy_candidates, 6),
        "sell_candidates": _candidate_payload_rows(sell_candidates, 6),
        "format": {
            "summary": "시장/섹터 톤 1~2문장",
            "buy": [{"symbol": "", "name": "", "brief": "", "risk": "", "plan": ""}],
            "sell": [{"symbol": "", "name": "", "brief": "", "risk": "", "plan": ""}],
        },
        "rules": [
            "한국어",
            "JSON only",
            "각 종목 2~3문장 수준의 매우 짧은 전문가 톤",
            "가격예측 단정 금지",
            "지표/시황 연결 중심",
            "입력에 없는 정보 추정 최소화",
        ],
    }
    req_body = {
        "model": "gpt-5-mini",
        "reasoning": {"effort": "low"},
        "input": [
            {"role": "system", "content": [{"type": "text", "text": "주식 데일리 브리퍼. 입력 지표와 시황만 사용. JSON만 출력."}]},
            {"role": "user", "content": [{"type": "text", "text": json.dumps(payload, ensure_ascii=False)}]},
        ],
        "text": {"format": {"type": "json_object"}},
        "max_output_tokens": 1600,
    }

    last_err: Exception | None = None
    for attempt in range(3):
        try:
            req = Request(
                "https://api.openai.com/v1/responses",
                data=json.dumps(req_body, ensure_ascii=False).encode("utf-8"),
                method="POST",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            )
            with urlopen(req, timeout=60) as resp:
                raw = resp.read().decode("utf-8", errors="ignore")
            obj = json.loads(raw)
            txt = _openai_extract_json_text(obj).strip()
            parsed = json.loads(txt)
            return parsed if isinstance(parsed, dict) else None
        except Exception as e:
            last_err = e
            time.sleep(min(6.0, (2**attempt) + 0.2))
    if last_err:
        LOG.warning("openai candidate brief failed: %s", last_err)
    return None


def fallback_candidate_brief_json(
    buy_candidates: list[dict[str, Any]],
    sell_candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    def one_line(r: dict[str, Any], side: str) -> dict[str, str]:
        name = str(r.get("name", ""))
        sym = str(r.get("symbol", ""))
        rsi = r.get("rsi14")
        adx = r.get("adx14")
        macd = r.get("macd")
        sig = r.get("macdSignal")
        score = int(r.get("score", 0))
        bb = str(r.get("bbText", ""))
        trend = "상승 우위" if (macd is not None and sig is not None and macd > sig) else "하락 우위"
        brief = f"점수 {score}/5, RSI {f'{float(rsi):.1f}' if rsi is not None else 'N/A'}, ADX {f'{float(adx):.1f}' if adx is not None else 'N/A'} 기준으로 {trend} 흐름 체크."
        risk = "변동성 확대 시 분할 대응" if side == "buy" else "반등 시 되돌림 실패 여부 확인"
        plan = "추격보다 눌림 확인 후 분할" if side == "buy" else "반등시 비중 축소/리스크 관리 우선"
        return {"symbol": sym, "name": name, "brief": brief, "risk": risk, "plan": f"{plan} · {bb}"}

    return {
        "summary": "후보 종목은 지표 점수와 MACD/ADX 추세를 우선 확인하고, 장중에는 환율·지수 방향성과 함께 보세요.",
        "buy": [one_line(r, "buy") for r in buy_candidates[:6]],
        "sell": [one_line(r, "sell") for r in sell_candidates[:6]],
    }


def get_or_build_candidate_brief_for_chat(
    state: dict[str, Any],
    today: str,
    market_rows: list[dict[str, Any]],
    report_stock_rows: list[dict[str, Any]],
) -> tuple[dict[str, Any], bool]:
    cached = state.get("candidate_brief_json")
    if state.get("candidate_brief_date") == today and isinstance(cached, dict):
        return cached, False

    buy_candidates = [r for r in report_stock_rows if bool(r.get("buyCandidate"))]
    sell_candidates = [r for r in report_stock_rows if bool(r.get("sellCandidate"))]
    if not buy_candidates and not sell_candidates:
        brief = {"summary": "", "buy": [], "sell": []}
    else:
        brief = openai_candidate_brief_json(market_rows, buy_candidates, sell_candidates) or fallback_candidate_brief_json(
            buy_candidates, sell_candidates
        )
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
    stock_rows: list[dict[str, Any]],
    manage_url: str,
    market_rows: list[dict[str, Any]] | None = None,
    candidate_brief: dict[str, Any] | None = None,
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
            return badge("매도", "p-sell")
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

    def stock_table_rows(rows_in: list[dict[str, Any]]) -> str:
        html_rows = []
        for r in rows_in:
            score = int(r.get("score", 0))
            tone = "buy" if score >= 4 else "mid" if score >= 2 else "sell"
            sym_txt = str(r.get("symbol", ""))
            name_txt = str(r.get("name", ""))
            chart_url = tradingview_chart_url(sym_txt)
            ccy = str(r.get("ccy", "USD"))
            chg_pct = r.get("changePct")
            macd_txt = f"{fmt_opt(r.get('macd'), 2)} / {fmt_opt(r.get('macdSignal'), 2)}"
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
                f"<td>{metric_value(fmt_report_pct(r.get('high52wDrawdownPct')), high52_badge(r.get('high52wDrawdownPct')))}</td>"
                f"<td class='bb-cell'>{escape(str(r.get('bbText', '')))}</td>"
                f"<td><span class='score-chip'>{int(r.get('score', 0))}/{int(r.get('max', 5))}</span></td>"
                "</tr>"
            )
        return "".join(html_rows)

    market_rows = market_rows or []
    candidate_brief = candidate_brief or {}
    groups = [
        ("📈 주요 지수", {"^KS11", "^KQ11", "^N225", "^DJI", "^IXIC", "^GSPC", "000001.SS", "^HSI"}),
        ("🪙 원자재/암호", {"BTC-USD", "GC=F", "CL=F"}),
        ("💱 환율", {"KRW=X"}),
        ("🏦 금리", {"^TNX"}),
        ("🧩 기타", set()),
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
    return f"""<!doctype html>
<html><head><meta charset='utf-8'><title>종목 현황</title>
<meta name='viewport' content='width=device-width, initial-scale=1'>
<style>
body{{font-family:Segoe UI,Apple SD Gothic Neo,Malgun Gothic,sans-serif;margin:0;background:linear-gradient(180deg,#f8fbff 0%,#eef4fb 100%);color:#0f172a}}
.wrap{{max-width:1500px;margin:20px auto;padding:0 14px 20px}}
.top{{display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap}}
.title{{font-size:24px;font-weight:700}}
.meta{{color:#475569;font-size:14px}}
.cards{{display:grid;grid-template-columns:repeat(2,minmax(220px,1fr));gap:10px;margin:14px 0}}
.card,.panel{{background:#fff;border:1px solid #dbe3ef;border-radius:14px;padding:12px;box-shadow:0 4px 16px rgba(15,23,42,.04)}}
.card .label{{font-size:12px;color:#64748b}}
.card .val{{font-size:14px;font-weight:700;margin-top:6px;line-height:1.5}}
.btn{{display:inline-block;background:#0f172a;color:#fff;padding:10px 12px;border-radius:10px;text-decoration:none}}
.panel h3{{margin:0 0 10px 0;font-size:15px}}
.metric-panel{{padding:14px}}
.mgrid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:10px}}
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
.ai-head{{font-size:13px;margin-bottom:6px}}
.ai-head span{{color:#64748b;font-size:12px;margin-left:4px}}
.ai-line{{font-size:12px;line-height:1.55;color:#334155;margin-top:4px}}
.ai-line b{{color:#0f172a}}
.row-buy{{background:#ecfdf5}}
.row-mid{{background:#fff}}
.row-sell{{background:#fef2f2}}
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
    <div class='title'>📊 종목 현황</div>
    <div class='meta'>{escape(today)} {escape(now_time)}</div>
  </div>
  {manage_html}
</div>
<div class='cards'>
  <div class='card'><div class='label'>매수 후보 리스트</div><div class='val'>{escape(', '.join(buy_list) if buy_list else '없음')}</div></div>
  <div class='card'><div class='label'>매도 후보 리스트</div><div class='val'>{escape(', '.join(sell_list) if sell_list else '없음')}</div></div>
</div>
<div class='panel ai-panel'>
  <h3>🤖 후보 브리핑</h3>
  <div class='ai-summary'>{escape(cb_summary or '후보 종목이 없거나 요약 데이터가 없습니다. 지표 기반 표를 우선 확인하세요.')}</div>
  {brief_cards(cb_buy, "🟢 매수 후보 브리핑", "buytone")}
  {brief_cards(cb_sell, "🔴 매도 후보 브리핑", "selltone")}
</div>
<div class='section-title'>🇰🇷 한국 주식</div>
<div class='scroll'>
<table>
<tr><th class='sticky-col'>종목</th><th>현재가</th><th>전일종가</th><th>등락률</th><th>RSI</th><th>StochRSI</th><th>MACD/Sig</th><th>ADX</th><th>52주고점비</th><th>Bollinger</th><th>Score</th></tr>
{stock_table_rows(kr_rows) if kr_rows else "<tr><td colspan='11'>표시할 종목이 없습니다.</td></tr>"}
</table>
</div>
<div class='section-title'>🇺🇸 미국 주식</div>
<div class='scroll'>
<table>
<tr><th class='sticky-col'>종목</th><th>현재가</th><th>전일종가</th><th>등락률</th><th>RSI</th><th>StochRSI</th><th>MACD/Sig</th><th>ADX</th><th>52주고점비</th><th>Bollinger</th><th>Score</th></tr>
{stock_table_rows(us_rows) if us_rows else "<tr><td colspan='11'>표시할 종목이 없습니다.</td></tr>"}
</table>
</div>
<div class='section-title'>📌 주요지표현황</div>
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
        return "🔴", "매도 경계(>80)"
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
        return "🔴 상단 밴드 터치(과열 경고)"
    return "⚪ 밴드 내부(중립)"


def total_score(rsi: float | None, stoch: float | None, macd: float | None, macd_sig: float | None, adx: float | None, high52w_dd: float | None) -> tuple[int, int]:
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
    return s, 5


def score_label(score: int, maxv: int) -> tuple[str, str]:
    ratio = score / maxv if maxv else 0
    if ratio >= 0.75:
        return "🟢", "강매수 구간"
    if ratio >= 0.5:
        return "🟢", "매수 우위"
    if ratio >= 0.25:
        return "🟡", "중립/관망"
    return "🔴", "보수적 접근"


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
    if web_base_url:
        market_brief_token = get_or_create_market_brief_report(
            conn,
            today,
            now_time,
            market_rows_for_report,
            naver_client_id,
            naver_client_secret,
        )

    chat_cfgs = {}
    for cid in chat_ids:
        base_cfg = load_chat_config(cfg, cid)
        ov = load_chat_config_override(conn, cid)
        chat_cfgs[cid] = merge_chat_config(base_cfg, ov)
    all_symbols = sorted({t["symbol"] for c in chat_cfgs.values() for t in c.get("tickers", []) if isinstance(t, dict) and t.get("symbol")})
    snap_map = load_latest_snapshots(conn, all_symbols, runtime.lookback)

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
        report_stock_rows: list[dict[str, Any]] = []

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
                        "max": 5,
                        "buyCandidate": False,
                        "sellCandidate": False,
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

            s_rsi = rsi_signal(rsi)
            s_stoch = stoch_rsi_signal(stoch)
            s_macd = macd_signal(macd, macd_sig)
            s_adx = adx_signal(adx)
            s_high52 = high52w_signal(high52_dd)
            bb = s.get("bollinger20", {})
            s_bb = bollinger_signal(s.get("curPrice") or ref_price, bb.get("middle"), bb.get("upper"), bb.get("lower"))
            score, maxv = total_score(rsi, stoch, macd, macd_sig, adx, high52_dd)
            score_text = score_label(score, maxv)

            is_buy = score >= 3 and macd is not None and macd_sig is not None and macd > macd_sig
            is_sell = (
                (rsi is not None and rsi >= 70)
                or (stoch is not None and stoch > 80)
                or (score <= 1 and macd is not None and macd_sig is not None and macd < macd_sig)
            )
            if is_buy:
                buy_list.append(f"{name}({sym})")
            if is_sell:
                sell_list.append(f"{name}({sym})")
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
                    "high52wDrawdownPct": high52_dd,
                    "bbText": s_bb,
                    "score": score,
                    "max": maxv,
                    "buyCandidate": is_buy,
                    "sellCandidate": is_sell,
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
            )
            if candidate_brief_dirty:
                dirty = True
            report_url = f"{web_base_url.rstrip('/')}/report/{report_token}"
            html = render_chat_report_html(
                today,
                now_time,
                buy_list,
                sell_list,
                report_stock_rows,
                manage_url,
                market_rows_for_report,
                candidate_brief_json,
            )
            save_chat_report(conn, chat_id, report_token, html)

        market_brief_url = f"{web_base_url.rstrip('/')}/report/{market_brief_token}" if (web_base_url and market_brief_token) else ""
        buttons = []
        if market_brief_url:
            buttons.append({"text": "📰 마켓 브리프", "url": market_brief_url})
        if report_url:
            buttons.append({"text": "📊 종목 현황", "url": report_url})
        if manage_url:
            buttons.append({"text": "⚙️ 내 종목 관리", "url": manage_url})
        short_msg_lines = [
            f"📮 투자 알림 ({today} {now_time})",
            f"🟢 매수 후보: {', '.join(buy_list[:6]) if buy_list else '없음'}",
            f"🔴 매도 후보: {', '.join(sell_list[:6]) if sell_list else '없음'}",
        ]
        if len(buy_list) > 6:
            short_msg_lines.append(f"… 매수 후보 추가 {len(buy_list) - 6}개")
        if len(sell_list) > 6:
            short_msg_lines.append(f"… 매도 후보 추가 {len(sell_list) - 6}개")
        if has_usd:
            short_msg_lines.append(f"💱 환율: {fmt_money(usdkrw, 'KRW')}" if usdkrw else "💱 환율: 조회 실패")
        short_msg_lines.append("자세한 내용은 아래 버튼에서 확인하세요.")
        short_msg = "\n".join(short_msg_lines)
        if buttons:
            tg.send_text_with_buttons(chat_id, short_msg, buttons)
        else:
            tg.send_text(chat_id, short_msg)

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
    chat_ids = load_chat_ids(args, cfg)
    if not chat_ids:
        raise RuntimeError("No chat ids found. Set --telegram-chat-ids-json or add chat ids in chat-configs.js")

    token = args.telegram_token or ""
    if not token:
        raise RuntimeError("--telegram-token (or env wrapper) is required")

    conn = sqlite3.connect(args.db_path)
    try:
        ensure_alert_schema(conn)
        tg = TelegramClient(token)
        runtime = default_runtime_config()
        web_base = (args.web_base_url or "").strip().rstrip("/")
        manage_url = (args.manage_page_url or "").strip()
        if not manage_url and web_base:
            manage_url = f"{web_base}/manage"

        if args.task == "daily_report":
            run_daily_report(
                conn,
                tg,
                cfg,
                chat_ids,
                runtime,
                manage_url,
                web_base,
                (os.getenv("NAVER_CLIENT_ID", "") or "").strip(),
                (os.getenv("NAVER_CLIENT_SECRET", "") or "").strip(),
            )
        elif args.task == "morning_card":
            run_morning_card(conn, tg, chat_ids)
        elif args.task == "envelope_watch":
            run_envelope_watch(conn, tg, chat_ids, max(2, int(args.envelope_period)), float(args.envelope_pct), cfg)
    finally:
        conn.close()


if __name__ == "__main__":
    main()


