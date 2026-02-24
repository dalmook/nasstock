import json
import os
import re
import sqlite3
import io
import time
import secrets
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request as UrlRequest, urlopen

from fastapi import FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse


DB_PATH = os.getenv("DB_PATH", "/data/stock_prices.db")
SYMBOLS_FILE = os.getenv("SYMBOLS_FILE", "/app/chat-configs.js")
SEED_USERS_JSON = os.getenv("CHAT_USERS_JSON", "").strip()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_BOT_USERNAME = os.getenv("TELEGRAM_BOT_USERNAME", "").strip().lstrip("@")
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()

SYMBOL_RE = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,19}$")
NAME_RE = re.compile(r"^[가-힣a-zA-Z0-9_.\-]{2,32}$")

app = FastAPI(title="nas-stock-web", version="2.0.0")
_KRX_CACHE: dict[str, Any] = {"ts": 0.0, "items": []}


def db_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def now_utc_iso() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()


def ensure_schema(conn: sqlite3.Connection):
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
        CREATE TABLE IF NOT EXISTS pending_registrations (
            name TEXT PRIMARY KEY,
            pin TEXT NOT NULL,
            status TEXT NOT NULL,
            chat_id TEXT,
            request_token TEXT NOT NULL,
            requested_at_utc TEXT NOT NULL,
            updated_at_utc TEXT NOT NULL,
            approved_by TEXT
        )
        """
    ) 
    conn.commit()


def seed_users_if_empty(conn: sqlite3.Connection):
    cnt = conn.execute("SELECT COUNT(*) FROM manage_users").fetchone()[0]
    if cnt != 0:
        return
    if not SEED_USERS_JSON:
        return
    try:
        rows = json.loads(SEED_USERS_JSON)
    except Exception:
        return
    if not isinstance(rows, list):
        return
    now = now_utc_iso()
    inserts = []
    for i, r in enumerate(rows):
        if not isinstance(r, dict):
            continue
        name = str(r.get("name", "")).strip().lower()
        pin = str(r.get("pin", "")).strip()
        chat_id = str(r.get("chat_id", "")).strip()
        if not name or not pin or not chat_id:
            continue
        is_admin = 1 if i == 0 else 0
        inserts.append((name, pin, chat_id, is_admin, 1, now))
    if not inserts:
        return
    with conn:
        conn.executemany(
            """
            INSERT OR IGNORE INTO manage_users(name, pin, chat_id, is_admin, enabled, updated_at_utc)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            inserts,
        )


def users_count(conn: sqlite3.Connection) -> int:
    return int(conn.execute("SELECT COUNT(*) FROM manage_users").fetchone()[0])


def normalize_name(name: str) -> str:
    return str(name or "").strip().lower()


def require_auth(conn: sqlite3.Connection, name: str | None, pin: str | None) -> sqlite3.Row:
    nm = normalize_name(name)
    row = conn.execute(
        "SELECT name, pin, chat_id, is_admin, enabled FROM manage_users WHERE name = ?",
        (nm,),
    ).fetchone()
    if not row:
        raise HTTPException(status_code=401, detail="invalid name/pin")
    if str(pin or "").strip() != str(row["pin"]):
        raise HTTPException(status_code=401, detail="invalid name/pin")
    if int(row["enabled"] or 0) != 1:
        raise HTTPException(status_code=401, detail="disabled user")
    return row


def upsert_user(conn: sqlite3.Connection, name: str, pin: str, chat_id: str, is_admin: int, enabled: int):
    now = now_utc_iso()
    with conn:
        conn.execute(
            """
            INSERT INTO manage_users(name, pin, chat_id, is_admin, enabled, updated_at_utc)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
              pin=excluded.pin,
              chat_id=excluded.chat_id,
              is_admin=excluded.is_admin,
              enabled=excluded.enabled,
              updated_at_utc=excluded.updated_at_utc
            """,
            (name, pin, chat_id, int(is_admin), int(enabled), now),
        )


def delete_user(conn: sqlite3.Connection, name: str):
    with conn:
        conn.execute("DELETE FROM manage_users WHERE name = ?", (normalize_name(name),))

def link_pending_registration_by_token(conn: sqlite3.Connection, token: str, chat_id: str) -> dict[str, Any] | None:
    t = str(token or "").strip()
    cid = str(chat_id or "").strip()
    if not t or not cid or not cid.lstrip("-").isdigit():
        return None
    row = conn.execute(
        "SELECT name, status FROM pending_registrations WHERE request_token = ?",
        (t,),
    ).fetchone()
    if not row:
        return None
    now = now_utc_iso()
    with conn:
        conn.execute(
            "UPDATE pending_registrations SET status='linked', chat_id=?, updated_at_utc=? WHERE request_token=?",
            (cid, now, t),
        )
    return {"name": str(row["name"]), "status": str(row["status"])}


def send_telegram_message(chat_id: str, text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN:
        return False
    req = UrlRequest(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        data=urlencode({"chat_id": chat_id, "text": text}).encode("utf-8"),
        method="POST",
        headers={"content-type": "application/x-www-form-urlencoded"},
    )
    try:
        with urlopen(req, timeout=20) as resp:
            payload = resp.read().decode("utf-8", errors="ignore")
        obj = json.loads(payload)
        return bool(obj.get("ok"))
    except Exception:
        return False


def create_pending_registration(conn: sqlite3.Connection, name: str, pin: str) -> str:
    now = now_utc_iso()
    token = secrets.token_urlsafe(18)
    with conn:
        conn.execute(
            """
            INSERT INTO pending_registrations(name, pin, status, chat_id, request_token, requested_at_utc, updated_at_utc, approved_by)
            VALUES (?, ?, 'requested', NULL, ?, ?, ?, NULL)
            ON CONFLICT(name) DO UPDATE SET
              pin=excluded.pin,
              status='requested',
              chat_id=NULL,
              request_token=excluded.request_token,
              requested_at_utc=excluded.requested_at_utc,
              updated_at_utc=excluded.updated_at_utc,
              approved_by=NULL
            """,
            (name, pin, token, now, now),
        )
    return token


def js_object_to_json_text(js: str) -> str:
    body = re.sub(r"//.*", "", js)
    body = re.sub(r"\bexport\s+default\s+chatConfigs\s*;", "", body)
    body = re.sub(r"\bconst\s+chatConfigs\s*=", "", body)
    body = body.strip().rstrip(";")

    def qk(m: re.Match) -> str:
        return f'{m.group(1)}"{m.group(2)}":'

    body = re.sub(r"([\{,]\s*)([A-Za-z_][A-Za-z0-9_]*)\s*:", qk, body)
    body = re.sub(r",\s*([}\]])", r"\1", body)
    return body


def load_base_config() -> dict[str, Any]:
    raw = Path(SYMBOLS_FILE).read_text(encoding="utf-8", errors="ignore")
    obj = json.loads(js_object_to_json_text(raw))
    if not isinstance(obj, dict):
        raise RuntimeError("chat-configs.js parse failed")
    return obj


def load_override(conn: sqlite3.Connection, chat_id: str) -> dict[str, Any] | None:
    row = conn.execute("SELECT override_json FROM chat_config_overrides WHERE chat_id = ?", (chat_id,)).fetchone()
    if not row:
        return None
    try:
        v = json.loads(row["override_json"])
        return v if isinstance(v, dict) else None
    except Exception:
        return None


def save_override(conn: sqlite3.Connection, chat_id: str, doc: dict[str, Any]):
    now = now_utc_iso()
    with conn:
        conn.execute(
            """
            INSERT INTO chat_config_overrides(chat_id, override_json, updated_at_utc)
            VALUES (?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
              override_json=excluded.override_json,
              updated_at_utc=excluded.updated_at_utc
            """,
            (chat_id, json.dumps(doc, ensure_ascii=False), now),
        )


def delete_override(conn: sqlite3.Connection, chat_id: str):
    with conn:
        conn.execute("DELETE FROM chat_config_overrides WHERE chat_id = ?", (chat_id,))


def merged_chat_config(base: dict[str, Any], override: dict[str, Any] | None, chat_id: str) -> dict[str, Any]:
    dft = base.get("default") if isinstance(base.get("default"), dict) else {}
    by_chat = base.get(chat_id) if isinstance(base.get(chat_id), dict) else {}
    tickers = by_chat.get("tickers") if isinstance(by_chat.get("tickers"), list) and by_chat.get("tickers") else dft.get("tickers", [])
    oversize = by_chat.get("oversize_drop_pct") if isinstance(by_chat.get("oversize_drop_pct"), dict) else dft.get("oversize_drop_pct", {})
    news_keywords = by_chat.get("news_keywords") if isinstance(by_chat.get("news_keywords"), list) else (
        dft.get("news_keywords", []) if isinstance(dft.get("news_keywords"), list) else []
    )
    if isinstance(override, dict):
        if isinstance(override.get("tickers"), list):
            tickers = override.get("tickers")
        if isinstance(override.get("oversize_drop_pct"), dict):
            oversize = override.get("oversize_drop_pct")
        if isinstance(override.get("news_keywords"), list):
            news_keywords = override.get("news_keywords")
    return {"tickers": tickers, "oversize_drop_pct": oversize, "news_keywords": news_keywords}


def sanitize_symbol(symbol: str) -> str | None:
    s = str(symbol or "").strip().upper()
    return s if SYMBOL_RE.fullmatch(s) else None


def sanitize_display_name(name: str, fallback: str) -> str:
    s = str(name or "").strip()
    return (s if s else fallback)[:40]


def sanitize_ticker_name(name: Any, fallback: str) -> str:
    s = str(name or "").strip()
    return (s if s else fallback)[:40]


def parse_news_keywords(raw: str) -> list[str]:
    items: list[str] = []
    for part in re.split(r"[\r\n,]+", str(raw or "")):
        s = re.sub(r"\s+", " ", part).strip()
        if not s or s in items:
            continue
        items.append(s[:40])
        if len(items) >= 10:
            break
    return items


def load_krx_listing_cache() -> list[dict[str, str]]:
    now = time.time()
    cached = _KRX_CACHE.get("items")
    if isinstance(cached, list) and cached and (now - float(_KRX_CACHE.get("ts") or 0.0) < 6 * 3600):
        return cached
    try:
        req = UrlRequest(
            "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13",
            headers={"user-agent": "Mozilla/5.0"},
        )
        with urlopen(req, timeout=20) as resp:
            raw = resp.read()
        html = raw.decode("cp949", errors="ignore")
        import pandas as pd

        tables = pd.read_html(io.StringIO(html))
        if not tables:
            return cached if isinstance(cached, list) else []
        df = tables[0]
        rows: list[dict[str, str]] = []
        for _, rec in df.iterrows():
            name = str(rec.get("회사명", "") or "").strip()
            market = str(rec.get("시장구분", "") or "").strip()
            code = str(rec.get("종목코드", "") or "").strip().upper()
            if not name or not code:
                continue
            if code.isdigit():
                code = code.zfill(6)
            suffix = ".KQ" if "코스닥" in market else ".KS"
            sym = sanitize_symbol(f"{code}{suffix}")
            if not sym:
                continue
            rows.append({"symbol": sym, "name": sanitize_ticker_name(name, sym), "market": market or "KRX"})
        _KRX_CACHE["ts"] = now
        _KRX_CACHE["items"] = rows
        return rows
    except Exception:
        return cached if isinstance(cached, list) else []


def krx_search_tickers(query: str, limit: int = 12) -> list[dict[str, Any]]:
    q = str(query or "").strip()
    if not q:
        return []
    q_lower = q.lower()
    out: list[dict[str, Any]] = []
    for it in load_krx_listing_cache():
        sym = str(it.get("symbol", "")).upper()
        name = str(it.get("name", ""))
        code = sym.split(".", 1)[0]
        if not (q_lower in name.lower() or code.startswith(q.upper()) or q_lower in sym.lower()):
            continue
        out.append({"symbol": sym, "name": name, "type": str(it.get("market") or "KRX")})
        if len(out) >= max(1, min(int(limit), 20)):
            break
    return out


def yahoo_search_tickers(query: str, limit: int = 12) -> list[dict[str, Any]]:
    q = str(query or "").strip()
    if not q:
        return []
    max_count = max(1, min(20, int(limit)))
    is_korean = re.search(r"[ㄱ-ㅎㅏ-ㅣ가-힣]", q) is not None
    is_numeric_code = re.fullmatch(r"\d{6}", q) is not None

    def fetch_json(url: str) -> dict[str, Any]:
        req = UrlRequest(url, headers={"user-agent": "Mozilla/5.0", "accept": "application/json,text/plain,*/*"})
        with urlopen(req, timeout=20) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
        return json.loads(raw)

    def yahoo_search_one(region: str, lang: str) -> list[dict[str, Any]]:
        try:
            url = (
                "https://query1.finance.yahoo.com/v1/finance/search?"
                + urlencode(
                    {
                        "q": q,
                        "quotesCount": max_count,
                        "newsCount": 0,
                        "region": region,
                        "lang": lang,
                    }
                )
            )
            j = fetch_json(url)
            rows = j.get("quotes")
            return rows if isinstance(rows, list) else []
        except Exception:
            return []

    quotes = yahoo_search_one("KR", "ko-KR") + yahoo_search_one("US", "en-US")
    krx_fallback = krx_search_tickers(q, limit=max_count) if (is_korean or is_numeric_code) else []
    if is_numeric_code:
        for code_sym in [f"{q}.KS", f"{q}.KQ"]:
            quotes.insert(0, {"symbol": code_sym, "shortname": code_sym, "longname": code_sym, "quoteType": "EQUITY"})

    seen = set()
    out = []
    for it in krx_fallback:
        if not isinstance(it, dict):
            continue
        sym = sanitize_symbol(it.get("symbol", ""))
        if not sym or sym in seen:
            continue
        name = sanitize_ticker_name(it.get("name") or sym, sym)
        out.append({"symbol": sym, "name": name, "type": str(it.get("type") or "KRX")})
        seen.add(sym)
        if len(out) >= max_count:
            return out
    for it in quotes:
        if not isinstance(it, dict):
            continue
        sym = sanitize_symbol(it.get("symbol", ""))
        if not sym or sym in seen:
            continue
        qtype = str(it.get("quoteType", "")).upper()
        if qtype and qtype not in {"EQUITY", "ETF", "INDEX", "FUTURE", "MUTUALFUND", "CRYPTOCURRENCY"}:
            continue
        name = sanitize_ticker_name(it.get("longname") or it.get("shortname") or sym, sym)
        if is_korean and not (sym.endswith(".KS") or sym.endswith(".KQ")):
            pass
        out.append({"symbol": sym, "name": name, "type": qtype})
        seen.add(sym)
        if len(out) >= max_count:
            break
    return out


def render_setup_page(msg: str = "") -> str:
    note = f"<p class='msg err'>{escape(msg)}</p>" if msg else ""
    return f"""
<!doctype html><html><head><meta charset='utf-8'><title>초기 관리자 생성</title>
<style>
body{{font-family:Segoe UI,Apple SD Gothic Neo,Malgun Gothic,sans-serif;background:#f3f6fb;color:#0f172a;margin:0}}
.wrap{{max-width:720px;margin:40px auto;padding:0 16px}}
.panel{{background:#fff;border:1px solid #dbe3ef;border-radius:14px;padding:18px}}
label{{font-size:13px;color:#475569}}
input{{width:100%;padding:10px;border:1px solid #cbd5e1;border-radius:10px;margin-top:6px}}
button{{background:#0f172a;color:#fff;border:0;padding:10px 14px;border-radius:10px;cursor:pointer}}
.msg.err{{color:#b91c1c}}
</style></head>
<body><div class='wrap'><div class='panel'>
<h2>초기 관리자 생성</h2>
{note}
<form method='post' action='/manage/setup'>
<label>이름(한글/영문/숫자 2~32)</label><input name='name' required/>
<br/><br/>
<label>PIN</label><input name='pin' type='password' required/>
<br/><br/>
<label>chat_id</label><input name='chat_id' required/>
<br/><br/>
<button type='submit'>생성</button>
</form>
</div></div></body></html>
"""


def render_login_page(msg: str = "", action_link: str = "") -> str:
    note = f"<p class='msg err'>{escape(msg)}</p>" if msg else ""
    action_link_html = (
        f"<p style='margin-top:10px'><a href='{escape(action_link)}' target='_blank' rel='noopener noreferrer'>텔레그램 봇 열기</a></p>"
        if action_link
        else ""
    )    
    return f"""
<!doctype html><html><head><meta charset='utf-8'><title>내 종목 관리</title>
<meta name='viewport' content='width=device-width, initial-scale=1'>
<style>
body{{font-family:Segoe UI,Apple SD Gothic Neo,Malgun Gothic,sans-serif;background:#f3f6fb;color:#0f172a;margin:0}}
.wrap{{max-width:720px;margin:24px auto;padding:0 14px}}
.panel{{background:#fff;border:1px solid #dbe3ef;border-radius:14px;padding:18px}}
label{{font-size:13px;color:#475569}}
input{{width:100%;padding:11px;border:1px solid #cbd5e1;border-radius:10px;margin-top:6px}}
button{{background:#0f172a;color:#fff;border:0;padding:11px 14px;border-radius:10px;cursor:pointer;width:100%}}
.msg.err{{color:#b91c1c}}
.mini{{font-size:12px;color:#64748b}}
@media (max-width:700px){{.wrap{{margin:12px auto}} .panel{{padding:14px}}}}
</style></head>
<body><div class='wrap'><div class='panel'>
<h2>내 종목 관리 로그인</h2>
{note}
{action_link_html}
<form method='get' action='/manage'>
<label>이름</label><input name='name' required/><br/><br/>
<label>PIN</label><input name='pin' type='password' required/><br/><br/>
<button type='submit'>로그인</button>
</form>
<p class='mini' style='margin-top:14px'>처음 사용자라면 아래 회원가입을 사용하세요.</p>
<form method='post' action='/manage/signup' style='display:grid;gap:8px;margin-top:8px'>
<label>회원가입용 이름</label><input name='name' required/>
<label>회원가입용 PIN</label><input name='pin' type='password' required/>
<button type='submit'>회원가입 + 텔레그램 등록 요청</button>
</form>
</div></div></body></html>
"""


def redirect_manage(name: str, pin: str, msg: str) -> RedirectResponse:
    q = urlencode({"name": name, "pin": pin, "msg": msg})
    return RedirectResponse(url=f"/manage?{q}", status_code=303)


def render_manage_page(name: str, pin: str, user_row: sqlite3.Row, cfg: dict[str, Any], users: list[sqlite3.Row], pending_regs: list[sqlite3.Row], msg: str = "") -> str:
    chat_id = str(user_row["chat_id"])
    is_admin = int(user_row["is_admin"] or 0) == 1

    tickers = cfg.get("tickers", [])
    oversize = cfg.get("oversize_drop_pct", {})
    raw_news_keywords = cfg.get("news_keywords", [])
    news_keywords = [str(x).strip() for x in raw_news_keywords if str(x).strip()] if isinstance(raw_news_keywords, list) else []
    news_keywords_text = "\n".join(news_keywords)
    rows = []
    ticker_cards = []
    for t in tickers:
        sym = str(t.get("symbol", ""))
        nm = str(t.get("name", sym))
        drop = oversize.get(sym, "")
        rows.append(
            "<tr>"
            f"<td>{escape(sym)}</td><td>{escape(nm)}</td><td>{escape(str(drop))}</td>"
            f"<td><form method='post' action='/manage/delete'>"
            f"<input type='hidden' name='name' value='{escape(name)}'/><input type='hidden' name='pin' value='{escape(pin)}'/>"
            f"<input type='hidden' name='symbol' value='{escape(sym)}'/><button type='submit'>삭제</button></form></td>"
            "</tr>"
        )
        ticker_cards.append(
            "<div class='ticker-card'>"
            "<div class='ticker-main'>"
            f"<div class='ticker-name'>{escape(nm)}</div>"
            f"<div class='ticker-symbol'>{escape(sym)}</div>"
            "</div>"
            "<div class='ticker-meta'>"
            f"<span class='chip'>{escape('drop ' + str(drop) if str(drop) != '' else 'drop 미설정')}</span>"
            "</div>"
            "<div class='ticker-actions'>"
            f"<form method='post' action='/manage/delete'>"
            f"<input type='hidden' name='name' value='{escape(name)}'/>"
            f"<input type='hidden' name='pin' value='{escape(pin)}'/>"
            f"<input type='hidden' name='symbol' value='{escape(sym)}'/>"
            "<button type='submit' class='btn danger'>삭제</button>"
            "</form>"
            "</div>"
            "</div>"
        )

    users_html = ""
    pending_rows = '<tr><td colspan="4">없음</td></tr>'
    if is_admin:
        urows = []
        for u in users:
            uname = str(u["name"])
            urows.append(
                "<tr>"
                f"<td>{escape(uname)}</td><td>{escape(str(u['chat_id']))}</td><td>{'Y' if int(u['enabled']) == 1 else 'N'}</td><td>{'Y' if int(u['is_admin']) == 1 else 'N'}</td>"
                f"<td><form method='post' action='/manage/admin/delete_user'>"
                f"<input type='hidden' name='name' value='{escape(name)}'/><input type='hidden' name='pin' value='{escape(pin)}'/>"
                f"<input type='hidden' name='target_name' value='{escape(uname)}'/><button type='submit'>삭제</button></form></td>"
                "</tr>"
            )
        pending_items = []
        for pr in pending_regs:
            pname = str(pr['name'])
            pchat = str(pr['chat_id'] or '-')
            pstatus = str(pr['status'])
            pending_items.append(
                "<tr>"
                f"<td>{escape(pname)}</td><td>{escape(pchat)}</td><td>{escape(pstatus)}</td>"
                f"<td><form method='post' action='/manage/admin/approve_signup'>"
                f"<input type='hidden' name='name' value='{escape(name)}'/><input type='hidden' name='pin' value='{escape(pin)}'/>"
                f"<input type='hidden' name='target_name' value='{escape(pname)}'/>"
                "<button type='submit'>승인</button></form></td>"
                "</tr>"
            )
        if pending_items:
            pending_rows = ''.join(pending_items)          
        users_html = f"""
<div class='panel'>
<h3>알림 대상(사용자) 관리</h3>
<form method='post' action='/manage/admin/add_user' class='stack-form'>
  <input type='hidden' name='name' value='{escape(name)}'/>
  <input type='hidden' name='pin' value='{escape(pin)}'/>
  <input name='new_name' placeholder='name' required/>
  <input name='new_pin' placeholder='pin' required/>
  <input name='new_chat_id' placeholder='chat_id' required/>
  <div class='row-check'>
    <label class='check-inline'><input type='checkbox' name='new_is_admin' value='1'/> admin</label>
    <label class='check-inline'><input type='checkbox' name='new_enabled' value='1' checked/> enabled</label>
  </div>
  <button type='submit' class='btn'>추가/수정</button>
</form>
<div class='table-wrap'>
<table>
<tr><th>name</th><th>chat_id</th><th>enabled</th><th>admin</th><th>action</th></tr>
{''.join(urows) if urows else "<tr><td colspan='5'>없음</td></tr>"}
</table>
</div>
</div>
<div class='panel'>
<h3>가입 승인 대기</h3>
<div class='table-wrap'>
<table><tr><th>name</th><th>chat_id</th><th>status</th><th>action</th></tr>{pending_rows}</table>
</div>
</div>
"""

    msg_html = f"<p class='msg ok'>{escape(msg)}</p>" if msg else ""
    return f"""
<!doctype html><html><head><meta charset='utf-8'><title>내 종목 관리</title>
<meta name='viewport' content='width=device-width, initial-scale=1'>
<style>
:root{{--bg:#edf3fb;--panel:#ffffff;--line:#dbe5f2;--text:#0f172a;--muted:#64748b;--accent:#0f172a;--danger:#b91c1c;}}
*{{box-sizing:border-box}}
body{{font-family:Segoe UI,Apple SD Gothic Neo,Malgun Gothic,sans-serif;background:linear-gradient(180deg,#f7fbff 0%, var(--bg) 100%);color:var(--text);margin:0}}
.wrap{{max-width:1200px;margin:14px auto;padding:0 12px 24px}}
.head{{display:flex;justify-content:space-between;gap:10px;align-items:flex-start;flex-wrap:wrap;margin-bottom:8px}}
.head h2{{margin:0;font-size:22px}}
.subhead{{font-size:12px;color:var(--muted)}}
.panel{{background:var(--panel);border:1px solid var(--line);border-radius:16px;padding:14px;margin-bottom:12px;box-shadow:0 6px 20px rgba(15,23,42,.04)}}
.panel h3{{margin:0 0 10px 0;font-size:16px}}
.grid{{display:grid;grid-template-columns:1.2fr 1fr;gap:12px}}
.grid2{{display:grid;grid-template-columns:1fr 1fr;gap:12px}}
.stack-form{{display:grid;gap:8px}}
.row2{{display:grid;grid-template-columns:1fr 1fr;gap:8px}}
.row-check{{display:flex;gap:14px;flex-wrap:wrap}}
label{{font-size:12px;color:#475569;display:block}}
.field{{margin-bottom:4px}}
input,textarea{{width:100%;padding:11px 12px;border:1px solid #cbd5e1;border-radius:12px;margin-top:5px;font-size:14px;background:#fff}}
textarea{{min-height:110px;resize:vertical}}
.btn{{background:var(--accent);color:#fff;border:0;padding:10px 14px;border-radius:12px;cursor:pointer;font-weight:600}}
.btn.secondary{{background:#e2e8f0;color:#0f172a}}
.btn.danger{{background:#fee2e2;color:#991b1b;border:1px solid #fecaca}}
.btn.full{{width:100%}}
.msg.ok{{color:#166534;background:#ecfdf5;border:1px solid #bbf7d0;padding:10px 12px;border-radius:12px}}
.mini{{font-size:12px;color:var(--muted)}}
.search-list{{max-height:260px;overflow:auto;border:1px solid var(--line);border-radius:12px;background:#fff}}
.search-item{{padding:10px 12px;border-bottom:1px solid #eef2f7;cursor:pointer}}
.search-item:last-child{{border-bottom:0}}
.search-item:hover{{background:#f8fbff}}
.search-symbol{{font-weight:700}}
.search-name{{font-size:13px}}
.search-type{{font-size:11px;color:var(--muted)}}
.hint-box{{display:flex;gap:8px;flex-wrap:wrap;margin-top:8px}}
.hint-chip{{font-size:11px;padding:5px 8px;border-radius:999px;background:#eef2ff;color:#334155;border:1px solid #dbe5f2}}
.section-title{{display:flex;align-items:center;justify-content:space-between;gap:8px;margin-bottom:8px}}
.ticker-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:10px}}
.ticker-card{{border:1px solid var(--line);border-radius:14px;padding:12px;background:#fbfdff;display:grid;gap:8px}}
.ticker-name{{font-weight:700}}
.ticker-symbol{{font-size:12px;color:var(--muted);margin-top:2px}}
.ticker-meta{{display:flex;gap:6px;flex-wrap:wrap}}
.chip{{display:inline-flex;align-items:center;padding:4px 8px;border-radius:999px;border:1px solid #dbe5f2;background:#f8fafc;color:#334155;font-size:11px}}
.ticker-actions form{{margin:0}}
.table-wrap{{overflow:auto;border:1px solid var(--line);border-radius:12px}}
table{{width:100%;border-collapse:collapse;background:#fff}}
th,td{{border-bottom:1px solid #e2e8f0;padding:8px;font-size:13px;white-space:nowrap;text-align:left}}
th{{background:#f8fafc;position:sticky;top:0}}
.desktop-only{{display:block}}
.mobile-only{{display:none}}
details summary{{cursor:pointer;font-weight:600;color:#334155}}
.check-inline{{display:inline-flex;align-items:center;gap:6px}}
.check-inline input{{width:auto;margin:0}}
@media (max-width: 920px) {{
  .grid,.grid2{{grid-template-columns:1fr}}
}}
@media (max-width: 700px) {{
  .wrap{{padding:0 10px 20px}}
  .head h2{{font-size:20px}}
  .panel{{padding:12px}}
  .row2{{grid-template-columns:1fr}}
  .desktop-only{{display:none}}
  .mobile-only{{display:block}}
  .ticker-grid{{grid-template-columns:1fr}}
  .btn{{padding:11px 14px}}
  .search-item{{padding:12px}}
}}
</style></head>
<body><div class='wrap'>
<div class='head'>
  <div>
    <h2>내 종목 관리</h2>
    <div class='subhead'>사용자: {escape(name)} / chat_id: {escape(chat_id)} {'(admin)' if is_admin else ''}</div>
  </div>
  <form method='post' action='/manage/reset'>
    <input type='hidden' name='name' value='{escape(name)}'/>
    <input type='hidden' name='pin' value='{escape(pin)}'/>
    <button type='submit' class='btn secondary'>개인 설정 초기화</button>
  </form>
</div>
{msg_html}
<div class='panel mobile-only'><h3>회원가입</h3><p class='mini'>로그아웃 상태에서 회원가입 가능하며, 관리자 승인 후 활성화됩니다.</p></div>
<div class='grid'>
<div>
<div class='panel'>
<div class='section-title'><h3>종목 검색 · 추가</h3><span class='mini'>한국주식 한글 검색 지원</span></div>
<div class='field'>
  <label>종목 검색</label>
  <input id='search_query' placeholder='예: 삼성전자, 하이닉스, TSLA, 005930'/>
</div>
<div id='search_results' class='search-list'></div>
<div class='hint-box'>
  <span class='hint-chip'>검색 결과 터치 → 심볼/표시명 자동입력</span>
  <span class='hint-chip'>한국주식은 한글명/종목코드 모두 가능</span>
</div>
<form method='post' action='/manage/add' class='stack-form' style='margin-top:10px'>
  <input type='hidden' name='name' value='{escape(name)}'/>
  <input type='hidden' name='pin' value='{escape(pin)}'/>
  <div class='row2'>
    <div class='field'><label>심볼</label><input id='symbol_input' name='symbol' placeholder='예: 005930.KS / TSLA' required/></div>
    <div class='field'><label>표시명</label><input id='name_input' name='ticker_name' placeholder='예: 삼성전자'/></div>
  </div>
  <button type='submit' class='btn full'>종목 추가</button>
</form>
</div>
<div class='panel'>
  <div class='section-title'><h3>현재 종목 ({len(tickers)})</h3><span class='mini'>모바일 카드형</span></div>
  <div class='ticker-grid'>
    {''.join(ticker_cards) if ticker_cards else "<div class='mini'>등록된 종목이 없습니다.</div>"}
  </div>
  <details class='desktop-only' style='margin-top:12px'>
    <summary>표 형태로 보기</summary>
    <div class='table-wrap' style='margin-top:10px'>
      <table>
      <tr><th>심볼</th><th>이름</th><th>drop rule</th><th>액션</th></tr>
      {''.join(rows) if rows else "<tr><td colspan='4'>없음</td></tr>"}
      </table>
    </div>
  </details>
</div>
</div>
<div class='grid2'>
<div class='panel'>
<h3>과대낙폭 룰 설정</h3>
<form method='post' action='/manage/set_drop' class='stack-form'>
  <input type='hidden' name='name' value='{escape(name)}'/>
  <input type='hidden' name='pin' value='{escape(pin)}'/>
  <div class='field'><label>심볼</label><input name='symbol' placeholder='예: TSLA 또는 005930.KS' required/></div>
  <div class='field'><label>drop_pct</label><input name='drop_pct' placeholder='예: -0.06' required/></div>
  <button type='submit' class='btn'>drop rule 저장</button>
</form>
<p class='mini'>예: -0.06 = 전일 대비 -6% 이하일 때 과대낙폭 조건 충족</p>
</div>
<div class='panel'>
<h3>뉴스 키워드 설정</h3>
<form method='post' action='/manage/set_news_keywords' class='stack-form'>
  <input type='hidden' name='name' value='{escape(name)}'/>
  <input type='hidden' name='pin' value='{escape(pin)}'/>
  <div class='field'>
    <label>키워드 (줄바꿈 또는 쉼표로 구분)</label>
    <textarea name='news_keywords' placeholder='예: 삼성전자&#10;반도체&#10;미국 금리'>{escape(news_keywords_text)}</textarea>
  </div>
  <button type='submit' class='btn'>뉴스 키워드 저장</button>
</form>
<p class='mini'>일일 투자 알림 발송 시 사용자별 키워드 기준 최신 뉴스 요약 + 링크를 함께 보냅니다. (최대 10개)</p>
</div>
</div>
</div>
{users_html}
<script>
(function(){{
  const q = document.getElementById('search_query');
  const box = document.getElementById('search_results');
  const sym = document.getElementById('symbol_input');
  const nm = document.getElementById('name_input');
  let timer = null;
  function render(items){{
    if(!Array.isArray(items) || items.length===0){{ box.innerHTML = "<div class='search-item'>검색 결과 없음</div>"; return; }}
    box.innerHTML = items.map(it=>`<div class="search-item" data-symbol="${{it.symbol}}" data-name="${{it.name}}"><div class="search-symbol">${{it.symbol}}</div><div class="search-name">${{it.name}}</div><div class="search-type">${{it.type||''}}</div></div>`).join('');
    box.querySelectorAll('.search-item').forEach(el=>{{
      el.addEventListener('click', ()=>{{
        sym.value = el.getAttribute('data-symbol') || '';
        nm.value = el.getAttribute('data-name') || '';
        sym.focus();
        sym.scrollIntoView({{behavior:'smooth', block:'center'}});
      }});
    }});
  }}
  async function doSearch(v){{
    const s = (v||'').trim();
    if(s.length < 1){{ box.innerHTML = ''; return; }}
    try {{
      const res = await fetch(`/api/search_tickers?q=${{encodeURIComponent(s)}}&limit=12`);
      const j = await res.json();
      render(j.items || []);
    }} catch(e) {{
      box.innerHTML = "<div class='search-item'>검색 오류</div>";
    }}
  }}
  q.addEventListener('input', (e)=>{{
    clearTimeout(timer);
    const v = e.target.value;
    timer = setTimeout(()=>doSearch(v), 250);
  }});
}})();
</script>
</div></body></html>
"""


@app.on_event("startup")
def _startup():
    with db_conn() as conn:
        ensure_schema(conn)
        seed_users_if_empty(conn)


@app.get("/", response_class=HTMLResponse)
def root():
    return "<html><body><h3>nas-stock-web ok</h3></body></html>"


@app.get("/manage", response_class=HTMLResponse)
def manage(name: str | None = Query(default=None), pin: str | None = Query(default=None), msg: str = Query(default="")):
    with db_conn() as conn:
        ensure_schema(conn)
        if users_count(conn) == 0:
            return render_setup_page(msg)
        if not name or not pin:
            return render_login_page(msg)
        try:
            user = require_auth(conn, name, pin)
        except HTTPException:
            return render_login_page("이름/PIN이 올바르지 않습니다.")
        base = load_base_config()
        ov = load_override(conn, str(user["chat_id"]))
        cfg = merged_chat_config(base, ov, str(user["chat_id"]))
        users = conn.execute("SELECT name, chat_id, is_admin, enabled FROM manage_users ORDER BY name").fetchall()
        pending_regs = conn.execute("SELECT name, chat_id, status FROM pending_registrations WHERE status = 'linked' ORDER BY requested_at_utc").fetchall()
        return render_manage_page(str(user["name"]), str(pin), user, cfg, users, pending_regs, msg=msg)

@app.get("/api/search_tickers")
def api_search_tickers(q: str = Query(default=""), limit: int = Query(default=12)):
    items = yahoo_search_tickers(q, limit=limit)
    return JSONResponse({"ok": True, "items": items})


@app.post("/manage/setup")
def manage_setup(name: str = Form(...), pin: str = Form(...), chat_id: str = Form(...)):
    nm = normalize_name(name)
    if not NAME_RE.fullmatch(nm):
        return HTMLResponse(render_setup_page("이름 형식 오류"), status_code=400)
    if not str(chat_id).strip().isdigit():
        return HTMLResponse(render_setup_page("chat_id 형식 오류"), status_code=400)
    with db_conn() as conn:
        if users_count(conn) != 0:
            return RedirectResponse(url="/manage", status_code=303)
        upsert_user(conn, nm, str(pin).strip(), str(chat_id).strip(), 1, 1)
    return redirect_manage(nm, str(pin).strip(), "초기 관리자 생성 완료")

@app.post("/manage/signup")
def manage_signup(name: str = Form(...), pin: str = Form(...)):
    nm = normalize_name(name)
    pinv = str(pin).strip()
    if not NAME_RE.fullmatch(nm):
        return HTMLResponse(render_login_page("이름 형식 오류"), status_code=400)
    if len(pinv) < 4:
        return HTMLResponse(render_login_page("PIN은 4자리 이상"), status_code=400)
    with db_conn() as conn:
        exists = conn.execute("SELECT 1 FROM manage_users WHERE name = ?", (nm,)).fetchone()
        if exists:
            return HTMLResponse(render_login_page("이미 존재하는 이름입니다"), status_code=400)
        token = create_pending_registration(conn, nm, pinv)

    deep_link = f"https://t.me/{TELEGRAM_BOT_USERNAME}?start=reg_{token}" if TELEGRAM_BOT_USERNAME else ""
    guide = "텔레그램 봇 설정이 없어 운영자에게 문의하세요."
    if deep_link:
        guide = "가입 요청 저장됨. 텔레그램 봇으로 이동해 시작 버튼을 누르세요(자동 연동)."
        return HTMLResponse(render_login_page(guide, action_link=deep_link))
    return HTMLResponse(render_login_page(f"가입 요청 저장됨. {guide}"))


@app.post("/telegram/webhook")
async def telegram_webhook(request: Request):
    if TELEGRAM_WEBHOOK_SECRET:
        if request.headers.get("x-telegram-bot-api-secret-token", "") != TELEGRAM_WEBHOOK_SECRET:
            return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    payload = await request.json()
    msg = payload.get("message") if isinstance(payload, dict) else None
    if not isinstance(msg, dict):
        return JSONResponse({"ok": True})
    txt = str(msg.get("text") or "").strip()
    chat = msg.get("chat") if isinstance(msg.get("chat"), dict) else {}
    chat_id = str(chat.get("id") or "").strip()
    token = ""
    if txt.startswith("/start "):
        token = txt.split(" ", 1)[1].strip()
    elif txt.startswith("reg_"):
        token = txt.strip()
    if token.startswith("reg_"):
        token = token[4:]
    if token and chat_id:
        with db_conn() as conn:
            linked = link_pending_registration_by_token(conn, token, chat_id)
        if linked:
            send_telegram_message(chat_id, "등록 요청이 접수되었습니다. 관리자 승인 후 알림이 활성화됩니다.")
    return JSONResponse({"ok": True})


@app.get("/telegram/link")
def telegram_link(token: str = Query(default=""), chat_id: str = Query(default="")):
    t = str(token or "").strip()
    cid = str(chat_id or "").strip()
    if not t or not cid:
        return JSONResponse({"ok": False, "error": "token/chat_id required"}, status_code=400)
    with db_conn() as conn:
        linked = link_pending_registration_by_token(conn, t, cid)
    if not linked:
        return JSONResponse({"ok": False, "error": "token not found"}, status_code=404)
    send_telegram_message(cid, "등록 요청이 접수되었습니다. 관리자 승인 후 알림이 활성화됩니다.")
    return JSONResponse({"ok": True, "chat_id": cid})

@app.post("/manage/add")
def manage_add(name: str = Form(...), pin: str = Form(...), symbol: str = Form(...), ticker_name: str = Form(default="")):
    sym = sanitize_symbol(symbol)
    if not sym:
        return redirect_manage(name, pin, "심볼 형식 오류")
    with db_conn() as conn:
        user = require_auth(conn, name, pin)
        chat_id = str(user["chat_id"])
        base = load_base_config()
        ov = load_override(conn, chat_id) or {}
        cfg = merged_chat_config(base, ov, chat_id)
        tickers = [x for x in cfg.get("tickers", []) if isinstance(x, dict)]
        if not any(str(x.get("symbol", "")).upper() == sym for x in tickers):
            tickers.append({"symbol": sym, "name": sanitize_display_name(ticker_name, sym)})
        ov["tickers"] = tickers
        ov.setdefault("oversize_drop_pct", cfg.get("oversize_drop_pct", {}))
        save_override(conn, chat_id, ov)
    return redirect_manage(name, pin, f"{sym} 추가 완료")


@app.post("/manage/delete")
def manage_delete(name: str = Form(...), pin: str = Form(...), symbol: str = Form(...)):
    sym = sanitize_symbol(symbol)
    if not sym:
        return redirect_manage(name, pin, "심볼 형식 오류")
    with db_conn() as conn:
        user = require_auth(conn, name, pin)
        chat_id = str(user["chat_id"])
        base = load_base_config()
        ov = load_override(conn, chat_id) or {}
        cfg = merged_chat_config(base, ov, chat_id)
        tickers = [x for x in cfg.get("tickers", []) if isinstance(x, dict) and str(x.get("symbol", "")).upper() != sym]
        oversize = dict(cfg.get("oversize_drop_pct", {}))
        oversize.pop(sym, None)
        ov["tickers"] = tickers
        ov["oversize_drop_pct"] = oversize
        save_override(conn, chat_id, ov)
    return redirect_manage(name, pin, f"{sym} 삭제 완료")


@app.post("/manage/set_drop")
def manage_set_drop(name: str = Form(...), pin: str = Form(...), symbol: str = Form(...), drop_pct: str = Form(...)):
    sym = sanitize_symbol(symbol)
    if not sym:
        return redirect_manage(name, pin, "심볼 형식 오류")
    try:
        drop = float(drop_pct)
    except Exception:
        return redirect_manage(name, pin, "drop_pct 숫자 형식 오류")
    with db_conn() as conn:
        user = require_auth(conn, name, pin)
        chat_id = str(user["chat_id"])
        base = load_base_config()
        ov = load_override(conn, chat_id) or {}
        cfg = merged_chat_config(base, ov, chat_id)
        oversize = dict(cfg.get("oversize_drop_pct", {}))
        oversize[sym] = drop
        ov["oversize_drop_pct"] = oversize
        ov.setdefault("tickers", cfg.get("tickers", []))
        save_override(conn, chat_id, ov)
    return redirect_manage(name, pin, f"{sym} drop rule 저장 완료")


@app.post("/manage/set_news_keywords")
def manage_set_news_keywords(name: str = Form(...), pin: str = Form(...), news_keywords: str = Form(default="")):
    kws = parse_news_keywords(news_keywords)
    with db_conn() as conn:
        user = require_auth(conn, name, pin)
        chat_id = str(user["chat_id"])
        base = load_base_config()
        ov = load_override(conn, chat_id) or {}
        cfg = merged_chat_config(base, ov, chat_id)
        ov["news_keywords"] = kws
        ov.setdefault("tickers", cfg.get("tickers", []))
        ov.setdefault("oversize_drop_pct", cfg.get("oversize_drop_pct", {}))
        save_override(conn, chat_id, ov)
    return redirect_manage(name, pin, f"뉴스 키워드 저장 완료 ({len(kws)}개)")


@app.post("/manage/reset")
def manage_reset(name: str = Form(...), pin: str = Form(...)):
    with db_conn() as conn:
        user = require_auth(conn, name, pin)
        delete_override(conn, str(user["chat_id"]))
    return redirect_manage(name, pin, "개인 오버라이드 초기화 완료")


@app.post("/manage/admin/add_user")
def manage_admin_add_user(
    name: str = Form(...),
    pin: str = Form(...),
    new_name: str = Form(...),
    new_pin: str = Form(...),
    new_chat_id: str = Form(...),
    new_is_admin: str | None = Form(default=None),
    new_enabled: str | None = Form(default=None),
):
    with db_conn() as conn:
        user = require_auth(conn, name, pin)
        if int(user["is_admin"] or 0) != 1:
            return redirect_manage(name, pin, "관리자 권한 필요")
        nm = normalize_name(new_name)
        if not NAME_RE.fullmatch(nm):
            return redirect_manage(name, pin, "새 사용자 이름 형식 오류")
        if not str(new_chat_id).strip().isdigit():
            return redirect_manage(name, pin, "chat_id 형식 오류")
        upsert_user(
            conn,
            nm,
            str(new_pin).strip(),
            str(new_chat_id).strip(),
            1 if new_is_admin else 0,
            1 if new_enabled else 0,
        )
    return redirect_manage(name, pin, f"사용자 {nm} 저장 완료")

@app.post("/manage/admin/approve_signup")
def manage_admin_approve_signup(name: str = Form(...), pin: str = Form(...), target_name: str = Form(...)):
    with db_conn() as conn:
        user = require_auth(conn, name, pin)
        if int(user["is_admin"] or 0) != 1:
            return redirect_manage(name, pin, "관리자 권한 필요")
        tn = normalize_name(target_name)
        row = conn.execute("SELECT name, pin, chat_id, status FROM pending_registrations WHERE name = ?", (tn,)).fetchone()
        if not row:
            return redirect_manage(name, pin, "승인 대상 없음")
        if str(row["status"]) != "linked" or not str(row["chat_id"] or "").strip().isdigit():
            return redirect_manage(name, pin, "텔레그램 연동 전입니다")
        upsert_user(conn, tn, str(row["pin"]), str(row["chat_id"]), 0, 1)
        with conn:
            conn.execute(
                "UPDATE pending_registrations SET status='approved', approved_by=?, updated_at_utc=? WHERE name = ?",
                (str(user["name"]), now_utc_iso(), tn),
            )
    send_telegram_message(str(row["chat_id"]), "관리자 승인 완료: 알림 구독이 활성화되었습니다.")
    return redirect_manage(name, pin, f"사용자 {tn} 승인 완료")



@app.post("/manage/admin/delete_user")
def manage_admin_delete_user(name: str = Form(...), pin: str = Form(...), target_name: str = Form(...)):
    with db_conn() as conn:
        user = require_auth(conn, name, pin)
        if int(user["is_admin"] or 0) != 1:
            return redirect_manage(name, pin, "관리자 권한 필요")
        tn = normalize_name(target_name)
        if tn == normalize_name(str(user["name"])):
            return redirect_manage(name, pin, "본인 계정은 삭제할 수 없음")
        delete_user(conn, tn)
    return redirect_manage(name, pin, f"사용자 {tn} 삭제 완료")


@app.get("/report/{token}", response_class=HTMLResponse)
def report(token: str, request: Request):
    t = str(token).strip()
    if not t:
        raise HTTPException(status_code=404, detail="not found")
    with db_conn() as conn:
        row = conn.execute("SELECT html_content FROM chat_reports WHERE report_token = ?", (t,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="report not found")
    return row["html_content"]


