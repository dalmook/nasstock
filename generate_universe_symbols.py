import argparse
import json
import re
from pathlib import Path

from urllib.request import Request, urlopen


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


def _build_kr_universes() -> tuple[list[str], list[str]]:
    items: list[dict[str, str]] = []
    seen: set[str] = set()
    for sosok, suffix, market in [("0", ".KS", "KOSPI"), ("1", ".KQ", "KOSDAQ")]:
        for page in range(1, 90):
            url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page={page}"
            req = Request(url, headers={"user-agent": "Mozilla/5.0"})
            with urlopen(req, timeout=20) as resp:
                html = resp.read().decode("euc-kr", errors="ignore")
            pairs = re.findall(r'/item/main\.naver\?code=(\d{6})[^>]*>([^<]+)</a>', html)
            if not pairs:
                break
            added = 0
            for code, raw_name in pairs:
                sym = f"{code}{suffix}".upper()
                if sym in seen:
                    continue
                items.append({"symbol": sym, "name": raw_name.strip(), "market": market})
                seen.add(sym)
                added += 1
            if added == 0 and page > 3:
                break

    kospi: list[str] = []
    kosdaq: list[str] = []
    for it in items:
        sym = str(it.get("symbol", "")).strip().upper()
        market = str(it.get("market", "")).strip().upper()
        if sym.endswith(".KS") and ("KOSPI" in market) and sym not in kospi:
            kospi.append(sym)
        if sym.endswith(".KQ") and ("KOSDAQ" in market) and sym not in kosdaq:
            kosdaq.append(sym)
    return kospi[:200], kosdaq[:100]


def main() -> int:
    p = argparse.ArgumentParser(description="Generate fixed universe_symbols.json")
    p.add_argument("--out", default="universe_symbols.json")
    args = p.parse_args()

    kospi200, kosdaq100 = _build_kr_universes()
    doc = {
        "meta": {
            "note": "KOSPI/KOSDAQ lists are derived from Naver market summary ordering and can be refreshed weekly.",
            "generated_by": "generate_universe_symbols.py",
        },
        "KOSPI200": kospi200,
        "KOSDAQ100": kosdaq100,
        "NASDAQ_MAJOR": UNIVERSE_NASDAQ_MAJOR_SYMBOLS,
        "DOW30": UNIVERSE_DOW30_SYMBOLS,
    }
    out = Path(args.out)
    out.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        json.dumps(
            {
                "ok": True,
                "path": str(out),
                "counts": {
                    "KOSPI200": len(kospi200),
                    "KOSDAQ100": len(kosdaq100),
                    "NASDAQ_MAJOR": len(UNIVERSE_NASDAQ_MAJOR_SYMBOLS),
                    "DOW30": len(UNIVERSE_DOW30_SYMBOLS),
                },
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
