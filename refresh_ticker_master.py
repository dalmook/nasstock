import argparse
import json
from pathlib import Path

import nas_web_app


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh ticker_master table from KRX/Naver sources")
    parser.add_argument("--db-path", default=None, help="SQLite DB path (defaults to DB_PATH env or app default)")
    parser.add_argument("--force", action="store_true", help="Force bypass in-memory source cache for this run")
    args = parser.parse_args()

    if args.db_path:
        nas_web_app.DB_PATH = str(Path(args.db_path))

    with nas_web_app.db_conn() as conn:
        nas_web_app.ensure_schema(conn)
        result = nas_web_app.refresh_ticker_master(conn, force_refresh=args.force)

    print(json.dumps({"ok": True, **result}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
