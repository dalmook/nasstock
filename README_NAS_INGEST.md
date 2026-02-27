# NAS 단독 운영 가이드 (DB 기반 알림 대상 관리)

현재 구성은 NAS에서 아래를 모두 처리합니다.
- 데이터 적재: `ingest_prices.py`
- 텔레그램 알림: `nas_alert_runner.py`
- HTML 리포트: `GET /report/{token}`
- 내 종목 관리 + 사용자/채팅방 관리: `GET/POST /manage`
  - 종목 검색 자동완성: `GET /api/search_tickers?q=...`

## 1) 파일 구성
`/volume1/docker/stock_alert`에 아래 파일 배치:
- `Dockerfile`
- `requirements.txt`
- `chat-configs.js`
- `ingest_prices.py`
- `nas_alert_runner.py`
- `nas_web_app.py`
- `synology_task_scheduler_commands.txt`

## 2) 이미지 빌드
```bash
cd /volume1/docker/stock_alert
docker build --no-cache -t stock-price-ingest:latest .
```

## 3) 웹 서비스 실행 (1회)
```bash
/usr/local/bin/docker run -d \
  --name stock-alert-web \
  --restart unless-stopped \
  -p 18080:8000 \
  -e DB_PATH=/data/stock_prices.db \
  -e SYMBOLS_FILE=/app/chat-configs.js \
  -v /volume1/docker/stock_alert/data:/data \
  -v /volume1/docker/stock_alert/chat-configs.js:/app/chat-configs.js:ro \
  stock-price-ingest:latest \
  uvicorn nas_web_app:app --host 0.0.0.0 --port 8000
```

## 4) 초기 관리자 생성 (최초 1회)
1. `https://<NAS_HOST>:18080/manage` 접속
2. "초기 관리자 생성" 화면에서 입력
- name: 로그인 ID
- pin: 비밀번호
- chat_id: 텔레그램 chat_id

## 5) 알림 대상(채팅방) 추가 방법 (이제 파일 수정 불필요)
1. `/manage` 로그인
2. 관리자 계정이면 하단 "알림 대상(사용자) 관리"에서 추가
- name/pin/chat_id 입력
- enabled 체크
3. 저장 후 다음 알림 스케줄부터 자동 반영

참고:
- `nas_alert_runner.py`는 우선 `manage_users(enabled=1)`의 chat_id를 대상으로 전송
- DB에 사용자가 없을 때만 `chat-configs.js`의 chat_id 키를 fallback으로 사용

## 6) 종목 추가/삭제
- 각 사용자가 `/manage`에서 직접 종목 추가/삭제, drop rule 수정 가능
- 변경값은 `chat_config_overrides` 테이블 저장
- `ingest_prices.py`도 override 종목까지 자동 적재

## 7) 수동 테스트
```bash
/usr/local/bin/docker run --rm -v /volume1/docker/stock_alert/data:/data -v /volume1/docker/stock_alert/chat-configs.js:/app/chat-configs.js:ro stock-price-ingest:latest python /app/ingest_prices.py --mode daily --db-path /data/stock_prices.db --symbols-file /app/chat-configs.js
```

```bash
BOT_TOKEN='YOUR_TELEGRAM_BOT_TOKEN'; WEB_BASE_URL='https://your-nas-host:18080'; /usr/local/bin/docker run --rm -v /volume1/docker/stock_alert/data:/data -v /volume1/docker/stock_alert/chat-configs.js:/app/chat-configs.js:ro stock-price-ingest:latest python /app/nas_alert_runner.py --task daily_report --db-path /data/stock_prices.db --symbols-file /app/chat-configs.js --telegram-token "$BOT_TOKEN" --web-base-url "$WEB_BASE_URL"
```

## 8) DB 확인
```bash
sqlite3 /volume1/docker/stock_alert/data/stock_prices.db "SELECT name, chat_id, enabled, is_admin, updated_at_utc FROM manage_users ORDER BY name;"
```
```bash
sqlite3 /volume1/docker/stock_alert/data/stock_prices.db "SELECT chat_id, report_token, updated_at_utc FROM chat_reports ORDER BY updated_at_utc DESC LIMIT 20;"
```
```bash
sqlite3 /volume1/docker/stock_alert/data/stock_prices.db "SELECT chat_id, updated_at_utc FROM chat_config_overrides ORDER BY updated_at_utc DESC LIMIT 20;"
```
