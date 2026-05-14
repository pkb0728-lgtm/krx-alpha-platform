# 운영 Runbook

이 문서는 KRX Alpha Platform을 매일 어떻게 실행하고, 결과를 어떤 순서로 확인하면 되는지 정리한 한국어 운영 가이드입니다.

중요: 이 프로젝트는 실제 주문을 보내지 않습니다. 현재 KIS 연동은 **모의투자 토큰 발급, 잔고 조회, 검토 후보 생성**까지만 지원합니다.

## 매일 실행하는 기본 명령어

VSCode 터미널에서 가상환경을 켭니다.

```powershell
.\.venv\Scripts\Activate.ps1
```

전체 운영 흐름을 실행합니다.

```powershell
python main.py run-daily-job --universe demo --start 2024-01-01 --end 2024-01-31 --kis-paper-candidates --telegram-dry-run
```

정상 실행 예시는 이런 형태입니다.

```text
Daily scheduled job completed.
Success: 3
Screening passed: 1/3
KIS review candidates: 0/3
Operations health: ...operations_health_latest.parquet
Telegram: dry-run
```

이 명령어가 하는 일:

1. 유니버스 종목 실행
2. 종목별 데이터/피처/신호 생성
3. 유니버스 리포트 생성
4. 자동 스크리너 생성
5. KIS 모의투자 잔고 기반 후보 생성
6. 페이퍼 포트폴리오 계산
7. 운영 상태 점검
8. 텔레그램 메시지 미리보기
9. 실험 로그 저장

## 실행 후 먼저 확인할 것

| 확인 항목 | 정상 상태 | 문제가 있을 때 |
| --- | --- | --- |
| Universe | `Success`가 `Total`과 같음 | CSV의 `error` 컬럼 확인 |
| Screening | `Screening passed`가 출력됨 | `reports/screening/` 확인 |
| KIS candidates | `No order was sent` 문구가 있음 | 이 문구가 없으면 중단 |
| Operations health | 문제 수가 0에 가까움 | `reports/monitoring/operations_health_latest.md` 확인 |
| Telegram | 개발 중에는 `dry-run` 사용 | 실제 전송 전 메시지 내용 확인 |

## 대시보드 확인 순서

대시보드 실행:

```powershell
streamlit run src/krx_alpha/dashboard/app.py
```

브라우저 주소:

```text
http://localhost:8501
```

확인 순서:

1. `Universe Ranking`: 종목들이 성공적으로 처리됐는지 확인
2. `Auto Screener`: 통과 후보와 보류 사유 확인
3. `KIS Paper Review Candidates`: KIS 모의투자 후보와 `Orders sent = 0` 확인
4. `Paper Portfolio`: 가상 포트폴리오 거래/현금/노출 확인
5. `Operations Health`: 산출물이 정상인지 확인

## KIS 후보 상태 해석

| 상태 | 의미 | 사람이 해야 할 일 |
| --- | --- | --- |
| `review_buy` | 매수 검토 후보 | 뉴스, 공시, 유동성, 리스크를 직접 확인 |
| `review_add` | 기존 보유 종목 추가 검토 | 현재 비중과 리스크를 직접 확인 |
| `manual_price_required` | 기준 가격이 부족함 | 가격/피처를 다시 확인 |
| `hold_review` | 관망 | 계속 모니터링 |
| `skip` | 조건 미달 또는 리스크 차단 | 특별한 근거 없으면 제외 |

`review_buy`라고 해도 바로 매수하라는 뜻이 아닙니다. 이 프로젝트의 출력은 항상 **사람 검토용 후보**입니다.

## 투자 판단 전 체크리스트

후보를 실제 투자 판단에 참고하기 전에는 반드시 확인합니다.

- 거래정지, 관리종목, 이상 공시 여부
- 최근 뉴스가 신호와 충돌하지 않는지
- 유동성이 충분한지
- 시장 국면이 `bear` 또는 `high_volatility`가 아닌지
- 신호가 한 가지 약한 근거에만 의존하지 않는지
- 포지션 비중이 계좌 리스크에 맞는지
- 실적 발표, 금리 발표, FOMC 등 이벤트 리스크가 있는지
- 데이터 기준일이 내가 보려는 날짜와 맞는지

하나라도 불확실하면 `hold_review` 또는 `skip`으로 보는 것이 안전합니다.

## 안전한 실행 모드

개발 중에는 항상 dry-run을 먼저 사용합니다.

```powershell
python main.py send-telegram-daily --dry-run
python main.py run-daily-job --universe demo --lookback-days 60 --telegram-dry-run
python main.py run-daily-job --universe demo --lookback-days 60 --kis-paper-candidates --telegram-dry-run
```

텔레그램 전송을 실제로 하고 싶을 때만:

```powershell
python main.py run-daily-job --universe demo --lookback-days 60 --kis-paper-candidates --telegram-send
```

이 경우에도 실제 주문은 나가지 않습니다.

## 자주 나오는 문제

### KIS 계좌번호 형식 오류

오류:

```text
KIS_ACCOUNT_NO must be 10 digits or formatted as 8 digits-2 digits.
```

`.env`를 이렇게 수정합니다.

```text
KIS_ACCOUNT_NO=12345678-01
```

실제 계좌번호는 채팅이나 GitHub에 올리지 말고 `.env`에만 저장합니다.

### KIS 후보가 0개인 경우

정상일 수 있습니다. 조건이 부족하면 후보를 만들지 않는 것이 오히려 안전합니다.

확인할 파일:

```text
reports/screening/
reports/kis_paper_candidates/
```

자주 나오는 이유:

- `final_action_watch`
- `confidence_below_threshold`
- `action_not_allowed`
- `risk_blocked`

### 대시보드가 예전 결과를 보여줄 때

먼저 daily job을 다시 실행합니다.

```powershell
python main.py run-daily-job --universe demo --start 2024-01-01 --end 2024-01-31 --kis-paper-candidates --telegram-dry-run
```

그 다음 브라우저를 새로고침합니다.

### 텔레그램 SSL 오류

`CERTIFICATE_VERIFY_FAILED`가 나오면 `docs/troubleshooting.md`를 확인합니다. 코드에서 SSL 검증을 끄면 안 됩니다.

## 주간 점검

일주일에 한 번 정도 실행합니다.

```powershell
pytest
python main.py check-operations --skip-apis
python main.py show-experiments --limit 20
```

확인할 것:

- 테스트가 통과하는지
- 운영 상태가 정상인지
- 실험 로그가 쌓이는지
- 후보 조건이 너무 빡빡하거나 너무 느슨하지 않은지
- Drift 경고가 반복되는지

## 면접에서 설명하기

짧게 설명하면:

```text
이 프로젝트는 한국 주식 데이터를 수집하고 검증한 뒤,
기술적 지표, 재무, 공시, 수급, 뉴스, 거시 데이터를 함께 반영해
설명 가능한 투자 검토 후보를 생성하는 운영형 데이터 플랫폼입니다.
실제 주문은 보내지 않고, 사람이 최종 판단하도록 설계했습니다.
```

강조하면 좋은 점:

- 단순 주가 예측이 아니라 운영 가능한 데이터 플랫폼
- ETL, Data Contract, Feature Store 구조
- 시장 국면과 리스크 필터를 먼저 확인
- 스크리너와 KIS 모의투자 후보 생성
- 백테스트와 Walk-forward 검증
- Telegram과 Streamlit으로 운영 흐름 제공
- API 키와 실제 주문 기능을 안전하게 분리
