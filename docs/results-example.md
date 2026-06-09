# Results Example

이 문서는 KRX Alpha Platform을 실행했을 때 나오는 대표 결과와 해석 방법을
정리한 예시입니다. 포트폴리오 리뷰에서는 “좋은 종목을 무조건 추천하는
도구”가 아니라, 데이터와 리스크 기준을 통과한 경우에만 사람이 다시 볼
후보를 만드는 시스템이라는 점을 강조하면 좋습니다.

## Latest Daily Job Example

Example command:

```powershell
python main.py run-daily-job --universe large_cap --lookback-days 180 --kis-paper-candidates --telegram-send
```

Latest local run:

```text
Universe: large_cap
Period: 2025-11-28 to 2026-05-27
Total: 10
Success: 10
Failed: 0
Screening passed: 0/10
KIS review candidates: 0/10
Paper trades: 0
Paper return: 0.00%
```

## Universe Summary

The latest `large_cap` universe run produced 10 successful ticker results.

| Ticker | Name | Action | Confidence | Market Regime |
| --- | --- | --- | ---: | --- |
| 068270 | 셀트리온 | watch | 55.23 | neutral |
| 105560 | KB금융 | hold | 53.62 | sideways |
| 055550 | 신한지주 | hold | 53.21 | neutral |
| 035420 | NAVER | blocked | 37.91 | bear |
| 005930 | 삼성전자 | blocked | 37.76 | high_volatility |
| 000270 | 기아 | blocked | 34.47 | high_volatility |
| 035720 | 카카오 | blocked | 34.14 | bear |
| 005380 | 현대차 | blocked | 33.85 | high_volatility |
| 000660 | SK하이닉스 | blocked | 31.85 | high_volatility |
| 051910 | LG화학 | blocked | 24.70 | high_volatility |

Interpretation:

- `watch` means the stock is worth monitoring, but it is not a buy candidate.
- `hold` means the signal is neutral or not strong enough.
- `blocked` means the risk filter blocked the signal.
- With `--lookback-days 180`, market regime labels became more informative than
  the shorter 60-day run. The result now separates neutral, sideways, bear, and
  high-volatility regimes.

## Auto Screener Result

Latest screener summary:

```text
Checked: 10
Passed: 0
```

Top review queue rows:

| Ticker | Name | Passed | Reason | Priority | Screen Score | Confidence | Risk Flags |
| --- | --- | --- | --- | --- | ---: | ---: | --- |
| 068270 | 셀트리온 | False | confidence_and_score_below_threshold | watchlist | 59.33 | 55.23 | none |
| 105560 | KB금융 | False | action_not_allowed | low | 35.77 | 53.62 | none |
| 055550 | 신한지주 | False | action_not_allowed | low | 35.62 | 53.21 | none |
| 035420 | NAVER | False | action_not_allowed | blocked | 0.00 | 37.91 | market_regime_bear |
| 005930 | 삼성전자 | False | action_not_allowed | blocked | 0.00 | 37.76 | high_short_term_volatility, weak_risk_score, market_regime_high_volatility |
| 000660 | SK하이닉스 | False | action_not_allowed | blocked | 0.00 | 31.85 | wide_daily_range, high_short_term_volatility, weak_risk_score, market_regime_high_volatility |

Important interpretation:

```text
0 passed candidates is not necessarily an error.
It means no ticker passed action, confidence, score, and risk-filter conditions
at the same time.
```

In this run, 셀트리온 was the closest review item, but it still failed the
threshold because both the score and confidence were below the configured
screening criteria.

## KIS Paper Candidate Result

The KIS paper candidate builder combines the screener output with mock account
information. It does not send real orders.

Latest example:

```text
Rows: 10
Review buy/add: 0
Manual price checks: 0
Orders sent: 0
```

All rows were `skip` because no ticker passed the screener. This is expected:
the KIS module should not create a mock buy candidate when the upstream risk and
screening rules reject every ticker.

## Paper Portfolio Result

Latest paper portfolio summary:

```text
Universe: large_cap
Loaded tickers: 10/10
Trades: 0
Cumulative return: 0.00%
Cash: 100.00%
Gross exposure: 0.00%
```

Interpretation:

- No trade was opened because no ticker became a valid buy candidate.
- The paper portfolio stayed fully in cash.
- This is a conservative and valid result, not a pipeline failure.

Dashboard audit:

```text
Filled trades: 0
Ending equity = cash + position value
10,000,000 = 10,000,000 + 0
Paper return = ending equity / initial cash - 1 = 0.00%
```

If the KIS mock-investment section shows a larger total evaluation amount, that
number comes from the KIS paper account balance endpoint. It is used only to
estimate candidate quantities and is separate from the paper portfolio's virtual
cash.

## Telegram Brief

Preview command:

```powershell
python main.py send-telegram-daily --dry-run
```

Example Korean message shape:

```text
KRX Alpha 일일 요약
생성 시각: 2026-05-27 21:00

전체 분석
- 분석 종목: 10개
- 정상 처리: 10개
- 실패: 0개

상위 종목
1. 068270 셀트리온 | 판단: 관망 | 신뢰도 55.23 | 시장: 중립
   해석: 관심 종목으로 지켜보되 아직 적극 매수 단계는 아닙니다.

자동 스크리너
- 검사 종목: 10개 / 통과: 0개
- 통과 종목 없음: 지금은 억지로 고를 종목이 없습니다.

KIS 모의투자 후보
- 전체 후보 행: 10개 / 매수·추가매수 검토: 0개 / 가격 확인 필요: 0개
- 실제 주문은 보내지 않고, 모의계좌 기준 검토 목록만 만듭니다.
```

## Dashboard

Run:

```powershell
streamlit run src/krx_alpha/dashboard/app.py
```

The dashboard displays:

- universe ranking
- beginner decision summary
- decision journal outcome tracking
- action distribution
- auto screener result and review queue
- KIS mock-investment review candidates
- news sentiment
- macro features
- paper portfolio summary
- paper portfolio audit table for trade count and ending equity
- backtest and walk-forward result
- walk-forward audit table for fold-level trade counts
- ML baseline result
- drift monitoring
- API and operations health

When candidates are zero, the dashboard shows a beginner-friendly explanation
that this can be a normal risk-management result.

## Operations Health

Example command:

```powershell
python main.py check-operations --include-apis --skip-pykrx
```

Latest local operations health had healthy core artifacts, with some optional
modeling or stale analysis artifacts marked as stale.

```text
OK: 10
STALE: 6
Problems: 0
```

Interpretation:

- `OK` means the latest required artifact exists and can be read.
- `STALE` means the file exists but is older than the configured freshness
  threshold.
- `STALE` is usually a maintenance warning, not a hard failure.

## API Health

Latest API checks were all OK:

| API | Status |
| --- | --- |
| OpenDART | OK |
| Naver Search | OK |
| Gemini | OK |
| Telegram | OK |
| KIS Paper | OK |
| FRED | OK |

This means the configured credentials were reachable at the time of the check.

## Backtest And Walk-Forward

Example command:

```powershell
python main.py run-pipeline --ticker 005380 --start 2024-01-01 --end 2024-03-31
python main.py analyze-regime --ticker 005380 --start 2024-01-01 --end 2024-03-31
python main.py backtest-stock --ticker 005380 --start 2024-01-01 --end 2024-03-31
python main.py walk-forward-backtest --ticker 005380 --start 2024-01-01 --end 2024-03-31 --train-size 20 --test-size 5 --step-size 5
```

The backtest is intentionally simple and exists to validate signal behavior. It
is not a production execution simulator.

## ML Baseline

Example command:

```powershell
python main.py build-ml-dataset --ticker 005380 --start 2024-01-01 --end 2024-03-31 --holding-days 5
python main.py train-ml-baseline --ticker 005380 --start 2024-01-01 --end 2024-03-31 --holding-days 5
```

The current ML baseline is intentionally explainable and lightweight. Its
purpose is to show the training/evaluation workflow before adding heavier
models such as LightGBM or an ensemble.

## Portfolio Review Talking Point

Use this short explanation in an interview:

```text
This project does not force a daily recommendation. It collects multiple Korean
market data sources, generates explainable signals, applies risk filters, and
only creates review candidates when the evidence and risk conditions are good
enough. If no stock passes the screen, the platform reports "no candidate" and
keeps the paper portfolio in cash.
```
