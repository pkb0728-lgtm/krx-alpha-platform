# 포트폴리오 제출 가이드

이 문서는 KRX Alpha Platform을 GitHub 포트폴리오로 보여줄 때 어떤 순서로
설명하면 좋은지 정리한 한국어 가이드입니다.

## 한 문장 소개

한국 주식 데이터를 수집하고 품질을 검증한 뒤, 시장 국면, 종목 점수,
리스크 필터, 자동 스크리너, 백테스트, 페이퍼트레이딩, 텔레그램 알림,
대시보드까지 연결한 설명 가능한 투자 의사결정 보조 플랫폼입니다.

## 보여줄 핵심 역량

| 영역 | 이 프로젝트에서 보여주는 것 |
| --- | --- |
| Python 백엔드 | Typer CLI, 모듈화된 패키지 구조, 예외 처리, 설정 관리 |
| 데이터 엔지니어링 | raw/processed/features/signals/backtest 계층형 ETL |
| 금융 데이터 | 가격, 수급, 공시, 재무, 뉴스, 거시지표 연동 구조 |
| 분석/모델링 | 기술적 피처, 시장 국면, 점수화, ML baseline, drift 점검 |
| 리스크 관리 | 변동성, 유동성, 리스크 플래그, 포지션 비중 제한 |
| 검증 | 백테스트, walk-forward, paper trading |
| 운영 | daily job, Telegram, Streamlit dashboard, API health check |
| 품질 관리 | pytest, ruff, mypy, pre-commit, GitHub Actions |
| 보안 | `.env`, `.env.example`, API 키 미커밋, 민감정보 로그 차단 |

## 면접에서 보여줄 실행 순서

1. 환경 확인

```powershell
python main.py doctor
```

2. API 연결 확인

```powershell
python main.py check-apis --save
```

3. 하루 운영 작업 미리보기

```powershell
python main.py run-daily-job --universe large_cap --lookback-days 180 --kis-paper-candidates --telegram-dry-run
```

4. 대시보드 실행

```powershell
streamlit run src/krx_alpha/dashboard/app.py
```

5. 테스트 실행

```powershell
pytest
```

## 캡처하면 좋은 화면

GitHub README나 발표 자료에 이미지를 넣을 때는 아래 화면을 캡처하면 좋습니다.

| 화면 | 보여주는 의미 |
| --- | --- |
| 대시보드 상단 요약 | 분석 종목 수, 성공/실패, 상위 종목 |
| 자동 스크리너 | 후보 선정과 탈락 사유 |
| KIS 모의투자 후보 | 실제 주문 없이 검토 후보만 생성하는 구조 |
| 텔레그램 메시지 | 장 마감 후 결과 요약 알림 |
| API Health Check | 외부 API 연결 상태 |
| pytest 결과 | 회귀 테스트와 품질 관리 |

## 후보가 0개일 때 설명하는 방법

후보가 0개라는 결과는 오류가 아닐 수 있습니다. 이 프로젝트는 무조건
매수 후보를 만드는 도구가 아니라, 조건이 부족하면 쉬는 판단을 내리는
리스크 우선 시스템입니다.

예를 들어 다음과 같은 경우 후보를 만들지 않습니다.

- 신뢰도나 스크리너 점수가 기준보다 낮음
- 단기 변동성이 높음
- 위험 점수가 약함
- 시장 국면 데이터가 부족하거나 비우호적임
- 최종 판단이 `watch`, `hold`, `blocked` 상태임

면접에서는 이렇게 설명하면 좋습니다.

```text
이 시스템은 매일 종목을 추천하는 것이 목적이 아닙니다.
데이터와 리스크 기준을 통과한 경우에만 사람이 다시 검토할 후보를 만들고,
조건이 부족하면 후보 없음으로 남기는 보수적인 의사결정 보조 도구입니다.
```

## 실제 주문을 넣지 않은 이유

이 프로젝트는 취업 포트폴리오와 개인 분석 도구 목적입니다. 그래서
한국투자증권 API는 모의투자 토큰, 잔고 조회, 후보 계산까지만 연결하고
실제 주문 API 호출은 구현하지 않았습니다.

이 설계는 다음 이유로 안전합니다.

- 실수로 실제 매매가 발생하지 않음
- Human-in-the-loop 구조를 명확히 보여줌
- 투자 조언이나 자동매매 서비스로 오해될 가능성을 줄임
- 리스크 관리와 설명 가능성을 우선한다는 설계 철학을 보여줌

## 현재 완성도

| 항목 | 상태 |
| --- | --- |
| 실행 가능한 MVP | 완료 |
| GitHub 포트폴리오 제출 | 1차 제출 가능 |
| 실제 매일 사용 | 가능하나 점수/리스크 기준 튜닝 필요 |
| 상용 서비스 수준 | 아님 |

## 다음 개선 방향

- 대시보드 스크린샷을 README에 추가
- 최신 실행 결과 예시를 `docs/results-example.md`에 반영
- 유니버스 확장: KOSPI200, KOSDAQ150, 관심종목 파일
- 시장 국면 분석을 위해 기본 lookback을 120~180일로 운영
- ML baseline을 LightGBM/앙상블로 고도화
- Windows 작업 스케줄러 기반 장 마감 후 자동 실행
