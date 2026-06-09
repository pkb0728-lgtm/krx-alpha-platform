# 최종 인수인계 가이드

이 문서는 Codex 도움 없이도 KRX Alpha Platform을 실행하고, 결과를 확인하고,
포트폴리오로 설명할 수 있도록 정리한 최종 체크리스트입니다.

## 현재 결론

이 프로젝트는 **로컬 PC에서 실행 가능한 운영형 금융 데이터 분석 플랫폼
MVP** 상태입니다. 실제 주문은 보내지 않고, 장 마감 후 데이터를 수집해서
종목 점수, 스크리너, 페이퍼 검증, 리포트, 대시보드, 텔레그램 알림을
만드는 구조입니다.

| 구분 | 상태 |
| --- | --- |
| 포트폴리오 제출 | 가능 |
| 로컬 실행 | 가능 |
| 매일 1회 분석 | 가능 |
| 실제 주문 | 구현하지 않음 |
| 상용 서비스 | 추가 운영 고도화 필요 |

## 매일 사용하는 순서

1. VSCode에서 프로젝트 폴더를 엽니다.

```text
C:\Users\USER\Documents\Codex\2026-05-13\role-python-mlops-github-vscode-python
```

2. VSCode 터미널에서 가상환경을 켭니다.

```powershell
.\.venv\Scripts\Activate.ps1
```

3. API 상태를 확인합니다.

```powershell
python main.py check-apis --save
```

4. 실제 텔레그램 전송 전 미리보기로 실행합니다.

```powershell
python main.py run-daily-job --universe large_cap --lookback-days 180 --kis-paper-candidates --telegram-dry-run
```

5. 결과가 괜찮으면 텔레그램으로 보냅니다.

```powershell
python main.py run-daily-job --universe large_cap --lookback-days 180 --kis-paper-candidates --telegram-send
```

6. 대시보드를 켭니다.

```powershell
streamlit run src/krx_alpha/dashboard/app.py
```

브라우저에서 엽니다.

```text
http://localhost:8501
```

## 대시보드에서 먼저 볼 것

| 화면 | 확인할 내용 |
| --- | --- |
| 유니버스 순위 | 어떤 종목이 상위 점수인지 확인 |
| 자동 스크리너 | 통과/탈락 사유 확인 |
| KIS 모의투자 검토 후보 | 실제 주문 없이 후보 수량만 확인 |
| 페이퍼 포트폴리오 | 가상 거래 수, 수익률, 최종 평가금액 확인 |
| 워크포워드 검증 | 기간을 나눠도 전략이 무너지지 않는지 확인 |
| ML 확률 베이스라인 | 단순 규칙 외 확률 모델의 참고 성능 확인 |
| 운영 상태 | 주요 산출물이 정상 생성됐는지 확인 |

후보가 0개여도 오류가 아닐 수 있습니다. 이 프로젝트는 조건이 부족하면
무리하게 매수 후보를 만들지 않는 보수적인 구조입니다.

## 결과를 나중에 비교하는 방법

매일 실행한 판단은 `decision_journal`에 누적됩니다. 며칠 뒤 실제 수익률과
비교하려면 아래 명령을 실행합니다.

```powershell
python main.py evaluate-decision-journal --holding-days 5
python main.py evaluate-decision-journal --holding-days 20
```

아직 미래 가격 데이터가 부족한 행은 `pending`으로 남습니다. 이는 정상입니다.

## 포트폴리오로 보여주는 순서

면접이나 GitHub 리뷰에서는 아래 순서로 보여주면 됩니다.

```powershell
python main.py doctor
python main.py check-apis --save
python main.py run-daily-job --universe large_cap --lookback-days 180 --kis-paper-candidates --telegram-dry-run
streamlit run src/krx_alpha/dashboard/app.py
pytest
```

설명할 핵심은 다음입니다.

- 단순 가격 예측기가 아니라 운영형 데이터 분석 플랫폼입니다.
- raw, processed, features, signals, backtest 계층으로 ETL을 구성했습니다.
- 뉴스, 거시환경, 수급, 재무, 공시, 가격 피처를 점수화합니다.
- 최종 판단은 사람이 하는 Human-in-the-loop 구조입니다.
- 실제 주문은 보내지 않고 KIS 모의투자 후보까지만 만듭니다.
- 백테스트, 워크포워드, 페이퍼 포트폴리오로 결과를 검증합니다.
- pytest, ruff, mypy, GitHub Actions로 품질을 관리합니다.

## 자주 막히는 상황

| 상황 | 해결 |
| --- | --- |
| `No such command check-api-health` | 올바른 명령은 `python main.py check-apis --save` |
| KIS 403, 1분당 1회 오류 | KIS 토큰 발급 제한입니다. 1분 정도 기다렸다가 다시 실행 |
| Streamlit 이메일 입력 화면 | 빈칸으로 Enter |
| 후보가 0개 | 오류가 아닐 수 있음. 기준을 통과한 종목이 없다는 뜻 |
| 대시보드가 예전 날짜처럼 보임 | `run-daily-job --lookback-days 180`을 다시 실행 |
| 텔레그램 전송 전 확인하고 싶음 | `--telegram-dry-run` 사용 |
| 빠른 로컬 점검만 하고 싶음 | `--no-score-external-features` 또는 `--no-refresh-dashboard-artifacts` 사용 |

## 종료 방법

대시보드를 끄려면 Streamlit이 실행 중인 터미널에서:

```powershell
Ctrl + C
```

그 다음 VSCode를 닫으면 됩니다.

## 더 개발한다면 우선순위

1. 실제 사용 결과를 2~4주 누적해서 의사결정 저널 평가
2. README에 대시보드 캡처 이미지 추가
3. 관심 종목 파일 기반 유니버스 추가
4. 업종/테마별 비교 화면 추가
5. LightGBM 또는 앙상블 모델 추가
6. Windows 작업 스케줄러로 장 마감 후 자동 실행

지금은 큰 기능을 더 넣기보다, 실제 실행 결과를 쌓고 문서와 화면을
정리하는 단계가 가장 좋습니다.
