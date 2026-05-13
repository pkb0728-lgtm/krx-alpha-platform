import pandas as pd

from krx_alpha.reports.backtest_report import BacktestReportGenerator


def test_backtest_report_generator_creates_markdown() -> None:
    trades = pd.DataFrame(
        {
            "ticker": ["005930"],
            "signal_date": ["2024-01-03"],
            "entry_date": ["2024-01-04"],
            "exit_date": ["2024-01-09"],
            "entry_price": [100.0],
            "exit_price": [110.0],
            "gross_return": [0.1],
            "net_return": [0.0975],
            "holding_days": [5],
            "signal_confidence": [75.0],
        }
    )
    metrics = pd.DataFrame(
        {
            "ticker": ["005930"],
            "trade_count": [1],
            "win_rate": [1.0],
            "average_return": [0.0975],
            "cumulative_return": [0.0975],
            "max_drawdown": [0.0],
            "sharpe_ratio": [0.0],
            "exposure_count": [1],
        }
    )

    report = BacktestReportGenerator().generate(trades, metrics)

    assert "# Backtest Report: 005930" in report
    assert "Trade count: 1" in report
    assert "Cumulative return: 9.75%" in report
