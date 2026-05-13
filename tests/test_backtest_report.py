import pandas as pd

from krx_alpha.reports.backtest_report import BacktestReportGenerator, WalkForwardReportGenerator


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


def test_walk_forward_report_generator_creates_markdown() -> None:
    folds = pd.DataFrame(
        {
            "ticker": ["005930"],
            "fold": [1],
            "train_start": ["2024-01-02"],
            "train_end": ["2024-01-15"],
            "test_start": ["2024-01-16"],
            "test_end": ["2024-01-20"],
            "signal_count": [5],
            "trade_count": [2],
            "win_rate": [0.5],
            "average_return": [0.02],
            "cumulative_return": [0.04],
            "max_drawdown": [-0.01],
            "sharpe_ratio": [1.2],
            "exposure_count": [5],
        }
    )
    summary = pd.DataFrame(
        {
            "ticker": ["005930"],
            "fold_count": [1],
            "total_trade_count": [2],
            "total_exposure_count": [5],
            "average_win_rate": [0.5],
            "average_return": [0.02],
            "compounded_return": [0.04],
            "worst_max_drawdown": [-0.01],
            "average_sharpe_ratio": [1.2],
            "positive_fold_ratio": [1.0],
        }
    )

    report = WalkForwardReportGenerator().generate(folds, summary)

    assert "# Walk-Forward Backtest Report: 005930" in report
    assert "Fold count: 1" in report
    assert "Positive fold ratio: 100.00%" in report
