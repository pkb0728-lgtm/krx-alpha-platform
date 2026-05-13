from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from krx_alpha.backtest.simple_backtester import BacktestConfig, SimpleBacktester
from krx_alpha.contracts.backtest_contract import (
    validate_walk_forward_folds,
    validate_walk_forward_summary,
)
from krx_alpha.contracts.price_contract import validate_processed_price_frame
from krx_alpha.contracts.signal_contract import validate_final_signal_frame

WALK_FORWARD_FOLD_COLUMNS = [
    "ticker",
    "fold",
    "train_start",
    "train_end",
    "test_start",
    "test_end",
    "signal_count",
    "trade_count",
    "win_rate",
    "average_return",
    "cumulative_return",
    "max_drawdown",
    "sharpe_ratio",
    "exposure_count",
]

WALK_FORWARD_SUMMARY_COLUMNS = [
    "ticker",
    "fold_count",
    "total_trade_count",
    "total_exposure_count",
    "average_win_rate",
    "average_return",
    "compounded_return",
    "worst_max_drawdown",
    "average_sharpe_ratio",
    "positive_fold_ratio",
]


@dataclass(frozen=True)
class WalkForwardConfig:
    train_size: int = 20
    test_size: int = 5
    step_size: int = 5
    holding_days: int = 5
    transaction_cost_bps: float = 15.0
    slippage_bps: float = 10.0


class WalkForwardBacktester:
    """Evaluate signal stability across rolling out-of-sample windows."""

    def __init__(self, config: WalkForwardConfig | None = None) -> None:
        self.config = config or WalkForwardConfig()

    def run(self, price_frame: Any, signal_frame: Any) -> tuple[Any, Any]:
        validate_processed_price_frame(price_frame)
        validate_final_signal_frame(signal_frame)

        if self.config.train_size <= 0 or self.config.test_size <= 0 or self.config.step_size <= 0:
            raise ValueError("Walk-forward train_size, test_size, and step_size must be positive.")

        prices = price_frame.copy().sort_values(["ticker", "date"]).reset_index(drop=True)
        signals = signal_frame.copy().sort_values(["ticker", "date"]).reset_index(drop=True)
        prices["date"] = pd.to_datetime(prices["date"]).dt.date
        signals["date"] = pd.to_datetime(signals["date"]).dt.date

        unique_dates = sorted(signals["date"].unique())
        fold_rows: list[dict[str, object]] = []
        fold_number = 1

        for start_index in range(self.config.train_size, len(unique_dates), self.config.step_size):
            train_dates = unique_dates[start_index - self.config.train_size : start_index]
            test_dates = unique_dates[start_index : start_index + self.config.test_size]
            if not test_dates:
                break

            fold_signals = signals[signals["date"].isin(test_dates)].reset_index(drop=True)
            if fold_signals.empty:
                continue

            _, metrics = SimpleBacktester(_simple_config(self.config)).run(prices, fold_signals)
            metric = metrics.iloc[0]
            fold_rows.append(
                {
                    "ticker": str(metric["ticker"]),
                    "fold": fold_number,
                    "train_start": train_dates[0],
                    "train_end": train_dates[-1],
                    "test_start": test_dates[0],
                    "test_end": test_dates[-1],
                    "signal_count": int(len(fold_signals)),
                    "trade_count": int(metric["trade_count"]),
                    "win_rate": float(metric["win_rate"]),
                    "average_return": float(metric["average_return"]),
                    "cumulative_return": float(metric["cumulative_return"]),
                    "max_drawdown": float(metric["max_drawdown"]),
                    "sharpe_ratio": float(metric["sharpe_ratio"]),
                    "exposure_count": int(metric["exposure_count"]),
                }
            )
            fold_number += 1

        folds = pd.DataFrame(fold_rows, columns=WALK_FORWARD_FOLD_COLUMNS)
        summary = _build_summary(folds, str(signals["ticker"].iloc[0]))

        validate_walk_forward_folds(folds)
        validate_walk_forward_summary(summary)
        return folds, summary


def _simple_config(config: WalkForwardConfig) -> BacktestConfig:
    return BacktestConfig(
        holding_days=config.holding_days,
        transaction_cost_bps=config.transaction_cost_bps,
        slippage_bps=config.slippage_bps,
    )


def _build_summary(folds: pd.DataFrame, ticker: str) -> pd.DataFrame:
    if folds.empty:
        return pd.DataFrame(
            [
                {
                    "ticker": ticker,
                    "fold_count": 0,
                    "total_trade_count": 0,
                    "total_exposure_count": 0,
                    "average_win_rate": 0.0,
                    "average_return": 0.0,
                    "compounded_return": 0.0,
                    "worst_max_drawdown": 0.0,
                    "average_sharpe_ratio": 0.0,
                    "positive_fold_ratio": 0.0,
                }
            ],
            columns=WALK_FORWARD_SUMMARY_COLUMNS,
        )

    cumulative_returns = folds["cumulative_return"].astype(float)
    compounded_return = float(np.prod(1 + cumulative_returns) - 1)
    return pd.DataFrame(
        [
            {
                "ticker": ticker,
                "fold_count": int(len(folds)),
                "total_trade_count": int(folds["trade_count"].sum()),
                "total_exposure_count": int(folds["exposure_count"].sum()),
                "average_win_rate": float(folds["win_rate"].mean()),
                "average_return": float(folds["average_return"].mean()),
                "compounded_return": compounded_return,
                "worst_max_drawdown": float(folds["max_drawdown"].min()),
                "average_sharpe_ratio": float(folds["sharpe_ratio"].mean()),
                "positive_fold_ratio": float((cumulative_returns > 0).mean()),
            }
        ],
        columns=WALK_FORWARD_SUMMARY_COLUMNS,
    )
