from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from krx_alpha.contracts.backtest_contract import (
    validate_backtest_metrics,
    validate_backtest_trades,
)
from krx_alpha.contracts.price_contract import validate_processed_price_frame
from krx_alpha.contracts.signal_contract import validate_final_signal_frame

TRADE_COLUMNS = [
    "ticker",
    "signal_date",
    "entry_date",
    "exit_date",
    "entry_price",
    "exit_price",
    "gross_return",
    "net_return",
    "holding_days",
    "signal_confidence",
]

METRIC_COLUMNS = [
    "ticker",
    "trade_count",
    "win_rate",
    "average_return",
    "cumulative_return",
    "max_drawdown",
    "sharpe_ratio",
    "exposure_count",
]


@dataclass(frozen=True)
class BacktestConfig:
    holding_days: int = 5
    transaction_cost_bps: float = 15.0
    slippage_bps: float = 10.0
    tradable_action: str = "buy_candidate"


class SimpleBacktester:
    """Backtest final signals using next-day entry to avoid lookahead bias."""

    def __init__(self, config: BacktestConfig | None = None) -> None:
        self.config = config or BacktestConfig()

    def run(self, price_frame: Any, signal_frame: Any) -> tuple[Any, Any]:
        validate_processed_price_frame(price_frame)
        validate_final_signal_frame(signal_frame)

        prices = price_frame.copy().sort_values(["ticker", "date"]).reset_index(drop=True)
        signals = signal_frame.copy().sort_values(["ticker", "date"]).reset_index(drop=True)
        prices["date"] = pd.to_datetime(prices["date"]).dt.date
        signals["date"] = pd.to_datetime(signals["date"]).dt.date

        trades = self._build_trades(prices, signals)
        metrics = self._build_metrics(trades, signals)

        validate_backtest_trades(trades)
        validate_backtest_metrics(metrics)
        return trades, metrics

    def _build_trades(self, prices: Any, signals: Any) -> Any:
        rows: list[dict[str, object]] = []
        tradable_signals = signals[signals["final_action"] == self.config.tradable_action]

        for _, signal in tradable_signals.iterrows():
            ticker_prices = prices[prices["ticker"] == signal["ticker"]].reset_index(drop=True)
            future_prices = ticker_prices[ticker_prices["date"] > signal["date"]].reset_index(
                drop=True
            )
            if len(future_prices) <= self.config.holding_days:
                continue

            entry = future_prices.iloc[0]
            exit_row = future_prices.iloc[self.config.holding_days]
            entry_price = float(entry["open"])
            exit_price = float(exit_row["close"])
            gross_return = exit_price / entry_price - 1
            total_cost = (self.config.transaction_cost_bps + self.config.slippage_bps) / 10000
            net_return = gross_return - total_cost

            rows.append(
                {
                    "ticker": str(signal["ticker"]),
                    "signal_date": signal["date"],
                    "entry_date": entry["date"],
                    "exit_date": exit_row["date"],
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "gross_return": gross_return,
                    "net_return": net_return,
                    "holding_days": self.config.holding_days,
                    "signal_confidence": float(signal["confidence_score"]),
                }
            )

        return pd.DataFrame(rows, columns=TRADE_COLUMNS)

    def _build_metrics(self, trades: Any, signals: Any) -> Any:
        ticker = str(signals["ticker"].iloc[0])
        exposure_count = int((signals["final_action"] == self.config.tradable_action).sum())

        if trades.empty:
            metrics = pd.DataFrame(
                [
                    {
                        "ticker": ticker,
                        "trade_count": 0,
                        "win_rate": 0.0,
                        "average_return": 0.0,
                        "cumulative_return": 0.0,
                        "max_drawdown": 0.0,
                        "sharpe_ratio": 0.0,
                        "exposure_count": exposure_count,
                    }
                ],
                columns=METRIC_COLUMNS,
            )
            return metrics

        returns = trades["net_return"].astype(float)
        equity_curve = (1 + returns).cumprod()
        drawdown = equity_curve / equity_curve.cummax() - 1
        sharpe_ratio = _sharpe_ratio(returns)

        metrics = pd.DataFrame(
            [
                {
                    "ticker": ticker,
                    "trade_count": int(len(trades)),
                    "win_rate": float((returns > 0).mean()),
                    "average_return": float(returns.mean()),
                    "cumulative_return": float(equity_curve.iloc[-1] - 1),
                    "max_drawdown": float(drawdown.min()),
                    "sharpe_ratio": sharpe_ratio,
                    "exposure_count": exposure_count,
                }
            ],
            columns=METRIC_COLUMNS,
        )
        return metrics


def _sharpe_ratio(returns: pd.Series) -> float:
    if len(returns) < 2:
        return 0.0

    standard_deviation = float(returns.std(ddof=1))
    if standard_deviation == 0:
        return 0.0

    return float(np.sqrt(252 / 5) * returns.mean() / standard_deviation)
