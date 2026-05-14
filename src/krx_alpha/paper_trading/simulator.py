from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from krx_alpha.contracts.paper_trading_contract import (
    validate_paper_positions,
    validate_paper_summary,
    validate_paper_trades,
)
from krx_alpha.contracts.price_contract import validate_processed_price_frame
from krx_alpha.contracts.signal_contract import validate_final_signal_frame

PAPER_TRADE_COLUMNS = [
    "date",
    "execution_date",
    "ticker",
    "side",
    "status",
    "shares",
    "execution_price",
    "gross_amount",
    "fees",
    "realized_pnl",
    "cash_after",
    "position_qty_after",
    "position_value_after",
    "equity_after",
    "signal_action",
    "confidence_score",
    "reason",
    "mode",
]

PAPER_POSITION_COLUMNS = [
    "ticker",
    "shares",
    "average_price",
    "cost_basis",
    "last_price",
    "market_value",
    "unrealized_pnl",
    "unrealized_return",
    "position_pct",
    "mode",
    "updated_at",
]

PAPER_SUMMARY_COLUMNS = [
    "ticker",
    "initial_cash",
    "ending_cash",
    "ending_position_value",
    "ending_equity",
    "cumulative_return",
    "realized_pnl",
    "unrealized_pnl",
    "trade_count",
    "buy_count",
    "sell_count",
    "exposure_count",
    "win_rate",
    "mode",
    "generated_at",
]


@dataclass(frozen=True)
class PaperTradingConfig:
    initial_cash: float = 10_000_000.0
    max_position_pct: float = 10.0
    transaction_cost_bps: float = 15.0
    slippage_bps: float = 10.0
    buy_actions: tuple[str, ...] = ("buy_candidate",)
    exit_actions: tuple[str, ...] = ("avoid", "blocked")


@dataclass
class _PositionState:
    shares: int = 0
    average_price: float = 0.0
    cost_basis: float = 0.0


class PaperTradingSimulator:
    """Convert final signals into paper-only executions and portfolio state."""

    def __init__(self, config: PaperTradingConfig | None = None) -> None:
        self.config = config or PaperTradingConfig()

    def run(self, price_frame: Any, signal_frame: Any) -> tuple[Any, Any, Any]:
        validate_processed_price_frame(price_frame)
        validate_final_signal_frame(signal_frame)

        prices = price_frame.copy().sort_values(["ticker", "date"]).reset_index(drop=True)
        signals = signal_frame.copy().sort_values(["date", "ticker"]).reset_index(drop=True)
        prices["date"] = pd.to_datetime(prices["date"]).dt.date
        signals["date"] = pd.to_datetime(signals["date"]).dt.date
        prices["ticker"] = prices["ticker"].astype(str).str.zfill(6)
        signals["ticker"] = signals["ticker"].astype(str).str.zfill(6)

        trades, positions = self._simulate(prices, signals)
        summary = self._summarize(prices, signals, trades, positions)

        validate_paper_trades(trades)
        validate_paper_positions(positions)
        validate_paper_summary(summary)
        return trades, positions, summary

    def _simulate(self, prices: pd.DataFrame, signals: pd.DataFrame) -> tuple[Any, Any]:
        cash = float(self.config.initial_cash)
        positions: dict[str, _PositionState] = {}
        rows: list[dict[str, object]] = []

        for _, signal in signals.iterrows():
            ticker = str(signal["ticker"])
            action = str(signal["final_action"])
            position = positions.setdefault(ticker, _PositionState())
            next_price = _next_open_price(prices, ticker, signal["date"])

            if action in self.config.buy_actions and position.shares == 0:
                if next_price is None:
                    rows.append(self._pending_row(signal, "buy", cash, position))
                    continue
                row, cash, positions[ticker] = self._buy(signal, next_price, cash, position)
                rows.append(row)
            elif action in self.config.exit_actions and position.shares > 0:
                if next_price is None:
                    rows.append(self._pending_row(signal, "sell", cash, position))
                    continue
                row, cash, positions[ticker] = self._sell(signal, next_price, cash, position)
                rows.append(row)

        trade_frame = pd.DataFrame(rows, columns=PAPER_TRADE_COLUMNS)
        position_frame = self._positions_frame(prices, positions, cash)
        return trade_frame, position_frame

    def _buy(
        self,
        signal: pd.Series,
        next_price: pd.Series,
        cash: float,
        position: _PositionState,
    ) -> tuple[dict[str, object], float, _PositionState]:
        execution_price = float(next_price["open"]) * (1 + self.config.slippage_bps / 10000)
        suggested_pct = _safe_float(
            signal.get("suggested_position_pct"),
            self.config.max_position_pct,
        )
        target_pct = min(max(suggested_pct, 0.0), self.config.max_position_pct)
        budget = cash * target_pct / 100
        shares = int(np.floor(budget / execution_price))
        if shares <= 0:
            row = self._skipped_row(signal, "buy", "skipped_insufficient_cash", cash, position)
            return row, cash, position

        gross_amount = shares * execution_price
        fees = gross_amount * self.config.transaction_cost_bps / 10000
        total_cost = gross_amount + fees
        if total_cost > cash:
            row = self._skipped_row(signal, "buy", "skipped_insufficient_cash", cash, position)
            return row, cash, position

        cash_after = cash - total_cost
        new_position = _PositionState(
            shares=shares,
            average_price=execution_price,
            cost_basis=total_cost,
        )
        position_value = shares * execution_price
        return (
            {
                "date": signal["date"],
                "execution_date": next_price["date"],
                "ticker": signal["ticker"],
                "side": "buy",
                "status": "filled",
                "shares": shares,
                "execution_price": execution_price,
                "gross_amount": gross_amount,
                "fees": fees,
                "realized_pnl": 0.0,
                "cash_after": cash_after,
                "position_qty_after": shares,
                "position_value_after": position_value,
                "equity_after": cash_after + position_value,
                "signal_action": signal["final_action"],
                "confidence_score": float(signal["confidence_score"]),
                "reason": signal["signal_reason"],
                "mode": "paper",
            },
            cash_after,
            new_position,
        )

    def _sell(
        self,
        signal: pd.Series,
        next_price: pd.Series,
        cash: float,
        position: _PositionState,
    ) -> tuple[dict[str, object], float, _PositionState]:
        execution_price = float(next_price["open"]) * (1 - self.config.slippage_bps / 10000)
        gross_amount = position.shares * execution_price
        fees = gross_amount * self.config.transaction_cost_bps / 10000
        proceeds = gross_amount - fees
        realized_pnl = proceeds - position.cost_basis
        cash_after = cash + proceeds
        new_position = _PositionState()
        return (
            {
                "date": signal["date"],
                "execution_date": next_price["date"],
                "ticker": signal["ticker"],
                "side": "sell",
                "status": "filled",
                "shares": position.shares,
                "execution_price": execution_price,
                "gross_amount": gross_amount,
                "fees": fees,
                "realized_pnl": realized_pnl,
                "cash_after": cash_after,
                "position_qty_after": 0,
                "position_value_after": 0.0,
                "equity_after": cash_after,
                "signal_action": signal["final_action"],
                "confidence_score": float(signal["confidence_score"]),
                "reason": signal["signal_reason"],
                "mode": "paper",
            },
            cash_after,
            new_position,
        )

    def _pending_row(
        self,
        signal: pd.Series,
        side: str,
        cash: float,
        position: _PositionState,
    ) -> dict[str, object]:
        return self._skipped_row(signal, side, "pending_no_next_price", cash, position)

    def _skipped_row(
        self,
        signal: pd.Series,
        side: str,
        status: str,
        cash: float,
        position: _PositionState,
    ) -> dict[str, object]:
        return {
            "date": signal["date"],
            "execution_date": pd.NaT,
            "ticker": signal["ticker"],
            "side": side,
            "status": status,
            "shares": 0,
            "execution_price": 0.0,
            "gross_amount": 0.0,
            "fees": 0.0,
            "realized_pnl": 0.0,
            "cash_after": cash,
            "position_qty_after": position.shares,
            "position_value_after": 0.0,
            "equity_after": cash,
            "signal_action": signal["final_action"],
            "confidence_score": float(signal["confidence_score"]),
            "reason": signal["signal_reason"],
            "mode": "paper",
        }

    def _positions_frame(
        self,
        prices: pd.DataFrame,
        positions: dict[str, _PositionState],
        cash: float,
    ) -> pd.DataFrame:
        rows: list[dict[str, object]] = []
        open_positions = {
            ticker: position for ticker, position in positions.items() if position.shares > 0
        }
        total_position_value = sum(
            position.shares * _last_close_price(prices, ticker)
            for ticker, position in open_positions.items()
        )
        ending_equity = cash + total_position_value

        for ticker, position in open_positions.items():
            last_price = _last_close_price(prices, ticker)
            market_value = position.shares * last_price
            unrealized_pnl = market_value - position.cost_basis
            rows.append(
                {
                    "ticker": ticker,
                    "shares": position.shares,
                    "average_price": position.average_price,
                    "cost_basis": position.cost_basis,
                    "last_price": last_price,
                    "market_value": market_value,
                    "unrealized_pnl": unrealized_pnl,
                    "unrealized_return": unrealized_pnl / position.cost_basis
                    if position.cost_basis
                    else 0.0,
                    "position_pct": market_value / ending_equity * 100 if ending_equity else 0.0,
                    "mode": "paper",
                    "updated_at": pd.Timestamp.now(tz="UTC"),
                }
            )
        return pd.DataFrame(rows, columns=PAPER_POSITION_COLUMNS)

    def _summarize(
        self,
        prices: pd.DataFrame,
        signals: pd.DataFrame,
        trades: pd.DataFrame,
        positions: pd.DataFrame,
    ) -> pd.DataFrame:
        filled_trades = trades[trades["status"] == "filled"] if not trades.empty else trades
        buy_count = int((filled_trades["side"] == "buy").sum()) if not filled_trades.empty else 0
        sell_rows = (
            filled_trades[filled_trades["side"] == "sell"]
            if not filled_trades.empty
            else filled_trades
        )
        sell_count = int(len(sell_rows))
        realized_pnl = float(sell_rows["realized_pnl"].sum()) if not sell_rows.empty else 0.0
        win_rate = float((sell_rows["realized_pnl"] > 0).mean()) if not sell_rows.empty else 0.0
        ending_position_value = (
            float(positions["market_value"].sum()) if not positions.empty else 0.0
        )
        unrealized_pnl = float(positions["unrealized_pnl"].sum()) if not positions.empty else 0.0
        ending_cash = _ending_cash(trades, self.config.initial_cash)
        ending_equity = ending_cash + ending_position_value
        exposure_count = int(signals["final_action"].isin(self.config.buy_actions).sum())
        tickers = ",".join(sorted({str(ticker) for ticker in prices["ticker"].unique()}))

        summary = pd.DataFrame(
            [
                {
                    "ticker": tickers,
                    "initial_cash": float(self.config.initial_cash),
                    "ending_cash": ending_cash,
                    "ending_position_value": ending_position_value,
                    "ending_equity": ending_equity,
                    "cumulative_return": ending_equity / self.config.initial_cash - 1,
                    "realized_pnl": realized_pnl,
                    "unrealized_pnl": unrealized_pnl,
                    "trade_count": int(len(filled_trades)),
                    "buy_count": buy_count,
                    "sell_count": sell_count,
                    "exposure_count": exposure_count,
                    "win_rate": win_rate,
                    "mode": "paper",
                    "generated_at": pd.Timestamp.now(tz="UTC"),
                }
            ],
            columns=PAPER_SUMMARY_COLUMNS,
        )
        return summary


def _next_open_price(prices: pd.DataFrame, ticker: str, signal_date: Any) -> pd.Series | None:
    ticker_prices = prices[prices["ticker"] == ticker].sort_values("date")
    future_prices = ticker_prices[ticker_prices["date"] > signal_date]
    if future_prices.empty:
        return None
    return future_prices.iloc[0]


def _last_close_price(prices: pd.DataFrame, ticker: str) -> float:
    ticker_prices = prices[prices["ticker"] == ticker].sort_values("date")
    if ticker_prices.empty:
        return 0.0
    return float(ticker_prices.iloc[-1]["close"])


def _ending_cash(trades: pd.DataFrame, initial_cash: float) -> float:
    if trades.empty or "cash_after" not in trades.columns:
        return float(initial_cash)
    filled_or_skipped = trades[trades["cash_after"].notna()]
    if filled_or_skipped.empty:
        return float(initial_cash)
    return float(filled_or_skipped.iloc[-1]["cash_after"])


def _safe_float(value: Any, default: float) -> float:
    try:
        if pd.isna(value):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default
