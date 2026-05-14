from typing import Any

REQUIRED_PAPER_TRADE_COLUMNS = {
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
}

REQUIRED_PAPER_POSITION_COLUMNS = {
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
}

REQUIRED_PAPER_SUMMARY_COLUMNS = {
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
}


def validate_paper_trades(frame: Any) -> None:
    missing_columns = REQUIRED_PAPER_TRADE_COLUMNS - set(frame.columns)
    if missing_columns:
        raise ValueError(f"Missing required paper trade columns: {sorted(missing_columns)}")

    if frame.empty:
        return

    if frame["ticker"].astype(str).str.fullmatch(r"\d{6}").all() is False:
        raise ValueError("Paper trade ticker must be a six-digit code.")

    if (frame["shares"].dropna() < 0).any():
        raise ValueError("Paper trade shares must be non-negative.")

    if (frame["cash_after"].dropna() < -1e-6).any():
        raise ValueError("Paper trading cash cannot be negative.")

    if not frame["mode"].eq("paper").all():
        raise ValueError("Paper trade mode must be paper.")


def validate_paper_positions(frame: Any) -> None:
    missing_columns = REQUIRED_PAPER_POSITION_COLUMNS - set(frame.columns)
    if missing_columns:
        raise ValueError(f"Missing required paper position columns: {sorted(missing_columns)}")

    if frame.empty:
        return

    if frame["ticker"].astype(str).str.fullmatch(r"\d{6}").all() is False:
        raise ValueError("Paper position ticker must be a six-digit code.")

    if (frame["shares"].dropna() <= 0).any():
        raise ValueError("Open paper positions must have positive shares.")

    if not frame["mode"].eq("paper").all():
        raise ValueError("Paper position mode must be paper.")


def validate_paper_summary(frame: Any) -> None:
    missing_columns = REQUIRED_PAPER_SUMMARY_COLUMNS - set(frame.columns)
    if missing_columns:
        raise ValueError(f"Missing required paper summary columns: {sorted(missing_columns)}")

    if frame.empty:
        raise ValueError("Paper trading summary frame is empty.")

    if (frame[["initial_cash", "ending_equity"]] < 0).any().any():
        raise ValueError("Paper trading summary contains negative capital.")

    if frame["win_rate"].dropna().between(0, 1).all() is False:
        raise ValueError("Paper trading win_rate values must be between 0 and 1.")

    if not frame["mode"].eq("paper").all():
        raise ValueError("Paper summary mode must be paper.")
