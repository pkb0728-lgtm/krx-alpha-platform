import pandas as pd

from krx_alpha.reports.paper_trading_report import PaperTradingReportGenerator


def test_paper_trading_report_generator_creates_markdown() -> None:
    trades = pd.DataFrame(
        {
            "date": ["2024-01-03"],
            "execution_date": ["2024-01-04"],
            "ticker": ["005930"],
            "side": ["buy"],
            "status": ["filled"],
            "shares": [10],
            "execution_price": [100.0],
            "gross_amount": [1000.0],
            "fees": [1.5],
            "realized_pnl": [0.0],
            "cash_after": [8998.5],
            "position_qty_after": [10],
            "position_value_after": [1000.0],
            "equity_after": [9998.5],
            "signal_action": ["buy_candidate"],
            "confidence_score": [75.0],
            "reason": ["paper buy"],
            "mode": ["paper"],
        }
    )
    positions = pd.DataFrame(
        {
            "ticker": ["005930"],
            "shares": [10],
            "average_price": [100.0],
            "cost_basis": [1001.5],
            "last_price": [110.0],
            "market_value": [1100.0],
            "unrealized_pnl": [98.5],
            "unrealized_return": [0.09835],
            "position_pct": [11.0],
            "mode": ["paper"],
            "updated_at": [pd.Timestamp("2026-05-14T00:00:00Z")],
        }
    )
    summary = pd.DataFrame(
        {
            "ticker": ["005930"],
            "initial_cash": [10_000.0],
            "ending_cash": [8998.5],
            "ending_position_value": [1100.0],
            "ending_equity": [10_098.5],
            "cumulative_return": [0.00985],
            "realized_pnl": [0.0],
            "unrealized_pnl": [98.5],
            "trade_count": [1],
            "buy_count": [1],
            "sell_count": [0],
            "exposure_count": [1],
            "win_rate": [0.0],
            "mode": ["paper"],
            "generated_at": [pd.Timestamp("2026-05-14T00:00:00Z")],
            "universe": ["demo"],
            "requested_ticker_count": [3],
            "loaded_ticker_count": [2],
            "skipped_tickers": ["000660"],
        }
    )

    report = PaperTradingReportGenerator().generate(trades, positions, summary)

    assert "# Paper Trading Report" in report
    assert "No broker API or real order was called" in report
    assert "- Universe: demo" in report
    assert "- Skipped tickers: 000660" in report
    assert "| 005930 | 10 |" in report
    assert "paper buy" in report
