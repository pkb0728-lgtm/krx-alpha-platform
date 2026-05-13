from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol

import pandas as pd

from krx_alpha.contracts.price_contract import validate_price_frame
from krx_alpha.utils.external_output import suppress_external_output

KOREAN_PRICE_COLUMNS = {
    "날짜": "date",
    "시가": "open",
    "고가": "high",
    "저가": "low",
    "종가": "close",
    "거래량": "volume",
    "거래대금": "trading_value",
    "등락률": "change_rate",
}

STANDARD_PRICE_COLUMNS = [
    "date",
    "ticker",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trading_value",
    "trading_value_is_estimated",
    "change_rate",
    "source",
    "collected_at",
]


class PriceProvider(Protocol):
    def __call__(self, start_date: str, end_date: str, ticker: str, adjusted: bool) -> Any:
        """Return raw OHLCV data for a ticker."""


@dataclass(frozen=True)
class PriceRequest:
    ticker: str
    start_date: date
    end_date: date
    adjusted: bool = True

    @classmethod
    def from_strings(
        cls,
        ticker: str,
        start_date: str,
        end_date: str,
        adjusted: bool = True,
    ) -> "PriceRequest":
        return cls(
            ticker=ticker.zfill(6),
            start_date=date.fromisoformat(start_date),
            end_date=date.fromisoformat(end_date),
            adjusted=adjusted,
        )

    @property
    def pykrx_start_date(self) -> str:
        return self.start_date.strftime("%Y%m%d")

    @property
    def pykrx_end_date(self) -> str:
        return self.end_date.strftime("%Y%m%d")


class PykrxPriceCollector:
    source_name = "pykrx"

    def __init__(self, provider: PriceProvider | None = None) -> None:
        self._provider = provider or self._load_default_provider()

    def collect(self, request: PriceRequest) -> Any:
        raw_frame = self._provider(
            request.pykrx_start_date,
            request.pykrx_end_date,
            request.ticker,
            request.adjusted,
        )
        frame = self._normalize(raw_frame, request.ticker)
        validate_price_frame(frame)
        return frame

    def _load_default_provider(self) -> PriceProvider:
        try:
            with suppress_external_output():
                from pykrx import stock
        except ImportError as exc:
            raise RuntimeError(
                "pykrx is not installed. Run: python -m pip install -e .[data]"
            ) from exc

        def provider(start_date: str, end_date: str, ticker: str, adjusted: bool) -> Any:
            with suppress_external_output():
                return stock.get_market_ohlcv_by_date(
                    start_date,
                    end_date,
                    ticker,
                    adjusted=adjusted,
                )

        return provider

    def _normalize(self, raw_frame: Any, ticker: str) -> Any:
        frame = raw_frame.copy()

        if "date" not in frame.columns and "날짜" not in frame.columns:
            frame.index.name = frame.index.name or "date"
            frame = frame.reset_index()

        frame = frame.rename(columns=KOREAN_PRICE_COLUMNS)
        first_column = frame.columns[0]
        if "date" not in frame.columns:
            frame = frame.rename(columns={first_column: "date"})

        frame["date"] = pd.to_datetime(frame["date"]).dt.date
        frame["ticker"] = ticker
        frame["source"] = self.source_name
        frame["collected_at"] = pd.Timestamp.now(tz="UTC")

        if "trading_value" not in frame.columns:
            frame["trading_value"] = frame["close"] * frame["volume"]
            frame["trading_value_is_estimated"] = True
        else:
            frame["trading_value_is_estimated"] = False

        if "change_rate" not in frame.columns:
            frame["change_rate"] = pd.NA

        return frame[STANDARD_PRICE_COLUMNS].sort_values(["date", "ticker"]).reset_index(drop=True)
