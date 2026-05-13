from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol

import pandas as pd

from krx_alpha.contracts.investor_flow_contract import validate_investor_flow_frame
from krx_alpha.utils.external_output import suppress_external_output

STANDARD_INVESTOR_FLOW_COLUMNS = [
    "date",
    "ticker",
    "individual_net_buy_value",
    "foreign_net_buy_value",
    "institution_net_buy_value",
    "other_net_buy_value",
    "total_net_buy_value",
    "individual_net_buy_volume",
    "foreign_net_buy_volume",
    "institution_net_buy_volume",
    "other_net_buy_volume",
    "total_net_buy_volume",
    "source",
    "collected_at",
]


class InvestorFlowProvider(Protocol):
    def __call__(self, start_date: str, end_date: str, ticker: str) -> tuple[Any, Any]:
        """Return investor trading value and volume frames."""


@dataclass(frozen=True)
class InvestorFlowRequest:
    ticker: str
    start_date: date
    end_date: date
    demo: bool = True

    @classmethod
    def from_strings(
        cls,
        ticker: str,
        start_date: str,
        end_date: str,
        demo: bool = True,
    ) -> "InvestorFlowRequest":
        return cls(
            ticker=ticker.zfill(6),
            start_date=date.fromisoformat(start_date),
            end_date=date.fromisoformat(end_date),
            demo=demo,
        )

    @property
    def pykrx_start_date(self) -> str:
        return self.start_date.strftime("%Y%m%d")

    @property
    def pykrx_end_date(self) -> str:
        return self.end_date.strftime("%Y%m%d")


class PykrxInvestorFlowCollector:
    source_name = "pykrx_investor_flow"
    demo_source_name = "demo_investor_flow"

    def __init__(self, provider: InvestorFlowProvider | None = None) -> None:
        self._provider = provider

    def collect(self, request: InvestorFlowRequest) -> Any:
        if request.demo:
            frame = _demo_investor_flow(request)
        else:
            provider = self._provider or self._load_default_provider()
            value_frame, volume_frame = provider(
                request.pykrx_start_date,
                request.pykrx_end_date,
                request.ticker,
            )
            frame = _normalize_investor_flow(
                value_frame=value_frame,
                volume_frame=volume_frame,
                ticker=request.ticker,
                source=self.source_name,
            )

        validate_investor_flow_frame(frame)
        return frame

    def _load_default_provider(self) -> InvestorFlowProvider:
        try:
            with suppress_external_output():
                from pykrx import stock
        except ImportError as exc:
            raise RuntimeError(
                "pykrx is not installed. Run: python -m pip install -e .[data]"
            ) from exc

        def provider(start_date: str, end_date: str, ticker: str) -> tuple[Any, Any]:
            with suppress_external_output():
                value_frame = stock.get_market_trading_value_by_date(start_date, end_date, ticker)
                volume_frame = stock.get_market_trading_volume_by_date(start_date, end_date, ticker)
            return value_frame, volume_frame

        return provider


def _normalize_investor_flow(
    value_frame: Any,
    volume_frame: Any,
    ticker: str,
    source: str,
) -> Any:
    values = _prepare_pykrx_frame(value_frame)
    volumes = _prepare_pykrx_frame(volume_frame)
    frame = pd.DataFrame(
        {
            "date": values["date"],
            "ticker": ticker.zfill(6),
            "individual_net_buy_value": _select_investor_column(values, "individual"),
            "foreign_net_buy_value": _select_investor_column(values, "foreign"),
            "institution_net_buy_value": _select_investor_column(values, "institution"),
            "other_net_buy_value": _select_investor_column(values, "other"),
            "total_net_buy_value": _select_investor_column(values, "total"),
            "individual_net_buy_volume": _select_investor_column(volumes, "individual"),
            "foreign_net_buy_volume": _select_investor_column(volumes, "foreign"),
            "institution_net_buy_volume": _select_investor_column(volumes, "institution"),
            "other_net_buy_volume": _select_investor_column(volumes, "other"),
            "total_net_buy_volume": _select_investor_column(volumes, "total"),
            "source": source,
            "collected_at": pd.Timestamp.now(tz="UTC"),
        }
    )
    return (
        frame[STANDARD_INVESTOR_FLOW_COLUMNS].sort_values(["date", "ticker"]).reset_index(drop=True)
    )


def _prepare_pykrx_frame(raw_frame: Any) -> pd.DataFrame:
    if raw_frame is None or raw_frame.empty:
        raise ValueError(
            "Investor flow provider returned no rows. "
            "Try a different date range or use --demo when KRX data is unavailable."
        )

    frame = raw_frame.copy()
    if "date" not in frame.columns:
        frame.index.name = frame.index.name or "date"
        frame = frame.reset_index()
    frame = frame.rename(columns={frame.columns[0]: "date"})
    frame["date"] = pd.to_datetime(frame["date"]).dt.date
    return frame


def _select_investor_column(frame: pd.DataFrame, investor: str) -> pd.Series:
    candidates = {
        "individual": ("개인", "individual"),
        "foreign": ("외국인합계", "외국인", "foreign"),
        "institution": ("기관합계", "기관", "institution"),
        "other": ("기타법인", "기타", "other"),
        "total": ("전체", "합계", "total"),
    }[investor]

    for candidate in candidates:
        for column in frame.columns:
            if candidate.lower() in str(column).lower():
                return pd.to_numeric(frame[column], errors="coerce").fillna(0.0)

    return pd.Series([0.0] * len(frame), index=frame.index)


def _demo_investor_flow(request: InvestorFlowRequest) -> Any:
    dates = pd.date_range(request.start_date, request.end_date, freq="B")
    rows = []
    for index, current_date in enumerate(dates):
        foreign_value = 9_000_000_000 + index * 700_000_000
        institution_value = 3_000_000_000 + index * 350_000_000
        if index % 7 == 0:
            foreign_value = -2_000_000_000
        if index % 9 == 0:
            institution_value = -1_500_000_000
        individual_value = -(foreign_value + institution_value)
        rows.append(
            {
                "date": current_date.date(),
                "ticker": request.ticker,
                "individual_net_buy_value": float(individual_value),
                "foreign_net_buy_value": float(foreign_value),
                "institution_net_buy_value": float(institution_value),
                "other_net_buy_value": 0.0,
                "total_net_buy_value": 0.0,
                "individual_net_buy_volume": float(individual_value / 70_000),
                "foreign_net_buy_volume": float(foreign_value / 70_000),
                "institution_net_buy_volume": float(institution_value / 70_000),
                "other_net_buy_volume": 0.0,
                "total_net_buy_volume": 0.0,
                "source": PykrxInvestorFlowCollector.demo_source_name,
                "collected_at": pd.Timestamp.now(tz="UTC"),
            }
        )
    return pd.DataFrame(rows, columns=STANDARD_INVESTOR_FLOW_COLUMNS)
