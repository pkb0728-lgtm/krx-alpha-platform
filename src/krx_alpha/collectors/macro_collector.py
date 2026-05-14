from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol

import pandas as pd

from krx_alpha.contracts.macro_contract import validate_macro_frame

FRED_OBSERVATIONS_URL = "https://api.stlouisfed.org/fred/series/observations"

DEFAULT_MACRO_SERIES = ("DGS10", "DFF", "DEXKOUS")
MACRO_SERIES_NAMES = {
    "DGS10": "US 10-Year Treasury Constant Maturity Rate",
    "DFF": "Effective Federal Funds Rate",
    "DEXKOUS": "South Korean Won to US Dollar Exchange Rate",
}

STANDARD_MACRO_COLUMNS = [
    "date",
    "series_id",
    "series_name",
    "value",
    "source",
    "collected_at",
]


class FredJsonProvider(Protocol):
    def __call__(
        self,
        series_id: str,
        start_date: str,
        end_date: str,
        api_key: str,
    ) -> dict[str, Any]:
        """Return a FRED observations JSON response for one series."""


@dataclass(frozen=True)
class MacroRequest:
    start_date: date
    end_date: date
    series_ids: tuple[str, ...] = DEFAULT_MACRO_SERIES
    demo: bool = True

    @classmethod
    def from_strings(
        cls,
        start_date: str,
        end_date: str,
        series_ids: str | tuple[str, ...] = DEFAULT_MACRO_SERIES,
        demo: bool = True,
    ) -> "MacroRequest":
        if isinstance(series_ids, str):
            parsed_series = tuple(
                item.strip().upper() for item in series_ids.split(",") if item.strip()
            )
        else:
            parsed_series = tuple(item.strip().upper() for item in series_ids if item.strip())

        if not parsed_series:
            parsed_series = DEFAULT_MACRO_SERIES

        return cls(
            start_date=date.fromisoformat(start_date),
            end_date=date.fromisoformat(end_date),
            series_ids=parsed_series,
            demo=demo,
        )

    @property
    def compact_start_date(self) -> str:
        return self.start_date.strftime("%Y%m%d")

    @property
    def compact_end_date(self) -> str:
        return self.end_date.strftime("%Y%m%d")

    @property
    def series_slug(self) -> str:
        return "_".join(self.series_ids)


class FredMacroCollector:
    source_name = "fred"
    demo_source_name = "fred_demo"

    def __init__(
        self,
        api_key: str | None = None,
        provider: FredJsonProvider | None = None,
    ) -> None:
        self.api_key = api_key
        self.provider = provider

    def collect(self, request: MacroRequest) -> pd.DataFrame:
        if request.demo:
            frame = _demo_macro_frame(request)
        else:
            if not self.api_key:
                raise ValueError("FRED_API_KEY must be set for live FRED collection.")
            provider = self.provider or _fred_observations_provider()
            frames = [
                _normalize_fred_payload(
                    provider(
                        series_id,
                        request.start_date.isoformat(),
                        request.end_date.isoformat(),
                        self.api_key,
                    ),
                    series_id=series_id,
                    source=self.source_name,
                )
                for series_id in request.series_ids
            ]
            frame = pd.concat(frames, ignore_index=True)

        frame = frame[STANDARD_MACRO_COLUMNS].sort_values(["date", "series_id"]).reset_index(
            drop=True
        )
        validate_macro_frame(frame)
        return frame


def _fred_observations_provider() -> FredJsonProvider:
    def provider(
        series_id: str,
        start_date: str,
        end_date: str,
        api_key: str,
    ) -> dict[str, Any]:
        try:
            import requests
        except ImportError as exc:
            raise RuntimeError(
                "requests is not installed. Run: python -m pip install -e .[data]"
            ) from exc

        response = requests.get(
            FRED_OBSERVATIONS_URL,
            params={
                "series_id": series_id,
                "api_key": api_key,
                "file_type": "json",
                "observation_start": start_date,
                "observation_end": end_date,
            },
            timeout=15,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("FRED response is not a JSON object.")
        return payload

    return provider


def _normalize_fred_payload(payload: dict[str, Any], series_id: str, source: str) -> pd.DataFrame:
    observations = payload.get("observations", [])
    if not isinstance(observations, list) or not observations:
        raise ValueError(f"FRED returned no observations for {series_id}.")

    rows: list[dict[str, Any]] = []
    for observation in observations:
        if not isinstance(observation, dict):
            continue

        observed_date = pd.to_datetime(observation.get("date"), errors="coerce")
        if pd.isna(observed_date):
            continue

        rows.append(
            {
                "date": observed_date.date(),
                "series_id": series_id.upper(),
                "series_name": MACRO_SERIES_NAMES.get(series_id.upper(), series_id.upper()),
                "value": _safe_float(observation.get("value")),
                "source": source,
                "collected_at": pd.Timestamp.now(tz="UTC"),
            }
        )

    if not rows:
        raise ValueError(f"No usable FRED observations remained for {series_id}.")
    return pd.DataFrame(rows, columns=STANDARD_MACRO_COLUMNS)


def _demo_macro_frame(request: MacroRequest) -> pd.DataFrame:
    dates = pd.date_range(request.start_date, request.end_date, freq="B")
    rows: list[dict[str, Any]] = []
    for index, current_date in enumerate(dates):
        demo_values = {
            "DGS10": 4.15 + index * 0.01,
            "DFF": 5.33,
            "DEXKOUS": 1320.0 + index * 1.8,
        }
        for series_id in request.series_ids:
            normalized_series = series_id.upper()
            rows.append(
                {
                    "date": current_date.date(),
                    "series_id": normalized_series,
                    "series_name": MACRO_SERIES_NAMES.get(normalized_series, normalized_series),
                    "value": float(demo_values.get(normalized_series, 50.0 + index)),
                    "source": FredMacroCollector.demo_source_name,
                    "collected_at": pd.Timestamp.now(tz="UTC"),
                }
            )
    return pd.DataFrame(rows, columns=STANDARD_MACRO_COLUMNS)


def _safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("nan")
