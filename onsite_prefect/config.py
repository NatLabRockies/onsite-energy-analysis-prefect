from __future__ import annotations

from enum import Enum
from functools import cache
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from pydantic.experimental.missing_sentinel import MISSING


class Technology(str, Enum):
    lfr = "CSP: Linear Fresnel Reflector"
    ptc = "CSP: Parabolic Trough Collector"
    pt = "CSP: Power Tower"
    pv = "PV"
    wind = "Wind"


class SizingStrategy(str, Enum):
    A = "A: Base Load"
    B = "B: Annual Load"
    C = "C: Max Generation"

    @property
    def cli_value(self) -> str:
        return self.name


MATCH_IDS_DIR = Path(__file__).resolve().parent / "match_ids"


class Range(BaseModel):
    model_config = ConfigDict(extra="forbid")

    start_index: int | MISSING = Field(
        default=MISSING,
        ge=1,
        json_schema_extra={"position": 0},
    )
    end_index: int | MISSING = Field(
        default=MISSING,
        ge=1,
        json_schema_extra={"position": 1},
    )


class SiteID(BaseModel):
    model_config = ConfigDict(title="Site IDs", extra="forbid")

    site_ids: list[str] = Field(
        title="Site IDs",
        min_length=1,
    )


class Config(BaseModel):
    model_config = ConfigDict(extra="forbid")

    technology: Technology = Field(
        description="Select the technology to run",
        json_schema_extra={"position": 0},
    )
    sizing_strategy: SizingStrategy = Field(
        json_schema_extra={"position": 1},
    )
    overwrite_existing_results: bool = Field(
        default=True,
        json_schema_extra={"position": 2},
    )
    sites: Range | SiteID = Field(
        default_factory=Range,
        json_schema_extra={"position": 3},
    )


def coerce_config(raw_config: Any) -> Config:
    if isinstance(raw_config, Config):
        return raw_config

    return Config.model_validate(raw_config)


def get_match_ids_path(technology: Technology) -> Path:
    match_ids_path = MATCH_IDS_DIR / f"{technology.name}.csv"
    if not match_ids_path.is_file():
        raise FileNotFoundError(f"Match IDs CSV not found for {technology.value}: {match_ids_path}")

    return match_ids_path


@cache
def load_match_ids(technology: Technology) -> tuple[str, ...]:
    match_ids_path = get_match_ids_path(technology)

    with match_ids_path.open("r", encoding="utf-8") as handle:
        return tuple(line.strip() for line in handle if line.strip())


def get_match_ids(technology: Technology) -> list[str]:
    return list(load_match_ids(technology))


def get_total_scenarios(technology: Technology) -> int:
    return len(load_match_ids(technology))


def get_requested_match_ids(config: Config) -> list[str]:
    if isinstance(config.sites, Range):
        start_index, end_index = resolve_range(config)
        return list(load_match_ids(config.technology)[start_index - 1:end_index])

    return list(config.sites.site_ids)


def resolve_range(config: Config) -> tuple[int, int]:
    if not isinstance(config.sites, Range):
        raise TypeError("Range-based site selection is required.")

    max_scenarios = get_total_scenarios(config.technology)
    start_index = 1 if config.sites.start_index is MISSING else config.sites.start_index
    end_index = max_scenarios if config.sites.end_index is MISSING else config.sites.end_index
    return start_index, end_index


def describe_site_selection(config: Config) -> str:
    if isinstance(config.sites, Range):
        start_index, end_index = resolve_range(config)
        return f"Range: {start_index:,} - {end_index:,}"

    num_sites = len(config.sites.site_ids)
    if num_sites == 1:
        return f"ID: {config.sites.site_ids[0]}"

    return f"IDs: {num_sites:,}"


def build_flow_run_name(config: Config) -> str:
    return (
        f"{config.technology.value} | "
        f"Option {config.sizing_strategy.value} | "
        f"{describe_site_selection(config)}"
    )


def get_total_target(config: Config) -> int:
    return len(get_requested_match_ids(config))


def validate_config(config: Config) -> None:
    if not isinstance(config.sites, Range):
        return

    max_scenarios = get_total_scenarios(config.technology)
    start_index, end_index = resolve_range(config)

    if start_index > max_scenarios:
        raise ValueError(
            f"start_index ({start_index:,}) cannot exceed the max for "
            f"{config.technology.value} ({max_scenarios:,})"
        )

    if end_index > max_scenarios:
        raise ValueError(
            f"end_index ({end_index:,}) cannot exceed the max for "
            f"{config.technology.value} ({max_scenarios:,})"
        )

    if end_index < start_index:
        raise ValueError(
            f"end_index ({end_index:,}) cannot be less than start_index ({start_index:,})"
        )
