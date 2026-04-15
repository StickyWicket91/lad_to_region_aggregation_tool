#!/usr/bin/env python3
"""
Config loader for the ONS MYE Aggregator.

Reads config.yaml from the project root (one level above this file) and
returns a typed Config dataclass.  If the file does not exist or a key is
absent, the built-in default for that key is used — so the pipeline works
out of the box with no config file at all.

Supported YAML sections
-----------------------
paths             data_dir, output_dir
[source]          years, url, filename, label
[geography]       geoportal_services
[validation]      max_yoy_pct
"""

import os
import warnings
from dataclasses import dataclass

import yaml

# ---------------------------------------------------------------------------
# PATHS
# ---------------------------------------------------------------------------

# Directory containing this file (code/)
_CODE_DIR = os.path.dirname(os.path.abspath(__file__))

# Project root — one level above code/
PROJECT_DIR = os.path.dirname(_CODE_DIR)

DEFAULT_CONFIG_PATH = os.path.join(_CODE_DIR, "config.yaml")

# ---------------------------------------------------------------------------
# BUILT-IN DEFAULTS
# (mirror of the values previously hard-coded in aggregate_mye.py)
# ---------------------------------------------------------------------------

_DEFAULT_DATA_DIR: str = os.path.join(PROJECT_DIR, "input")
_DEFAULT_OUTPUT_DIR: str = os.path.join(PROJECT_DIR, "output")

_DEFAULT_YEARS: list[int] = list(range(2011, 2025))

_DEFAULT_URL: str = (
    "https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/"
    "populationandmigration/populationestimates/datasets/"
    "populationestimatesforukenglandandwalesscotlandandnorthernireland/"
    "mid2011tomid2024/myebtablesuk20112024.xlsx"
)

_DEFAULT_FILENAME: str = "myebtablesuk20112024.xlsx"

_DEFAULT_LABEL: str = (
    "MYE 2011-2024 (single file, post-2021 Census methodology)"
)

_DEFAULT_GEOPORTAL_SERVICES: list[str] = [
    "LAD24_RGN24_EN_LU",
    "LAD23_RGN23_EN_LU",
    "LAD22_RGN22_EN_LU",
]

_DEFAULT_MAX_YOY_PCT: float = 3.0

# ---------------------------------------------------------------------------
# CONFIG DATACLASS
# ---------------------------------------------------------------------------


@dataclass
class Config:
    """
    Typed configuration for the pipeline and QA validator.

    All fields have built-in defaults and can be overridden via config.yaml.
    """

    # [paths]
    data_dir: str
    """Absolute path to the directory containing raw input data files."""

    output_dir: str
    """Absolute path to the directory where output CSVs are written."""

    # [source]
    years: list[int]
    """Calendar years to aggregate (e.g. [2011, ..., 2024])."""

    url: str
    """ONS download URL.  Set to '' to use a pre-placed file in data_dir."""

    filename: str
    """Expected filename inside data_dir."""

    label: str
    """Human-readable description shown in pipeline logs."""

    # [geography]
    geoportal_services: list[str]
    """Geoportal LAD->Region service names to try, newest-boundary first."""

    # [validation]
    max_yoy_pct: float
    """Maximum plausible year-on-year population change (%) per area."""


# ---------------------------------------------------------------------------
# LOADER
# ---------------------------------------------------------------------------


def load_config(path: str | None = None) -> Config:
    """
    Load configuration from a YAML file, merging with built-in defaults.

    Parameters
    ----------
    path : str or None
        Path to a YAML config file.  Defaults to config.yaml in the project
        root (one level above this module).  If the file does not exist, all
        built-in defaults are used silently.

    Returns
    -------
    Config
    """
    if path is None:
        path = DEFAULT_CONFIG_PATH

    raw: dict = {}
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as fh:
                raw = yaml.safe_load(fh) or {}
        except Exception as exc:  # noqa: BLE001
            warnings.warn(
                f"Could not parse config file '{path}': {exc}. "
                "Using built-in defaults.",
                stacklevel=2,
            )

    paths = raw.get("paths", {})
    src = raw.get("source", {})
    geo = raw.get("geography", {})
    val = raw.get("validation", {})

    # Resolve data_dir / output_dir: if relative, treat as relative to PROJECT_DIR
    def _resolve(value: str) -> str:
        if os.path.isabs(value):
            return value
        return os.path.normpath(os.path.join(PROJECT_DIR, value))

    data_dir = _resolve(paths.get("data_dir", _DEFAULT_DATA_DIR))
    output_dir = _resolve(paths.get("output_dir", _DEFAULT_OUTPUT_DIR))

    return Config(
        data_dir=data_dir,
        output_dir=output_dir,
        years=src.get("years", _DEFAULT_YEARS),
        url=src.get("url", _DEFAULT_URL),
        filename=src.get("filename", _DEFAULT_FILENAME),
        label=src.get("label", _DEFAULT_LABEL),
        geoportal_services=geo.get("geoportal_services", _DEFAULT_GEOPORTAL_SERVICES),
        max_yoy_pct=float(val.get("max_yoy_pct", _DEFAULT_MAX_YOY_PCT)),
    )
