#!/usr/bin/env python3
"""
ONS Mid-Year Estimates Aggregator — 2011 to 2024
=================================================
Aggregates ONS local-authority mid-year population estimates to:
  - Region level for England (9 regions)
  - Country level for Wales, Scotland, and Northern Ireland

Data sources
------------
  MYE data  : ONS "Population estimates for UK, England and Wales,
               Scotland and Northern Ireland" dataset
               https://www.ons.gov.uk/peoplepopulationandcommunity/
               populationandmigration/populationestimates/datasets/
               populationestimatesforukenglandandwalesscotlandandnorthernireland

  Geography : ONS Open Geography Portal — LAD to Region lookup (England)
              https://geoportal.statistics.gov.uk/

Usage
-----
    pip install -r requirements.txt
    python aggregate_mye.py

Output
------
    output/mye_region_country_2011_2024.csv

    Columns: year | area_code | area_name | area_type | population
"""

import logging
import os
import re
import sys

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# SETUP
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

from config_loader import load_config as _load_config  # noqa: E402

_cfg = _load_config()
DATA_DIR = _cfg.data_dir
OUTPUT_DIR = _cfg.output_dir

for _d in (DATA_DIR, OUTPUT_DIR):
    os.makedirs(_d, exist_ok=True)

# ---------------------------------------------------------------------------
# DATA SOURCE CONFIGURATION
# ---------------------------------------------------------------------------
# All user-facing parameters (years, URL, filename, paths, Geoportal services,
# validation thresholds) are read from config.yaml at runtime.
# Edit config.yaml to change pipeline behaviour; the defaults below are
# used only if config.yaml is absent or a key is missing.

# ONS Open Geography Portal — LAD to Region lookup.
# Tries services from newest to oldest boundary year so the most current
# mapping is used.  England has ~300 LADs so a single 1000-record request
# is sufficient.
_GEOPORTAL_BASE = (
    "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services"
)

# Standard ONS country codes (used as area_code in output)
_COUNTRY_META = {
    "W": ("W92000004", "Wales"),
    "S": ("S92000003", "Scotland"),
    "N": ("N92000002", "Northern Ireland"),
}


# ---------------------------------------------------------------------------
# DOWNLOAD HELPERS
# ---------------------------------------------------------------------------

def _download_file(url: str, dest: str) -> None:
    """Download *url* to *dest*, skipping if already present."""
    if os.path.exists(dest):
        log.info("    Cached  : %s", os.path.basename(dest))
        return
    log.info("    Fetching: %s", url)
    headers = {"User-Agent": "Mozilla/5.0 (ONS-MYE-Aggregator/1.0)"}
    resp = requests.get(url, headers=headers, timeout=180, stream=True)
    resp.raise_for_status()
    with open(dest, "wb") as fh:
        for chunk in resp.iter_content(chunk_size=65536):
            fh.write(chunk)
    size_kb = os.path.getsize(dest) // 1024
    log.info("    Saved   : %s (%d KB)", os.path.basename(dest), size_kb)


def _fetch_geoportal_lookup(services: list[str] | None = None) -> pd.DataFrame:
    """
    Retrieve LAD->Region lookup from the ONS Open Geography Portal.

    Tries *services* in order and returns the first successful response.
    Results are cached to data/lad_to_region_lookup.csv.

    Parameters
    ----------
    services : list[str] or None
        Geoportal service names to try.  If None, the list is read from
        config.toml (falling back to built-in defaults).

    Returns
    -------
    pd.DataFrame
        Columns: lad_code, lad_name, rgn_code, rgn_name
    """
    if services is None:
        services = _load_config().geoportal_services

    cache = os.path.join(DATA_DIR, "lad_to_region_lookup.csv")
    if os.path.exists(cache):
        log.info("    Using cached LAD->Region lookup.")
        return pd.read_csv(cache)

    last_err = None
    for service in services:
        # Extract two-digit year suffix, e.g. "24" from "LAD24_RGN24_EN_LU"
        yr = service[3:5]
        url = f"{_GEOPORTAL_BASE}/{service}/FeatureServer/0/query"
        params = {
            "where": "1=1",
            "outFields": f"LAD{yr}CD,LAD{yr}NM,RGN{yr}CD,RGN{yr}NM",
            "f": "json",
            "resultRecordCount": 1000,
        }
        try:
            log.info("    Trying Geoportal service: %s", service)
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            payload = resp.json()
            features = payload.get("features", [])
            if not features:
                log.warning("    No features returned from %s.", service)
                continue
            rows = [f["attributes"] for f in features]
            df = pd.DataFrame(rows)
            df.columns = ["lad_code", "lad_name", "rgn_code", "rgn_name"]
            df.to_csv(cache, index=False)
            log.info(
                "    Loaded %d LAD→Region mappings from %s.", len(df), service
            )
            return df
        except Exception as exc:  # noqa: BLE001
            log.warning("    %s failed: %s", service, exc)
            last_err = exc

    raise RuntimeError(
        f"Could not fetch LAD->Region lookup from any Geoportal service. "
        f"Last error: {last_err}"
    )


# ---------------------------------------------------------------------------
# DATA PARSING  —  MYEB B-table format
# ---------------------------------------------------------------------------
# The ONS B-table file uses a long layout:
#   Row 0 : title string (skipped)
#   Row 1 : column headers
#             ladcode23 | laname23 | country | sex | age
#             | population_2011 | population_2012 | … | population_2024
#   Data  : one row per LAD × sex × single year of age
#
# To obtain total population by LAD and year we sum across all age rows
# (and across both sexes if the sheet contains separate male/female rows).
# ONS persons sheets do not mix a persons total with male/female subtotals
# in the same sheet, so a plain sum gives the correct all-ages total.

# Preferred persons sheet names in priority order
_PERSONS_SHEETS = ["MYEB1", "MYE2 - Persons", "MYE2", "Persons", "Table 1"]

# Matches population_YYYY column names
_POP_COL_RE = re.compile(r"^population_(\d{4})$")


def _find_persons_sheet(xf: pd.ExcelFile) -> str:
    """Return the name of the persons data sheet from *xf*."""
    for candidate in _PERSONS_SHEETS:
        if candidate in xf.sheet_names:
            return candidate
    # Fuzzy fallback: any sheet with "person" but not sex-specific
    for s in xf.sheet_names:
        sl = s.lower()
        if "person" in sl and "male" not in sl and "female" not in sl:
            return s
    raise ValueError(
        f"Cannot identify persons sheet. Available sheets: {xf.sheet_names}"
    )


def _parse_myeb_table(xf: pd.ExcelFile, sheet: str) -> pd.DataFrame:
    """
    Parse an ONS MYEB B-table sheet into a long-format DataFrame.

    The sheet has one title row (row 0) then a header row (row 1), followed
    by data rows at LAD x sex x single-year-of-age granularity, with each
    year's population as a separate column (population_2011 ... population_2024).

    Sex and age are preserved in the output.  Rows are summed per
    (LAD, sex, age) to handle any unexpected duplicates in the source.

    Returns
    -------
    pd.DataFrame
        Columns: lad_code (str), lad_name (str), sex (str), age (int),
                 year (int), population (int)
    """
    # Row 0 is a title; row 1 is the real header
    raw = xf.parse(sheet, header=1, dtype=str)
    raw.columns = [str(c).strip().lower() for c in raw.columns]

    # ------------------------------------------------------------------ #
    # 1. Identify LAD code and name columns                              #
    # ------------------------------------------------------------------ #
    # Column names contain a boundary-year suffix (e.g. ladcode23, laname23).
    code_col = next(
        (c for c in raw.columns if re.match(r"ladcode\d*$", c) or c == "code"),
        None,
    )
    name_col = next(
        (c for c in raw.columns if re.match(r"laname\d*$", c) or c == "name"),
        None,
    )

    if code_col is None:
        raise ValueError(
            f"Cannot find LAD code column in sheet '{sheet}'. "
            f"Columns present: {list(raw.columns)}"
        )

    # ------------------------------------------------------------------ #
    # 1b. Identify sex and age columns                                   #
    # ------------------------------------------------------------------ #
    sex_col = next((c for c in raw.columns if c == "sex"), None)
    age_col = next((c for c in raw.columns if c == "age"), None)

    if sex_col is None:
        raise ValueError(
            f"Cannot find 'sex' column in sheet '{sheet}'. "
            f"Columns present: {list(raw.columns)}"
        )
    if age_col is None:
        raise ValueError(
            f"Cannot find 'age' column in sheet '{sheet}'. "
            f"Columns present: {list(raw.columns)}"
        )

    # ------------------------------------------------------------------ #
    # 2. Identify population_YYYY year columns                           #
    # ------------------------------------------------------------------ #
    year_cols: dict[str, int] = {}
    for col in raw.columns:
        m = _POP_COL_RE.match(col)
        if m:
            yr = int(m.group(1))
            if 2011 <= yr <= 2024:
                year_cols[col] = yr

    if not year_cols:
        raise ValueError(
            f"Cannot find population_YYYY columns in sheet '{sheet}'. "
            f"Columns present: {list(raw.columns)}"
        )

    log.info("    Year columns found: %s", sorted(year_cols.values()))

    # ------------------------------------------------------------------ #
    # 3. Filter to valid LAD rows and coerce population values           #
    # ------------------------------------------------------------------ #
    raw["lad_code"] = raw[code_col].astype(str).str.strip()
    raw = raw[raw["lad_code"].str.match(r"^[EWSN]\d{8}$", na=False)].copy()

    for col in year_cols:
        raw[col] = pd.to_numeric(raw[col], errors="coerce").fillna(0)

    # ------------------------------------------------------------------ #
    # 4. Aggregate: sum per (LAD, sex, age)                             #
    # The source has exactly one row per combination; the groupby acts  #
    # as a structural check and collapses any unexpected duplicates.    #
    # ------------------------------------------------------------------ #
    group_cols = (
        ["lad_code"]
        + ([name_col] if name_col else [])
        + [sex_col, age_col]
    )
    agg = raw.groupby(group_cols)[list(year_cols)].sum().reset_index()

    if name_col:
        agg.rename(columns={name_col: "lad_name"}, inplace=True)
    else:
        agg["lad_name"] = agg["lad_code"]

    # Normalise column names and convert age to int
    if sex_col != "sex":
        agg.rename(columns={sex_col: "sex"}, inplace=True)
    if age_col != "age":
        agg.rename(columns={age_col: "age"}, inplace=True)
    agg["age"] = pd.to_numeric(agg["age"], errors="coerce").fillna(-1).astype(int)

    # ------------------------------------------------------------------ #
    # 5. Melt to long format: one row per LAD × sex × age × year        #
    # ------------------------------------------------------------------ #
    long = agg.melt(
        id_vars=["lad_code", "lad_name", "sex", "age"],
        value_vars=list(year_cols),
        var_name="year_col",
        value_name="population",
    )
    long["year"] = long["year_col"].map(year_cols)
    long["population"] = long["population"].astype(int)
    long = long.drop(columns=["year_col"])

    if long.empty:
        raise ValueError(
            f"No valid LAD rows extracted from sheet '{sheet}'. "
            "Check the source file."
        )

    return long[["lad_code", "lad_name", "sex", "age", "year", "population"]].reset_index(drop=True)


# ---------------------------------------------------------------------------
# AGGREGATION
# ---------------------------------------------------------------------------

def _aggregate(
    lad_df: pd.DataFrame,
    lookup_df: pd.DataFrame,
    year: int,
) -> pd.DataFrame:
    """
    Aggregate LAD populations to regions (England) and countries
    (Wales, Scotland, Northern Ireland).

    England LADs are matched to regions via *lookup_df*.  Wales, Scotland,
    and Northern Ireland LADs are identified by their code prefix (W/S/N)
    and summed to a single country total each.

    Sex and age are preserved throughout; aggregation sums across LADs only.

    Returns
    -------
    pd.DataFrame
        Columns: year, area_code, area_name, area_type, sex, age, population
    """
    lad_df = lad_df.copy()
    lad_df["prefix"] = lad_df["lad_code"].str[0]

    # ------------------------------------------------------------------
    # England → regions
    # ------------------------------------------------------------------
    eng = lad_df[lad_df["prefix"] == "E"].merge(
        lookup_df[["lad_code", "rgn_code", "rgn_name"]],
        on="lad_code",
        how="left",
    )

    unmapped_lads = eng.loc[eng["rgn_code"].isna(), "lad_code"].unique()
    if len(unmapped_lads):
        log.warning(
            "  [%d] %d England LAD(s) not in region lookup "
            "(possible boundary change): %s",
            year, len(unmapped_lads), unmapped_lads.tolist(),
        )

    rgn_agg = (
        eng.dropna(subset=["rgn_code"])
        .groupby(["rgn_code", "rgn_name", "sex", "age"], as_index=False)["population"]
        .sum()
        .rename(columns={"rgn_code": "area_code", "rgn_name": "area_name"})
        .assign(area_type="Region")
    )

    # ------------------------------------------------------------------
    # Wales, Scotland, Northern Ireland → country totals
    # ------------------------------------------------------------------
    # Build lookup Series from the _COUNTRY_META dict
    prefix_to_code = pd.Series({p: v[0] for p, v in _COUNTRY_META.items()})
    prefix_to_name = pd.Series({p: v[1] for p, v in _COUNTRY_META.items()})

    non_eng = (
        lad_df[lad_df["prefix"] != "E"]
        .groupby(["prefix", "sex", "age"], as_index=False)["population"]
        .sum()
        .assign(
            area_code=lambda d: d["prefix"].map(prefix_to_code),
            area_name=lambda d: d["prefix"].map(prefix_to_name),
            area_type="Country",
        )
        .drop(columns=["prefix"])
    )

    missing_countries = set(_COUNTRY_META) - set(lad_df[lad_df["prefix"] != "E"]["prefix"])
    for p in missing_countries:
        log.warning(
            "  [%d] No LADs found for %s (prefix '%s') — check source file.",
            year, _COUNTRY_META[p][1], p,
        )

    out = (
        pd.concat([rgn_agg, non_eng], ignore_index=True)
        .assign(year=year)
        [["year", "area_code", "area_name", "area_type", "sex", "age", "population"]]
    )
    return out


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main() -> None:
    cfg = _load_config()
    log.info("ONS Mid-Year Estimates Aggregator")
    log.info("=" * 52)
    log.info("  Config  : %s", cfg.label)
    log.info("  Years   : %d-%d", min(cfg.years), max(cfg.years))

    # ------------------------------------------------------------------
    # Step 1 — Download raw ONS MYE file
    # ------------------------------------------------------------------
    log.info("")
    log.info("[1/3] Downloading ONS MYE data file...")
    dest = os.path.join(DATA_DIR, cfg.filename)
    if cfg.url:
        try:
            _download_file(cfg.url, dest)
        except requests.HTTPError as exc:
            log.error("  HTTP %s downloading '%s'.", exc.response.status_code, cfg.filename)
            log.error(
                "  Download the file manually from:\n"
                "  https://www.ons.gov.uk/peoplepopulationandcommunity/"
                "populationandmigration/populationestimates/datasets/"
                "populationestimatesforukenglandandwalesscotlandandnorthernireland\n"
                "  and place it as:  data/%s",
                cfg.filename,
            )
            sys.exit(1)
    else:
        if not os.path.exists(dest):
            log.error(
                "  url is empty in config.toml and file not found: %s", dest
            )
            sys.exit(1)
        log.info("  url is empty — using pre-placed file: %s", cfg.filename)

    # ------------------------------------------------------------------
    # Step 2 — Geography lookup
    # ------------------------------------------------------------------
    log.info("")
    log.info("[2/3] Fetching LAD->Region geography lookup...")
    try:
        lookup_df = _fetch_geoportal_lookup(services=cfg.geoportal_services)
    except RuntimeError as exc:
        log.error("  %s", exc)
        sys.exit(1)

    # ------------------------------------------------------------------
    # Step 3 — Parse and aggregate each year
    # ------------------------------------------------------------------
    log.info("")
    log.info("[3/3] Processing years %d-%d...", min(cfg.years), max(cfg.years))
    all_results: list[pd.DataFrame] = []

    xf = pd.ExcelFile(dest, engine="openpyxl")
    sheet = _find_persons_sheet(xf)
    log.info("  Persons sheet : '%s'", sheet)

    lad_all = _parse_myeb_table(xf, sheet)
    log.info("  Rows loaded (LAD x sex x age x year): %d", len(lad_all))

    for year in cfg.years:
        log.info("  Aggregating mid-%d ...", year)
        lad_year = lad_all[lad_all["year"] == year].copy()
        if lad_year.empty:
            log.error("  No data found for year %d — check source file.", year)
            sys.exit(1)
        log.info("    LADs : %d  (rows: %d)", lad_year["lad_code"].nunique(), len(lad_year))
        agg = _aggregate(lad_year, lookup_df, year)
        all_results.append(agg)

    # ------------------------------------------------------------------
    # Output — wide format (areas as rows, years as columns)
    # ------------------------------------------------------------------
    long = pd.concat(all_results, ignore_index=True)

    wide = (
        long
        .pivot_table(
            index=["area_code", "area_name", "area_type", "sex", "age"],
            columns="year",
            values="population",
            aggfunc="sum",
        )
        .rename_axis(columns=None)          # remove the "year" axis label
        .reset_index()
        .sort_values(["area_type", "area_name", "sex", "age"])
        .reset_index(drop=True)
    )

    output_path = os.path.join(OUTPUT_DIR, "mye_region_country_age_sex_2011_2024.csv")
    wide.to_csv(output_path, index=False)

    year_cols = [c for c in wide.columns if isinstance(c, int)]
    n_areas = wide["area_code"].nunique()
    log.info("")
    log.info("=" * 52)
    log.info("Output  : %s", output_path)
    log.info("Rows    : %d  (%d areas x 2 sexes x 91 ages)", len(wide), n_areas)
    log.info("Years   : %d-%d  (%d columns)", min(year_cols), max(year_cols), len(year_cols))
    log.info("")
    log.info("UK population totals by year:")
    for yr in year_cols:
        log.info("  %d : %s", yr, f"{wide[yr].sum():,}")

    # ------------------------------------------------------------------
    # QA validation
    # ------------------------------------------------------------------
    log.info("")
    log.info("Running QA validation...")
    try:
        from validate_output import run_validation  # noqa: PLC0415
        has_failures = run_validation(output_path)  # path already points to new file
        if has_failures:
            log.error("QA validation FAILED — review the report above.")
            sys.exit(1)
        log.info("QA validation passed.")
    except ImportError:
        log.warning(
            "validate_output.py not found — skipping QA validation. "
            "Run 'python validate_output.py' separately to check the output."
        )


if __name__ == "__main__":
    main()
