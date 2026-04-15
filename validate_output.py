#!/usr/bin/env python3
"""
QA Validator for ONS MYE Region/Country Output
===============================================
Validates mye_region_country_age_sex_2011_2024.csv against:

  - Structural completeness  - row count, column set, area codes, area types
  - Value integrity          - no nulls, all populations positive
  - Temporal plausibility    - year-on-year change within +/-3 %
  - Published reference      - exact match against stored baseline totals
  - LAD->Region lookup        - no duplicates, all 9 regions covered, expected
                               LAD counts per region

Exit codes
----------
  0 - all checks passed (FAIL count = 0)
  1 - one or more FAIL checks

Result tiers
------------
  FAIL - output is provably wrong; do not use
  WARN - output may be acceptable but requires manual review
  INFO - contextual / informational

Usage
-----
    python validate_output.py
    python validate_output.py --output path/to/output.csv
    python validate_output.py --no-reference   # skip reference comparison
    python validate_output.py --no-lookup      # skip lookup integrity checks
"""

import argparse
import os
import sys
from dataclasses import dataclass
from typing import Literal

import pandas as pd

# ---------------------------------------------------------------------------
# PATHS  (resolved via config.yaml)
# ---------------------------------------------------------------------------

from config_loader import load_config as _load_config  # noqa: E402

_cfg = _load_config()
DATA_DIR = _cfg.data_dir
OUTPUT_DIR = _cfg.output_dir

DEFAULT_OUTPUT = os.path.join(OUTPUT_DIR, "mye_region_country_age_sex_2011_2024.csv")
DEFAULT_REFERENCE = os.path.join(DATA_DIR, "published_reference_totals.csv")
DEFAULT_LOOKUP = os.path.join(DATA_DIR, "lad_to_region_lookup.csv")

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------

EXPECTED_YEARS = list(range(2011, 2025))

EXPECTED_REGION_CODES = {
    "E12000001",  # North East
    "E12000002",  # North West
    "E12000003",  # Yorkshire and The Humber
    "E12000004",  # East Midlands
    "E12000005",  # West Midlands
    "E12000006",  # East of England
    "E12000007",  # London
    "E12000008",  # South East
    "E12000009",  # South West
}
EXPECTED_COUNTRY_CODES = {
    "W92000004",  # Wales
    "S92000003",  # Scotland
    "N92000002",  # Northern Ireland
}
EXPECTED_AREA_CODES = EXPECTED_REGION_CODES | EXPECTED_COUNTRY_CODES

# Expected LAD count per region (2024 boundaries).
# A mismatch triggers a WARN rather than FAIL because boundary reorganisations
# legitimately change these counts.
EXPECTED_LAD_COUNTS: dict[str, int] = {
    "E12000001": 12,
    "E12000002": 35,
    "E12000003": 15,
    "E12000004": 35,
    "E12000005": 30,
    "E12000006": 45,
    "E12000007": 33,
    "E12000008": 64,
    "E12000009": 27,
}

# Maximum plausible year-on-year population change (%) at region/country level.
MAX_YOY_PCT = 3.0

# ---------------------------------------------------------------------------
# FINDING TYPE
# ---------------------------------------------------------------------------

Level = Literal["FAIL", "WARN", "INFO"]


@dataclass
class Finding:
    """A single QA check result with a severity level, check name, and message."""

    level: Level
    check: str
    message: str


def _f(level: Level, check: str, message: str) -> Finding:
    return Finding(level=level, check=check, message=message)


# ---------------------------------------------------------------------------
# CHECK: structural completeness
# ---------------------------------------------------------------------------

def check_structure(df: pd.DataFrame) -> list[Finding]:
    """Row count, column set, area codes, area types, sex and age presence."""
    findings: list[Finding] = []

    # --- Row count: 12 areas x 2 sexes x 91 ages ---
    expected_rows = 12 * 2 * 91  # 2,184
    if len(df) != expected_rows:
        findings.append(_f("FAIL", "row_count",
                           f"Expected {expected_rows} rows "
                           f"(12 areas x 2 sexes x 91 ages), found {len(df)}."))
    else:
        findings.append(_f("INFO", "row_count",
                           f"{expected_rows} rows present "
                           f"(12 areas x 2 sexes x 91 ages). [ok]"))

    # --- Required text columns ---
    for col in ("area_code", "area_name", "area_type", "sex", "age"):
        if col not in df.columns:
            findings.append(_f("FAIL", "required_columns",
                               f"Required column '{col}' is missing."))

    if "area_code" not in df.columns:
        return findings  # nothing more to check without area_code

    # --- Year columns ---
    year_cols = [c for c in df.columns if isinstance(c, int)]
    present_years = set(year_cols)
    missing_years = sorted(set(EXPECTED_YEARS) - present_years)
    extra_years = sorted(present_years - set(EXPECTED_YEARS))

    if missing_years:
        findings.append(_f("FAIL", "year_columns",
                           f"Missing year column(s): {missing_years}."))
    if extra_years:
        findings.append(_f("WARN", "year_columns",
                           f"Unexpected year column(s) outside 2011-2024: {extra_years}."))
    if not missing_years:
        findings.append(_f("INFO", "year_columns",
                           f"All {len(EXPECTED_YEARS)} year columns (2011-2024) present. [ok]"))

    # --- Area codes ---
    actual_codes = set(df["area_code"].astype(str))
    missing_codes = sorted(EXPECTED_AREA_CODES - actual_codes)
    extra_codes = sorted(actual_codes - EXPECTED_AREA_CODES)

    if missing_codes:
        findings.append(_f("FAIL", "area_codes",
                           f"Missing area code(s): {missing_codes}."))
    if extra_codes:
        findings.append(_f("FAIL", "area_codes",
                           f"Unexpected area code(s): {extra_codes}."))
    if not missing_codes and not extra_codes:
        findings.append(_f("INFO", "area_codes",
                           "All 12 expected area codes present with no extras. [ok]"))

    # --- Area types ---
    if "area_type" in df.columns and "area_code" in df.columns:
        bad_types = df[~df["area_type"].isin({"Region", "Country"})]["area_type"].unique()
        if len(bad_types):
            findings.append(_f("FAIL", "area_types",
                               f"Invalid area_type value(s): {bad_types.tolist()}."))

        # Count distinct area codes per type (each appears 2*91 times)
        n_region = df[df["area_type"] == "Region"]["area_code"].nunique()
        n_country = df[df["area_type"] == "Country"]["area_code"].nunique()

        if n_region != 9:
            findings.append(_f("FAIL", "area_types",
                               f"Expected 9 Region area codes, found {n_region}."))
        if n_country != 3:
            findings.append(_f("FAIL", "area_types",
                               f"Expected 3 Country area codes, found {n_country}."))
        if n_region == 9 and n_country == 3:
            findings.append(_f("INFO", "area_types",
                               "9 Region codes and 3 Country codes. [ok]"))

    return findings


# ---------------------------------------------------------------------------
# CHECK: value integrity
# ---------------------------------------------------------------------------

def check_values(df: pd.DataFrame) -> list[Finding]:
    """No null values, no negative populations, valid sex and age ranges."""
    findings: list[Finding] = []

    year_cols = [c for c in df.columns if isinstance(c, int)]
    if not year_cols:
        findings.append(_f("WARN", "value_integrity",
                           "No year columns found - value checks skipped."))
        return findings

    # Null check
    null_count = int(df[year_cols].isnull().sum().sum())
    if null_count:
        null_locs = [(str(row), str(col))
                     for col in year_cols
                     for row in df.index[df[col].isnull()]]
        findings.append(_f("FAIL", "null_values",
                           f"{null_count} null population value(s). "
                           f"First locations: {null_locs[:5]}"
                           f"{'...' if len(null_locs) > 5 else ''}."))
    else:
        findings.append(_f("INFO", "null_values", "No null values. [ok]"))

    # Negative populations (zeros are valid at single-year-of-age level)
    neg_mask = df[year_cols] < 0
    n_neg = int(neg_mask.sum().sum())
    if n_neg:
        findings.append(_f("FAIL", "negative_populations",
                           f"{n_neg} negative population value(s) found."))
    else:
        findings.append(_f("INFO", "negative_populations",
                           "No negative population values. [ok]"))

    # Sex values
    if "sex" in df.columns:
        bad_sex = df[~df["sex"].isin({"m", "f"})]["sex"].unique()
        if len(bad_sex):
            findings.append(_f("FAIL", "sex_values",
                               f"Unexpected sex value(s): {bad_sex.tolist()}. "
                               "Expected 'm' and 'f'."))
        else:
            n_m = int((df["sex"] == "m").sum())
            n_f = int((df["sex"] == "f").sum())
            findings.append(_f("INFO", "sex_values",
                               f"Sex values: m={n_m:,} rows, f={n_f:,} rows. [ok]"))

    # Age range (0–90, 91 distinct values)
    if "age" in df.columns:
        ages = pd.to_numeric(df["age"], errors="coerce")
        min_age = int(ages.min())
        max_age = int(ages.max())
        n_ages = int(ages.nunique())
        if min_age < 0 or max_age > 90 or n_ages != 91:
            findings.append(_f("FAIL", "age_values",
                               f"Unexpected age range: {min_age}-{max_age} "
                               f"({n_ages} unique values). Expected 0-90 (91 values)."))
        else:
            findings.append(_f("INFO", "age_values",
                               f"Age range 0-90 ({n_ages} unique values). [ok]"))

    return findings


# ---------------------------------------------------------------------------
# CHECK: temporal plausibility
# ---------------------------------------------------------------------------

def check_temporal_plausibility(
    df: pd.DataFrame, max_yoy_pct: float = MAX_YOY_PCT
) -> list[Finding]:
    """Year-on-year population change within +/-max_yoy_pct for every area."""
    findings: list[Finding] = []

    year_cols = sorted([c for c in df.columns if isinstance(c, int)])
    if len(year_cols) < 2 or "area_name" not in df.columns:
        return findings

    # Collapse sex/age to area-level totals before checking YoY rates
    group_cols = [c for c in ("area_code", "area_name", "area_type") if c in df.columns]
    area_totals = df.groupby(group_cols)[year_cols].sum().reset_index()

    # Also synthesise an implied England row (sum of 9 regions)
    if "area_type" in area_totals.columns:
        regions = area_totals[area_totals["area_type"] == "Region"][year_cols].sum()
    else:
        regions = area_totals[year_cols].sum()
    england_row = pd.Series({"area_name": "England (derived)", **regions.to_dict()})
    check_df = pd.concat(
        [area_totals[["area_name"] + year_cols], england_row.to_frame().T],
        ignore_index=True,
    )
    for yr in year_cols:
        check_df[yr] = pd.to_numeric(check_df[yr], errors="coerce")

    flagged: list[str] = []
    for _, row in check_df.iterrows():
        for i in range(1, len(year_cols)):
            prev, curr = year_cols[i - 1], year_cols[i]
            prev_pop = row[prev]
            curr_pop = row[curr]
            if pd.notna(prev_pop) and prev_pop > 0:
                pct = (curr_pop - prev_pop) / prev_pop * 100
                if abs(pct) > max_yoy_pct:
                    flagged.append(
                        f"{row['area_name']}  {prev}->{curr}: {pct:+.2f}%"
                    )

    if flagged:
        findings.append(_f(
            "WARN", "temporal_plausibility",
            f"{len(flagged)} year-on-year change(s) exceed +/-{max_yoy_pct}%:\n"
            + "\n".join(f"      {line}" for line in flagged),
        ))
    else:
        findings.append(_f("INFO", "temporal_plausibility",
                           f"All year-on-year changes within +/-{max_yoy_pct}%. [ok]"))

    return findings


# ---------------------------------------------------------------------------
# CHECK: UK and nation/England totals (informational)
# ---------------------------------------------------------------------------

def check_uk_totals(df: pd.DataFrame) -> list[Finding]:
    """Report UK total, England (derived), and each devolved nation per year."""
    findings: list[Finding] = []

    year_cols = sorted([c for c in df.columns if isinstance(c, int)])
    if not year_cols or "area_type" not in df.columns:
        return findings

    # Collapse sex/age to area-level totals before reporting
    group_cols = [c for c in ("area_code", "area_name", "area_type") if c in df.columns]
    area_df = df.groupby(group_cols)[year_cols].sum().reset_index()

    regions_df = area_df[area_df["area_type"] == "Region"]
    countries_df = area_df[area_df["area_type"] == "Country"]

    lines: list[str] = []
    for yr in year_cols:
        uk = int(area_df[yr].sum())
        eng = int(regions_df[yr].sum())
        nations = {
            row["area_name"]: int(row[yr])
            for _, row in countries_df.iterrows()
        }
        nation_str = "  ".join(f"{n}: {p:,}" for n, p in nations.items())
        lines.append(f"  {yr}:  UK {uk:,}   England {eng:,}   {nation_str}")

    findings.append(_f("INFO", "uk_totals",
                       "Population totals by year:\n" + "\n".join(lines)))

    # Sanity: UK total must have grown 2011->latest
    first_yr, last_yr = year_cols[0], year_cols[-1]
    uk_first = int(area_df[first_yr].sum())
    uk_last = int(area_df[last_yr].sum())
    if uk_last <= uk_first:
        findings.append(_f(
            "WARN", "uk_growth",
            f"UK total did not grow overall: "
            f"{uk_first:,} ({first_yr}) -> {uk_last:,} ({last_yr}).",
        ))

    return findings


# ---------------------------------------------------------------------------
# CHECK: reference comparison
# ---------------------------------------------------------------------------

def check_against_reference(
    df: pd.DataFrame, ref_df: pd.DataFrame
) -> list[Finding]:
    """Exact integer comparison against stored reference totals."""
    findings: list[Finding] = []

    # Detect stale reference generated before the age/sex output format
    if "sex" not in ref_df.columns or "age" not in ref_df.columns:
        findings.append(_f(
            "WARN", "reference_format",
            "Reference file is missing 'sex' and/or 'age' columns. "
            "It was generated before the age/sex output format was introduced. "
            "Regenerate: copy output/mye_region_country_age_sex_2011_2024.csv "
            "to data/published_reference_totals.csv",
        ))
        return findings

    if len(ref_df) == 0:
        findings.append(_f(
            "WARN", "reference_data",
            "Reference file has no data rows. "
            "Regenerate: copy output/mye_region_country_age_sex_2011_2024.csv "
            "to data/published_reference_totals.csv",
        ))
        return findings

    key_cols = ["area_code", "sex", "age"]

    # Coerce age to int in both frames so keys align correctly
    df = df.copy()
    ref_df = ref_df.copy()
    df["age"] = pd.to_numeric(df["age"], errors="coerce").astype(int)
    ref_df["age"] = pd.to_numeric(ref_df["age"], errors="coerce").astype(int)

    out_years = sorted(c for c in df.columns if isinstance(c, int))
    ref_years = sorted(c for c in ref_df.columns if isinstance(c, int))
    common_years = sorted(set(out_years) & set(ref_years))

    skipped_years = sorted((set(EXPECTED_YEARS) & set(ref_years)) - set(out_years))
    if skipped_years:
        findings.append(_f("WARN", "reference_coverage",
                           f"Year(s) in reference not found in output: {skipped_years}."))

    out_keyed = df.set_index(key_cols)[common_years].sort_index()
    ref_keyed = ref_df.set_index(key_cols)[common_years].sort_index()

    common_idx = out_keyed.index.intersection(ref_keyed.index)
    missing_in_output = ref_keyed.index.difference(out_keyed.index)
    missing_in_ref = out_keyed.index.difference(ref_keyed.index)

    if len(missing_in_output):
        findings.append(_f("FAIL", "reference_coverage",
                           f"{len(missing_in_output)} key(s) in reference "
                           "but absent from output."))
    if len(missing_in_ref):
        findings.append(_f("WARN", "reference_coverage",
                           f"{len(missing_in_ref)} key(s) in output "
                           "but absent from reference."))

    out_common = out_keyed.loc[common_idx].astype(int)
    ref_common = ref_keyed.loc[common_idx].astype(int)
    diff_mask = out_common != ref_common
    n_diffs = int(diff_mask.sum().sum())

    if n_diffs:
        # Report up to 20 individual mismatches
        rows_with_diffs = diff_mask.any(axis=1)
        examples: list[str] = []
        for idx_key in diff_mask[rows_with_diffs].head(20).index:
            area_code, sex, age = idx_key
            for yr in common_years:
                if diff_mask.loc[idx_key, yr]:
                    out_val = int(out_common.loc[idx_key, yr])
                    ref_val = int(ref_common.loc[idx_key, yr])
                    examples.append(
                        f"{area_code} sex={sex} age={age}  {yr}: "
                        f"output={out_val:,}  reference={ref_val:,}  "
                        f"diff={out_val - ref_val:+,}"
                    )
        tail = (f"\n      ... and more" if len(examples) >= 20 else "")
        findings.append(_f(
            "FAIL", "reference_match",
            f"{n_diffs} cell(s) differ from reference "
            "(update data/published_reference_totals.csv if the source "
            "file has been intentionally refreshed):\n"
            + "\n".join(f"      {e}" for e in examples[:20])
            + tail,
        ))
    else:
        findings.append(_f(
            "INFO", "reference_match",
            f"All values match reference "
            f"({len(common_idx)} keys x {len(common_years)} years). [ok]",
        ))

    return findings


# ---------------------------------------------------------------------------
# CHECK: LAD->Region lookup integrity
# ---------------------------------------------------------------------------

def check_lookup_integrity(lookup_df: pd.DataFrame) -> list[Finding]:
    """Duplicate LADs, region code coverage, LAD counts per region."""
    findings: list[Finding] = []

    required_cols = {"lad_code", "rgn_code", "rgn_name"}
    missing_cols = required_cols - set(lookup_df.columns)
    if missing_cols:
        findings.append(_f("FAIL", "lookup_columns",
                           f"Lookup missing required column(s): "
                           f"{sorted(missing_cols)}."))
        return findings

    # Duplicate LAD codes
    dups = lookup_df[lookup_df["lad_code"].duplicated(keep=False)]
    if not dups.empty:
        findings.append(_f(
            "FAIL", "lookup_duplicates",
            f"{len(dups)} row(s) share a duplicate lad_code "
            f"(double-counting risk): "
            f"{dups['lad_code'].unique().tolist()[:10]}.",
        ))
    else:
        findings.append(_f("INFO", "lookup_duplicates",
                           f"No duplicate LAD codes ({len(lookup_df)} unique LADs). [ok]"))

    # Total LAD count
    n_lads = len(lookup_df)
    if n_lads != 296:
        findings.append(_f(
            "WARN", "lookup_total_lads",
            f"Expected 296 England LADs, found {n_lads}. "
            f"May indicate a boundary reorganisation.",
        ))
    else:
        findings.append(_f("INFO", "lookup_total_lads",
                           "296 England LADs in lookup. [ok]"))

    # All 9 regions present
    actual_rgns = set(lookup_df["rgn_code"].unique())
    missing_rgns = sorted(EXPECTED_REGION_CODES - actual_rgns)
    extra_rgns = sorted(actual_rgns - EXPECTED_REGION_CODES)

    if missing_rgns:
        findings.append(_f("FAIL", "lookup_region_codes",
                           f"Lookup is missing region code(s): {missing_rgns}. "
                           f"These regions will be absent from output."))
    if extra_rgns:
        findings.append(_f("WARN", "lookup_region_codes",
                           f"Unexpected region code(s) in lookup: {extra_rgns}."))
    if not missing_rgns:
        findings.append(_f("INFO", "lookup_region_codes",
                           "All 9 region codes present in lookup. [ok]"))

    # LAD counts per region vs expected 2024 boundaries
    actual_counts = (
        lookup_df.groupby("rgn_code")["lad_code"].count().to_dict()
    )
    mismatches: list[str] = []
    for rgn_code, expected_n in EXPECTED_LAD_COUNTS.items():
        actual_n = actual_counts.get(rgn_code, 0)
        if actual_n != expected_n:
            rgn_name = lookup_df.loc[
                lookup_df["rgn_code"] == rgn_code, "rgn_name"
            ].iloc[0] if rgn_code in actual_counts else rgn_code
            mismatches.append(
                f"{rgn_name} ({rgn_code}): "
                f"expected {expected_n}, found {actual_n}"
            )

    if mismatches:
        findings.append(_f(
            "WARN", "lookup_lad_counts",
            "LAD count mismatch(es) vs expected 2024 boundaries "
            "(likely a boundary reorganisation - verify intentional):\n"
            + "\n".join(f"      {m}" for m in mismatches),
        ))
    else:
        findings.append(_f("INFO", "lookup_lad_counts",
                           "LAD counts per region match 2024 boundaries. [ok]"))

    return findings


# ---------------------------------------------------------------------------
# REPORT
# ---------------------------------------------------------------------------

def _print_report(findings: list[Finding]) -> bool:
    """Print a structured report. Returns True if any FAIL findings exist."""
    n_fail = sum(1 for f in findings if f.level == "FAIL")
    n_warn = sum(1 for f in findings if f.level == "WARN")
    n_info = sum(1 for f in findings if f.level == "INFO")

    print()
    print("=" * 64)
    print("  ONS MYE Output - QA Validation Report")
    print("=" * 64)

    labels = {"FAIL": "X FAIL", "WARN": "! WARN", "INFO": "  INFO"}
    for level in ("FAIL", "WARN", "INFO"):
        group = [f for f in findings if f.level == level]
        if not group:
            continue
        print()
        for f in group:
            print(f"  [{labels[level]}]  {f.check}")
            # indent multi-line messages
            for line in f.message.splitlines():
                print(f"           {line}")

    print()
    print("=" * 64)
    print(f"  Checks : {n_fail} FAIL  |  {n_warn} WARN  |  {n_info} INFO")
    status = "PASS" if n_fail == 0 else "FAIL - output should not be used"
    print(f"  Status : {status}")
    print("=" * 64)
    print()

    return n_fail > 0


# ---------------------------------------------------------------------------
# PUBLIC ENTRY POINT
# ---------------------------------------------------------------------------

def run_validation(
    output_path: str = DEFAULT_OUTPUT,
    reference_path: str | None = DEFAULT_REFERENCE,
    lookup_path: str | None = DEFAULT_LOOKUP,
) -> bool:
    """
    Run all QA checks against *output_path*.

    Parameters
    ----------
    output_path : str
        Path to the wide-format output CSV.
    reference_path : str or None
        Path to the reference totals CSV.  Pass None to skip.
    lookup_path : str or None
        Path to the LAD->Region lookup CSV.  Pass None to skip.

    Returns
    -------
    bool
        True if any FAIL findings were raised (output is invalid).
    """
    cfg = _load_config()

    findings: list[Finding] = []

    # Load output
    if not os.path.exists(output_path):
        print(f"\nERROR: Output file not found: {output_path}\n")
        return True

    df = pd.read_csv(output_path)
    # Normalise year column names to int
    df.columns = [int(c) if str(c).isdigit() else c for c in df.columns]

    findings.append(_f("INFO", "source_file", f"Validating: {output_path}"))

    # Core checks
    findings.extend(check_structure(df))
    findings.extend(check_values(df))
    findings.extend(check_temporal_plausibility(df, max_yoy_pct=cfg.max_yoy_pct))
    findings.extend(check_uk_totals(df))

    # Reference comparison
    if reference_path and os.path.exists(reference_path):
        ref_df = pd.read_csv(reference_path)
        ref_df.columns = [int(c) if str(c).isdigit() else c for c in ref_df.columns]
        findings.extend(check_against_reference(df, ref_df))
    elif reference_path:
        os.makedirs(os.path.dirname(reference_path), exist_ok=True)
        import shutil
        shutil.copy2(output_path, reference_path)
        findings.append(_f(
            "INFO", "reference_check",
            f"Reference file not found - created from current output: "
            f"'{reference_path}'.",
        ))

    # Lookup integrity
    if lookup_path and os.path.exists(lookup_path):
        lookup_df = pd.read_csv(lookup_path)
        findings.extend(check_lookup_integrity(lookup_df))
    else:
        findings.append(_f(
            "WARN", "lookup_check",
            f"Lookup file not found at '{lookup_path}' - "
            "skipping lookup integrity checks. "
            "Run with --no-lookup to silence this warning.",
        ))

    return _print_report(findings)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    """Parse CLI arguments and run validation, exiting with code 1 on failure."""
    parser = argparse.ArgumentParser(
        description="QA validation for ONS MYE region/country output.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        metavar="PATH",
        help=f"Output CSV to validate (default: output/mye_region_country_2011_2024.csv)",
    )
    parser.add_argument(
        "--reference",
        default=DEFAULT_REFERENCE,
        metavar="PATH",
        help="Reference totals CSV for regression comparison "
             "(default: data/published_reference_totals.csv)",
    )
    parser.add_argument(
        "--no-reference",
        action="store_true",
        help="Skip reference comparison.",
    )
    parser.add_argument(
        "--lookup",
        default=DEFAULT_LOOKUP,
        metavar="PATH",
        help="LAD->Region lookup CSV (default: data/lad_to_region_lookup.csv)",
    )
    parser.add_argument(
        "--no-lookup",
        action="store_true",
        help="Skip lookup integrity checks.",
    )
    args = parser.parse_args()

    has_failures = run_validation(
        output_path=args.output,
        reference_path=None if args.no_reference else args.reference,
        lookup_path=None if args.no_lookup else args.lookup,
    )
    sys.exit(1 if has_failures else 0)


if __name__ == "__main__":
    main()
