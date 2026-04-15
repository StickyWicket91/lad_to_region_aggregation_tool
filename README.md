# ONS Mid-Year Estimates Aggregator (2011–2024)

Aggregates ONS local authority mid-year population estimates to region level for England and country level for Wales, Scotland, and Northern Ireland, producing a single tidy CSV covering 2011 to 2024.

## Background

The ONS publishes annual mid-year population estimates at local authority district (LAD) level for the whole of the UK. This programme aggregates those estimates upward to two summary geographies:

- **England** — the 9 statistical regions (e.g. London, South East, North West)
- **Wales, Scotland, Northern Ireland** — each treated as a single country total

The source data uses the post-2021 Census revised methodology, which provides a consistent series from 2011 onwards.

## Data sources

- **[ONS Mid-Year Population Estimates](https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/populationestimatesforukenglandandwalesscotlandandnorthernireland)** — local authority mid-year estimates, 2011–2024 (single Excel file downloaded automatically)
- **[ONS Open Geography Portal](https://geoportal.statistics.gov.uk/)** — LAD to Region lookup for England (fetched via REST API)

Both sources are downloaded automatically on first run and cached locally so subsequent runs do not re-fetch them.

## Requirements

- Python 3.9 or later
- The packages listed in `requirements.txt`

Install dependencies with:

```bash
pip install -r requirements.txt
```

## Usage

```bash
cd processing
python aggregate_mye.py
```

The script will:

1. Download the ONS MYE Excel file to `input/`
2. Fetch the LAD→Region geography lookup from the ONS Geoportal and cache it to `input/lad_to_region_lookup.csv`
3. Process each year from 2011 to 2024
4. Write the aggregated output to `output/mye_region_country_age_sex_2011_2024.csv`
5. Run QA validation against the output automatically

Progress is logged to the terminal at each step.

## Output

The output file `output/mye_region_country_age_sex_2011_2024.csv` contains one row per area × sex × age combination, with a column for each year. Both `input/` and `output/` are created at the project root on first run.

| Column | Type | Description |
| --- | --- | --- |
| `area_code` | string | ONS area code for the region or country |
| `area_name` | string | Area name |
| `area_type` | string | `Region` (England) or `Country` (Wales, Scotland, Northern Ireland) |
| `sex` | string | `m` or `f` |
| `age` | integer | Single year of age (0–90) |
| `2011`–`2024` | integer | Mid-year population estimate for that year |

### Example rows

| area_code | area_name | area_type | sex | age | 2011 | 2024 |
| --- | --- | --- | --- | --- | --- | --- |
| E12000007 | London | Region | m | 30 | … | … |
| W92000004 | Wales | Country | f | 0 | … | … |
| E12000001 | North East | Region | m | 65 | … | … |

## Project structure

```text
lad_to_region_aggregation_tool/
├── processing/
│   ├── aggregate_mye.py          # Main aggregation script
│   ├── validate_output.py        # QA validation script
│   ├── config_loader.py          # Config loader
│   ├── config.yaml               # Pipeline configuration (edit here)
│   ├── requirements.txt          # Python dependencies
│   ├── LICENSE
│   └── README.md
├── input/                        # Created on first run
│   ├── myebtablesuk20112024.xlsx # Downloaded ONS source file
│   ├── lad_to_region_lookup.csv  # Cached geography lookup
│   └── published_reference_totals.csv  # QA baseline (auto-created)
└── output/                       # Created on first run
    └── mye_region_country_age_sex_2011_2024.csv
```

## Development

This programme was created using [Claude Code](https://claude.ai/code), Anthropic's AI coding assistant.

## Notes

- **Boundary changes**: LAD boundaries change periodically due to local government reorganisations. The script uses the most recent available LAD→Region lookup from the ONS Geoportal. Any LADs that cannot be matched to a region (due to a boundary change not covered by the lookup) are logged as warnings and excluded from the England regional totals rather than silently distorting figures.
- **QA reference baseline**: On the first run after a fresh install, `validate_output.py` creates `input/published_reference_totals.csv` automatically as a copy of the output. Subsequent runs compare the output against this baseline and fail if values differ unexpectedly. To reset the baseline after an intentional data update, delete `published_reference_totals.csv` and re-run.
- **Cached downloads**: If the `input/` directory already contains the source files, they will not be re-downloaded. Delete the cached files to force a fresh download.
- **URL changes**: ONS occasionally restructures its website. If a download fails with an HTTP error, the script prints the expected filename and a link to the ONS dataset page where the current file can be found and downloaded manually.
