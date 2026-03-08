# NYPD CompStat Scraper

Automated scraper for NYPD weekly CompStat crime data. Downloads Excel files from the NYPD website, parses the 7 Major Felonies plus additional crime categories, and outputs clean JSON and CSV.

## Data Source

The NYPD publishes weekly CompStat reports as Excel and PDF files at:
https://www.nyc.gov/site/nypd/stats/crime-statistics/citywide-crime-stats.page

Data is typically updated on Fridays.

## What It Scrapes

- **7 Major Felonies**: Murder, Rape, Robbery, Felony Assault, Burglary, Grand Larceny, Grand Larceny Auto
- **Additional categories**: Transit crime, Housing crime, Petit Larceny, Retail Theft, Misdemeanor Assault, Shootings, Hate Crimes, Traffic Fatalities
- **Time periods**: Week-to-date, 28-day, Year-to-date, plus 2-year/16-year/33-year trend percentages
- **Historical perspective**: Annual totals back to 1990
- **Geography**: Citywide, 8 patrol boroughs, 77 precincts, NYCHA Housing, Dept. of Correction

## Quick Start (Mac)

```bash
# 1. Clone or download this repo
git clone https://github.com/YOUR_ORG/nypd-compstat-scraper.git
cd nypd-compstat-scraper

# 2. Install dependencies
pip3 install -r requirements.txt

# 3. Run (citywide only)
python3 scraper.py

# 4. Run with boroughs
python3 scraper.py --boroughs

# 5. Run everything
python3 scraper.py --all --format both
```

Output goes to `./output/latest_compstat.json` (and `.csv` with `--format both`).

## CLI Options

| Flag | Description |
|------|-------------|
| `--output DIR` | Output directory (default: `./output`) |
| `--all` | Scrape everything: citywide + boroughs + all 77 precincts + housing + DOC |
| `--boroughs` | Include 8 patrol borough breakdowns |
| `--precincts 1 13 73` | Specific precincts (or `--precincts` with no args for all 77) |
| `--housing` | Include NYCHA Housing crime data |
| `--doc` | Include Dept. of Correction data |
| `--format json\|csv\|both` | Output format (default: json) |

## Automated Scraping with GitHub Actions

The included workflow (`.github/workflows/scrape.yml`) runs every Wednesday at 2 PM ET:

1. Create a GitHub repo and push this code
2. The Action runs automatically on schedule
3. Results are committed to the `data/` directory
4. Your dashboard can fetch `latest_compstat.json` directly from the repo

To trigger manually: Go to Actions tab → "Scrape NYPD CompStat" → "Run workflow"

## JSON Schema

```
{
  "citywide": {
    "source": "Citywide",
    "scraped_at": "2026-03-08T18:00:00Z",
    "report_period": {
      "week_start": "2/16/2026",
      "week_end": "2/22/2026",
      "volume": "33",
      "number": "8"
    },
    "seven_major_felonies": {
      "Murder": {
        "week_to_date": { "current_year": 2, "prior_year": 5, "pct_change": -60.0 },
        "twenty_eight_day": { "current_year": 15, "prior_year": 24, "pct_change": -37.5 },
        "year_to_date": { "current_year": 28, "prior_year": 48, "pct_change": -41.7 },
        "two_year_pct_change": -46.2,
        "long_term_pct_change_1": -56.9,
        "long_term_pct_change_2": -90.4
      },
      ...
    },
    "total_seven_major": { ... },
    "additional_stats": { "Transit": {...}, "Shooting Inc.": {...}, ... },
    "historical": {
      "Murder": { "1990": 2262, "1993": 1927, "1998": 629, "2001": 649, "latest_full_year": 309 },
      ...
    }
  },
  "boroughs": { ... },
  "precincts": { ... }
}
```

## Connecting to a Dashboard

Point your React dashboard at the raw JSON URL from your GitHub repo:

```javascript
const COMPSTAT_URL = "https://raw.githubusercontent.com/YOUR_ORG/nypd-compstat-scraper/main/data/latest_compstat.json";

async function fetchCompStat() {
  const resp = await fetch(COMPSTAT_URL);
  return resp.json();
}
```

## Notes

- CompStat figures are preliminary and subject to revision by NYPD
- Crime statistics use NYS Penal Law definitions (not FBI UCR categories)
- The scraper archives raw Excel files in `output/raw/` with date stamps
- Borough codes: ms (Manhattan South), mn (Manhattan North), bx (Bronx), bks (Brooklyn South), bkn (Brooklyn North), qs (Queens South), qn (Queens North), si (Staten Island)
