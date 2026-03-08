#!/usr/bin/env python3
"""
NYPD CompStat Crime Data Scraper
Downloads and parses weekly CompStat Excel files from the NYPD website.

Outputs clean JSON files for:
  - Citywide 7 Major Felonies (week-to-date, 28-day, year-to-date, historical)
  - Borough-level breakdowns
  - Precinct-level breakdowns
  - Transit, Housing, DOC crime stats
  - Shooting data
  - Hate crimes, traffic fatalities

Data source: https://www.nyc.gov/site/nypd/stats/crime-statistics/citywide-crime-stats.page
Updated weekly by NYPD, typically on Fridays.

Usage:
    python3 scraper.py                    # Scrape citywide only
    python3 scraper.py --all              # Scrape citywide + all boroughs + all precincts
    python3 scraper.py --boroughs         # Scrape citywide + borough-level
    python3 scraper.py --precincts 1 13 73  # Scrape citywide + specific precincts
    python3 scraper.py --output ./data    # Custom output directory
"""

import argparse
import json
import logging
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://www.nyc.gov/assets/nypd/downloads/excel/crime_statistics"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# NYPD file naming conventions
CITYWIDE_FILE = "cs-en-us-city.xlsx"
HOUSING_FILE = "hb_eagle_report.xlsx"
DOC_FILE = "cs-en-us-Correction.xlsx"

BOROUGH_FILES = {
    "Manhattan South": "cs-en-us-pbms.xlsx",
    "Manhattan North": "cs-en-us-pbmn.xlsx",
    "Bronx": "cs-en-us-pbbx.xlsx",
    "Brooklyn South": "cs-en-us-pbbks.xlsx",
    "Brooklyn North": "cs-en-us-pbbkn.xlsx",
    "Queens South": "cs-en-us-pbqs.xlsx",
    "Queens North": "cs-en-us-pbqn.xlsx",
    "Staten Island": "cs-en-us-pbsi.xlsx",
}

# All 77 NYPD precincts
ALL_PRECINCTS = [
    1, 5, 6, 7, 9, 10, 13, 14, 17, 18, 19, 20, 22, 23, 24, 25, 26, 28, 30, 32, 33, 34,
    40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 52,
    60, 61, 62, 63, 66, 67, 68, 69, 70, 71, 72, 73, 75, 76, 77, 78, 79, 81, 83, 84, 88, 90,
    100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 120, 121, 122, 123
]

# ─── Crime categories and their row labels in the Excel ───
SEVEN_MAJOR = ["Murder", "Rape", "Robbery", "Fel. Assault", "Burglary", "Gr. Larceny", "G.L.A."]
ADDITIONAL_CATEGORIES = [
    "Transit", "Housing", "Petit Larceny", "Retail Theft",
    "Misd. Assault", "UCR Rape*", "Other Sex Crimes",
    "Shooting Vic.", "Shooting Inc.", "Hate Crimes", "Traffic Fatalities"
]


def download_excel(filename: str) -> bytes | None:
    """Download an Excel file from the NYPD website. Returns bytes or None on failure."""
    url = f"{BASE_URL}/{filename}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        logger.info(f"Downloaded {filename} ({len(resp.content):,} bytes)")
        return resp.content
    except requests.RequestException as e:
        logger.error(f"Failed to download {filename}: {e}")
        return None


def build_column_mapping(df: pd.DataFrame) -> dict:
    """
    Dynamically discover column positions by scanning header rows for known labels.
    This is more resilient than hardcoding column indices — if NYPD ever inserts
    or reorders columns, the parser adapts rather than silently producing wrong numbers.
    """
    mapping = {}
    for idx in range(min(10, len(df))):
        row_strs = [str(v).lower().strip() for v in df.iloc[idx]]
        for col_idx, val in enumerate(row_strs):
            if "week to date" in val or "w-t-d" in val:
                mapping["wtd_current"] = col_idx
                mapping["wtd_prior"] = col_idx + 1
                mapping["wtd_pct"] = col_idx + 2
            elif "28 day" in val:
                mapping["28d_current"] = col_idx
                mapping["28d_prior"] = col_idx + 1
                mapping["28d_pct"] = col_idx + 2
            elif "year to date" in val or "y-t-d" in val:
                mapping["ytd_current"] = col_idx
                mapping["ytd_prior"] = col_idx + 1
                mapping["ytd_pct"] = col_idx + 2
            elif "2 yr" in val or "2 year" in val:
                mapping["2yr_pct"] = col_idx
            elif any(x in val for x in ("16 yr", "15 yr", "16 year", "15 year")):
                mapping["long_term_1_pct"] = col_idx
            elif any(x in val for x in ("33 yr", "33 year", "32 yr", "32 year")):
                mapping["long_term_2_pct"] = col_idx
    return mapping


def parse_compstat_excel(content: bytes, source_label: str = "Citywide") -> dict:
    """
    Parse a CompStat Excel file into structured data.
    
    Dynamically discovers column layout from header rows, then extracts crime
    data rows by matching known category labels.
    
    Returns a dict with all parsed crime data.
    """
    try:
        df = pd.read_excel(
            pd.io.common.BytesIO(content),
            header=None,
            engine="openpyxl"
        )
    except Exception as e:
        logger.error(f"Failed to parse Excel for {source_label}: {e}")
        return {}

    result = {
        "source": source_label,
        "scraped_at": datetime.utcnow().isoformat() + "Z",
        "report_period": extract_report_period(df),
        "seven_major_felonies": {},
        "total_seven_major": {},
        "additional_stats": {},
        "historical": {},
    }

    # Dynamically discover column positions from header rows
    col_map = build_column_mapping(df)
    if not col_map:
        logger.warning(f"Could not discover column layout for {source_label}; falling back to positional.")

    # Find the data rows by looking for crime category labels
    for idx, row in df.iterrows():
        label = clean_label(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
        
        if not label:
            continue

        # Match against known categories
        matched_category = match_category(label, SEVEN_MAJOR + ["TOTAL"] + ADDITIONAL_CATEGORIES)
        if not matched_category:
            continue

        crime_data = extract_row_data(row, col_map)

        if matched_category in SEVEN_MAJOR:
            result["seven_major_felonies"][matched_category] = crime_data
        elif matched_category == "TOTAL":
            result["total_seven_major"] = crime_data
        elif matched_category in ADDITIONAL_CATEGORIES:
            result["additional_stats"][matched_category] = crime_data

    # Try to extract historical perspective section
    result["historical"] = extract_historical(df)

    return result


def extract_report_period(df: pd.DataFrame) -> dict:
    """Extract the report date range from the Excel header rows."""
    period = {"raw": "", "week_start": "", "week_end": "", "volume": "", "number": ""}
    
    for idx in range(min(10, len(df))):
        row_text = " ".join(str(v) for v in df.iloc[idx] if pd.notna(v))
        
        # Look for date range like "2/16/2026 Through 2/22/2026"
        date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})\s+Through\s+(\d{1,2}/\d{1,2}/\d{4})', row_text)
        if date_match:
            period["week_start"] = date_match.group(1)
            period["week_end"] = date_match.group(2)
            period["raw"] = date_match.group(0)
        
        # Look for Volume/Number
        vol_match = re.search(r'Volume\s+(\d+)\s+Number\s+(\d+)', row_text)
        if vol_match:
            period["volume"] = vol_match.group(1)
            period["number"] = vol_match.group(2)
    
    return period


def clean_label(val) -> str:
    """Clean and normalize a cell value to a string label."""
    s = str(val).strip()
    # Remove asterisks, extra whitespace
    s = re.sub(r'\s+', ' ', s)
    return s


def match_category(label: str, categories: list) -> str | None:
    """Match a cell label to a known crime category."""
    label_lower = label.lower().strip()
    for cat in categories:
        cat_lower = cat.lower().strip()
        if label_lower == cat_lower or label_lower.startswith(cat_lower):
            return cat
    # Handle common variations
    variations = {
        "felony assault": "Fel. Assault",
        "fel assault": "Fel. Assault",
        "grand larceny auto": "G.L.A.",
        "grand larceny of motor vehicle": "G.L.A.",
        "gla": "G.L.A.",
        "gr larceny": "Gr. Larceny",
        "grand larceny": "Gr. Larceny",
        "shooting victims": "Shooting Vic.",
        "shooting incidents": "Shooting Inc.",
    }
    for var, cat in variations.items():
        if var in label_lower:
            return cat
    return None


def extract_row_data(row: pd.Series, col_map: dict) -> dict:
    """
    Extract crime numbers from a data row using dynamically discovered column positions.
    Falls back to hardcoded positions if column mapping is empty.
    """
    def safe_num(val):
        if pd.isna(val):
            return None
        try:
            s = str(val).replace(",", "").replace("*", "").replace("%", "").strip()
            if s in ("", "-", "***.*", "***"):
                return None
            return float(s) if "." in s else int(s)
        except (ValueError, TypeError):
            return None

    values = list(row)

    def get_val(key, fallback_idx=None):
        """Get value by mapped column, or fall back to positional index."""
        idx = col_map.get(key, fallback_idx)
        if idx is not None and idx < len(values):
            return safe_num(values[idx])
        return None

    data = {}

    if col_map:
        # Use dynamically discovered columns
        if "wtd_current" in col_map:
            data["week_to_date"] = {
                "current_year": get_val("wtd_current"),
                "prior_year": get_val("wtd_prior"),
                "pct_change": get_val("wtd_pct"),
            }
        if "28d_current" in col_map:
            data["twenty_eight_day"] = {
                "current_year": get_val("28d_current"),
                "prior_year": get_val("28d_prior"),
                "pct_change": get_val("28d_pct"),
            }
        if "ytd_current" in col_map:
            data["year_to_date"] = {
                "current_year": get_val("ytd_current"),
                "prior_year": get_val("ytd_prior"),
                "pct_change": get_val("ytd_pct"),
            }
        if "2yr_pct" in col_map:
            data["two_year_pct_change"] = get_val("2yr_pct")
        if "long_term_1_pct" in col_map:
            data["long_term_pct_change_1"] = get_val("long_term_1_pct")
        if "long_term_2_pct" in col_map:
            data["long_term_pct_change_2"] = get_val("long_term_2_pct")
    else:
        # Positional fallback (standard NYPD layout)
        if len(values) >= 4:
            data["week_to_date"] = {
                "current_year": safe_num(values[1]),
                "prior_year": safe_num(values[2]),
                "pct_change": safe_num(values[3]),
            }
        if len(values) >= 7:
            data["twenty_eight_day"] = {
                "current_year": safe_num(values[4]),
                "prior_year": safe_num(values[5]),
                "pct_change": safe_num(values[6]),
            }
        if len(values) >= 10:
            data["year_to_date"] = {
                "current_year": safe_num(values[7]),
                "prior_year": safe_num(values[8]),
                "pct_change": safe_num(values[9]),
            }
        if len(values) >= 11:
            data["two_year_pct_change"] = safe_num(values[10])
        if len(values) >= 12:
            data["long_term_pct_change_1"] = safe_num(values[11])
        if len(values) >= 13:
            data["long_term_pct_change_2"] = safe_num(values[12])

    return data


def extract_historical(df: pd.DataFrame) -> dict:
    """Extract the Historical Perspective table if present."""
    historical = {}
    in_historical = False
    
    for idx, row in df.iterrows():
        row_text = " ".join(str(v) for v in row if pd.notna(v))
        
        if "historical perspective" in row_text.lower():
            in_historical = True
            continue
        
        if in_historical:
            label = clean_label(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
            matched = match_category(label, SEVEN_MAJOR + ["TOTAL"])
            if matched:
                vals = [v for v in row if pd.notna(v)]
                # Historical rows have: label, 1990, 1993, 1998, 2001, latest_year, then % changes
                nums = []
                for v in vals[1:]:
                    try:
                        nums.append(float(str(v).replace(",", "").replace("%", "").strip()))
                    except ValueError:
                        continue
                if len(nums) >= 5:
                    historical[matched] = {
                        "1990": int(nums[0]) if nums[0] == int(nums[0]) else nums[0],
                        "1993": int(nums[1]) if nums[1] == int(nums[1]) else nums[1],
                        "1998": int(nums[2]) if nums[2] == int(nums[2]) else nums[2],
                        "2001": int(nums[3]) if nums[3] == int(nums[3]) else nums[3],
                        "latest_full_year": int(nums[4]) if nums[4] == int(nums[4]) else nums[4],
                    }
            # Stop if we hit the footnotes
            if "figures are preliminary" in row_text.lower():
                break
    
    return historical


def scrape_citywide(output_dir: Path) -> dict:
    """Download and parse citywide CompStat data."""
    content = download_excel(CITYWIDE_FILE)
    if not content:
        logger.error("Could not download citywide Excel file.")
        return {}
    
    data = parse_compstat_excel(content, "Citywide")
    
    # Save raw Excel for archival
    raw_dir = output_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d")
    (raw_dir / f"cs-en-us-city_{timestamp}.xlsx").write_bytes(content)
    
    return data


def scrape_boroughs(output_dir: Path) -> dict:
    """Download and parse all borough-level CompStat data."""
    boroughs = {}
    for borough_name, filename in BOROUGH_FILES.items():
        content = download_excel(filename)
        if content:
            boroughs[borough_name] = parse_compstat_excel(content, borough_name)
    return boroughs


def scrape_precincts(precinct_list: list[int], output_dir: Path) -> dict:
    """Download and parse precinct-level CompStat data."""
    precincts = {}
    for pct in precinct_list:
        pct_str = str(pct).zfill(3)
        filename = f"cs-en-us-{pct_str}pct.xlsx"
        content = download_excel(filename)
        if content:
            precincts[f"Precinct {pct}"] = parse_compstat_excel(content, f"Precinct {pct}")
    return precincts


def scrape_housing(output_dir: Path) -> dict:
    """Download and parse NYCHA housing crime data."""
    content = download_excel(HOUSING_FILE)
    if content:
        return parse_compstat_excel(content, "NYCHA Housing")
    return {}


def scrape_doc(output_dir: Path) -> dict:
    """Download and parse Department of Correction crime data."""
    content = download_excel(DOC_FILE)
    if content:
        return parse_compstat_excel(content, "Dept. of Correction")
    return {}


def main():
    parser = argparse.ArgumentParser(description="NYPD CompStat Crime Data Scraper")
    parser.add_argument("--output", "-o", type=str, default="./data",
                        help="Output directory for JSON files (default: ./data)")
    parser.add_argument("--all", action="store_true",
                        help="Scrape everything: citywide + boroughs + all precincts + housing + DOC")
    parser.add_argument("--boroughs", action="store_true",
                        help="Include borough-level data")
    parser.add_argument("--precincts", nargs="*", type=int, default=None,
                        help="Specific precinct numbers to scrape (e.g., --precincts 1 13 73)")
    parser.add_argument("--housing", action="store_true",
                        help="Include NYCHA Housing data")
    parser.add_argument("--doc", action="store_true",
                        help="Include Dept. of Correction data")
    parser.add_argument("--format", choices=["json", "csv", "both"], default="json",
                        help="Output format (default: json)")

    args = parser.parse_args()
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Always scrape citywide
    logger.info("Scraping citywide CompStat data...")
    result = {"citywide": scrape_citywide(output_dir)}

    if args.all or args.boroughs:
        logger.info("Scraping borough-level data...")
        result["boroughs"] = scrape_boroughs(output_dir)

    if args.all or args.precincts is not None:
        pct_list = ALL_PRECINCTS if (args.all or args.precincts == []) else args.precincts
        logger.info(f"Scraping {len(pct_list)} precincts...")
        result["precincts"] = scrape_precincts(pct_list, output_dir)

    if args.all or args.housing:
        logger.info("Scraping NYCHA Housing data...")
        result["housing"] = scrape_housing(output_dir)

    if args.all or args.doc:
        logger.info("Scraping Dept. of Correction data...")
        result["doc"] = scrape_doc(output_dir)

    # Write JSON output
    json_path = output_dir / "latest_compstat.json"
    with open(json_path, "w") as f:
        json.dump(result, f, indent=2)
    logger.info(f"Wrote {json_path} ({json_path.stat().st_size:,} bytes)")

    # Optionally write CSV
    if args.format in ("csv", "both"):
        write_csv(result, output_dir)

    # Print summary
    print_summary(result)

    return result


def write_csv(result: dict, output_dir: Path):
    """Write a flat CSV of the 7 major felonies for easy spreadsheet use."""
    rows = []
    
    citywide = result.get("citywide", {})
    if citywide:
        for crime, data in citywide.get("seven_major_felonies", {}).items():
            row = {"geography": "Citywide", "crime": crime}
            for period in ("week_to_date", "twenty_eight_day", "year_to_date"):
                period_data = data.get(period, {})
                row[f"{period}_current"] = period_data.get("current_year")
                row[f"{period}_prior"] = period_data.get("prior_year")
                row[f"{period}_pct_change"] = period_data.get("pct_change")
            rows.append(row)
    
    for borough_name, borough_data in result.get("boroughs", {}).items():
        for crime, data in borough_data.get("seven_major_felonies", {}).items():
            row = {"geography": borough_name, "crime": crime}
            for period in ("week_to_date", "twenty_eight_day", "year_to_date"):
                period_data = data.get(period, {})
                row[f"{period}_current"] = period_data.get("current_year")
                row[f"{period}_prior"] = period_data.get("prior_year")
                row[f"{period}_pct_change"] = period_data.get("pct_change")
            rows.append(row)
    
    if rows:
        df = pd.DataFrame(rows)
        csv_path = output_dir / "latest_compstat.csv"
        df.to_csv(csv_path, index=False)
        logger.info(f"Wrote {csv_path}")


def print_summary(result: dict):
    """Print a human-readable summary of scraped data."""
    citywide = result.get("citywide", {})
    if not citywide:
        print("\nNo citywide data available.")
        return

    period = citywide.get("report_period", {})
    print(f"\n{'='*60}")
    print(f"NYPD CompStat Summary")
    print(f"Week: {period.get('week_start', '?')} - {period.get('week_end', '?')}")
    print(f"{'='*60}")
    
    felonies = citywide.get("seven_major_felonies", {})
    if felonies:
        print(f"\n{'Crime':<16} {'WTD':>6} {'WTD LY':>7} {'Chg':>7}  {'YTD':>7} {'YTD LY':>7} {'Chg':>7}")
        print("-" * 60)
        for crime in SEVEN_MAJOR:
            data = felonies.get(crime, {})
            wtd = data.get("week_to_date", {})
            ytd = data.get("year_to_date", {})
            print(f"{crime:<16} {fmt(wtd.get('current_year')):>6} {fmt(wtd.get('prior_year')):>7} "
                  f"{fmt_pct(wtd.get('pct_change')):>7}  "
                  f"{fmt(ytd.get('current_year')):>7} {fmt(ytd.get('prior_year')):>7} "
                  f"{fmt_pct(ytd.get('pct_change')):>7}")
        
        total = citywide.get("total_seven_major", {})
        if total:
            wtd = total.get("week_to_date", {})
            ytd = total.get("year_to_date", {})
            print("-" * 60)
            print(f"{'TOTAL':<16} {fmt(wtd.get('current_year')):>6} {fmt(wtd.get('prior_year')):>7} "
                  f"{fmt_pct(wtd.get('pct_change')):>7}  "
                  f"{fmt(ytd.get('current_year')):>7} {fmt(ytd.get('prior_year')):>7} "
                  f"{fmt_pct(ytd.get('pct_change')):>7}")


def fmt(val) -> str:
    if val is None:
        return "-"
    return f"{val:,.0f}" if isinstance(val, (int, float)) else str(val)


def fmt_pct(val) -> str:
    if val is None:
        return "-"
    return f"{val:+.1f}%"


if __name__ == "__main__":
    main()
