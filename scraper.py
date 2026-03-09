#!/usr/bin/env python3
"""
NYPD CompStat Crime Data Scraper - Final Version
Features: 
- Dynamic column mapping to handle NYPD header changes
- Full Borough processing and 28-day data extraction
- Historical data protection (prevents 1990s totals from overwriting weekly stats)
- Automated archiving and index.json generation for Dashboard history
- Python 3.9 compatibility
"""

import argparse
import io
import json
import logging
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://www.nyc.gov/assets/nypd/downloads/excel/crime_statistics"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

CITYWIDE_FILE = "cs-en-us-city.xlsx"

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

# The 7 Major Felonies (Index Crimes)
SEVEN_MAJOR = ["Murder", "Rape", "Robbery", "Fel. Assault", "Burglary", "Gr. Larceny", "G.L.A."]

# Additional QoL and specific incident categories
ADDITIONAL_CATEGORIES = [
    "Transit", "Housing", "Petit Larceny", "Retail Theft",
    "Misd. Assault", "UCR Rape*", "Other Sex Crimes",
    "Shooting Vic.", "Shooting Inc.", "Hate Crimes", "Traffic Fatalities"
]

def download_excel(filename: str):
    """Download an Excel file from the NYPD website."""
    url = f"{BASE_URL}/{filename}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        logger.info(f"Downloaded {filename} ({len(resp.content):,} bytes)")
        return resp.content
    except requests.RequestException as e:
        logger.error(f"Failed to download {filename}: {e}")
        return None

def build_column_mapping(df: pd.DataFrame):
    """Detects which columns hold current year vs prior year data."""
    mapping = {}
    current_year = datetime.utcnow().year

    # Strategy 1: Look for numeric year values in the first 15 rows
    for idx in range(min(15, len(df))):
        row_vals = list(df.iloc[idx])
        year_positions = []
        for col_idx, val in enumerate(row_vals):
            try:
                num = int(float(str(val).strip()))
                if num in (current_year, current_year - 1, current_year + 1):
                    year_positions.append((col_idx, num))
            except (ValueError, TypeError):
                pass

        if len(year_positions) >= 4:
            groups = []
            i = 0
            while i < len(year_positions) - 1:
                curr_col, curr_yr = year_positions[i]
                next_col, next_yr = year_positions[i + 1]
                if curr_yr >= next_yr and next_col == curr_col + 1:
                    groups.append({"current": curr_col, "prior": next_col, "pct": next_col + 1})
                    i += 2
                else:
                    i += 1
            # Ensure we capture WTD, 28-Day, and YTD metrics
            if len(groups) >= 3:
                mapping["wtd_current"] = groups[0]["current"]
                mapping["wtd_prior"] = groups[0]["prior"]
                mapping["wtd_pct"] = groups[0]["pct"]
                mapping["28d_current"] = groups[1]["current"]
                mapping["28d_prior"] = groups[1]["prior"]
                mapping["28d_pct"] = groups[1]["pct"]
                mapping["ytd_current"] = groups[2]["current"]
                mapping["ytd_prior"] = groups[2]["prior"]
                mapping["ytd_pct"] = groups[2]["pct"]
                return mapping

    # Strategy 2: Fallback to text matching
    for idx in range(min(10, len(df))):
        row_strs = [str(v).lower().strip() for v in df.iloc[idx]]
        for col_idx, val in enumerate(row_strs):
            if "week to date" in val or "w-t-d" in val:
                mapping["wtd_current"], mapping["wtd_prior"], mapping["wtd_pct"] = col_idx, col_idx+1, col_idx+2
            elif "28 day" in val:
                mapping["28d_current"], mapping["28d_prior"], mapping["28d_pct"] = col_idx, col_idx+1, col_idx+2
            elif "year to date" in val or "y-t-d" in val:
                mapping["ytd_current"], mapping["ytd_prior"], mapping["ytd_pct"] = col_idx, col_idx+1, col_idx+2
    return mapping

def parse_compstat_excel(content: bytes, source_label: str = "Citywide") -> dict:
    """Core logic to turn the spreadsheet into structured data."""
    try:
        df = pd.read_excel(io.BytesIO(content), header=None, engine="openpyxl")
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
    }

    col_map = build_column_mapping(df)

    for idx, row in df.iterrows():
        label = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ""
        if not label: continue
        
        # Stop processing when we hit the historical section.
        # This prevents 1990s totals from overwriting current weekly stats.
        if "historical perspective" in label.lower():
            break

        matched_category = match_category(label, SEVEN_MAJOR + ["TOTAL"] + ADDITIONAL_CATEGORIES)
        if not matched_category: continue

        crime_data = extract_row_data(row, col_map)
        if matched_category in SEVEN_MAJOR:
            result["seven_major_felonies"][matched_category] = crime_data
        elif matched_category == "TOTAL":
            result["total_seven_major"] = crime_data
        elif matched_category in ADDITIONAL_CATEGORIES:
            result["additional_stats"][matched_category] = crime_data

    return result

def extract_report_period(df: pd.DataFrame) -> dict:
    """Finds the 'Through' date range and volume info in the sheet."""
    period = {"raw": "", "week_start": "", "week_end": "", "volume": "", "number": ""}
    for idx in range(min(10, len(df))):
        row_text = " ".join(str(v) for v in df.iloc[idx] if pd.notna(v))
        
        date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})\s+Through\s+(\d{1,2}/\d{1,2}/\d{4})', row_text, re.IGNORECASE)
        if date_match:
            period["week_start"], period["week_end"] = date_match.group(1), date_match.group(2)
            period["raw"] = date_match.group(0)
            
        vol_match = re.search(r'Vol\.\s*(\d+)\s*No\.\s*(\d+)', row_text, re.IGNORECASE)
        if vol_match:
            period["volume"] = vol_match.group(1)
            period["number"] = vol_match.group(2)
            
    return period

def match_category(label: str, categories: list):
    """Matches a row label to a known crime category."""
    label_lower = label.lower().strip()
    for cat in categories:
        if label_lower == cat.lower() or label_lower.startswith(cat.lower()):
            return cat
    
    variations = {
        "felony assault": "Fel. Assault", 
        "gla": "G.L.A.", 
        "grand larceny": "Gr. Larceny", 
        "shooting victims": "Shooting Vic.",
        "grand larceny auto": "G.L.A."
    }
    for var, cat in variations.items():
        if var in label_lower: return cat
    return None

def extract_row_data(row: pd.Series, col_map: dict) -> dict:
    """Extracts numeric values based on column mapping."""
    def safe_num(val):
        if pd.isna(val): return None
        try:
            s = str(val).replace(",", "").replace("*", "").replace("%", "").strip()
            if s in ("", "-", "***.*"): return None
            return float(s) if "." in s else int(s)
        except: return None

    values = list(row)
    data = {}
    if col_map:
        # Includes the 28-day data points required by your pipeline
        for k, p in [("wtd", "week_to_date"), ("28d", "twenty_eight_day"), ("ytd", "year_to_date")]:
            if f"{k}_current" in col_map:
                data[p] = {
                    "current_year": safe_num(values[col_map[f"{k}_current"]]),
                    "prior_year": safe_num(values[col_map[f"{k}_prior"]]),
                    "pct_change": safe_num(values[col_map[f"{k}_pct"]])
                }
    return data

def write_csv(result: dict, output_dir: Path):
    """Transforms the JSON payload into a flat CSV format matching your schema."""
    rows = []
    # Loop over Citywide and all Boroughs
    for geo, geo_data in result.items():
        # Only process if we successfully parsed the data
        if not isinstance(geo_data, dict) or "source" not in geo_data:
            continue
            
        geography_label = geo_data.get("source", geo.title())
        
        for category in ["seven_major_felonies", "additional_stats"]:
            for crime, stats in geo_data.get(category, {}).items():
                row = {"geography": geography_label, "crime": crime}
                for period, p_data in stats.items():
                    if isinstance(p_data, dict):
                        for k, v in p_data.items():
                            # Reverts 'current_year' to 'current' for CSV compatibility
                            k_csv = k.replace("_year", "")
                            row[f"{period}_{k_csv}"] = v
                rows.append(row)
                
    if rows:
        df = pd.DataFrame(rows)
        csv_path = output_dir / "latest_compstat.csv"
        df.to_csv(csv_path, index=False)
        logger.info(f"Updated {csv_path}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", "-o", type=str, default="./data")
    parser.add_argument("--format", type=str, default="both")
    # Parse known args prevents crashing if GitHub Action passes unexpected flags
    args, unknown = parser.parse_known_args() 
    
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    result = {}

    # 1. Scrape Citywide Data
    logger.info("Scraping citywide CompStat data...")
    city_content = download_excel(CITYWIDE_FILE)
    if city_content:
        result["citywide"] = parse_compstat_excel(city_content, "Citywide")
    else:
        logger.error("Failed to download Citywide file. Exiting to trigger Action failure.")
        sys.exit(1)

    # 2. Scrape Borough Data (Restored loop)
    for borough_name, filename in BOROUGH_FILES.items():
        logger.info(f"Scraping {borough_name} data...")
        b_content = download_excel(filename)
        if b_content:
            result[borough_name] = parse_compstat_excel(b_content, borough_name)

    # 3. Output Latest JSON
    json_path = output_dir / "latest_compstat.json"
    with open(json_path, "w") as f:
        json.dump(result, f, indent=2)
    logger.info(f"Updated {json_path}")

    # 4. Output Latest CSV
    if args.format in ("csv", "both"):
        write_csv(result, output_dir)

    # 5. Archiving & Indexing Logic
    week_end = result.get("citywide", {}).get("report_period", {}).get("week_end")
    if week_end:
        try:
            date_obj = datetime.strptime(week_end.strip(), "%m/%d/%Y")
            date_str = date_obj.strftime("%Y-%m-%d")
            
            archive_dir = output_dir / "archive"
            archive_dir.mkdir(parents=True, exist_ok=True)
            
            archive_path = archive_dir / f"{date_str}.json"
            with open(archive_path, "w") as f:
                json.dump(result, f, indent=2)

            index_path = output_dir / "index.json"
            history = []
            if index_path.exists():
                try:
                    with open(index_path, "r") as f: 
                        history = json.load(f)
                    if not isinstance(history, list):
                        history = []
                except json.JSONDecodeError:
                    logger.warning("Corrupted index.json found. Creating a new one.")
                    history = []
            
            if not any(h.get('date') == date_str for h in history):
                history.append({
                    "date": date_str, 
                    "label": f"Week Ending {week_end.strip()}", 
                    "path": f"archive/{date_str}.json"
                })
                history.sort(key=lambda x: x['date'], reverse=True)
                with open(index_path, "w") as f: 
                    json.dump(history, f, indent=2)
            
            logger.info(f"Archive and index updated for {date_str}")
        except Exception as e:
            logger.error(f"Archiving failed: {e}")

    logger.info("Scrape complete.")

if __name__ == "__main__":
    main()
