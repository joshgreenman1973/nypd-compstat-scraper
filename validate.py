#!/usr/bin/env python3
"""
Validates CompStat scraper output before committing to the repo.
Checks that the JSON has the expected structure and non-null values
for key crime categories. Run this in CI to catch format changes early.
"""

import argparse
import json
import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

EXPECTED_CRIMES = ["Murder", "Rape", "Robbery", "Fel. Assault", "Burglary", "Gr. Larceny", "G.L.A."]


def validate_data(filepath: str) -> bool:
    """Validate the scraped CompStat JSON. Returns True if valid."""
    try:
        with open(filepath, "r") as f:
            data = json.load(f)
    except FileNotFoundError:
        logger.error(f"File not found: {filepath}")
        return False
    except json.JSONDecodeError:
        logger.error(f"Invalid JSON in: {filepath}")
        return False

    if "citywide" not in data:
        logger.error("Missing 'citywide' key in JSON.")
        return False

    citywide = data["citywide"]

    # Check report period was parsed
    period = citywide.get("report_period", {})
    if not period.get("week_start") or not period.get("week_end"):
        logger.error("Missing report period dates.")
        return False
    logger.info(f"Report period: {period.get('week_start')} - {period.get('week_end')}")

    # Check all 7 major felonies are present
    felonies = citywide.get("seven_major_felonies", {})
    missing = [c for c in EXPECTED_CRIMES if c not in felonies]
    if missing:
        logger.error(f"Missing crime categories: {missing}")
        return False

    # Check each category has numeric values
    errors = 0
    for crime in EXPECTED_CRIMES:
        crime_data = felonies[crime]
        for period_key in ("week_to_date", "twenty_eight_day", "year_to_date"):
            period_data = crime_data.get(period_key, {})
            current = period_data.get("current_year")
            if current is None or not isinstance(current, (int, float)):
                logger.error(f"{crime} → {period_key} → current_year is missing or non-numeric: {current}")
                errors += 1

    # Check total exists
    total = citywide.get("total_seven_major", {})
    if not total or not total.get("year_to_date", {}).get("current_year"):
        logger.error("Missing or empty total_seven_major.")
        errors += 1

    if errors > 0:
        logger.error(f"Validation failed with {errors} error(s).")
        return False

    # Summary
    ytd_total = total.get("year_to_date", {}).get("current_year", "?")
    ytd_prior = total.get("year_to_date", {}).get("prior_year", "?")
    ytd_chg = total.get("year_to_date", {}).get("pct_change", "?")
    logger.info(f"7 Major Felonies YTD: {ytd_total} vs {ytd_prior} ({ytd_chg}%)")
    logger.info("Validation passed.")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate CompStat scraper output")
    parser.add_argument("--file", required=True, help="Path to the JSON file to validate")
    args = parser.parse_args()

    if not validate_data(args.file):
        sys.exit(1)
