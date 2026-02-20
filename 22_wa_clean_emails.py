"""
22_wa_clean_emails.py

Removes invalid/fake emails from the WA schools CSV and deployed JSON.

"Fake" emails are those where the word "at" inside a word was misidentified
as @ by the text scraper (e.g. educ@ion.we, innov@ion.the, p@hway.we).

For each invalid email:
  - Clears public_email in the CSV
  - Resets website_checked to "false" so 16_wa_enrich_contacts.py re-scrapes
  - Clears public_email in docs/data/wa/schools.min.json (immediate fix)

Usage:
    python 22_wa_clean_emails.py           # apply changes
    python 22_wa_clean_emails.py --dry-run # report only, no changes written
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
CSV_PATH = ROOT / "outputs" / "schools_wa_contacts.csv"
JSON_PATH = ROOT / "docs" / "data" / "wa" / "schools.min.json"

ALLOWED_TLDS = {"au", "com", "org", "net", "edu", "gov", "school", "online"}
EMAIL_EXACT_RE = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.IGNORECASE)
INVISIBLE_CHARS_RE = re.compile(r"[\u200b\u200c\u200d\u2060\ufeff]")


def is_valid_email(email: str | None) -> bool:
    if not email:
        return False
    clean = INVISIBLE_CHARS_RE.sub("", str(email)).strip().lower().strip(".,;:!?\"'`()[]{}<>")
    if not clean or " " in clean or clean.count("@") != 1:
        return False
    if not EMAIL_EXACT_RE.match(clean):
        return False
    local, domain = clean.split("@", 1)
    if len(local) < 2:
        return False
    if "." not in domain:
        return False
    tld = domain.rsplit(".", 1)[-1]
    if not re.fullmatch(r"[a-z]{2,}", tld):
        return False
    return tld in ALLOWED_TLDS


def clean_csv(dry_run: bool) -> int:
    df = pd.read_csv(CSV_PATH, dtype=str)
    cleared = 0
    for i, row in df.iterrows():
        email = str(row.get("public_email") or "").strip()
        if email and email.lower() != "nan" and not is_valid_email(email):
            if not dry_run:
                df.at[i, "public_email"] = None
                df.at[i, "website_checked"] = "false"
            cleared += 1
    if not dry_run:
        df.to_csv(CSV_PATH, index=False)
    return cleared


def clean_json(dry_run: bool) -> int:
    schools = json.loads(JSON_PATH.read_text(encoding="utf-8"))
    cleared = 0
    for school in schools:
        email = school.get("public_email") or ""
        if email and not is_valid_email(email):
            if not dry_run:
                school["public_email"] = ""
            cleared += 1
    if not dry_run:
        JSON_PATH.write_text(
            json.dumps(schools, separators=(",", ":"), ensure_ascii=True),
            encoding="utf-8",
        )
    return cleared


def main() -> None:
    parser = argparse.ArgumentParser(description="Remove fake emails from WA schools data")
    parser.add_argument("--dry-run", action="store_true", help="Report without writing changes")
    args = parser.parse_args()

    tag = "[dry-run] " if args.dry_run else ""

    csv_cleared = clean_csv(args.dry_run)
    verb = "would clear" if args.dry_run else "cleared"
    print(f"{tag}CSV: {verb} {csv_cleared} bad emails, website_checked reset -> {CSV_PATH}")

    json_cleared = clean_json(args.dry_run)
    print(f"{tag}JSON: {verb} {json_cleared} bad emails -> {JSON_PATH}")

    if not args.dry_run:
        print()
        print("Next steps:")
        print("  1. Run 16_wa_enrich_contacts.py to re-scrape the cleared schools")
        print("  2. Run: python 07_export_state_static_data.py --state wa --csv outputs/schools_wa_contacts.csv")
        print("     to regenerate the JSON after re-scraping")


if __name__ == "__main__":
    main()
