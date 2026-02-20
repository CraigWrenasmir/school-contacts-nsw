from __future__ import annotations

import argparse
import sqlite3
from collections import Counter
from pathlib import Path

import pandas as pd

from utils.extractors import classify_public_email

ROOT = Path(__file__).resolve().parent

CSV_PATHS = {
    "nsw": ROOT / "outputs" / "schools_nsw_contacts.csv",
    "vic": ROOT / "outputs" / "schools_vic_contacts.csv",
    "qld": ROOT / "outputs" / "schools_qld_contacts.csv",
    "wa": ROOT / "outputs" / "schools_wa_contacts.csv",
}

SQLITE_PATHS = {
    "nsw": ROOT / "outputs" / "schools_nsw_contacts.sqlite",
    "vic": ROOT / "outputs" / "schools_vic_contacts.sqlite",
    "qld": ROOT / "outputs" / "schools_qld_contacts.sqlite",
    "wa": ROOT / "outputs" / "schools_wa_contacts.sqlite",
}


def save_sqlite(df: pd.DataFrame, db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        df.to_sql("schools_contacts", conn, if_exists="replace", index=False)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_school_name ON schools_contacts (school_name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_suburb ON schools_contacts (suburb)")
        if "lat" in df.columns and "lon" in df.columns:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_lat_lon ON schools_contacts (lat, lon)")
        conn.commit()
    finally:
        conn.close()


def clean_state(state: str) -> None:
    in_csv = CSV_PATHS[state]
    out_sqlite = SQLITE_PATHS[state]

    if not in_csv.exists():
        print(f"[{state}] skipped (missing CSV): {in_csv}")
        return

    df = pd.read_csv(in_csv, dtype=str)
    if "public_email" not in df.columns:
        print(f"[{state}] skipped (missing public_email column)")
        return

    statuses: list[str] = []
    reasons: list[str] = []
    cleaned: list[str] = []

    for _, row in df.iterrows():
        source_email = str(row.get("public_email") or "").strip()
        website_url = str(row.get("website_url") or "").strip() or None
        normalised, status, reason = classify_public_email(source_email, website_url=website_url, source="mixed")
        statuses.append(status)
        reasons.append(reason)
        cleaned.append(normalised if status == "valid" else "")

    before = int((df["public_email"].fillna("").astype(str).str.strip() != "").sum())
    df["public_email"] = cleaned
    after = int((df["public_email"].fillna("").astype(str).str.strip() != "").sum())

    # Keep diagnostics to help audit false positives over time.
    df["email_validation_status"] = statuses
    df["email_validation_reason"] = reasons

    df.to_csv(in_csv, index=False)
    if out_sqlite.exists() or state in {"nsw", "vic", "qld", "wa"}:
        save_sqlite(df, out_sqlite)

    status_counts = Counter(statuses)
    print(
        f"[{state}] rows={len(df)} emails_before={before} emails_after={after} "
        f"valid={status_counts.get('valid',0)} suspicious={status_counts.get('suspicious',0)} invalid={status_counts.get('invalid',0)}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean false-positive emails in published state datasets")
    parser.add_argument(
        "--states",
        nargs="+",
        default=["nsw", "vic", "qld", "wa"],
        help="States to clean (default: nsw vic qld wa)",
    )
    args = parser.parse_args()

    for state in args.states:
        code = state.strip().lower()
        if code not in CSV_PATHS:
            print(f"[{code}] skipped (unknown state code)")
            continue
        clean_state(code)


if __name__ == "__main__":
    main()
