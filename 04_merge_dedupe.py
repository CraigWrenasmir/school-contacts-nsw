from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path

import pandas as pd
import yaml

from utils.cleaner import dedupe_prefer_email, standardise_dataframe

ROOT = Path(__file__).resolve().parent
CONFIG = yaml.safe_load((ROOT / "config.yml").read_text())


def load_or_empty(path: Path) -> pd.DataFrame:
    if path.exists():
        return pd.read_csv(path, dtype=str)
    return pd.DataFrame()


def sector_summary(df: pd.DataFrame, sector: str) -> str:
    sec = df[df["sector"] == sector]
    total = len(sec)
    if total == 0:
        return f"{sector}: total=0 | email%=0.00 | contact_form_only%=0.00"

    email_count = sec["public_email"].notna().sum()
    form_only = sec["public_email"].isna() & sec["contact_form_url"].notna()
    form_only_count = form_only.sum()

    email_pct = (email_count / total) * 100
    form_only_pct = (form_only_count / total) * 100
    return (
        f"{sector}: total={total} | email%={email_pct:.2f} | "
        f"contact_form_only%={form_only_pct:.2f}"
    )


def save_sqlite(df: pd.DataFrame, db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        df.to_sql("schools_nsw_contacts", conn, if_exists="replace", index=False)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_school_name ON schools_nsw_contacts (school_name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_suburb ON schools_nsw_contacts (suburb)")
        conn.commit()
    finally:
        conn.close()


def main() -> None:
    gov = load_or_empty(ROOT / CONFIG["output"]["government_csv"])
    indep = load_or_empty(ROOT / CONFIG["output"]["independent_csv"])
    cath = load_or_empty(ROOT / CONFIG["output"]["catholic_csv"])

    merged = pd.concat([gov, indep, cath], ignore_index=True)
    if merged.empty:
        print("No sector outputs found. Run sector scripts first.")
        return

    merged = standardise_dataframe(merged)
    merged["last_verified_date"] = date.today().isoformat()
    merged = dedupe_prefer_email(merged)

    merged_csv = ROOT / CONFIG["output"]["merged_csv"]
    merged_db = ROOT / CONFIG["output"]["merged_sqlite"]
    merged.to_csv(merged_csv, index=False)
    save_sqlite(merged, merged_db)

    print(f"Merged records: {len(merged)}")
    print(sector_summary(merged, "government"))
    print(sector_summary(merged, "independent"))
    print(sector_summary(merged, "catholic"))
    print("NSW School Contact Database Build Complete")


if __name__ == "__main__":
    main()
