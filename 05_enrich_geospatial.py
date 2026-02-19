from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import pgeocode
import yaml

ROOT = Path(__file__).resolve().parent
CONFIG = yaml.safe_load((ROOT / "config.yml").read_text())


def normalise_postcode(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return None
    return digits.zfill(4)[-4:]


def build_postcode_lookup() -> pd.DataFrame:
    nomi = pgeocode.Nominatim("au")
    base = nomi._data.copy()
    base["postal_code"] = base["postal_code"].astype(str).str.zfill(4)
    base["state_code"] = base["state_code"].astype(str).str.upper()
    base = base[base["state_code"] == "NSW"][["postal_code", "latitude", "longitude"]]
    base = base.dropna(subset=["latitude", "longitude"]).drop_duplicates(subset=["postal_code"])
    return base


def save_sqlite(df: pd.DataFrame, db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        df.to_sql("schools_nsw_contacts", conn, if_exists="replace", index=False)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_school_name ON schools_nsw_contacts (school_name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_suburb ON schools_nsw_contacts (suburb)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_lat_lon ON schools_nsw_contacts (lat, lon)")
        conn.commit()
    finally:
        conn.close()


def main() -> None:
    merged_csv = ROOT / CONFIG["output"]["merged_csv"]
    merged_sqlite = ROOT / CONFIG["output"]["merged_sqlite"]

    df = pd.read_csv(merged_csv, dtype=str)
    df["postcode_norm"] = df["postcode"].map(normalise_postcode)

    postcodes = build_postcode_lookup()
    df = df.merge(postcodes, how="left", left_on="postcode_norm", right_on="postal_code")
    df = df.rename(columns={"latitude": "lat", "longitude": "lon"})
    df = df.drop(columns=["postal_code", "postcode_norm"])

    # Keep numeric columns as text-compatible floats in CSV and proper REAL in SQLite.
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")

    df.to_csv(merged_csv, index=False)
    save_sqlite(df, merged_sqlite)
    print(f"Geospatial enrichment complete: {len(df)} rows updated with lat/lon where postcode matched.")


if __name__ == "__main__":
    main()
