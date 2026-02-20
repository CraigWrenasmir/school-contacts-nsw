from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
ACARA_DATA_PAGE_URL = "https://acaraweb.azurewebsites.net/contact-us/acara-data-access"
SCHOOL_PROFILE_XLSX = (
    "https://dataandreporting.blob.core.windows.net/anrdataportal/Data-Access-Program/School%20Profile%202025.xlsx"
)
SCHOOL_LOCATION_XLSX = (
    "https://dataandreporting.blob.core.windows.net/anrdataportal/Data-Access-Program/School%20Location%202025.xlsx"
)
OUT_CSV = ROOT / "outputs" / "schools_tas_contacts.csv"
OUT_SQLITE = ROOT / "outputs" / "schools_tas_contacts.sqlite"


def clean_text(value: object) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return None if not s or s.lower() == "nan" else s


def ensure_http(url: str | None) -> str | None:
    if not url:
        return None
    s = str(url).strip()
    if not s or s.lower() == "nan":
        return None
    if s.startswith("//"):
        return "https:" + s
    if s.startswith("http://") or s.startswith("https://"):
        return s
    return "https://" + s


def map_sector(value: str | None) -> str:
    s = (value or "").strip().lower()
    if s.startswith("gov"):
        return "government"
    if s.startswith("cath"):
        return "catholic"
    if s.startswith("ind"):
        return "independent"
    return "unknown"


def norm_name(value: str | None) -> str:
    return " ".join((value or "").strip().lower().split())


def save_sqlite(df: pd.DataFrame, db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        df.to_sql("schools_contacts", conn, if_exists="replace", index=False)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_school_name ON schools_contacts (school_name)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_suburb ON schools_contacts (suburb)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_lat_lon ON schools_contacts (lat, lon)")
        conn.commit()
    finally:
        conn.close()


def main() -> None:
    profile = pd.read_excel(SCHOOL_PROFILE_XLSX, sheet_name="SchoolProfile 2025", dtype=str)
    profile = profile[profile["State"].fillna("").str.upper() == "TAS"].copy()

    location = pd.read_excel(SCHOOL_LOCATION_XLSX, sheet_name="SchoolLocations 2025", dtype=str)
    location = location[location["State"].fillna("").str.upper() == "TAS"].copy()

    loc_idx: dict[tuple[str, str, str], tuple[float | None, float | None]] = {}
    for _, row in location.iterrows():
        key = (
            norm_name(clean_text(row.get("School Name"))),
            str(clean_text(row.get("Postcode")) or ""),
            map_sector(clean_text(row.get("School Sector"))),
        )
        lat = pd.to_numeric(clean_text(row.get("Latitude")), errors="coerce")
        lon = pd.to_numeric(clean_text(row.get("Longitude")), errors="coerce")
        if pd.isna(lat) or pd.isna(lon):
            continue
        loc_idx[key] = (float(lat), float(lon))

    rows = []
    for _, row in profile.iterrows():
        sector = map_sector(clean_text(row.get("School Sector")))
        school_name = clean_text(row.get("School Name"))
        suburb = clean_text(row.get("Suburb"))
        postcode = clean_text(row.get("Postcode"))
        website = ensure_http(clean_text(row.get("School URL")))

        key = (norm_name(school_name), str(postcode or ""), sector)
        lat, lon = loc_idx.get(key, (None, None))

        rows.append(
            {
                "sector": sector,
                "school_name": school_name,
                "suburb": (suburb or "").title() if suburb else None,
                "postcode": postcode,
                "phone": None,
                "public_email": None,
                "contact_form_url": None,
                "website_url": website,
                "source_directory_url": ACARA_DATA_PAGE_URL,
                "last_verified_date": date.today().isoformat(),
                "lat": lat,
                "lon": lon,
                "website_checked": "false",
            }
        )

    out = pd.DataFrame(rows)
    out = out.drop_duplicates(subset=["school_name", "suburb"], keep="first")
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_CSV, index=False)
    save_sqlite(out, OUT_SQLITE)

    print(f"TAS schools saved: {len(out)} -> {OUT_CSV}")
    print(f"TAS sqlite saved: {OUT_SQLITE}")


if __name__ == "__main__":
    main()
