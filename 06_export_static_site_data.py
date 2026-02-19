from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
CSV_PATH = ROOT / "outputs" / "schools_nsw_contacts.csv"
DOCS_DATA_DIR = ROOT / "docs" / "data"


def clean_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() == "nan" else text


def normalise_postcode(value: object) -> str:
    text = clean_text(value)
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return ""
    return digits.zfill(4)[-4:]


def main() -> None:
    df = pd.read_csv(CSV_PATH, dtype=str)
    if "lat" not in df.columns or "lon" not in df.columns:
        raise RuntimeError("CSV is missing lat/lon columns. Run 05_enrich_geospatial.py first.")

    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df = df.dropna(subset=["lat", "lon"]).copy()

    schools = []
    for _, row in df.iterrows():
        schools.append(
            {
                "sector": clean_text(row.get("sector")),
                "school_name": clean_text(row.get("school_name")),
                "suburb": clean_text(row.get("suburb")),
                "postcode": normalise_postcode(row.get("postcode")),
                "phone": clean_text(row.get("phone")),
                "public_email": clean_text(row.get("public_email")),
                "contact_form_url": clean_text(row.get("contact_form_url")),
                "website_url": clean_text(row.get("website_url")),
                "lat": float(row["lat"]),
                "lon": float(row["lon"]),
            }
        )

    postcodes = (
        df.assign(postcode_norm=df["postcode"].map(normalise_postcode))
        .query("postcode_norm != ''")
        .groupby("postcode_norm", as_index=False)[["lat", "lon"]]
        .mean()
    )
    postcode_centroids = {
        str(row["postcode_norm"]): {"lat": float(row["lat"]), "lon": float(row["lon"])}
        for _, row in postcodes.iterrows()
    }

    suburbs = (
        df.assign(suburb_norm=df["suburb"].fillna("").astype(str).str.strip())
        .query("suburb_norm != ''")
        .groupby("suburb_norm", as_index=False)[["lat", "lon"]]
        .mean()
    )
    suburb_centroids = [
        {"suburb": str(row["suburb_norm"]), "lat": float(row["lat"]), "lon": float(row["lon"])}
        for _, row in suburbs.iterrows()
    ]

    DOCS_DATA_DIR.mkdir(parents=True, exist_ok=True)
    (DOCS_DATA_DIR / "schools.min.json").write_text(
        json.dumps(schools, separators=(",", ":"), ensure_ascii=True),
        encoding="utf-8",
    )
    (DOCS_DATA_DIR / "postcode_centroids.min.json").write_text(
        json.dumps(postcode_centroids, separators=(",", ":"), ensure_ascii=True),
        encoding="utf-8",
    )
    (DOCS_DATA_DIR / "suburb_centroids.min.json").write_text(
        json.dumps(suburb_centroids, separators=(",", ":"), ensure_ascii=True),
        encoding="utf-8",
    )
    print(f"Exported static data: {len(schools)} schools")


if __name__ == "__main__":
    main()
