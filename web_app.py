from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
import pgeocode
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "outputs" / "schools_nsw_contacts.sqlite"
TEMPLATES = Jinja2Templates(directory=str(ROOT / "templates"))
POSTCODE_RE = re.compile(r"^\d{4}$")

app = FastAPI(title="NSW School Contact Radius Search")
_nomi = pgeocode.Nominatim("au")


def load_schools() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query("SELECT * FROM schools_nsw_contacts", conn)
    finally:
        conn.close()

    if "lat" not in df.columns or "lon" not in df.columns:
        raise RuntimeError("Database missing lat/lon. Run 05_enrich_geospatial.py first.")

    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df = df.dropna(subset=["lat", "lon"]).copy()
    return df


def resolve_query_location(query: str, schools_df: pd.DataFrame) -> tuple[float, float, str]:
    text = (query or "").strip()
    if not text:
        raise ValueError("Location query is required.")

    if POSTCODE_RE.match(text):
        row = _nomi.query_postal_code(text)
        if pd.isna(row.latitude) or pd.isna(row.longitude) or str(row.state_code).upper() != "NSW":
            raise ValueError(f"No NSW coordinate found for postcode '{text}'.")
        label = f"Postcode {text}"
        return float(row.latitude), float(row.longitude), label

    suburb_df = schools_df.dropna(subset=["suburb"]).copy()
    suburb_df["suburb_key"] = suburb_df["suburb"].astype(str).str.strip().str.lower()
    key = text.lower()
    exact = suburb_df[suburb_df["suburb_key"] == key]
    if not exact.empty:
        lat = float(exact["lat"].mean())
        lon = float(exact["lon"].mean())
        return lat, lon, f"Suburb {text.title()}"

    data = _nomi._data.copy()
    data["state_code"] = data["state_code"].astype(str).str.upper()
    data = data[(data["state_code"] == "NSW") & data["latitude"].notna() & data["longitude"].notna()]
    match = data[data["place_name"].astype(str).str.contains(text, case=False, na=False)].head(1)
    if not match.empty:
        row = match.iloc[0]
        return float(row["latitude"]), float(row["longitude"]), str(row["place_name"])

    raise ValueError(f"Could not resolve location '{text}' to NSW coordinates.")


def haversine_km(lat1: float, lon1: float, lat2: np.ndarray, lon2: np.ndarray) -> np.ndarray:
    r = 6371.0
    p = np.pi / 180.0
    dlat = (lat2 - lat1) * p
    dlon = (lon2 - lon1) * p
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1 * p) * np.cos(lat2 * p) * np.sin(dlon / 2.0) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    return r * c


def run_radius_search(location: str, radius_km: float, limit: int = 500) -> dict:
    if radius_km <= 0:
        raise ValueError("radius_km must be greater than 0.")

    schools = load_schools()
    lat, lon, resolved_label = resolve_query_location(location, schools)
    schools["distance_km"] = haversine_km(lat, lon, schools["lat"].to_numpy(), schools["lon"].to_numpy())
    result = schools[schools["distance_km"] <= radius_km].copy()
    result = result.sort_values("distance_km").head(limit)

    cols = [
        "sector",
        "school_name",
        "suburb",
        "postcode",
        "phone",
        "public_email",
        "contact_form_url",
        "website_url",
        "distance_km",
    ]
    payload = result[cols].fillna("").to_dict(orient="records")
    for row in payload:
        row["distance_km"] = round(float(row["distance_km"]), 2)

    return {
        "query": location,
        "resolved_location": resolved_label,
        "center": {"lat": lat, "lon": lon},
        "radius_km": radius_km,
        "count": len(payload),
        "results": payload,
    }


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return TEMPLATES.TemplateResponse("index.html", {"request": request})


@app.get("/api/search", response_class=JSONResponse)
async def api_search(
    location: str = Query(..., description="NSW postcode or location text"),
    radius_km: float = Query(20, ge=0.1, le=500),
    limit: int = Query(500, ge=1, le=2000),
) -> JSONResponse:
    try:
        data = run_radius_search(location=location, radius_km=radius_km, limit=limit)
        return JSONResponse(data)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    except Exception as exc:
        return JSONResponse({"error": f"Unexpected error: {exc}"}, status_code=500)
