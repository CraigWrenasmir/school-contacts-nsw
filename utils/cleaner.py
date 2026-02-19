from __future__ import annotations

import re
from typing import Optional

import pandas as pd

EMAIL_RE = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.IGNORECASE)


def normalise_suburb(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    clean = str(value).strip()
    return clean.title() if clean else None


def clean_str(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    clean = str(value).strip()
    return clean or None


def validate_email(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    if pd.isna(value):
        return None
    email = str(value).strip().lower()
    if not email:
        return None
    return email if EMAIL_RE.match(email) else None


def standardise_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    expected = [
        "sector",
        "school_name",
        "suburb",
        "postcode",
        "phone",
        "public_email",
        "contact_form_url",
        "website_url",
        "source_directory_url",
        "last_verified_date",
    ]

    for col in expected:
        if col not in df.columns:
            df[col] = None

    df = df[expected].copy()
    for col in ["school_name", "postcode", "phone", "contact_form_url", "website_url", "source_directory_url"]:
        df[col] = df[col].map(clean_str)

    df["suburb"] = df["suburb"].map(normalise_suburb)
    df["public_email"] = df["public_email"].map(validate_email)

    return df


def dedupe_prefer_email(df: pd.DataFrame) -> pd.DataFrame:
    working = df.copy()
    working["_name_key"] = working["school_name"].fillna("").str.strip().str.lower()
    working["_suburb_key"] = working["suburb"].fillna("").str.strip().str.lower()
    working["_has_email"] = working["public_email"].notna().astype(int)

    working = working.sort_values(["_name_key", "_suburb_key", "_has_email"], ascending=[True, True, False])
    working = working.drop_duplicates(subset=["_name_key", "_suburb_key"], keep="first")

    return working.drop(columns=["_name_key", "_suburb_key", "_has_email"])
