from __future__ import annotations

import logging
import re
import argparse
from datetime import date
from pathlib import Path

import pandas as pd
import yaml
from bs4 import BeautifulSoup

from utils.extractors import (
    choose_general_email,
    extract_contact_form_url,
    extract_emails_from_text,
    extract_mailto_emails,
)
from utils.http_client import EthicalHttpClient, HttpConfig

ROOT = Path(__file__).resolve().parent
CONFIG = yaml.safe_load((ROOT / "config.yml").read_text())
IN_CSV = ROOT / "outputs" / "schools_vic_contacts.csv"
OUT_CSV = ROOT / "outputs" / "schools_vic_contacts.csv"

FINDMYSCHOOL_GEOJSON = "https://www.findmyschool.vic.gov.au/schools/schools-2025.json"


def build_loggers() -> tuple[logging.Logger, logging.Logger]:
    logging_cfg = CONFIG["logging"]
    scrape_logger = logging.getLogger("scrape")
    scrape_logger.setLevel(logging.INFO)
    if not scrape_logger.handlers:
        fh = logging.FileHandler(ROOT / logging_cfg["scrape_log"])
        fh.setFormatter(logging.Formatter("%(asctime)s | %(message)s"))
        scrape_logger.addHandler(fh)

    error_logger = logging.getLogger("errors")
    error_logger.setLevel(logging.ERROR)
    if not error_logger.handlers:
        eh = logging.FileHandler(ROOT / logging_cfg["error_log"])
        eh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        error_logger.addHandler(eh)

    return scrape_logger, error_logger


def norm_name(value: str) -> str:
    v = (value or "").strip().lower()
    v = re.sub(r"[^a-z0-9]+", " ", v)
    v = re.sub(r"\s+", " ", v).strip()
    return v


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


def load_vic_gov_index(client: EthicalHttpClient) -> dict[tuple[str, str], dict]:
    r = client.get(FINDMYSCHOOL_GEOJSON)
    r.raise_for_status()
    data = r.json()
    idx: dict[tuple[str, str], dict] = {}
    for f in data.get("features", []):
        p = f.get("properties", {})
        name = norm_name(p.get("School_Name", ""))
        postcode = str(p.get("Campus_Postcode") or "").strip()
        if not name:
            continue
        idx[(name, postcode)] = p
    return idx


def enrich_from_homepage(client: EthicalHttpClient, website_url: str) -> tuple[str | None, str | None]:
    try:
        resp = client.get(website_url)
        if resp.status_code >= 400:
            return None, None
        soup = BeautifulSoup(resp.text, "lxml")
        all_text = soup.get_text("\n", strip=True)
        emails = extract_mailto_emails(soup) + extract_emails_from_text(all_text)
        email = choose_general_email(emails)
        form_url = extract_contact_form_url(soup, website_url)
        return email, form_url
    except Exception:
        return None, None


def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich VIC contacts from official school websites")
    parser.add_argument("--max-sites", type=int, default=0, help="Optional limit of website rows to process (0=all)")
    parser.add_argument("--checkpoint-every", type=int, default=100, help="Save CSV every N processed rows")
    args = parser.parse_args()

    scrape_logger, error_logger = build_loggers()
    df = pd.read_csv(IN_CSV, dtype=str)

    if "public_email" not in df.columns:
        df["public_email"] = None
    if "contact_form_url" not in df.columns:
        df["contact_form_url"] = None
    if "website_url" not in df.columns:
        df["website_url"] = None

    http_cfg = HttpConfig(
        user_agent=CONFIG["user_agent"],
        request_delay_seconds=CONFIG["request_delay_seconds"],
        timeout_seconds=min(int(CONFIG["timeout_seconds"]), 10),
        max_retries=1,
        backoff_factor=0.5,
    )
    client = EthicalHttpClient(http_cfg, scrape_logger=scrape_logger)

    gov_index = load_vic_gov_index(client)

    mapped = 0
    for i, row in df.iterrows():
        if (row.get("sector") or "").strip().lower() != "government":
            continue
        key = (norm_name(row.get("school_name", "")), str(row.get("postcode") or "").strip())
        p = gov_index.get(key)
        if not p:
            continue
        website = ensure_http(p.get("School_Website"))
        phone = str(p.get("School_Phone") or "").strip()
        if website:
            df.at[i, "website_url"] = website
        if phone:
            df.at[i, "phone"] = phone
        mapped += 1

    processed = 0
    attempted = 0
    for i, row in df.iterrows():
        website = ensure_http(row.get("website_url"))
        if not website:
            continue
        existing_email = str(row.get("public_email") or "").strip()
        existing_form = str(row.get("contact_form_url") or "").strip()
        if existing_email and existing_email.lower() != "nan" and existing_form and existing_form.lower() != "nan":
            continue

        if args.max_sites and attempted >= args.max_sites:
            break
        attempted += 1

        try:
            email, form_url = enrich_from_homepage(client, website)
            if email and (not existing_email or existing_email.lower() == "nan"):
                df.at[i, "public_email"] = email
            if form_url and (not existing_form or existing_form.lower() == "nan"):
                df.at[i, "contact_form_url"] = form_url
            processed += 1
            if processed % args.checkpoint_every == 0:
                df["last_verified_date"] = date.today().isoformat()
                df.to_csv(OUT_CSV, index=False)
                print(f"VIC website enrichment processed: {processed} (checkpoint saved)", flush=True)
        except Exception as exc:
            error_logger.exception("VIC website enrichment failed (%s): %s", website, exc)

    df["last_verified_date"] = date.today().isoformat()
    df.to_csv(OUT_CSV, index=False)
    print(f"VIC gov website mapping complete: {mapped} schools", flush=True)
    print(f"VIC website enrichment complete on {processed} rows (attempted {attempted} sites)", flush=True)
    print(f"Saved: {OUT_CSV}")


if __name__ == "__main__":
    main()
