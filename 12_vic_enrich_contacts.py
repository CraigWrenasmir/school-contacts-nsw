from __future__ import annotations

import logging
import re
import argparse
import subprocess
from io import BytesIO
from datetime import date
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests
import yaml
from bs4 import BeautifulSoup

from utils.extractors import (
    choose_general_email,
    extract_cloudflare_protected_emails,
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
ACARA_SCHOOL_PROFILE_XLSX = (
    "https://dataandreporting.blob.core.windows.net/anrdataportal/Data-Access-Program/School%20Profile%202025.xlsx"
)


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


def load_vic_gov_index(client: EthicalHttpClient) -> tuple[dict[tuple[str, str], dict], dict[str, dict]]:
    r = client.get(FINDMYSCHOOL_GEOJSON)
    r.raise_for_status()
    data = r.json()
    idx: dict[tuple[str, str], dict] = {}
    by_name: dict[str, list[dict]] = {}
    for f in data.get("features", []):
        p = f.get("properties", {})
        name = norm_name(p.get("School_Name", ""))
        postcode = str(p.get("Campus_Postcode") or "").strip()
        if not name:
            continue
        idx[(name, postcode)] = p
        by_name.setdefault(name, []).append(p)
    unique_name = {name: rows[0] for name, rows in by_name.items() if len(rows) == 1}
    return idx, unique_name


def load_vic_school_urls_from_acara(client: EthicalHttpClient) -> tuple[dict[tuple[str, str], str], dict[str, str]]:
    try:
        resp = client.get(ACARA_SCHOOL_PROFILE_XLSX)
        if resp.status_code >= 400:
            return {}, {}
        raw = pd.read_excel(BytesIO(resp.content), sheet_name="SchoolProfile 2025", dtype=str)
    except Exception:
        return {}, {}
    raw = raw[raw["State"].fillna("").str.upper() == "VIC"].copy()

    idx: dict[tuple[str, str], str] = {}
    by_name: dict[str, list[str]] = {}
    for _, row in raw.iterrows():
        school_name = norm_name(row.get("School Name", ""))
        postcode = str(row.get("Postcode") or "").strip()
        website = ensure_http(row.get("School URL"))
        if not school_name or not postcode or not website:
            continue
        idx[(school_name, postcode)] = website
        by_name.setdefault(school_name, []).append(website)
    unique_name = {name: urls[0] for name, urls in by_name.items() if len(set(urls)) == 1}
    return idx, unique_name


def get_with_tls_fallback(
    client: EthicalHttpClient,
    url: str,
    error_logger: logging.Logger,
) -> tuple[int | None, str | None]:
    try:
        resp = client.get(url)
        return int(resp.status_code), resp.text
    except PermissionError as exc:
        error_logger.error("Blocked by robots.txt for %s: %s", url, exc)
        return None, None
    except requests.exceptions.SSLError as exc:
        error_logger.error("SSL failed via requests for %s; trying curl fallback: %s", url, exc)
        try:
            cmd = [
                "curl",
                "-L",
                "-sS",
                "--max-time",
                str(int(CONFIG["timeout_seconds"])),
                "-A",
                CONFIG["user_agent"],
                url,
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, check=False)
            if result.returncode != 0:
                error_logger.error("curl fallback failed (%s): %s", url, (result.stderr or "").strip())
                return None, None
            return 200, result.stdout
        except Exception as curl_exc:
            error_logger.error("curl fallback exception (%s): %s", url, curl_exc)
            return None, None


def enrich_from_homepage(client: EthicalHttpClient, website_url: str) -> tuple[str | None, str | None]:
    def extract_from_soup(soup: BeautifulSoup, base_url: str) -> tuple[str | None, str | None]:
        text = soup.get_text("\n", strip=True)
        mailto_emails = extract_mailto_emails(soup)
        cloudflare_emails = extract_cloudflare_protected_emails(soup)
        text_emails = extract_emails_from_text(text)
        email = (
            choose_general_email(mailto_emails, website_url=base_url, source="mailto")
            or choose_general_email(cloudflare_emails, website_url=base_url, source="cloudflare")
            or choose_general_email(text_emails, website_url=base_url, source="text")
        )
        form_url = extract_contact_form_url(soup, base_url)
        return email, form_url

    def candidate_contact_urls(soup: BeautifulSoup, base_url: str) -> list[str]:
        candidates: list[str] = []
        for a in soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            label = (a.get_text(" ", strip=True) or "").lower()
            if not href:
                continue
            if "contact" in href.lower() or "contact" in label:
                candidates.append(urljoin(base_url, href))
        for path in [
            "/contact",
            "/contact-us",
            "/contactus",
            "/about/contact",
            "/about-us/contact",
            "/enrolments",
        ]:
            candidates.append(urljoin(base_url, path))
        # de-dupe preserving order
        seen = set()
        out = []
        for u in candidates:
            key = u.lower().rstrip("/")
            if key in seen:
                continue
            seen.add(key)
            out.append(u)
        return out[:8]

    try:
        status_code, html = get_with_tls_fallback(client, website_url, error_logger=logging.getLogger("errors"))
        if not html or (status_code is not None and status_code >= 400):
            return None, None
        soup = BeautifulSoup(html, "lxml")
        email, form_url = extract_from_soup(soup, website_url)
        if email and form_url:
            return email, form_url

        # Follow likely contact pages to maximize email capture.
        for cu in candidate_contact_urls(soup, website_url):
            try:
                c_status_code, c_html = get_with_tls_fallback(client, cu, error_logger=logging.getLogger("errors"))
                if not c_html or (c_status_code is not None and c_status_code >= 400):
                    continue
                cs = BeautifulSoup(c_html, "lxml")
                ce, cf = extract_from_soup(cs, cu)
                if ce and not email:
                    email = ce
                if cf and not form_url:
                    form_url = cf
                if email and form_url:
                    break
            except Exception:
                continue
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
    if "website_checked" not in df.columns:
        df["website_checked"] = "false"

    http_cfg = HttpConfig(
        user_agent=CONFIG["user_agent"],
        request_delay_seconds=CONFIG["request_delay_seconds"],
        timeout_seconds=min(int(CONFIG["timeout_seconds"]), 7),
        max_retries=0,
        backoff_factor=0.0,
    )
    client = EthicalHttpClient(http_cfg, scrape_logger=scrape_logger)

    gov_index, gov_by_name = load_vic_gov_index(client)
    acara_urls, acara_by_name = load_vic_school_urls_from_acara(client)

    mapped = 0
    for i, row in df.iterrows():
        key = (norm_name(row.get("school_name", "")), str(row.get("postcode") or "").strip())
        school_name_key = norm_name(row.get("school_name", ""))
        sector = (row.get("sector") or "").strip().lower()

        # ACARA official school profile has cross-sector website URLs.
        acara_website = acara_urls.get(key) or acara_by_name.get(school_name_key)
        if acara_website:
            df.at[i, "website_url"] = acara_website

        # Keep authoritative VIC government phone + website where available.
        if sector == "government":
            p = gov_index.get(key) or gov_by_name.get(school_name_key)
            if not p:
                continue
            website = ensure_http(p.get("School_Website"))
            phone = str(p.get("School_Phone") or "").strip()
            if website:
                df.at[i, "website_url"] = website
            if phone:
                df.at[i, "phone"] = phone
            mapped += 1
    print(f"VIC mapping prepared: gov_mapped={mapped}, acara_url_keys={len(acara_urls)}", flush=True)

    processed = 0
    attempted = 0
    try:
        for i, row in df.iterrows():
            website = ensure_http(row.get("website_url"))
            if not website:
                continue
            checked = str(row.get("website_checked") or "").strip().lower() == "true"
            if checked:
                continue
            existing_email = str(row.get("public_email") or "").strip()
            existing_form = str(row.get("contact_form_url") or "").strip()

            if args.max_sites and attempted >= args.max_sites:
                break
            attempted += 1

            try:
                email, form_url = enrich_from_homepage(client, website)
                if email and (not existing_email or existing_email.lower() == "nan"):
                    df.at[i, "public_email"] = email
                if form_url and (not existing_form or existing_form.lower() == "nan"):
                    df.at[i, "contact_form_url"] = form_url
                df.at[i, "website_checked"] = "true"
                processed += 1
            except Exception as exc:
                df.at[i, "website_checked"] = "true"
                error_logger.exception("VIC website enrichment failed (%s): %s", website, exc)

            if attempted % args.checkpoint_every == 0:
                df["last_verified_date"] = date.today().isoformat()
                df.to_csv(OUT_CSV, index=False)
                print(
                    f"VIC website enrichment attempted: {attempted}, processed: {processed} (checkpoint saved)",
                    flush=True,
                )
    except KeyboardInterrupt:
        print("VIC enrichment interrupted; saving progress...", flush=True)
    finally:
        df["last_verified_date"] = date.today().isoformat()
        df.to_csv(OUT_CSV, index=False)

    print(f"VIC gov website mapping complete: {mapped} schools", flush=True)
    print(f"VIC website enrichment complete on {processed} rows (attempted {attempted} sites)", flush=True)
    print(f"Saved: {OUT_CSV}")


if __name__ == "__main__":
    main()
