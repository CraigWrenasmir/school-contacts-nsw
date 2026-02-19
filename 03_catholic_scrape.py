from __future__ import annotations

import json
import logging
import re
from datetime import date
from pathlib import Path
from urllib.parse import urlencode

import pandas as pd
import yaml
from bs4 import BeautifulSoup
from tqdm import tqdm

from utils.cleaner import standardise_dataframe
from utils.extractors import (
    choose_general_email,
    extract_contact_form_url,
    extract_emails_from_text,
    extract_mailto_emails,
)
from utils.http_client import EthicalHttpClient, HttpConfig

ROOT = Path(__file__).resolve().parent
CONFIG = yaml.safe_load((ROOT / "config.yml").read_text())


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


def ensure_http(url: str | None) -> str | None:
    if not url:
        return None
    clean = str(url).strip()
    if not clean:
        return None
    if clean.startswith("//"):
        return f"https:{clean}"
    if clean.startswith("http://") or clean.startswith("https://"):
        return clean
    return f"https://{clean}"


def extract_csnsw_search_path(html_text: str) -> str:
    m = re.search(r'"csnswSchoolFinder"\s*:\s*\{[^}]*"searchUrl"\s*:\s*"([^"]+)"', html_text)
    if not m:
        raise ValueError("Could not find csnsw searchUrl in page settings")
    return m.group(1).replace("\\/", "/")


def query_catholic_directory(client: EthicalHttpClient, search_path: str) -> list[dict]:
    base = "https://www.csnsw.catholic.edu.au/api/v1/elasticsearch/"
    endpoint = f"{base}{search_path}"

    source_payload = {
        "from": 0,
        "size": 5000,
        "query": {"match_all": {}},
        "fields": [
            "uuid",
            "age",
            "facilities",
            "type",
            "diocese",
            "latlon",
            "location",
            "email",
            "name",
            "phone_area_code",
            "phone_number",
            "postcode",
            "school_url",
            "sector",
            "state",
            "suburb",
            "year_range",
        ],
        "_source": False,
    }

    params = urlencode(
        {
            "source_content_type": "application/json",
            "source": json.dumps(source_payload, separators=(",", ":")),
        }
    )
    response = client.get(f"{endpoint}?{params}")
    response.raise_for_status()
    return response.json().get("hits", {}).get("hits", [])


def first_or_none(value):
    if isinstance(value, list) and value:
        return value[0]
    return None


def combine_phone(area: str | None, number: str | None) -> str | None:
    area_clean = (area or "").strip()
    number_clean = (number or "").strip()
    if area_clean and number_clean:
        return f"{area_clean} {number_clean}"
    if number_clean:
        return number_clean
    return None


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
    scrape_logger, error_logger = build_loggers()

    http_cfg = HttpConfig(
        user_agent=CONFIG["user_agent"],
        request_delay_seconds=CONFIG["request_delay_seconds"],
        # Keep scraping resilient but prevent long stalls on slow school sites.
        timeout_seconds=min(int(CONFIG["timeout_seconds"]), 10),
        max_retries=1,
        backoff_factor=0.5,
    )
    client = EthicalHttpClient(http_cfg, scrape_logger=scrape_logger)

    source_cfg = CONFIG["sources"]["catholic"]
    output_file = ROOT / CONFIG["output"]["catholic_csv"]
    output_file.parent.mkdir(parents=True, exist_ok=True)

    rows = []
    processed = 0

    try:
        directory_url = source_cfg["directory_urls"][0]
        directory_resp = client.get(directory_url)
        directory_resp.raise_for_status()
        search_path = extract_csnsw_search_path(directory_resp.text)
        hits = query_catholic_directory(client, search_path)

        for hit in tqdm(hits, desc="Catholic schools"):
            try:
                fields = hit.get("fields", {})

                website_url = ensure_http(first_or_none(fields.get("school_url")))
                public_email = choose_general_email(fields.get("email") or [])
                contact_form_url = None

                if not public_email and website_url:
                    homepage_email, homepage_form = enrich_from_homepage(client, website_url)
                    if homepage_email:
                        public_email = homepage_email
                    contact_form_url = homepage_form

                rows.append(
                    {
                        "sector": "catholic",
                        "school_name": first_or_none(fields.get("name")),
                        "suburb": first_or_none(fields.get("suburb")),
                        "postcode": first_or_none(fields.get("postcode")),
                        "phone": combine_phone(
                            first_or_none(fields.get("phone_area_code")),
                            first_or_none(fields.get("phone_number")),
                        ),
                        "public_email": public_email,
                        "contact_form_url": contact_form_url,
                        "website_url": website_url,
                        "source_directory_url": source_cfg["source_directory_url"],
                        "last_verified_date": date.today().isoformat(),
                    }
                )

                processed += 1
                if processed % 100 == 0:
                    print(f"Catholic processed: {processed}")

            except Exception as exc:
                error_logger.exception("Catholic hit parse failed (%s): %s", hit.get("_id"), exc)

    except Exception as exc:
        error_logger.exception("Catholic directory failed: %s", exc)

    df = standardise_dataframe(pd.DataFrame(rows))
    df.to_csv(output_file, index=False)
    print(f"Catholic schools saved: {len(df)} -> {output_file}")


if __name__ == "__main__":
    main()
