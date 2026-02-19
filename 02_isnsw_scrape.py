from __future__ import annotations

import html
import json
import logging
import re
from datetime import date
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlparse

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
INERTIA_RE = re.compile(r'data-page="([^"]+)"')


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


def parse_data_page_json(page_html: str) -> dict:
    m = INERTIA_RE.search(page_html)
    if not m:
        raise ValueError("Could not find data-page JSON")
    return json.loads(html.unescape(m.group(1)))


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


def extract_unifyd_search_urls(soup: BeautifulSoup, page_url: str) -> list[str]:
    urls: list[str] = []
    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if not href:
            continue
        full = urljoin(page_url, href)
        lower = full.lower()
        if "directory.unifyd.com.au" in lower and "/search" in lower:
            urls.append(full)

    if "directory.unifyd.com.au" in page_url.lower() and "/search" in page_url.lower():
        urls.append(page_url)

    if not urls:
        urls.append("https://isnsw-school-finder.directory.unifyd.com.au/search")

    return list(dict.fromkeys(urls))


def page_count_from_links(links: list[dict]) -> int:
    pages = [1]
    for link in links:
        url = link.get("url") if isinstance(link, dict) else None
        if not url:
            continue
        try:
            q = parse_qs(urlparse(url).query)
            if "page" in q and q["page"]:
                pages.append(int(q["page"][0]))
        except Exception:
            continue
    return max(pages)


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

    source_cfg = CONFIG["sources"]["independent"]
    output_file = ROOT / CONFIG["output"]["independent_csv"]
    output_file.parent.mkdir(parents=True, exist_ok=True)

    rows = []
    processed = 0

    for seed_url in source_cfg["directory_urls"]:
        try:
            seed_resp = client.get(seed_url)
            seed_resp.raise_for_status()
            seed_soup = BeautifulSoup(seed_resp.text, "lxml")
            app_urls = extract_unifyd_search_urls(seed_soup, seed_url)

            for app_url in app_urls:
                first = client.get(app_url)
                first.raise_for_status()
                first_data = parse_data_page_json(first.text)

                total_pages = page_count_from_links(first_data.get("props", {}).get("links", []))
                if total_pages < 1:
                    total_pages = 1

                for page_no in range(1, total_pages + 1):
                    page_url = f"{app_url}?page={page_no}"
                    page_resp = first if page_no == 1 else client.get(page_url)
                    if page_resp.status_code >= 400:
                        continue

                    page_data = parse_data_page_json(page_resp.text)
                    listings = page_data.get("props", {}).get("listings", {}).get("data", [])

                    for listing in tqdm(listings, desc=f"ISNSW page {page_no}/{total_pages}"):
                        addr = listing.get("primaryAddress") or {}
                        phone_obj = listing.get("primaryPhoneNumber") or {}

                        website_url = ensure_http(listing.get("websiteUrl"))
                        public_email = choose_general_email([listing.get("emailAddress")])
                        contact_form_url = None

                        if not public_email and website_url:
                            homepage_email, homepage_form = enrich_from_homepage(client, website_url)
                            if homepage_email:
                                public_email = homepage_email
                            contact_form_url = homepage_form

                        rows.append(
                            {
                                "sector": "independent",
                                "school_name": listing.get("primaryName"),
                                "suburb": addr.get("city"),
                                "postcode": addr.get("postcode"),
                                "phone": phone_obj.get("raw"),
                                "public_email": public_email,
                                "contact_form_url": contact_form_url,
                                "website_url": website_url,
                                "source_directory_url": source_cfg["source_directory_url"],
                                "last_verified_date": date.today().isoformat(),
                            }
                        )

                        processed += 1
                        if processed % 100 == 0:
                            print(f"Independent processed: {processed}")

        except Exception as exc:
            error_logger.exception("ISNSW scrape failed (%s): %s", seed_url, exc)

    df = standardise_dataframe(pd.DataFrame(rows))
    df.to_csv(output_file, index=False)
    print(f"Independent schools saved: {len(df)} -> {output_file}")


if __name__ == "__main__":
    main()
