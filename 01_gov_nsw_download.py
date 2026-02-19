from __future__ import annotations

import io
import logging
from datetime import date
from pathlib import Path

import pandas as pd
import yaml

from utils.cleaner import standardise_dataframe
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


def main() -> None:
    scrape_logger, error_logger = build_loggers()

    http_cfg = HttpConfig(
        user_agent=CONFIG["user_agent"],
        request_delay_seconds=CONFIG["request_delay_seconds"],
        timeout_seconds=CONFIG["timeout_seconds"],
        max_retries=CONFIG["max_retries"],
        backoff_factor=CONFIG["backoff_factor"],
    )
    client = EthicalHttpClient(http_cfg, scrape_logger=scrape_logger)

    source_cfg = CONFIG["sources"]["government"]
    output_file = ROOT / CONFIG["output"]["government_csv"]
    output_file.parent.mkdir(parents=True, exist_ok=True)

    try:
        response = client.get(source_cfg["dataset_csv_url"])
        response.raise_for_status()
        df_raw = pd.read_csv(io.StringIO(response.text), dtype=str)

        column_aliases = {
            "school_name": ["School_name", "school_name"],
            "suburb": ["Town_suburb", "suburb", "Suburb"],
            "postcode": ["Postcode", "postcode"],
            "phone": ["Phone", "phone"],
            "public_email": ["School_Email", "Email", "email"],
            "website_url": ["Website", "website"],
        }

        mapped = {}
        for target, aliases in column_aliases.items():
            mapped[target] = None
            for candidate in aliases:
                if candidate in df_raw.columns:
                    mapped[target] = df_raw[candidate]
                    break

        out = pd.DataFrame(mapped)
        out["sector"] = "government"
        out["contact_form_url"] = None
        out["source_directory_url"] = source_cfg["source_directory_url"]
        out["last_verified_date"] = date.today().isoformat()
        out = standardise_dataframe(out)
        out.to_csv(output_file, index=False)

        print(f"Government schools saved: {len(out)} -> {output_file}")
    except Exception as exc:
        error_logger.exception("Government download failed: %s", exc)
        raise


if __name__ == "__main__":
    main()
