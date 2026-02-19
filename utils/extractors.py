from __future__ import annotations

import json
import re
from typing import Iterable, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

GENERAL_PREFIXES = ("info@", "admin@", "office@", "contact@")
EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
PERSONAL_STYLE_RE = re.compile(r"^[a-z]+\.[a-z]+@", re.IGNORECASE)
POSTCODE_RE = re.compile(r"\b(\d{4})\b")


def _unique(values: Iterable[str]) -> list[str]:
    seen = set()
    out = []
    for v in values:
        key = v.strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(v.strip())
    return out


def extract_emails_from_text(text: str) -> list[str]:
    return _unique(EMAIL_RE.findall(text or ""))


def choose_general_email(emails: Iterable[str]) -> Optional[str]:
    clean = _unique(e for e in emails if e)
    if not clean:
        return None

    preferred = [e for e in clean if e.lower().startswith(GENERAL_PREFIXES)]
    if preferred:
        return preferred[0].lower()

    non_personal = [e for e in clean if not PERSONAL_STYLE_RE.match(e.lower())]
    if non_personal:
        return non_personal[0].lower()

    return None


def extract_mailto_emails(soup: BeautifulSoup) -> list[str]:
    emails = []
    for a in soup.select("a[href^='mailto:']"):
        href = a.get("href", "").split("mailto:", 1)[-1].split("?", 1)[0].strip()
        if href:
            emails.append(href)
    return _unique(emails)


def extract_contact_form_url(soup: BeautifulSoup, base_url: str) -> Optional[str]:
    form = soup.find("form")
    if form:
        action = (form.get("action") or "").strip()
        if action:
            return urljoin(base_url, action)
        return base_url

    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        label = (a.get_text(" ", strip=True) or "").lower()
        if "contact" in href.lower() or "contact" in label:
            return urljoin(base_url, href)

    return None


def extract_school_core_fields(soup: BeautifulSoup, page_url: str) -> dict:
    result = {
        "school_name": None,
        "suburb": None,
        "postcode": None,
        "phone": None,
        "public_email": None,
        "website_url": None,
    }

    h1 = soup.find("h1")
    if h1:
        result["school_name"] = h1.get_text(" ", strip=True)

    all_text = soup.get_text("\n", strip=True)

    phone_match = re.search(r"(?:\+?61\s?|0)[2-9]\d(?:[\s-]?\d){7,8}", all_text)
    if phone_match:
        result["phone"] = phone_match.group(0)

    emails = extract_mailto_emails(soup) + extract_emails_from_text(all_text)
    result["public_email"] = choose_general_email(emails)

    postcode_match = POSTCODE_RE.search(all_text)
    if postcode_match:
        result["postcode"] = postcode_match.group(1)

    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if "http" in href and any(k in href.lower() for k in ["school", "college", ".edu", ".nsw"]):
            if "isnsw" not in href and "csnsw" not in href:
                result["website_url"] = href
                break

    for script in soup.select("script[type='application/ld+json']"):
        try:
            data = json.loads(script.get_text(strip=True))
        except Exception:
            continue

        items = data if isinstance(data, list) else [data]
        for item in items:
            if not isinstance(item, dict):
                continue
            if not result["school_name"]:
                result["school_name"] = item.get("name")
            if not result["phone"]:
                result["phone"] = item.get("telephone")
            if not result["website_url"]:
                result["website_url"] = item.get("url")
            if not result["public_email"]:
                result["public_email"] = choose_general_email([item.get("email", "")])
            address = item.get("address")
            if isinstance(address, dict):
                if not result["suburb"]:
                    result["suburb"] = address.get("addressLocality")
                if not result["postcode"]:
                    result["postcode"] = address.get("postalCode")

    return result
