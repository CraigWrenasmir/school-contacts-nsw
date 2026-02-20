from __future__ import annotations

import json
import re
from typing import Iterable, Optional
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

GENERAL_PREFIXES = (
    "info@",
    "admin@",
    "office@",
    "contact@",
    "enquiries@",
    "enquiry@",
    "reception@",
    "registrar@",
    "school@",
)
EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
EMAIL_EXACT_RE = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.IGNORECASE)
# Treat obvious personal-address pattern as lower priority only (not a hard reject).
# Require both segments to be longer, so school aliases like "highgate.ps@" are kept.
PERSONAL_STYLE_RE = re.compile(r"^[a-z]{3,}\.[a-z]{3,}@", re.IGNORECASE)
OBFUSCATED_EMAIL_BRACKET_RE = re.compile(
    r"\b([A-Z0-9._%+-]{2,})\s*(?:\(|\[|\{)\s*at\s*(?:\)|\]|\})\s*([A-Z0-9.-]{2,})\s*(?:\(|\[|\{)\s*dot\s*(?:\)|\]|\})\s*([A-Z]{2,})\b",
    re.IGNORECASE,
)
OBFUSCATED_EMAIL_WORD_RE = re.compile(
    r"\b([A-Z0-9._%+-]{2,})\s+at\s+([A-Z0-9.-]{2,})\s+dot\s+([A-Z]{2,})\b",
    re.IGNORECASE,
)
POSTCODE_RE = re.compile(r"\b(\d{4})\b")
INVISIBLE_CHARS_RE = re.compile(r"[\u200b\u200c\u200d\u2060\ufeff]")
ALLOWED_TLDS = {"au", "com", "org", "net", "edu", "gov", "school", "online"}
PLACEHOLDER_DOMAINS = {"example.com", "test.com", "domain.com", "email.com", "yourdomain.com"}

# Australian state education departments use centralised email domains that differ
# from individual school website domains (e.g. *.vic.edu.au websites use
# @education.vic.gov.au email addresses). These trusted domains bypass the
# website/email domain-relationship check so valid school emails aren't rejected.
TRUSTED_GOV_EMAIL_DOMAINS = {
    "education.vic.gov.au",   # VIC govt: schoolname.ps@education.vic.gov.au
    "edumail.vic.gov.au",     # VIC govt: legacy domain still in use
    "eq.edu.au",              # QLD govt: schoolname@eq.edu.au (on *.qld.edu.au sites)
    "qed.qld.gov.au",         # QLD govt: alternative dept domain
}


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


def _normalise_email_candidate(value: str) -> str:
    clean = INVISIBLE_CHARS_RE.sub("", value or "")
    clean = clean.strip().lower().strip(".,;:!?\"'`()[]{}<>")
    # Strip URL-encoded spaces that sometimes leak from href attributes (%20, +)
    while clean.startswith(("%20", "+")):
        clean = clean.lstrip("%20").lstrip("+").lstrip()
    return clean


def _extract_hostname(url: str | None) -> str | None:
    if not url:
        return None
    raw = str(url).strip()
    if not raw:
        return None
    if "://" not in raw:
        raw = "https://" + raw
    try:
        host = (urlparse(raw).hostname or "").strip().lower()
    except Exception:
        return None
    if host.startswith("www."):
        host = host[4:]
    return host or None


def _registrable_domain(host: str) -> str:
    labels = [p for p in host.split(".") if p]
    if len(labels) < 2:
        return host
    joined = ".".join(labels)
    if joined.endswith((".com.au", ".edu.au", ".gov.au", ".org.au", ".net.au")) and len(labels) >= 3:
        return ".".join(labels[-3:])
    return ".".join(labels[-2:])


def _domains_related(email_domain: str, website_host: str) -> bool:
    if (
        email_domain == website_host
        or email_domain.endswith("." + website_host)
        or website_host.endswith("." + email_domain)
    ):
        return True
    return _registrable_domain(email_domain) == _registrable_domain(website_host)


def classify_public_email(
    email: str | None, website_url: str | None = None, source: str = "text"
) -> tuple[str | None, str, str]:
    if not email:
        return None, "invalid", "empty"

    clean = _normalise_email_candidate(str(email))
    if not clean:
        return None, "invalid", "empty"
    if " " in clean or clean.count("@") != 1:
        return None, "invalid", "invalid_format"
    if not EMAIL_EXACT_RE.match(clean):
        return None, "invalid", "invalid_format"

    local_part, domain = clean.split("@", 1)
    if len(local_part) < 2:
        return None, "invalid", "local_too_short"
    if domain in PLACEHOLDER_DOMAINS:
        return None, "invalid", "placeholder_domain"
    if "." not in domain:
        return None, "invalid", "missing_tld"

    tld = domain.rsplit(".", 1)[-1]
    if not re.fullmatch(r"[a-z]{2,}", tld):
        return None, "invalid", "invalid_tld_format"
    if tld not in ALLOWED_TLDS:
        return None, "invalid", "invalid_tld"

    website_host = _extract_hostname(website_url)
    # For directory-provided records, domain mismatch with website is common and not
    # a reliable signal. Only enforce strict domain relationship for website text extraction.
    # Trusted government education domains are exempt: state depts use a central email
    # domain (e.g. education.vic.gov.au) that differs from school website domains (*.vic.edu.au).
    if (
        website_host
        and source not in {"directory", "mixed"}
        and domain not in TRUSTED_GOV_EMAIL_DOMAINS
        and not _domains_related(domain, website_host)
    ):
        if source == "text":
            return None, "invalid", "unrelated_domain_low_confidence"
        return clean, "suspicious", "unrelated_domain"

    return clean, "valid", "ok"


def extract_emails_from_text(text: str) -> list[str]:
    if not text:
        return []
    emails = EMAIL_RE.findall(text)
    for local, domain, tld in OBFUSCATED_EMAIL_BRACKET_RE.findall(text):
        emails.append(f"{local}@{domain}.{tld}")
    for local, domain, tld in OBFUSCATED_EMAIL_WORD_RE.findall(text):
        emails.append(f"{local}@{domain}.{tld}")
    return _unique(emails)


def extract_mailto_emails(soup: BeautifulSoup) -> list[str]:
    emails = []
    for a in soup.select("a[href^='mailto:']"):
        href = a.get("href", "").split("mailto:", 1)[-1].split("?", 1)[0].strip()
        if not href:
            continue
        for part in re.split(r"[;,]", href):
            part = part.strip()
            if part:
                emails.append(part)
    return _unique(emails)


def _decode_cloudflare_email(encoded: str) -> str:
    try:
        raw = bytes.fromhex(encoded)
    except Exception:
        return ""
    if not raw:
        return ""
    key = raw[0]
    decoded = "".join(chr(b ^ key) for b in raw[1:])
    return decoded.strip()


def extract_cloudflare_protected_emails(soup: BeautifulSoup) -> list[str]:
    emails = []
    for node in soup.select("[data-cfemail]"):
        encoded = (node.get("data-cfemail") or "").strip()
        if not encoded:
            continue
        decoded = _decode_cloudflare_email(encoded)
        if decoded:
            emails.append(decoded)
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


def choose_general_email(
    emails: Iterable[str], website_url: str | None = None, source: str = "text"
) -> Optional[str]:
    clean = _unique(e for e in emails if e)
    if not clean:
        return None

    validated = []
    for email in clean:
        normalised, status, _ = classify_public_email(email, website_url=website_url, source=source)
        if not normalised:
            continue
        if status == "valid":
            validated.append(normalised)
    candidates = validated
    if not candidates:
        return None

    preferred = [e for e in candidates if e.lower().startswith(GENERAL_PREFIXES)]
    if preferred:
        return preferred[0].lower()

    non_personal = [e for e in candidates if not PERSONAL_STYLE_RE.match(e.lower())]
    if non_personal:
        return non_personal[0].lower()

    # If only personal-style candidates exist, still return one to avoid
    # dropping publicly listed general addresses with dot-style aliases.
    return candidates[0].lower()


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

    mailto_emails = extract_mailto_emails(soup)
    text_emails = extract_emails_from_text(all_text)
    result["public_email"] = choose_general_email(
        mailto_emails, website_url=page_url, source="mailto"
    ) or choose_general_email(text_emails, website_url=page_url, source="text")

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
                result["public_email"] = choose_general_email(
                    [item.get("email", "")], website_url=page_url, source="jsonld"
                )
            address = item.get("address")
            if isinstance(address, dict):
                if not result["suburb"]:
                    result["suburb"] = address.get("addressLocality")
                if not result["postcode"]:
                    result["postcode"] = address.get("postalCode")

    return result
