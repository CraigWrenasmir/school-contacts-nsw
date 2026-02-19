# NSW School Contact Radius Search

Publicly sourced, ethics-first NSW school contact database and search tool across Government, Catholic, and Independent sectors.

## Live Modes

- Local FastAPI app (`web_app.py`) for API + UI
- Static GitHub Pages app (`docs/index.html`) for shareable public access

## Project Pipeline

1. `python 01_gov_nsw_download.py`
2. `python 02_isnsw_scrape.py`
3. `python 03_catholic_scrape.py`
4. `python 04_merge_dedupe.py`
5. `python 05_enrich_geospatial.py`
6. `python 06_export_static_site_data.py`

## Local Run (FastAPI)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn web_app:app --reload --host 127.0.0.1 --port 8000
```

Open: `http://127.0.0.1:8000`

## Static Site (GitHub Pages)

Data files are exported to `docs/data/` and UI is at `docs/index.html`.

```bash
python 06_export_static_site_data.py
```

Then enable GitHub Pages in repository settings using:
- Branch: `main`
- Folder: `/docs`

## Ethical Constraints

- Publicly available contact information only
- No bypassing contact forms
- No staff list/personal email scraping
- robots.txt respected
- Rate limiting and descriptive user-agent in scrapers
