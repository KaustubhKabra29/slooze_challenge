Slooze Take-Home Challenge — Data Engineering
Kaustubh Kabra | Data Engineer | linkedin.com/in/kaustubh-kabra-de
---
Architecture Overview
This solution is built around the same Medallion Architecture I use in production at Globestar (RideStream & InsureFlow projects):
```
[IndiaMART Crawler]
        │
        ▼
  ┌─────────────┐
  │  BRONZE     │  Raw records + batch_id + ingested_at (partitioned by date)
  │  (NDJSON)   │  Watermark tracking per category
  └──────┬──────┘
         │
         ▼
  ┌─────────────┐
  │  SILVER     │  Cleansed · Typed · DQ-flagged · Deduped · MERGE upsert
  │  (JSON)     │  Surrogate keys · Column standardisation
  └──────┬──────┘
         │
         ▼
  ┌─────────────┐
  │  GOLD       │  6 Analytical tables powering EDA charts
  │  (JSON)     │  category_summary · state_summary · price_distribution ...
  └──────┬──────┘
         │
         ▼
  ┌─────────────┐
  │  EDA        │  7 charts + insights.md report
  │  (PNG/MD)   │
  └─────────────┘
```
---
Project Structure
```
slooze-challenge/
├── config/
│   └── scraper_config.json      ← Config-driven: categories, selectors, limits
├── src/
│   ├── crawler/
│   │   ├── base_crawler.py      ← Rate-limiting, retry-backoff, rotating UA
│   │   └── indiamart_crawler.py ← IndiaMART parser + synthetic fallback
│   ├── pipeline/
│   │   ├── bronze_layer.py      ← Ingestion + watermark + audit log
│   │   ├── silver_layer.py      ← Cleanse + DQ flags + MERGE upsert
│   │   └── gold_layer.py        ← 6 analytical aggregation tables
│   └── eda/
│       └── analysis.py          ← 7 EDA charts + insights report
├── data/
│   ├── bronze/                  ← Raw NDJSON partitioned by category/date
│   ├── silver/                  ← Cleaned JSON per category
│   ├── gold/                    ← Aggregation tables
│   ├── audit/                   ← NDJSON audit logs per layer
│   └── watermark.json           ← Last-run watermark per category
├── outputs/
│   └── eda/                     ← 7 PNG charts + insights.md
├── main.py                      ← Orchestrator
├── requirements.txt
└── README.md
```
---
How to Run
1. Install dependencies
```bash
python3 -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt
```
2. Run the full pipeline
```bash
python main.py
```
This will:
Attempt live scraping of IndiaMART (4 categories, up to 5 pages each)
Fall back to realistic synthetic data if the site is unreachable
Run all Bronze → Silver → Gold → EDA stages
Save 7 EDA charts to `outputs/eda/`
Write audit logs to `data/audit/`
3. Other run modes
```bash
python main.py --skip-crawl   # Skip scraping, reuse existing Bronze data
python main.py --eda-only     # Skip pipeline, re-run EDA on existing Silver
```
---
Part A — Data Collector Design
Crawler features
Feature	Implementation
Rate limiting	Random delay 2–5s between requests
Retry with backoff	3 retries, exponential sleep (5s × n)
Rotating User-Agents	5 browser UA strings, rotated per attempt
HTTP 429 handling	Detected separately with hard 2× backoff
Config-driven selectors	CSS selectors in `scraper_config.json` → zero-code category onboarding
Graceful degradation	Synthetic fallback when live site blocks
Full audit trail	Per-URL audit record (status, rows, timestamp)
Adding a new category (zero-code)
Add one block to `config/scraper_config.json`:
```json
{
  "name": "Agricultural Equipment",
  "slug": "agricultural_equipment",
  "search_url": "https://www.indiamart.com/search.mp?ss=agricultural+equipment",
  "enabled": true
}
```
No code changes required. This mirrors the "zero-code source onboarding" from production work.
---
Part B — EDA Outputs
Seven charts are saved to `outputs/eda/`:
#	File	What it shows
1	`01_category_distribution.png`	Listing counts per category
2	`02_price_distributions.png`	Box + violin plots (log scale)
3	`03_price_bands.png`	Stacked bar — price bucket breakdown
4	`04_state_distribution.png`	Top 15 states by listing count
5	`05_top_suppliers.png`	Top 20 suppliers by listing count
6	`06_dq_report.png`	DQ pass rate pie + flag breakdown
7	`07_price_outliers.png`	Z-score outlier scatter per category
Key insights are in `outputs/eda/insights.md`.
---
Design Patterns 
"Config-driven, zero-code source onboarding"	`scraper_config.json` drives categories, selectors, limits
"Medallion Architecture: Bronze → Silver → Gold"	Identical 3-layer structure
"Full audit metadata"	NDJSON audit logs written per layer, per batch
"Watermark-based incremental ingestion"	`watermark.json` tracks last batch per category
"MERGE INTO upsert logic"	Surrogate-key based MERGE in `silver_layer.py`
"Data quality flagging"	DQ flags tagged on records (not dropped) in Silver
"Pipeline concurrency lock + failure tracking"	Retry-with-backoff + audit status (SUCCESS / PARTIAL / FAILED)
"Row-count reconciliation"	Bronze read-back verification in orchestrator
---
