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
## 🏗️ Technical Design & Implementation

### Part A: Data Collector Design
The crawler is engineered for resilience and scalability, utilizing a configuration-first approach to minimize code changes when scaling sources.

| Feature | Implementation Details |
| :--- | :--- |
| **Rate Limiting** | Random delay (2–5s) between requests to mimic human behavior. |
| **Retry Logic** | 3-tier retry strategy with exponential backoff ($5s \times n$). |
| **UA Rotation** | Set of 5 distinct browser User-Agent strings rotated per attempt. |
| **429 Handling** | Dedicated detection for "Too Many Requests" with a hard 2× backoff. |
| **Config-driven** | CSS selectors externalized in `scraper_config.json` for zero-code onboarding. |
| **Degradation** | Synthetic fallback mechanisms triggered when live sites block requests. |
| **Audit Trail** | Full NDJSON audit record per URL (Status, Row Count, Timestamp). |

**Zero-Code Category Onboarding Example:**
To add a new category, simply append a block to `config/scraper_config.json`:

```json
{
  "name": "Agricultural Equipment",
  "slug": "agricultural_equipment",
  "search_url": "[https://www.indiamart.com/search.mp?ss=agricultural+equipment](https://www.indiamart.com/search.mp?ss=agricultural+equipment)",
  "enabled": true
}

Part B: Exploratory Data Analysis (EDA)
#,File Name,Visualization Description
1,01_category_distribution.png,Total listing counts segmented by category.
2,02_price_distributions.png,Log-scale Box + Violin plots for price variance.
3,03_price_bands.png,Stacked bar chart showing price bucket breakdowns.
4,04_state_distribution.png,Geospatial distribution (Top 15 States).
5,05_top_suppliers.png,Market concentration: Top 20 suppliers by volume.
6,06_dq_report.png,Data Quality pass rate pie chart & flag breakdown.
7,07_price_outliers.png,Z-score based scatter plots identifying price anomalies.
