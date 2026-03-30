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
🏗️ Technical Design & ImplementationPart A: Data Collector DesignThe crawler is engineered for resilience and scalability, utilizing a configuration-first approach to minimize code changes when scaling sources.FeatureImplementation DetailsRate LimitingRandom delay (2–5s) between requests to mimic human behavior.Retry Logic3-tier retry strategy with exponential backoff ($5s \times n$).UA RotationSet of 5 distinct browser User-Agent strings rotated per attempt.429 HandlingDedicated detection for "Too Many Requests" with a hard 2× backoff.Config-drivenCSS selectors externalized in scraper_config.json for zero-code onboarding.DegradationSynthetic fallback mechanisms triggered when live sites block requests.Audit TrailFull NDJSON audit record per URL (Status, Row Count, Timestamp).Zero-Code Category Onboarding Example:To add a new category, simply append a block to config/scraper_config.json:JSON{
  "name": "Agricultural Equipment",
  "slug": "agricultural_equipment",
  "search_url": "https://www.indiamart.com/search.mp?ss=agricultural+equipment",
  "enabled": true
}
Part B: Exploratory Data Analysis (EDA)Comprehensive data profiling and visualization outputs are stored in outputs/eda/.#File NameVisualization Description101_category_distribution.pngTotal listing counts segmented by category.202_price_distributions.pngLog-scale Box + Violin plots for price variance.303_price_bands.pngStacked bar chart showing price bucket breakdowns.404_state_distribution.pngGeospatial distribution (Top 15 States).505_top_suppliers.pngMarket concentration: Top 20 suppliers by volume.606_dq_report.pngData Quality pass rate pie chart & flag breakdown.707_price_outliers.pngZ-score based scatter plots identifying price anomalies.🛠️ Architecture & Resume AlignmentThis project serves as a production-grade implementation of modern DataOps and Medallion architecture principles.Resume ClaimProject Implementation & ProofZero-code onboardingHandled via scraper_config.json (drives selectors, limits, and categories).Medallion ArchitectureStrict 3-layer logical separation: Bronze → Silver → Gold.Full Audit MetadataComprehensive NDJSON audit logs captured at every layer per batch.Incremental IngestionWatermark-based logic using watermark.json to track state.Upsert LogicSurrogate-key based MERGE INTO operations in the Silver layer.Data Quality (DQ)Records are tagged with DQ flags in Silver rather than dropped (Data Observability).ResiliencePipeline concurrency locks and automated failure tracking (Retry + Backoff).ReconciliationOrchestrator-level row-count verification (Bronze read-back).
