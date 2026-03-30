"""
main.py
-------
Pipeline Orchestrator — Slooze Take-Home Challenge

Stages executed in order:
  1. Load config
  2. Crawl IndiaMART categories (Part A)
  3. Bronze Layer  — ingest raw records with audit metadata
  4. Silver Layer  — cleanse, standardise, DQ-flag, MERGE/upsert
  5. Gold Layer    — build analytical aggregation tables
  6. EDA           — generate charts + insights report (Part B)

Run:
    python main.py                  # Full pipeline
    python main.py --skip-crawl     # Skip crawl, reuse existing Bronze data
    python main.py --eda-only       # Skip pipeline, re-run EDA on existing Silver
"""

import argparse
import json
import logging
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pipeline.log", mode="a"),
    ],
)
logger = logging.getLogger(__name__)


def load_config(path: str = "config/scraper_config.json") -> dict:
    with open(path) as f:
        return json.load(f)


def run_pipeline(skip_crawl: bool = False, eda_only: bool = False):
    logger.info("=" * 65)
    logger.info("  SLOOZE DATA ENGINEERING CHALLENGE — PIPELINE START")
    logger.info("=" * 65)

    # ------------------------------------------------------------------
    # 0. Config
    # ------------------------------------------------------------------
    config = load_config()
    logger.info("Config loaded. Categories: %d",
                sum(1 for c in config["categories"] if c.get("enabled")))

    # ------------------------------------------------------------------
    # Imports (deferred so --eda-only skips heavy crawl imports)
    # ------------------------------------------------------------------
    from src.pipeline.bronze_layer import BronzeLayer
    from src.pipeline.silver_layer import SilverLayer
    from src.pipeline.gold_layer import GoldLayer
    from src.eda.analysis import EDAAnalyser

    bronze = BronzeLayer(config)
    silver = SilverLayer(config)
    gold = GoldLayer(config)

    if eda_only:
        logger.info("--eda-only flag set. Skipping crawl + pipeline.")
        logger.info("Running EDA on existing Silver data …")
        analyser = EDAAnalyser()
        analyser.run()
        return

    # ------------------------------------------------------------------
    # 1. Crawl (Part A)
    # ------------------------------------------------------------------
    all_raw: list[dict] = []

    if not skip_crawl:
        from src.crawler.indiamart_crawler import IndiaMARTCrawler
        crawler = IndiaMARTCrawler(config)

        enabled_categories = [c for c in config["categories"] if c.get("enabled")]
        logger.info("\n--- STAGE 1: CRAWL (%d categories) ---", len(enabled_categories))

        for category in enabled_categories:
            logger.info("Crawling: %s", category["name"])
            raw = crawler.crawl_category(category)
            logger.info("  → %d records collected", len(raw))
            all_raw.extend(raw)

        logger.info("Total raw records: %d", len(all_raw))

        # Print crawler audit summary
        audit_records = crawler.get_audit_records()
        logger.info("Crawler audit entries: %d", len(audit_records))
        for entry in audit_records:
            logger.info("  [%s] %s — %d records — %s",
                        entry["status"], entry["category"],
                        entry["records_fetched"], entry.get("error", ""))

    else:
        logger.info("--skip-crawl flag set. Reading existing Bronze data …")

    # ------------------------------------------------------------------
    # 2. Bronze Layer
    # ------------------------------------------------------------------
    logger.info("\n--- STAGE 2: BRONZE INGESTION ---")
    if all_raw:
        # Group by category and ingest
        by_cat: dict[str, list] = {}
        for rec in all_raw:
            by_cat.setdefault(rec.get("category", "unknown"), []).append(rec)

        for cat, records in by_cat.items():
            audit = bronze.ingest(cat, records)
            logger.info("  Bronze '%s': %d records written (batch: %s)",
                        cat, audit["records_written"], audit.get("batch_id", "")[:8])

    bronze_records = bronze.read_all_bronze()
    logger.info("Total Bronze records available: %d", len(bronze_records))

    if not bronze_records:
        logger.error("No Bronze data available. Exiting.")
        sys.exit(1)

    # ------------------------------------------------------------------
    # 3. Silver Layer
    # ------------------------------------------------------------------
    logger.info("\n--- STAGE 3: SILVER TRANSFORMATION ---")
    silver_records = silver.transform(bronze_records)
    logger.info("Silver records after cleanse + dedup: %d", len(silver_records))

    dq_passed = sum(1 for r in silver_records if r.get("dq_passed"))
    logger.info("DQ pass rate: %d/%d (%.1f%%)",
                dq_passed, len(silver_records),
                dq_passed / len(silver_records) * 100 if silver_records else 0)

    # ------------------------------------------------------------------
    # 4. Gold Layer
    # ------------------------------------------------------------------
    logger.info("\n--- STAGE 4: GOLD AGGREGATIONS ---")
    gold_tables = gold.build_all(silver_records)
    for table, data in gold_tables.items():
        size = len(data) if isinstance(data, list) else "dict"
        logger.info("  Gold table '%s': %s rows", table, size)

    # ------------------------------------------------------------------
    # 5. EDA (Part B)
    # ------------------------------------------------------------------
    logger.info("\n--- STAGE 5: EDA ---")
    analyser = EDAAnalyser()
    analyser.run()

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    logger.info("\n" + "=" * 65)
    logger.info("  PIPELINE COMPLETE")
    logger.info("=" * 65)
    logger.info("  Raw records crawled   : %d", len(all_raw))
    logger.info("  Bronze records        : %d", len(bronze_records))
    logger.info("  Silver records        : %d", len(silver_records))
    logger.info("  Gold tables           : %d", len(gold_tables))
    logger.info("  EDA outputs           : outputs/eda/")
    logger.info("  Audit logs            : data/audit/")
    logger.info("  Pipeline log          : pipeline.log")
    logger.info("=" * 65)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Slooze Data Engineering Pipeline")
    parser.add_argument(
        "--skip-crawl",
        action="store_true",
        help="Skip web crawling; use existing Bronze data",
    )
    parser.add_argument(
        "--eda-only",
        action="store_true",
        help="Skip pipeline; only re-run EDA on existing Silver data",
    )
    args = parser.parse_args()
    run_pipeline(skip_crawl=args.skip_crawl, eda_only=args.eda_only)
