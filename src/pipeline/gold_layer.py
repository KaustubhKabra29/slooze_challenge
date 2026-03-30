"""
gold_layer.py
-------------
Gold layer of the Medallion Architecture.

Computes aggregated analytical tables from Silver records:
  1. category_summary     — counts, price stats per category
  2. state_summary        — supplier and product distribution by state
  3. price_distribution   — price buckets across categories
  4. top_suppliers        — most frequent suppliers
  5. dq_summary           — data quality report

Mirrors the Gold Star Schema + KPI tables in the RideStream project.
"""

import json
import logging
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean, median, stdev
from typing import Any

logger = logging.getLogger(__name__)


class GoldLayer:
    """
    Builds analytical Gold tables from Silver data.
    Outputs are saved as JSON to data/gold/.
    """

    def __init__(self, config: dict):
        pipeline_cfg = config.get("pipeline", {})
        self.gold_dir = Path(pipeline_cfg.get("gold_dir", "data/gold"))
        self.audit_dir = Path(pipeline_cfg.get("audit_log_dir", "data/audit"))
        self.gold_dir.mkdir(parents=True, exist_ok=True)
        self.audit_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def build_all(self, silver_records: list[dict]) -> dict[str, Any]:
        """
        Build all Gold tables and return a dict of table_name → data.
        """
        if not silver_records:
            logger.warning("[Gold] No Silver records. Skipping Gold build.")
            return {}

        logger.info("[Gold] Building analytical tables from %d Silver records …",
                    len(silver_records))

        tables = {
            "category_summary": self._category_summary(silver_records),
            "state_summary": self._state_summary(silver_records),
            "price_distribution": self._price_distribution(silver_records),
            "top_suppliers": self._top_suppliers(silver_records),
            "dq_summary": self._dq_summary(silver_records),
            "price_by_category_box": self._price_by_category_box(silver_records),
        }

        for table_name, data in tables.items():
            out_file = self.gold_dir / f"{table_name}.json"
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info("[Gold] '%s' → %s", table_name, out_file)

        self._write_audit({
            "layer": "gold",
            "tables_built": list(tables.keys()),
            "silver_records_processed": len(silver_records),
            "built_at": datetime.utcnow().isoformat(),
            "status": "SUCCESS",
        })

        return tables

    def read_gold(self, table_name: str) -> Any:
        """Read a Gold table by name."""
        f = self.gold_dir / f"{table_name}.json"
        if not f.exists():
            logger.warning("[Gold] Table '%s' not found.", table_name)
            return None
        with open(f) as fh:
            return json.load(fh)

    # ------------------------------------------------------------------
    # Analytical computations
    # ------------------------------------------------------------------
    def _category_summary(self, records: list[dict]) -> list[dict]:
        """Count, avg/median/min/max price per category."""
        by_cat: dict[str, list] = defaultdict(list)
        for r in records:
            cat = r.get("category", "unknown")
            if r.get("price_inr") is not None:
                by_cat[cat].append(r["price_inr"])

        result = []
        cat_counts = Counter(r.get("category") for r in records)
        for cat, prices in by_cat.items():
            entry = {
                "category": cat,
                "total_products": cat_counts[cat],
                "products_with_price": len(prices),
                "avg_price_inr": round(mean(prices), 2) if prices else None,
                "median_price_inr": round(median(prices), 2) if prices else None,
                "min_price_inr": min(prices) if prices else None,
                "max_price_inr": max(prices) if prices else None,
                "stddev_price_inr": round(stdev(prices), 2) if len(prices) > 1 else None,
            }
            result.append(entry)

        # Include categories with no prices
        for cat in cat_counts:
            if cat not in by_cat:
                result.append({
                    "category": cat,
                    "total_products": cat_counts[cat],
                    "products_with_price": 0,
                    "avg_price_inr": None,
                    "median_price_inr": None,
                    "min_price_inr": None,
                    "max_price_inr": None,
                    "stddev_price_inr": None,
                })

        return sorted(result, key=lambda x: x["total_products"], reverse=True)

    def _state_summary(self, records: list[dict]) -> list[dict]:
        """Product and supplier distribution by state."""
        state_products: dict[str, Counter] = defaultdict(Counter)
        state_suppliers: dict[str, set] = defaultdict(set)

        for r in records:
            state = r.get("state") or "Unknown"
            cat = r.get("category", "unknown")
            supplier = r.get("supplier_name", "")
            state_products[state][cat] += 1
            if supplier:
                state_suppliers[state].add(supplier)

        result = []
        for state in state_products:
            result.append({
                "state": state,
                "total_listings": sum(state_products[state].values()),
                "unique_suppliers": len(state_suppliers[state]),
                "category_breakdown": dict(state_products[state]),
            })

        return sorted(result, key=lambda x: x["total_listings"], reverse=True)

    def _price_distribution(self, records: list[dict]) -> list[dict]:
        """Bucket products into price bands."""
        buckets = [
            ("Under ₹500", 0, 500),
            ("₹500–₹5,000", 500, 5_000),
            ("₹5,000–₹50,000", 5_000, 50_000),
            ("₹50,000–₹5,00,000", 50_000, 500_000),
            ("Above ₹5,00,000", 500_000, float("inf")),
        ]

        by_bucket: dict[str, Counter] = defaultdict(Counter)
        for r in records:
            price = r.get("price_inr")
            if price is None:
                continue
            cat = r.get("category", "unknown")
            for label, lo, hi in buckets:
                if lo <= price < hi:
                    by_bucket[label][cat] += 1
                    break

        result = []
        for label, lo, hi in buckets:
            entry = {"price_band": label, "total": sum(by_bucket[label].values())}
            entry.update(by_bucket[label])
            result.append(entry)

        return result

    def _top_suppliers(self, records: list[dict], top_n: int = 20) -> list[dict]:
        """Top N suppliers by listing count."""
        counter = Counter(
            r.get("supplier_name") for r in records if r.get("supplier_name")
        )
        return [
            {"supplier_name": name, "listing_count": count}
            for name, count in counter.most_common(top_n)
        ]

    def _dq_summary(self, records: list[dict]) -> dict:
        """Data quality summary — mirrors audit reports in production pipelines."""
        total = len(records)
        passed = sum(1 for r in records if r.get("dq_passed"))
        failed = total - passed

        flag_counter: Counter = Counter()
        for r in records:
            for flag in r.get("dq_flags", []):
                flag_counter[flag] += 1

        return {
            "total_records": total,
            "dq_passed": passed,
            "dq_failed": failed,
            "pass_rate_pct": round(passed / total * 100, 2) if total else 0,
            "flag_breakdown": dict(flag_counter),
        }

    def _price_by_category_box(self, records: list[dict]) -> list[dict]:
        """
        Percentile data for box-plots (p25, p50, p75) per category.
        """
        by_cat: dict[str, list] = defaultdict(list)
        for r in records:
            if r.get("price_inr") is not None:
                by_cat[r.get("category", "unknown")].append(r["price_inr"])

        result = []
        for cat, prices in by_cat.items():
            prices_sorted = sorted(prices)
            n = len(prices_sorted)
            result.append({
                "category": cat,
                "p25": prices_sorted[n // 4],
                "p50": prices_sorted[n // 2],
                "p75": prices_sorted[3 * n // 4],
                "min": prices_sorted[0],
                "max": prices_sorted[-1],
                "count": n,
            })

        return result

    # ------------------------------------------------------------------
    # Audit logging
    # ------------------------------------------------------------------
    def _write_audit(self, entry: dict):
        audit_file = self.audit_dir / "gold_audit.ndjson"
        with open(audit_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")
