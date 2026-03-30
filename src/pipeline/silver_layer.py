"""
silver_layer.py
---------------
Silver layer of the Medallion Architecture.

Responsibilities:
  - Column standardisation (normalise price, location, supplier)
  - Deduplication (by product_name + supplier_name + category)
  - Data Quality (DQ) flagging — records tagged, not dropped
  - Type casting (price as float, scraped_at as datetime string)
  - MERGE / upsert semantics using a surrogate key

Design mirrors InsureFlow Silver layer:
  metadata-driven column mapping, null handling, MERGE INTO upsert logic.
"""

import hashlib
import json
import logging
import re
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

# Compiled regex for price extraction (handles ₹1,23,456 / Unit formats)
_PRICE_RE = re.compile(r"[\d,]+(?:\.\d+)?")


class SilverLayer:
    """
    Transforms Bronze records into clean, typed Silver records.
    Persists to data/silver/<category>.json (full replace per run).
    """

    def __init__(self, config: dict):
        pipeline_cfg = config.get("pipeline", {})
        self.silver_dir = Path(pipeline_cfg.get("silver_dir", "data/silver"))
        self.audit_dir = Path(pipeline_cfg.get("audit_log_dir", "data/audit"))
        self.silver_dir.mkdir(parents=True, exist_ok=True)
        self.audit_dir.mkdir(parents=True, exist_ok=True)

        # Existing silver records for MERGE (upsert deduplication)
        self._silver_store: dict[str, dict] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def transform(self, bronze_records: list[dict]) -> list[dict]:
        """
        Full Bronze → Silver transformation.
        Returns the list of silver records written.
        """
        if not bronze_records:
            logger.warning("[Silver] No Bronze records to transform.")
            return []

        logger.info("[Silver] Transforming %d Bronze records …", len(bronze_records))

        # 1. Clean + type-cast each record
        cleaned = [self._clean_record(r) for r in bronze_records]

        # 2. DQ flag
        flagged = [self._apply_dq_flags(r) for r in cleaned]

        # 3. Add surrogate key
        keyed = [self._add_surrogate_key(r) for r in flagged]

        # 4. MERGE (upsert by surrogate key — last-write-wins)
        merged = self._merge(keyed)

        # 5. Dedup within batch
        deduped = self._dedup(merged)

        # 6. Persist per-category
        by_category: dict[str, list] = {}
        for rec in deduped:
            by_category.setdefault(rec["category"], []).append(rec)

        total_written = 0
        for cat, records in by_category.items():
            out_file = self.silver_dir / f"{cat}.json"
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(records, f, ensure_ascii=False, indent=2)
            logger.info("[Silver] '%s' → %d records → %s", cat, len(records), out_file)
            total_written += len(records)

            self._write_audit({
                "layer": "silver",
                "category": cat,
                "records_in": len([r for r in bronze_records if r.get("category") == cat]),
                "records_out": len(records),
                "transformed_at": datetime.utcnow().isoformat(),
                "status": "SUCCESS",
            })

        logger.info("[Silver] Transformation complete. %d records written.", total_written)
        return deduped

    def read_silver(self, category: str = None) -> list[dict]:
        """Read Silver records, optionally filtered by category."""
        records: list[dict] = []
        pattern = f"{category}.json" if category else "*.json"
        for f in self.silver_dir.glob(pattern):
            with open(f, "r", encoding="utf-8") as fh:
                records.extend(json.load(fh))
        return records

    # ------------------------------------------------------------------
    # Transformation steps
    # ------------------------------------------------------------------
    def _clean_record(self, rec: dict) -> dict:
        """Standardise fields — mirrors column standardisation in Silver notebooks."""
        cleaned = {**rec}

        # Normalise product name
        cleaned["product_name"] = (rec.get("product_name") or "").strip().title()

        # Parse price to float
        cleaned["price_inr"] = self._parse_price(rec.get("price_raw", ""))
        cleaned["price_unit"] = self._parse_unit(rec.get("price_raw", ""))

        # Normalise location
        city, state = self._parse_location(rec.get("location_raw", ""))
        cleaned["city"] = city
        cleaned["state"] = state

        # Standardise supplier
        cleaned["supplier_name"] = (rec.get("supplier_name") or "Unknown").strip().title()

        # Normalise category label
        cleaned["category"] = (rec.get("category") or "").lower().strip()

        # Ensure timestamp is ISO string
        raw_ts = rec.get("scraped_at", "")
        cleaned["scraped_at"] = self._normalise_ts(raw_ts)

        # Layer stamp
        cleaned["_layer"] = "silver"
        cleaned["_transformed_at"] = datetime.utcnow().isoformat()

        return cleaned

    def _apply_dq_flags(self, rec: dict) -> dict:
        """
        Tag data quality issues without dropping records.
        Mirrors DQ flagging in the InsureFlow Silver layer.
        """
        issues = []

        if not rec.get("product_name"):
            issues.append("MISSING_PRODUCT_NAME")
        if rec.get("price_inr") is None:
            issues.append("MISSING_PRICE")
        if not rec.get("city"):
            issues.append("MISSING_LOCATION")
        if not rec.get("supplier_name") or rec["supplier_name"] == "Unknown":
            issues.append("MISSING_SUPPLIER")
        if rec.get("price_inr") and rec["price_inr"] <= 0:
            issues.append("INVALID_PRICE")
        if rec.get("description") and len(rec["description"]) < 10:
            issues.append("SPARSE_DESCRIPTION")

        rec["dq_flags"] = issues
        rec["dq_passed"] = len(issues) == 0
        return rec

    def _add_surrogate_key(self, rec: dict) -> dict:
        """
        Create a deterministic surrogate key for MERGE / dedup.
        Key = MD5(product_name + supplier_name + category).
        """
        raw = "|".join([
            (rec.get("product_name") or "").lower(),
            (rec.get("supplier_name") or "").lower(),
            (rec.get("category") or "").lower(),
        ])
        rec["_sk"] = hashlib.md5(raw.encode()).hexdigest()
        return rec

    def _merge(self, records: list[dict]) -> list[dict]:
        """
        Upsert: last-seen record for each surrogate key wins.
        Mirrors MERGE INTO upsert logic from InsureFlow.
        """
        store: dict[str, dict] = {}
        for r in records:
            store[r["_sk"]] = r          # last-write-wins
        return list(store.values())

    def _dedup(self, records: list[dict]) -> list[dict]:
        """Remove exact duplicates after merge (safety net)."""
        seen = set()
        out = []
        for r in records:
            if r["_sk"] not in seen:
                seen.add(r["_sk"])
                out.append(r)
        return out

    # ------------------------------------------------------------------
    # Field parsers
    # ------------------------------------------------------------------
    @staticmethod
    def _parse_price(raw: str) -> float | None:
        """Extract numeric price from strings like '₹ 1,23,456 / Piece'."""
        if not raw:
            return None
        nums = _PRICE_RE.findall(raw.replace(",", ""))
        if nums:
            try:
                val = float(nums[0])
                return val if val > 0 else None
            except ValueError:
                return None
        return None

    @staticmethod
    def _parse_unit(raw: str) -> str:
        """Extract unit from price string like '₹ 500 / Meter'."""
        if not raw:
            return "Unknown"
        parts = raw.split("/")
        if len(parts) > 1:
            return parts[-1].strip()
        return "Unknown"

    @staticmethod
    def _parse_location(raw: str) -> tuple[str, str]:
        """Split 'City, State' into (city, state)."""
        if not raw:
            return "", ""
        parts = [p.strip() for p in raw.split(",")]
        if len(parts) >= 2:
            return parts[0], parts[1]
        if len(parts) == 1:
            return parts[0], ""
        return "", ""

    @staticmethod
    def _normalise_ts(raw: str) -> str:
        """Ensure timestamp is a valid ISO string."""
        if not raw:
            return datetime.utcnow().isoformat()
        try:
            dt = datetime.fromisoformat(raw)
            return dt.isoformat()
        except ValueError:
            return datetime.utcnow().isoformat()

    # ------------------------------------------------------------------
    # Audit logging
    # ------------------------------------------------------------------
    def _write_audit(self, entry: dict):
        audit_file = self.audit_dir / "silver_audit.ndjson"
        with open(audit_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")
