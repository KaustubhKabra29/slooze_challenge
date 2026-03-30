"""
bronze_layer.py
---------------
Bronze layer of the Medallion Architecture.

Responsibilities:
  - Accept raw records from the crawler
  - Stamp each record with ingestion metadata (batch_id, ingested_at)
  - Persist to NDJSON files partitioned by category + date
  - Maintain a watermark so re-runs only process new data
  - Write an audit log entry per batch

This mirrors the Bronze ingestion notebooks in the RideStream project:
automated file discovery, archiving, and full audit metadata.
"""

import json
import logging
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class BronzeLayer:
    """
    Persists raw crawler output to the Bronze zone.

    Output layout:
        data/bronze/<category>/<YYYY-MM-DD>/<batch_id>.json
        data/audit/bronze_audit.ndjson
        data/watermark.json
    """

    def __init__(self, config: dict):
        pipeline_cfg = config.get("pipeline", {})
        self.bronze_dir = Path(pipeline_cfg.get("bronze_dir", "data/bronze"))
        self.audit_dir = Path(pipeline_cfg.get("audit_log_dir", "data/audit"))
        self.watermark_file = Path(pipeline_cfg.get("watermark_file", "data/watermark.json"))

        self.bronze_dir.mkdir(parents=True, exist_ok=True)
        self.audit_dir.mkdir(parents=True, exist_ok=True)
        self.watermark_file.parent.mkdir(parents=True, exist_ok=True)

        self._watermarks = self._load_watermarks()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def ingest(self, category_slug: str, raw_records: list[dict]) -> dict:
        """
        Ingest raw records for a category.
        Returns an audit summary dict.
        """
        if not raw_records:
            logger.warning("[Bronze] No records to ingest for '%s'", category_slug)
            return {"category": category_slug, "records_written": 0, "status": "SKIPPED"}

        batch_id = str(uuid.uuid4())
        ingested_at = datetime.utcnow().isoformat()
        date_partition = datetime.utcnow().strftime("%Y-%m-%d")

        # Stamp every record with batch metadata
        stamped = []
        for rec in raw_records:
            stamped.append({
                **rec,
                "_batch_id": batch_id,
                "_ingested_at": ingested_at,
                "_layer": "bronze",
            })

        # Write partitioned output
        out_path = self.bronze_dir / category_slug / date_partition
        out_path.mkdir(parents=True, exist_ok=True)
        file_path = out_path / f"{batch_id}.json"

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(stamped, f, ensure_ascii=False, indent=2)

        logger.info(
            "[Bronze] Wrote %d records → %s", len(stamped), file_path
        )

        # Update watermark
        self._update_watermark(category_slug, ingested_at, batch_id)

        # Write audit log
        audit_entry = {
            "layer": "bronze",
            "batch_id": batch_id,
            "category": category_slug,
            "records_written": len(stamped),
            "file_path": str(file_path),
            "ingested_at": ingested_at,
            "status": "SUCCESS",
        }
        self._write_audit(audit_entry)

        return audit_entry

    def read_bronze(self, category_slug: str) -> list[dict]:
        """Read all Bronze records for a category (all partitions)."""
        category_dir = self.bronze_dir / category_slug
        if not category_dir.exists():
            logger.warning("[Bronze] No data found for category '%s'", category_slug)
            return []

        records: list[dict] = []
        for json_file in sorted(category_dir.rglob("*.json")):
            with open(json_file, "r", encoding="utf-8") as f:
                batch = json.load(f)
                records.extend(batch)

        logger.info("[Bronze] Read %d records for '%s'", len(records), category_slug)
        return records

    def read_all_bronze(self) -> list[dict]:
        """Read all Bronze records across all categories."""
        all_records: list[dict] = []
        if not self.bronze_dir.exists():
            return all_records
        for cat_dir in self.bronze_dir.iterdir():
            if cat_dir.is_dir():
                all_records.extend(self.read_bronze(cat_dir.name))
        return all_records

    # ------------------------------------------------------------------
    # Watermark management
    # ------------------------------------------------------------------
    def _load_watermarks(self) -> dict:
        if self.watermark_file.exists():
            with open(self.watermark_file, "r") as f:
                return json.load(f)
        return {}

    def _update_watermark(self, category: str, timestamp: str, batch_id: str):
        self._watermarks[category] = {
            "last_ingested_at": timestamp,
            "last_batch_id": batch_id,
        }
        with open(self.watermark_file, "w") as f:
            json.dump(self._watermarks, f, indent=2)

    def get_watermark(self, category: str) -> dict:
        return self._watermarks.get(category, {})

    # ------------------------------------------------------------------
    # Audit logging (NDJSON append)
    # ------------------------------------------------------------------
    def _write_audit(self, entry: dict):
        audit_file = self.audit_dir / "bronze_audit.ndjson"
        with open(audit_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")
