"""
indiamart_crawler.py
--------------------
IndiaMART-specific crawler.  Inherits rate-limiting, retry logic, and
audit-logging from BaseCrawler.

Design choices that mirror resume experience:
- Config-driven selectors (zero-code category onboarding via scraper_config.json)
- Full audit metadata per crawl run (category, URL, row count, status)
- Graceful degradation: if live scraping is blocked, falls back to a
  realistic synthetic dataset so the downstream pipeline + EDA always runs.
"""

import json
import logging
import random
from datetime import datetime, timedelta
from typing import Optional

from bs4 import BeautifulSoup

from src.crawler.base_crawler import BaseCrawler

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Synthetic-data pools (used when live scraping is unavailable)
# ---------------------------------------------------------------------------
_SYNTHETIC = {
    "industrial_machinery": {
        "products": [
            "Hydraulic Press Machine", "CNC Lathe Machine", "Air Compressor",
            "Diesel Generator", "Conveyor Belt System", "Industrial Pump",
            "Welding Machine", "Injection Moulding Machine", "Gear Box",
            "Centrifugal Pump", "Vertical Milling Machine", "Ball Valve",
            "Industrial Boiler", "Cooling Tower", "Screw Conveyor",
        ],
        "price_range": (15000, 5000000),
        "units": ["Piece", "Set", "Unit", "No"],
    },
    "electronics_components": {
        "products": [
            "Arduino Uno Board", "Raspberry Pi 4", "MOSFET Transistor",
            "LED Strip Light", "Capacitor 100uF", "Resistor Pack",
            "PCB Board", "Microcontroller Module", "DC Motor Driver",
            "Bluetooth Module HC-05", "Solar Panel 100W", "Li-Ion Battery Pack",
            "OLED Display Module", "Temperature Sensor DS18B20", "Relay Module",
        ],
        "price_range": (50, 50000),
        "units": ["Piece", "Pack", "Roll", "Meter"],
    },
    "textile_fabrics": {
        "products": [
            "Cotton Fabric", "Polyester Fabric", "Silk Saree Fabric",
            "Denim Fabric", "Woolen Fabric", "Jute Fabric",
            "Rayon Fabric", "Linen Fabric", "Velvet Fabric",
            "Net Fabric", "Georgette Fabric", "Chiffon Fabric",
            "Canvas Fabric", "Lycra Fabric", "Satin Fabric",
        ],
        "price_range": (100, 2000),
        "units": ["Meter", "Roll", "Kg"],
    },
    "chemical_products": {
        "products": [
            "Sodium Hydroxide", "Hydrochloric Acid", "Sulphuric Acid",
            "Calcium Carbonate", "Titanium Dioxide", "Activated Carbon",
            "Citric Acid", "Sodium Bicarbonate", "Potassium Chloride",
            "Acetic Acid", "Ferric Chloride", "Ammonium Nitrate",
            "Zinc Oxide", "Hydrogen Peroxide", "Caustic Soda Flakes",
        ],
        "price_range": (500, 200000),
        "units": ["Kg", "Litre", "Ton", "MT"],
    },
}

_CITIES = [
    ("Mumbai", "Maharashtra"), ("Delhi", "Delhi"), ("Bengaluru", "Karnataka"),
    ("Ahmedabad", "Gujarat"), ("Pune", "Maharashtra"), ("Surat", "Gujarat"),
    ("Jaipur", "Rajasthan"), ("Hyderabad", "Telangana"), ("Ludhiana", "Punjab"),
    ("Kanpur", "Uttar Pradesh"), ("Coimbatore", "Tamil Nadu"), ("Nagpur", "Maharashtra"),
    ("Kolkata", "West Bengal"), ("Indore", "Madhya Pradesh"), ("Chennai", "Tamil Nadu"),
    ("Rajkot", "Gujarat"), ("Vadodara", "Gujarat"), ("Faridabad", "Haryana"),
    ("Meerut", "Uttar Pradesh"), ("Nashik", "Maharashtra"),
]

_SUPPLIERS = [
    "Enterprises", "Industries", "Traders", "Corporation", "Pvt Ltd",
    "Solutions", "Manufacturing", "International", "Exports", "Co.",
]

_FIRST_NAMES = [
    "Rajesh", "Suresh", "Mahesh", "Dinesh", "Ramesh",
    "Priya", "Anita", "Kavita", "Sunita", "Pooja",
    "Anil", "Vijay", "Sanjay", "Ajay", "Rohit",
]


class IndiaMARTCrawler(BaseCrawler):
    """
    Crawls IndiaMART product listing pages.

    Falls back to synthetic data generation if the live site is unreachable
    or returns no parseable content (common in CI/CD / offline environments).
    """

    def __init__(self, config: dict):
        super().__init__(config)
        self.selectors = config.get("selectors", {})
        self.max_pages = config.get("scraper", {}).get("max_pages_per_category", 5)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------
    def crawl_category(self, category: dict) -> list[dict]:
        """
        Crawl up to max_pages for a category.
        Returns a flat list of raw product dicts with full audit metadata.
        """
        slug = category["slug"]
        base_url = category["search_url"]
        all_records: list[dict] = []

        logger.info("Starting crawl: category='%s'", slug)

        for page in range(1, self.max_pages + 1):
            url = f"{base_url}&page={page}" if page > 1 else base_url
            logger.info("  Fetching page %d → %s", page, url)

            response = self.get(url)
            if response is None:
                self.log_audit(slug, url, 0, "FAILED", "No response from server")
                logger.warning("  Page %d failed — stopping category crawl.", page)
                break

            records = self.parse_page(response.text, slug)

            if not records:
                logger.info("  No records on page %d — end of results.", page)
                self.log_audit(slug, url, 0, "PARTIAL", "Empty page")
                break

            all_records.extend(records)
            self.log_audit(slug, url, len(records), "SUCCESS")
            logger.info("  Page %d: %d records scraped (total so far: %d)",
                        page, len(records), len(all_records))

        # Fallback: generate synthetic data if nothing was scraped
        if not all_records:
            logger.warning(
                "No live data scraped for '%s'. Generating synthetic dataset.", slug
            )
            all_records = self._generate_synthetic(slug, n=random.randint(60, 100))
            self.log_audit(slug, base_url, len(all_records), "SYNTHETIC",
                           "Live scraping returned no data; synthetic fallback used")

        return all_records

    # ------------------------------------------------------------------
    # HTML parsing
    # ------------------------------------------------------------------
    def parse_page(self, html: str, category_slug: str) -> list[dict]:
        """
        Parse one IndiaMART listing page.
        CSS selectors are loaded from config (config-driven design).
        """
        soup = BeautifulSoup(html, "html.parser")
        records: list[dict] = []

        # Config-driven selector lookup
        card_sel = self.selectors.get("product_card", ".product-list-item")
        cards = soup.select(card_sel)

        if not cards:
            # Fallback: try common alternative selectors
            for alt in [".imp-list-item", ".listing-item", "[data-pid]"]:
                cards = soup.select(alt)
                if cards:
                    break

        for card in cards:
            record = self._extract_card(card, category_slug)
            if record:
                records.append(record)

        return records

    def _extract_card(self, card, category_slug: str) -> Optional[dict]:
        """Extract fields from a single product card element."""
        try:
            def txt(sel: str) -> str:
                el = card.select_one(sel)
                return el.get_text(strip=True) if el else ""

            name_sel = self.selectors.get("product_name", ".prod-name")
            price_sel = self.selectors.get("price", ".price")
            supplier_sel = self.selectors.get("supplier_name", ".sup-name")
            location_sel = self.selectors.get("location", ".loc")
            desc_sel = self.selectors.get("description", ".prod-desc")

            name = txt(name_sel)
            if not name:                   # Skip cards with no product name
                return None

            return {
                "product_name": name,
                "price_raw": txt(price_sel),
                "supplier_name": txt(supplier_sel),
                "location_raw": txt(location_sel),
                "description": txt(desc_sel)[:300],
                "category": category_slug,
                "source_url": "",          # Populated by caller if needed
                "scraped_at": datetime.utcnow().isoformat(),
                "data_source": "live",
            }
        except Exception as exc:
            logger.debug("Card parse error: %s", exc)
            return None

    # ------------------------------------------------------------------
    # Synthetic data fallback
    # ------------------------------------------------------------------
    def _generate_synthetic(self, category_slug: str, n: int = 80) -> list[dict]:
        """
        Generate realistic-looking B2B marketplace records.
        Prices, locations, and supplier names follow observed IndiaMART patterns.
        """
        pool = _SYNTHETIC.get(category_slug, _SYNTHETIC["industrial_machinery"])
        records = []

        # Spread scraped_at across the last 30 days (simulates incremental loads)
        base_ts = datetime.utcnow()

        for i in range(n):
            product = random.choice(pool["products"])
            city, state = random.choice(_CITIES)
            supplier_type = random.choice(_SUPPLIERS)
            first_name = random.choice(_FIRST_NAMES)
            supplier = f"{first_name} {supplier_type}"

            lo, hi = pool["price_range"]
            price = random.randint(lo // 100, hi // 100) * 100
            unit = random.choice(pool["units"])
            min_order = random.choice([1, 5, 10, 50, 100])

            # Simulate some missing/dirty data (5% chance) for EDA realism
            if random.random() < 0.05:
                price = None
            if random.random() < 0.03:
                city, state = "", ""

            scraped_offset = timedelta(
                days=random.randint(0, 30),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
            )

            records.append({
                "product_name": product,
                "price_raw": f"₹ {price:,} / {unit}" if price else "",
                "supplier_name": supplier,
                "location_raw": f"{city}, {state}" if city else "",
                "description": (
                    f"We are leading manufacturer and exporter of {product.lower()}. "
                    f"Minimum order: {min_order} {unit}. "
                    f"Quality assured, bulk discounts available."
                ),
                "category": category_slug,
                "source_url": (
                    f"https://www.indiamart.com/proddetail/{product.lower().replace(' ', '-')}"
                    f"-{random.randint(1000000, 9999999)}.html"
                ),
                "scraped_at": (base_ts - scraped_offset).isoformat(),
                "data_source": "synthetic",
            })

        logger.info("Generated %d synthetic records for '%s'", len(records), category_slug)
        return records
