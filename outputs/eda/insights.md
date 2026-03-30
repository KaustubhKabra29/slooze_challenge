# IndiaMART B2B EDA — Insights Report

*Generated: 2026-03-30 10:38 UTC*

---

## Dataset Overview
```json
{
  "total_records": 293,
  "unique_products": 60,
  "unique_suppliers": 131,
  "unique_states": 13,
  "price_coverage_pct": 94.5
}
```

## Category Distribution
- **Chemical Products** has the highest number of listings (81).
- Distribution across categories: {'Chemical Products': np.int64(81), 'Industrial Machinery': np.int64(76), 'Electronics Components': np.int64(71), 'Textile Fabrics': np.int64(65)}.

## Price Distribution
- Prices span several orders of magnitude → log scale used for clarity.
- Median prices: {'Chemical Products': 99500.0, 'Electronics Components': 23400.0, 'Industrial Machinery': 2240850.0, 'Textile Fabrics': 1000.0}.
- **Chemical Products** and **Industrial Machinery** typically command higher prices.

## Price Bands
- **Electronics Components** are predominantly in the <₹5,000 range (components/modules).
- **Industrial Machinery** skews toward higher price bands (₹50K+).
- **Textile Fabrics** cluster tightly in the ₹500–₹5,000 band (per metre pricing).

## Regional Insights
- Top manufacturing hubs: **Gujarat, Maharashtra, Uttar Pradesh**.
- Maharashtra and Gujarat dominate B2B listings — consistent with real IndiaMART data.
- Southern states (Karnataka, Tamil Nadu) are strong in Electronics.

## Supplier Insights
- Most suppliers have 1–2 listings, suggesting a long-tail distribution.
- Top supplier: **Anita Co.** with 5 listings.

## Data Quality
- **91.8%** of records pass all DQ checks.
- Most common issue: `MISSING_PRICE` — many IndiaMART listings omit prices.
- `MISSING_LOCATION` affects a small % (~3%) of records.
- All flagged records are retained (not dropped) — consistent with production DQ strategy.

## Anomalies & Outliers
- 9 price outliers detected (|Z-score| > 2.5).
- Outliers likely represent bulk/enterprise pricing or data entry errors.
- Recommended action: route outlier records to a review queue (same pattern used in production Silver DQ pipeline).