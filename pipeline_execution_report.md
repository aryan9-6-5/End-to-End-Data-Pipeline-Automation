# 🚀 MassMutual Data Pipeline Execution & Diff Report
**Execution Timestamp:** 2026-09-05 15:08:29  
**Total Runtime:** 70.07 seconds  

## 📊 Data Volume Progression & Quality Healing
| Entity / Table | Raw Parquet Rows | Post-Ingestion | Post-Healing (Clean) | Quarantined Rows | Status / Impact |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **customers** | 50,000 | 50,000 | 23,953 | 78,141 | Filtered 26,047 invalid rows |
| **policies** | 200,000 | 0 | 200,000 | 595,755 | 100% clean |
| **claims** | 500,000 | 125,077 | 125,077 | 874,925 | Filtered 374,923 invalid rows |
| **payments** | 1,000,000 | 15,010 | 15,010 | 1,969,980 | Filtered 984,990 invalid rows |
| **agents** | 500 | 500 | 500 | 0 | 100% clean |
| **branches** | 100 | 100 | 100 | 0 | 100% clean |
| **policy_types** | 7 | 7 | 7 | 0 | 100% clean |
| **currency_rates** | 15 | 15 | 15 | 0 | 100% clean |
| **coverage_levels** | 5 | 5 | 5 | 0 | 100% clean |

## 📈 Transformed Analytics Schema Metrics
- **`transformed.policy_summary` Rows:** 23,953
- **Active Policies Total:** 0
- **Total Premium Amount:** `$343,502,340.00`
- **`transformed.claims_trends` Rows:** 483
- **Total Claim Amount:** `$15,628,038,596.33`

## 🛠️ Pipeline Stages Completed
1. **`load_massmutual_data`**: Ingested Parquet files into PostgreSQL raw tables.
2. **`heal_massmutual_data`**: Fixed anomalies (negative premiums, invalid dates, risk scores) and isolated corrupt duplicates/orphans to `quarantine`.
3. **`transform_massmutual_manual`**: Generated aggregated business-ready analytical datasets.