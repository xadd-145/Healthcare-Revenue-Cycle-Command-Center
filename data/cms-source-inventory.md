# CMS Source Inventory

## Purpose

This file documents the public CMS source datasets used for Sprint 1 of the Healthcare Revenue Cycle Command Center.

Full raw CMS files are stored locally under `local/raw/` and are not committed to GitHub. Only verified source headers and 1,000-row sample files are committed.

---

## Dataset Inventory

| Priority | Dataset | File Name | Dataset Year | Sprint Role | Bronze Table | Source Row Count | File Size MB | Notes |
|---:|---|---|---:|---|---|---:|---:|---|
| 1 | Medicare Physician & Other Practitioners by Provider and Service | provider_utilization_2024.csv | 2024 | Must | bronze_cms_provider_utilization_raw | 9,781,673 | 3099.71 | Mandatory IL/IN/WI filter before Bronze write |
| 2 | Hospital General Information | hospital_general_information.csv | Current downloaded file | Must | bronze_cms_hospital_general_raw | 5,432 | 1.49 | Full file load |
| 3 | Medicare Inpatient Hospitals by Provider and Service | inpatient_provider_service_2024.csv | 2024 | Stretch | bronze_cms_inpatient_payment_raw | 145,879 | 36.22 | Small enough to include in Sprint 1 Bronze |
| 4 | Medicare Outpatient Hospitals by Provider and Service | outpatient_provider_service_2023.csv | 2023 | Stretch | bronze_cms_outpatient_payment_raw | 116,799 | 26.87 | Accepted as stretch source even though year differs |

---

## Header Files

| Dataset | Header File |
|---|---|
| Provider Utilization | `data/source-headers/provider_utilization_headers.txt` |
| Hospital General | `data/source-headers/hospital_general_headers.txt` |
| Inpatient Payment | `data/source-headers/inpatient_payment_headers.txt` |
| Outpatient Payment | `data/source-headers/outpatient_payment_headers.txt` |

---

## Sample Files

| Dataset | Sample File |
|---|---|
| Provider Utilization | `data/sample-files/sample_provider_utilization.csv` |
| Hospital General | `data/sample-files/sample_hospital_general.csv` |
| Inpatient Payment | `data/sample-files/sample_inpatient_payment.csv` |
| Outpatient Payment | `data/sample-files/sample_outpatient_payment.csv` |

---

## Provider Utilization Development Filter

The provider utilization source file contains 9,781,673 rows and is approximately 3.1 GB. To keep Sprint 1 development manageable, Bronze ingestion applies a regional development filter:

```sql
rndrng_prvdr_state_abrvtn IN ('IL', 'IN', 'WI')