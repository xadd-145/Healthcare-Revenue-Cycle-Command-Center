# Healthcare Revenue Cycle Command Center

An enterprise healthcare revenue cycle analytics platform built with Microsoft Fabric, Power BI, SQL, and public CMS data to analyze claims, payments, denials, accounts receivable, payer performance, revenue leakage, and denial risk.

## Project Status

Current phase: **Sprint 1 — Data Foundation**

Sprint 1 focuses only on the data foundation:

- CMS source dataset selection
- CMS file inventory
- source header inspection
- 1,000-row sample file creation
- Bronze Lakehouse schema design
- Bronze ingestion pipeline design
- audit metadata
- row-count reconciliation
- initial data dictionary
- synthetic operational extension rules and scaffold

This sprint does **not** include Power BI dashboards, Gold modeling, SQL denial risk scoring, or real-time streaming.

## Product Goal

Hospitals lose revenue when claims are denied, underpaid, delayed, written off, or trapped in accounts receivable. This project simulates a healthcare revenue cycle analytics platform that helps finance, billing, payer management, and analytics teams identify where revenue is leaking and what to fix first.

The final platform will demonstrate:

- Microsoft Fabric medallion lakehouse architecture
- public CMS healthcare data ingestion
- Bronze, Silver, and Gold data layers
- Fabric Warehouse star schema
- Power BI semantic modeling and dashboards
- SQL-based denial risk scoring
- real-time denial spike monitoring
- data quality, governance, and pipeline observability

## Technology Stack

| Area | Technology |
|---|---|
| Data platform | Microsoft Fabric |
| Storage | Fabric Lakehouse / Delta tables |
| Transformation | PySpark notebooks |
| Orchestration | Fabric Data Factory pipelines |
| Warehouse | Fabric Warehouse |
| Reporting | Power BI |
| SQL scoring | T-SQL |
| Real-time monitoring | Fabric Eventstream, Eventhouse/KQL, Activator |
| Local development | Python, pandas |
| Version control | GitHub |

## Data Sources

Sprint 1 uses public CMS data only.

### Must-have CMS sources

| Priority | Dataset | Purpose |
|---:|---|---|
| 1 | CMS Medicare Physician & Other Practitioners by Provider and Service | Provider, procedure, utilization, submitted charge, allowed amount, and Medicare payment data |
| 2 | CMS Hospital General Information | Facility master data including hospital name, type, ownership, location, and rating |

### Stretch CMS sources

| Priority | Dataset | Purpose |
|---:|---|---|
| 3 | CMS Medicare Inpatient Hospitals by Provider and Service | DRG-level inpatient hospital charges and Medicare payments |
| 4 | CMS Medicare Outpatient Hospitals by Provider and Service | APC-level outpatient hospital charges and Medicare payments |

Full raw CMS files are not committed to GitHub. Only verified headers and 1,000-row sample files are committed.

## PHI-Safe Design

This project does not use real patient-level PHI.

The platform uses:

- public CMS provider-level and facility-level datasets
- synthetic claim workflow fields
- synthetic claim IDs
- synthetic denial events
- synthetic prior authorization records
- synthetic A/R snapshots

The project does **not** use:

- real patient names
- real MRNs
- real member IDs
- real dates of birth
- real patient addresses
- clinical notes
- real patient-level identifiers

## Sprint 1 Scope

Sprint 1 deliverables:

- CMS source inventory
- downloaded CMS files stored locally
- source headers extracted from actual downloaded files
- 1,000-row sample files
- Bronze schema definitions
- source-to-Bronze mapping
- ingestion pipeline design
- pipeline run log schema
- data quality result schema
- initial data dictionary
- synthetic operational extension rules
- synthetic generator scaffold

Sprint 1 acceptance criteria:

- at least two CMS datasets ingested into Bronze
- Bronze audit columns populated
- row counts reconciled between source and Bronze
- pipeline run log populated
- data quality result table populated
- data dictionary started

## Repository Structure

```text
rcm-command-center/
│
├── README.md
├── executive-summary.md
├── requirements.txt
│
├── architecture/
│
├── data/
│   ├── cms-source-inventory.md
│   ├── data-dictionary.md
│   ├── hipaa-disclaimer.md
│   ├── source-headers/
│   ├── sample-files/
│   └── synthetic-generator/
│       ├── README.md
│       ├── synthetic-rules.md
│       └── generate_operational_extensions.py
│
├── fabric/
│   ├── bronze-ddl.sql
│   ├── pipeline-run-log-ddl.sql
│   ├── data-quality-rules.sql
│   ├── lakehouse-tables.md
│   ├── notebooks/
│   └── pipelines/
│
├── governance/
│   ├── hipaa-safe-design.md
│   ├── data-quality-framework.md
│   └── pipeline-monitoring.md
│
├── demo/
│   └── sprint-1-validation-notes.md
│
└── local/
    ├── raw/
    └── working/