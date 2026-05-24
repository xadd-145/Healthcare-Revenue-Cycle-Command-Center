-- ============================================================
-- Healthcare Revenue Cycle Command Center
-- Sprint 1: Bronze Data Quality Rules
-- Purpose:
--   Documents the initial validation rules executed after
--   Bronze ingestion.
--
-- Note:
--   These rules are implemented in PySpark in:
--   fabric/notebooks/02_validate_bronze_row_counts.py
-- ============================================================

-- ------------------------------------------------------------
-- Rule category: Completeness
-- ------------------------------------------------------------

-- DQ-001
-- Rule:
--   source_system must not be null.
-- Applies to:
--   All Bronze tables.
-- Severity:
--   High.

-- DQ-002
-- Rule:
--   source_dataset_name must not be null.
-- Applies to:
--   All Bronze tables.
-- Severity:
--   High.

-- DQ-003
-- Rule:
--   source_file_name must not be null.
-- Applies to:
--   All Bronze tables.
-- Severity:
--   High.

-- DQ-004
-- Rule:
--   source_file_row_number must not be null.
-- Applies to:
--   All Bronze tables.
-- Severity:
--   High.

-- DQ-005
-- Rule:
--   ingestion_timestamp must not be null.
-- Applies to:
--   All Bronze tables.
-- Severity:
--   High.

-- DQ-006
-- Rule:
--   pipeline_run_id must not be null.
-- Applies to:
--   All Bronze tables.
-- Severity:
--   High.

-- DQ-007
-- Rule:
--   record_hash must not be null.
-- Applies to:
--   All Bronze tables.
-- Severity:
--   High.

-- DQ-008
-- Rule:
--   raw_record_status must not be null.
-- Applies to:
--   All Bronze tables.
-- Severity:
--   High.

-- ------------------------------------------------------------
-- Rule category: Source key completeness
-- ------------------------------------------------------------

-- DQ-101
-- Rule:
--   rndrng_npi must not be null or blank.
-- Applies to:
--   bronze_cms_provider_utilization_raw.
-- Severity:
--   High.

-- DQ-102
-- Rule:
--   hcpcs_cd must not be null or blank.
-- Applies to:
--   bronze_cms_provider_utilization_raw.
-- Severity:
--   High.

-- DQ-103
-- Rule:
--   facility_id must not be null or blank.
-- Applies to:
--   bronze_cms_hospital_general_raw.
-- Severity:
--   High.

-- DQ-104
-- Rule:
--   rndrng_prvdr_ccn must not be null or blank.
-- Applies to:
--   bronze_cms_inpatient_payment_raw.
-- Severity:
--   High.

-- DQ-105
-- Rule:
--   drg_cd must not be null or blank.
-- Applies to:
--   bronze_cms_inpatient_payment_raw.
-- Severity:
--   High.

-- DQ-106
-- Rule:
--   rndrng_prvdr_ccn must not be null or blank.
-- Applies to:
--   bronze_cms_outpatient_payment_raw.
-- Severity:
--   High.

-- DQ-107
-- Rule:
--   apc_cd must not be null or blank.
-- Applies to:
--   bronze_cms_outpatient_payment_raw.
-- Severity:
--   High.

-- ------------------------------------------------------------
-- Rule category: Uniqueness
-- ------------------------------------------------------------

-- DQ-201
-- Rule:
--   record_hash should be unique within each Bronze table
--   for a single pipeline run.
-- Applies to:
--   All Bronze tables.
-- Severity:
--   Medium.

-- ------------------------------------------------------------
-- Rule category: Row-count reconciliation
-- ------------------------------------------------------------

-- DQ-301
-- Rule:
--   rows_written should match rows_read for full-load datasets.
-- Applies to:
--   bronze_cms_hospital_general_raw,
--   bronze_cms_inpatient_payment_raw,
--   bronze_cms_outpatient_payment_raw.
-- Severity:
--   High.

-- DQ-302
-- Rule:
--   rows_written may be lower than rows_read when a documented
--   development filter is applied.
-- Applies to:
--   bronze_cms_provider_utilization_raw.
-- Expected filter:
--   rndrng_prvdr_state_abrvtn IN ('IL', 'IN', 'WI')
-- Severity:
--   Informational.

-- ------------------------------------------------------------
-- Rule category: Bronze schema integrity
-- ------------------------------------------------------------

-- DQ-401
-- Rule:
--   All expected Bronze audit columns must exist.
-- Applies to:
--   All Bronze tables.
-- Severity:
--   High.

-- DQ-402
-- Rule:
--   All source columns from verified header files must exist
--   after snake_case normalization.
-- Applies to:
--   All Bronze tables.
-- Severity:
--   High.