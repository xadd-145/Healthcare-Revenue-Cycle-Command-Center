-- ============================================================
-- Healthcare Revenue Cycle Command Center
-- Sprint 1: Pipeline Metadata DDL
-- Purpose:
--   Defines pipeline observability tables for Bronze ingestion.
-- ============================================================

-- ------------------------------------------------------------
-- Pipeline run log
-- One row per dataset ingestion attempt.
-- ------------------------------------------------------------

CREATE TABLE pipeline_run_log (
    pipeline_run_id VARCHAR(100),
    pipeline_name VARCHAR(255),
    notebook_name VARCHAR(255),
    source_system VARCHAR(100),
    source_dataset_name VARCHAR(255),
    source_file_name VARCHAR(255),
    target_table_name VARCHAR(255),
    filter_applied VARCHAR(1000),
    start_time DATETIME2,
    end_time DATETIME2,
    status VARCHAR(50),
    rows_read BIGINT,
    rows_written BIGINT,
    rows_rejected BIGINT,
    error_message VARCHAR(4000),
    created_timestamp DATETIME2
);

-- ------------------------------------------------------------
-- Data quality rule result
-- One row per data quality rule execution.
-- ------------------------------------------------------------

CREATE TABLE data_quality_rule_result (
    rule_id VARCHAR(100),
    rule_name VARCHAR(255),
    table_name VARCHAR(255),
    rule_category VARCHAR(100),
    severity VARCHAR(50),
    records_checked BIGINT,
    records_failed BIGINT,
    failure_rate DECIMAL(10,4),
    pipeline_run_id VARCHAR(100),
    run_timestamp DATETIME2,
    status VARCHAR(50)
);