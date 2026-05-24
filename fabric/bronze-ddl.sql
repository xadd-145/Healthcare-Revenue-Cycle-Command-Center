-- ============================================================
-- Healthcare Revenue Cycle Command Center
-- Sprint 1: Bronze Layer DDL
-- Source: Verified CMS headers
-- ============================================================

-- ============================================================
-- Required Dataset 1:
-- CMS Medicare Physician & Other Practitioners
-- by Provider and Service
--
-- Source file:
-- provider_utilization_2024.csv
--
-- Source rows:
-- 9,781,673
--
-- Grain:
-- provider + HCPCS/service + place of service
--
-- Development filter applied during ingestion:
-- rndrng_prvdr_state_abrvtn IN ('IL', 'IN', 'WI')
-- ============================================================

CREATE TABLE bronze_cms_provider_utilization_raw (
    rndrng_npi VARCHAR(20),
    rndrng_prvdr_last_org_name VARCHAR(255),
    rndrng_prvdr_first_name VARCHAR(255),
    rndrng_prvdr_mi VARCHAR(50),
    rndrng_prvdr_crdntls VARCHAR(100),
    rndrng_prvdr_ent_cd VARCHAR(20),
    rndrng_prvdr_st1 VARCHAR(255),
    rndrng_prvdr_st2 VARCHAR(255),
    rndrng_prvdr_city VARCHAR(100),
    rndrng_prvdr_state_abrvtn VARCHAR(10),
    rndrng_prvdr_state_fips VARCHAR(10),
    rndrng_prvdr_zip5 VARCHAR(20),
    rndrng_prvdr_ruca VARCHAR(20),
    rndrng_prvdr_ruca_desc VARCHAR(255),
    rndrng_prvdr_cntry VARCHAR(100),
    rndrng_prvdr_type VARCHAR(255),
    rndrng_prvdr_mdcr_prtcptg_ind VARCHAR(20),
    hcpcs_cd VARCHAR(20),
    hcpcs_desc VARCHAR(1000),
    hcpcs_drug_ind VARCHAR(20),
    place_of_srvc VARCHAR(50),
    tot_benes VARCHAR(50),
    tot_srvcs VARCHAR(50),
    tot_bene_day_srvcs VARCHAR(50),
    avg_sbmtd_chrg VARCHAR(50),
    avg_mdcr_alowd_amt VARCHAR(50),
    avg_mdcr_pymt_amt VARCHAR(50),
    avg_mdcr_stdzd_amt VARCHAR(50),

    source_system VARCHAR(100),
    source_dataset_name VARCHAR(255),
    source_file_name VARCHAR(255),
    source_file_row_number BIGINT,
    ingestion_timestamp DATETIME2,
    pipeline_run_id VARCHAR(100),
    record_hash VARCHAR(256),
    raw_record_status VARCHAR(50)
);

-- ============================================================
-- Required Dataset 2:
-- CMS Hospital General Information
--
-- Source file:
-- hospital_general_information.csv
--
-- Source rows:
-- 5,432
--
-- Grain:
-- one row per hospital/facility
-- ============================================================

CREATE TABLE bronze_cms_hospital_general_raw (
    facility_id VARCHAR(20),
    facility_name VARCHAR(255),
    address VARCHAR(255),
    city_town VARCHAR(100),
    state VARCHAR(10),
    zip_code VARCHAR(20),
    county_parish VARCHAR(100),
    telephone_number VARCHAR(50),
    hospital_type VARCHAR(100),
    hospital_ownership VARCHAR(255),
    emergency_services VARCHAR(20),
    meets_criteria_for_birthing_friendly_designation VARCHAR(50),
    hospital_overall_rating VARCHAR(50),
    hospital_overall_rating_footnote VARCHAR(1000),
    mort_group_measure_count VARCHAR(50),
    count_of_facility_mort_measures VARCHAR(50),
    count_of_mort_measures_better VARCHAR(50),
    count_of_mort_measures_no_different VARCHAR(50),
    count_of_mort_measures_worse VARCHAR(50),
    mort_group_footnote VARCHAR(1000),
    safety_group_measure_count VARCHAR(50),
    count_of_facility_safety_measures VARCHAR(50),
    count_of_safety_measures_better VARCHAR(50),
    count_of_safety_measures_no_different VARCHAR(50),
    count_of_safety_measures_worse VARCHAR(50),
    safety_group_footnote VARCHAR(1000),
    readm_group_measure_count VARCHAR(50),
    count_of_facility_readm_measures VARCHAR(50),
    count_of_readm_measures_better VARCHAR(50),
    count_of_readm_measures_no_different VARCHAR(50),
    count_of_readm_measures_worse VARCHAR(50),
    readm_group_footnote VARCHAR(1000),
    pt_exp_group_measure_count VARCHAR(50),
    count_of_facility_pt_exp_measures VARCHAR(50),
    pt_exp_group_footnote VARCHAR(1000),
    te_group_measure_count VARCHAR(50),
    count_of_facility_te_measures VARCHAR(50),
    te_group_footnote VARCHAR(1000),

    source_system VARCHAR(100),
    source_dataset_name VARCHAR(255),
    source_file_name VARCHAR(255),
    source_file_row_number BIGINT,
    ingestion_timestamp DATETIME2,
    pipeline_run_id VARCHAR(100),
    record_hash VARCHAR(256),
    raw_record_status VARCHAR(50)
);

-- ============================================================
-- Stretch Dataset 3:
-- CMS Medicare Inpatient Hospitals
-- by Provider and Service
--
-- Source file:
-- inpatient_provider_service_2024.csv
--
-- Source rows:
-- 145,879
--
-- Grain:
-- facility/provider CCN + DRG
-- ============================================================

CREATE TABLE bronze_cms_inpatient_payment_raw (
    rndrng_prvdr_ccn VARCHAR(20),
    rndrng_prvdr_org_name VARCHAR(255),
    rndrng_prvdr_city VARCHAR(100),
    rndrng_prvdr_st VARCHAR(255),
    rndrng_prvdr_state_fips VARCHAR(10),
    rndrng_prvdr_zip5 VARCHAR(20),
    rndrng_prvdr_state_abrvtn VARCHAR(10),
    rndrng_prvdr_ruca VARCHAR(20),
    rndrng_prvdr_ruca_desc VARCHAR(255),
    drg_cd VARCHAR(20),
    drg_desc VARCHAR(1000),
    tot_dschrgs VARCHAR(50),
    avg_submtd_cvrd_chrg VARCHAR(50),
    avg_tot_pymt_amt VARCHAR(50),
    avg_mdcr_pymt_amt VARCHAR(50),

    source_system VARCHAR(100),
    source_dataset_name VARCHAR(255),
    source_file_name VARCHAR(255),
    source_file_row_number BIGINT,
    ingestion_timestamp DATETIME2,
    pipeline_run_id VARCHAR(100),
    record_hash VARCHAR(256),
    raw_record_status VARCHAR(50)
);

-- ============================================================
-- Stretch Dataset 4:
-- CMS Medicare Outpatient Hospitals
-- by Provider and Service
--
-- Source file:
-- outpatient_provider_service_2023.csv
--
-- Source rows:
-- 116,799
--
-- Grain:
-- facility/provider CCN + APC
-- ============================================================

CREATE TABLE bronze_cms_outpatient_payment_raw (
    rndrng_prvdr_ccn VARCHAR(20),
    rndrng_prvdr_org_name VARCHAR(255),
    rndrng_prvdr_st VARCHAR(255),
    rndrng_prvdr_city VARCHAR(100),
    rndrng_prvdr_state_abrvtn VARCHAR(10),
    rndrng_prvdr_state_fips VARCHAR(10),
    rndrng_prvdr_zip5 VARCHAR(20),
    rndrng_prvdr_ruca VARCHAR(20),
    rndrng_prvdr_ruca_desc VARCHAR(255),
    apc_cd VARCHAR(20),
    apc_desc VARCHAR(1000),
    bene_cnt VARCHAR(50),
    capc_srvcs VARCHAR(50),
    avg_tot_sbmtd_chrgs VARCHAR(50),
    avg_mdcr_alowd_amt VARCHAR(50),
    avg_mdcr_pymt_amt VARCHAR(50),
    outlier_srvcs VARCHAR(50),
    avg_mdcr_outlier_amt VARCHAR(50),

    source_system VARCHAR(100),
    source_dataset_name VARCHAR(255),
    source_file_name VARCHAR(255),
    source_file_row_number BIGINT,
    ingestion_timestamp DATETIME2,
    pipeline_run_id VARCHAR(100),
    record_hash VARCHAR(256),
    raw_record_status VARCHAR(50)
);