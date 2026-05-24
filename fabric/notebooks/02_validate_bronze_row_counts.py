from pyspark.sql import functions as F
from datetime import datetime

BRONZE_TABLES = [
    {
        "table_name": "bronze_cms_provider_utilization_raw",
        "key_columns": ["rndrng_npi", "hcpcs_cd", "place_of_srvc"],
        "required_columns": [
            "source_system",
            "source_dataset_name",
            "source_file_name",
            "source_file_row_number",
            "ingestion_timestamp",
            "pipeline_run_id",
            "record_hash",
            "raw_record_status"
        ]
    },
    {
        "table_name": "bronze_cms_hospital_general_raw",
        "key_columns": ["facility_id"],
        "required_columns": [
            "source_system",
            "source_dataset_name",
            "source_file_name",
            "source_file_row_number",
            "ingestion_timestamp",
            "pipeline_run_id",
            "record_hash",
            "raw_record_status"
        ]
    },
    {
        "table_name": "bronze_cms_inpatient_payment_raw",
        "key_columns": ["rndrng_prvdr_ccn", "drg_cd"],
        "required_columns": [
            "source_system",
            "source_dataset_name",
            "source_file_name",
            "source_file_row_number",
            "ingestion_timestamp",
            "pipeline_run_id",
            "record_hash",
            "raw_record_status"
        ]
    },
    {
        "table_name": "bronze_cms_outpatient_payment_raw",
        "key_columns": ["rndrng_prvdr_ccn", "apc_cd"],
        "required_columns": [
            "source_system",
            "source_dataset_name",
            "source_file_name",
            "source_file_row_number",
            "ingestion_timestamp",
            "pipeline_run_id",
            "record_hash",
            "raw_record_status"
        ]
    }
]


def table_exists(table_name):
    try:
        spark.table(table_name)
        return True
    except Exception:
        return False


def write_dq_result(rule_id, rule_name, table_name, rule_category, severity, records_checked, records_failed, pipeline_run_id):
    failure_rate = 0 if records_checked == 0 else records_failed / records_checked
    status = "PASS" if records_failed == 0 else "FAIL"

    result_df = spark.createDataFrame(
        [(
            rule_id,
            rule_name,
            table_name,
            rule_category,
            severity,
            records_checked,
            records_failed,
            float(failure_rate),
            pipeline_run_id,
            datetime.utcnow(),
            status
        )],
        [
            "rule_id",
            "rule_name",
            "table_name",
            "rule_category",
            "severity",
            "records_checked",
            "records_failed",
            "failure_rate",
            "pipeline_run_id",
            "run_timestamp",
            "status"
        ]
    )

    result_df.write.mode("append").format("delta").saveAsTable("data_quality_rule_result")


for table_config in BRONZE_TABLES:
    table_name = table_config["table_name"]

    if not table_exists(table_name):
        print(f"SKIP: {table_name} does not exist yet.")
        continue

    df = spark.table(table_name)
    total_rows = df.count()

    pipeline_run_rows = (
        df.select("pipeline_run_id")
          .where(F.col("pipeline_run_id").isNotNull())
          .limit(1)
          .collect()
    )

    latest_pipeline_run_id = pipeline_run_rows[0][0] if pipeline_run_rows else "UNKNOWN"

    for col_name in table_config["required_columns"]:
        failed = df.filter(F.col(col_name).isNull()).count()

        write_dq_result(
            rule_id=f"{table_name}_{col_name}_not_null",
            rule_name=f"{col_name} is not null",
            table_name=table_name,
            rule_category="Completeness",
            severity="High",
            records_checked=total_rows,
            records_failed=failed,
            pipeline_run_id=latest_pipeline_run_id
        )

    for key_col in table_config["key_columns"]:
        if key_col in df.columns:
            failed = df.filter(F.col(key_col).isNull() | (F.trim(F.col(key_col)) == "")).count()

            write_dq_result(
                rule_id=f"{table_name}_{key_col}_not_null",
                rule_name=f"{key_col} is not null",
                table_name=table_name,
                rule_category="Completeness",
                severity="High",
                records_checked=total_rows,
                records_failed=failed,
                pipeline_run_id=latest_pipeline_run_id
            )

    duplicate_hash_count = (
        df.groupBy("record_hash")
          .count()
          .filter(F.col("count") > 1)
          .count()
    )

    write_dq_result(
        rule_id=f"{table_name}_duplicate_record_hash",
        rule_name="Duplicate record hash check",
        table_name=table_name,
        rule_category="Uniqueness",
        severity="Medium",
        records_checked=total_rows,
        records_failed=duplicate_hash_count,
        pipeline_run_id=latest_pipeline_run_id
    )

    print(f"Validated {table_name}: {total_rows} rows")