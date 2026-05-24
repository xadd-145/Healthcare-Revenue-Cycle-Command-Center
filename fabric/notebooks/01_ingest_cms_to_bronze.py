from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import uuid
import re

PIPELINE_NAME = "pl_ingest_cms_to_bronze"

DATASETS = [
    {
        "dataset_key": "provider_utilization",
        "source_system": "CMS_PROVIDER_PPAS",
        "source_dataset_name": "Medicare Physician and Other Practitioners by Provider and Service",
        "source_file_path": "Files/landing/cms/provider_utilization/provider_utilization_2024.csv",
        "target_table_name": "bronze_cms_provider_utilization_raw",
        "state_filter_column": "rndrng_prvdr_state_abrvtn",
        "state_filter_values": ["IL", "IN", "WI"],
        "required": True
    },
    {
        "dataset_key": "hospital_general",
        "source_system": "CMS_HOSPITAL_GENERAL",
        "source_dataset_name": "Hospital General Information",
        "source_file_path": "Files/landing/cms/hospital_general/hospital_general_information.csv",
        "target_table_name": "bronze_cms_hospital_general_raw",
        "state_filter_column": None,
        "state_filter_values": None,
        "required": True
    },
    {
        "dataset_key": "inpatient_payment",
        "source_system": "CMS_INPATIENT_DRG",
        "source_dataset_name": "Medicare Inpatient Hospitals by Provider and Service",
        "source_file_path": "Files/landing/cms/inpatient_payment/inpatient_provider_service_2024.csv",
        "target_table_name": "bronze_cms_inpatient_payment_raw",
        "state_filter_column": None,
        "state_filter_values": None,
        "required": False
    },
    {
        "dataset_key": "outpatient_payment",
        "source_system": "CMS_OUTPATIENT_APC",
        "source_dataset_name": "Medicare Outpatient Hospitals by Provider and Service",
        "source_file_path": "Files/landing/cms/outpatient_payment/outpatient_provider_service_2023.csv",
        "target_table_name": "bronze_cms_outpatient_payment_raw",
        "state_filter_column": None,
        "state_filter_values": None,
        "required": False
    }
]


def generate_pipeline_run_id() -> str:
    return f"RUN-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}-{str(uuid.uuid4())[:8]}"


def to_snake_case(col_name: str) -> str:
    col_name = col_name.strip()
    col_name = re.sub(r"[^A-Za-z0-9]+", "_", col_name)
    col_name = re.sub(r"_+", "_", col_name)
    return col_name.strip("_").lower()


def normalize_column_names(df):
    renamed_cols = [to_snake_case(c) for c in df.columns]
    return df.toDF(*renamed_cols), renamed_cols


def add_source_file_row_number(df):
    window_spec = Window.orderBy(F.monotonically_increasing_id())
    return df.withColumn("source_file_row_number", F.row_number().over(window_spec))


def add_record_hash(df, source_columns):
    normalized_cols = [
        F.coalesce(F.trim(F.col(c).cast("string")), F.lit(""))
        for c in source_columns
    ]

    return df.withColumn(
        "record_hash",
        F.sha2(F.concat_ws("||", *normalized_cols), 256)
    )


def add_bronze_audit_columns(
    df,
    source_system,
    source_dataset_name,
    source_file_name,
    pipeline_run_id,
    source_columns
):
    df = add_source_file_row_number(df)
    df = add_record_hash(df, source_columns)

    return (
        df.withColumn("source_system", F.lit(source_system))
          .withColumn("source_dataset_name", F.lit(source_dataset_name))
          .withColumn("source_file_name", F.lit(source_file_name))
          .withColumn("ingestion_timestamp", F.current_timestamp())
          .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
          .withColumn("raw_record_status", F.lit("LOADED"))
    )


def log_pipeline_run(
    pipeline_run_id,
    pipeline_name,
    notebook_name,
    source_system,
    source_dataset_name,
    source_file_name,
    target_table_name,
    filter_applied,
    start_time,
    end_time,
    status,
    rows_read,
    rows_written,
    rows_rejected,
    error_message
):
    log_df = spark.createDataFrame(
        [(
            pipeline_run_id,
            pipeline_name,
            notebook_name,
            source_system,
            source_dataset_name,
            source_file_name,
            target_table_name,
            filter_applied,
            start_time,
            end_time,
            status,
            rows_read,
            rows_written,
            rows_rejected,
            error_message,
            datetime.utcnow()
        )],
        [
            "pipeline_run_id",
            "pipeline_name",
            "notebook_name",
            "source_system",
            "source_dataset_name",
            "source_file_name",
            "target_table_name",
            "filter_applied",
            "start_time",
            "end_time",
            "status",
            "rows_read",
            "rows_written",
            "rows_rejected",
            "error_message",
            "created_timestamp"
        ]
    )

    log_df.write.mode("append").format("delta").saveAsTable("pipeline_run_log")


def ingest_dataset(dataset_config, pipeline_run_id):
    start_time = datetime.utcnow()
    source_file_path = dataset_config["source_file_path"]
    source_file_name = source_file_path.split("/")[-1]
    target_table_name = dataset_config["target_table_name"]

    try:
        df_raw = (
            spark.read
                 .option("header", "true")
                 .option("inferSchema", "false")
                 .csv(source_file_path)
        )

        rows_read = df_raw.count()

        df, source_columns = normalize_column_names(df_raw)

        filter_applied = None

        if dataset_config["state_filter_column"]:
            filter_col = dataset_config["state_filter_column"]
            filter_values = dataset_config["state_filter_values"]

            if filter_col not in df.columns:
                raise ValueError(f"Filter column {filter_col} not found in {target_table_name}")

            df = df.filter(F.col(filter_col).isin(filter_values))
            filter_applied = f"{filter_col} IN ({', '.join(filter_values)})"

        df_bronze = add_bronze_audit_columns(
            df=df,
            source_system=dataset_config["source_system"],
            source_dataset_name=dataset_config["source_dataset_name"],
            source_file_name=source_file_name,
            pipeline_run_id=pipeline_run_id,
            source_columns=source_columns
        )

        rows_written = df_bronze.count()

        (
            df_bronze.write
                     .mode("overwrite")
                     .format("delta")
                     .saveAsTable(target_table_name)
        )

        end_time = datetime.utcnow()

        log_pipeline_run(
            pipeline_run_id=pipeline_run_id,
            pipeline_name=PIPELINE_NAME,
            notebook_name="01_ingest_cms_to_bronze",
            source_system=dataset_config["source_system"],
            source_dataset_name=dataset_config["source_dataset_name"],
            source_file_name=source_file_name,
            target_table_name=target_table_name,
            filter_applied=filter_applied,
            start_time=start_time,
            end_time=end_time,
            status="SUCCESS",
            rows_read=rows_read,
            rows_written=rows_written,
            rows_rejected=0,
            error_message=None
        )

        print(f"SUCCESS: {target_table_name} rows_read={rows_read}, rows_written={rows_written}")

    except Exception as e:
        end_time = datetime.utcnow()

        log_pipeline_run(
            pipeline_run_id=pipeline_run_id,
            pipeline_name=PIPELINE_NAME,
            notebook_name="01_ingest_cms_to_bronze",
            source_system=dataset_config["source_system"],
            source_dataset_name=dataset_config["source_dataset_name"],
            source_file_name=source_file_name,
            target_table_name=target_table_name,
            filter_applied=None,
            start_time=start_time,
            end_time=end_time,
            status="FAILED",
            rows_read=0,
            rows_written=0,
            rows_rejected=0,
            error_message=str(e)
        )

        if dataset_config["required"]:
            raise e

        print(f"SKIPPED/FAILED optional dataset: {target_table_name}. Error: {e}")


pipeline_run_id = generate_pipeline_run_id()

for dataset in DATASETS:
    ingest_dataset(dataset, pipeline_run_id)