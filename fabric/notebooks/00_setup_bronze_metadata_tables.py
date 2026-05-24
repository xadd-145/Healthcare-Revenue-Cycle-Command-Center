from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    TimestampType, DecimalType
)

# ------------------------------------------------------------
# pipeline_run_log
# ------------------------------------------------------------

pipeline_run_log_schema = StructType([
    StructField("pipeline_run_id", StringType(), True),
    StructField("pipeline_name", StringType(), True),
    StructField("notebook_name", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("source_dataset_name", StringType(), True),
    StructField("source_file_name", StringType(), True),
    StructField("target_table_name", StringType(), True),
    StructField("filter_applied", StringType(), True),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("status", StringType(), True),
    StructField("rows_read", LongType(), True),
    StructField("rows_written", LongType(), True),
    StructField("rows_rejected", LongType(), True),
    StructField("error_message", StringType(), True),
    StructField("created_timestamp", TimestampType(), True),
])

empty_pipeline_log_df = spark.createDataFrame([], pipeline_run_log_schema)

empty_pipeline_log_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("pipeline_run_log")

# ------------------------------------------------------------
# data_quality_rule_result
# ------------------------------------------------------------

dq_schema = StructType([
    StructField("rule_id", StringType(), True),
    StructField("rule_name", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("rule_category", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("records_checked", LongType(), True),
    StructField("records_failed", LongType(), True),
    StructField("failure_rate", DecimalType(10, 4), True),
    StructField("pipeline_run_id", StringType(), True),
    StructField("run_timestamp", TimestampType(), True),
    StructField("status", StringType(), True),
])

empty_dq_df = spark.createDataFrame([], dq_schema)

empty_dq_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable("data_quality_rule_result")

print("Created metadata tables:")
print("- pipeline_run_log")
print("- data_quality_rule_result")