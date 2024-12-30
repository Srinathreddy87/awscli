from pyspark.sql import SparkSession
import json

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("DataFilesExample") \
    .getOrCreate()

# Define the main table and the shallow clone table names
main_table = "data_catlg"
shallow_clone_table = "data_catlg_sclone"

# Function to get the data files from a table using DESCRIBE DETAIL
def get_data_files(table_name):
    details_df = spark.sql(f"DESCRIBE DETAIL {table_name}")
    details_json = details_df.toJSON().collect()[0]
    details = json.loads(details_json)
    return details['location']

# Function to get the data files from a table using DESCRIBE HISTORY
def get_operation_metrics(table_name):
    history_df = spark.sql(f"DESCRIBE HISTORY {table_name}")
    operation_metrics = history_df.select("operationMetrics").collect()
    return operation_metrics

# Get data files location for the main table
main_table_location = get_data_files(main_table)
print(f"Data files location for main table '{main_table}': {main_table_location}")

# Get data files location for the shallow clone table
shallow_clone_table_location = get_data_files(shallow_clone_table)
print(f"Data files location for shallow clone table '{shallow_clone_table}': {shallow_clone_table_location}")

# Get operation metrics for the main table
main_table_metrics = get_operation_metrics(main_table)
print(f"Operation metrics for main table '{main_table}':")
for metrics in main_table_metrics:
    print(metrics)

# Get operation metrics for the shallow clone table
shallow_clone_table_metrics = get_operation_metrics(shallow_clone_table)
print(f"Operation metrics for shallow clone table '{shallow_clone_table}':")
for metrics in shallow_clone_table_metrics:
    print(metrics)
