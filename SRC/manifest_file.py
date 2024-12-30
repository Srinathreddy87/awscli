from pyspark.sql import SparkSession

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
    return details_df.collect()

# Function to get the data files from a table using DESCRIBE HISTORY
def get_operation_metrics(table_name):
    history_df = spark.sql(f"DESCRIBE HISTORY {table_name}")
    return history_df.select("operationMetrics").collect()

# Get data files location for the main table
main_table_details = get_data_files(main_table)
print(f"Data files details for main table '{main_table}':")
for detail in main_table_details:
    print(detail)

# Get data files location for the shallow clone table
shallow_clone_table_details = get_data_files(shallow_clone_table)
print(f"Data files details for shallow clone table '{shallow_clone_table}':")
for detail in shallow_clone_table_details:
    print(detail)

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
