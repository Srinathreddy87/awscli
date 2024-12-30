from pyspark.sql import SparkSession
import json

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("DataFilesComparison") \
    .getOrCreate()

# Define the main table and the shallow clone table names
main_table = "data_catlg"
shallow_clone_table = "data_catlg_sclone"

# Function to get the data files from a table using DESCRIBE DETAIL
def get_data_files(table_name):
    details_df = spark.sql(f"DESCRIBE DETAIL {table_name}")
    details = details_df.collect()[0].asDict()
    return details['location']

# Function to get the data files from a table using DESCRIBE HISTORY
def get_data_files_from_history(table_name):
    history_df = spark.sql(f"DESCRIBE HISTORY {table_name}")
    operation_metrics = []
    for row in history_df.collect():
        metrics = row.asDict().get('operationMetrics')
        if metrics and 'addedFiles' in metrics:
            operation_metrics.append(json.loads(metrics)['addedFiles'])
    return operation_metrics

# Get data files for the main table
main_table_files = get_data_files_from_history(main_table)
print(f"Data files for main table '{main_table}': {main_table_files}")

# Get data files for the shallow clone table
shallow_clone_files = get_data_files_from_history(shallow_clone_table)
print(f"Data files for shallow clone table '{shallow_clone_table}': {shallow_clone_files}")

# Compare the data files and print the result
if set(main_table_files) == set(shallow_clone_files):
    print("The data files used by the main table and the shallow clone table match.")
else:
    print("The data files used by the main table and the shallow clone table do not match.")
