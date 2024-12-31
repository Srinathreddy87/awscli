from pyspark.sql.functions import explode, col

# Define table names
main_table_name = "main_table_name"
shallow_clone_table_name = "shallow_clone_table_name"

# Use DESCRIBE DETAIL to get the table paths
main_table_location = spark.sql(f"DESCRIBE DETAIL {main_table_name}").filter(col("col_name") == "location").select("data_type").collect()[0][0]
clone_table_location = spark.sql(f"DESCRIBE DETAIL {shallow_clone_table_name}").filter(col("col_name") == "location").select("data_type").collect()[0][0]

# Load Delta log JSON files to get file paths
main_log = spark.read.json(f"{main_table_location}/_delta_log/*.json")
clone_log = spark.read.json(f"{clone_table_location}/_delta_log/*.json")

# Extract "add" actions, which represent data files
main_files = main_log.filter(col("add").isNotNull()).select(col("add.path").alias("file_path")).distinct()
clone_files = clone_log.filter(col("add").isNotNull()).select(col("add.path").alias("file_path")).distinct()

# Compare file paths
main_only_files = main_files.subtract(clone_files)
clone_only_files = clone_files.subtract(main_files)

# Display differences
print("Files present in Main Table but not in Shallow Clone:")
main_only_files.show(truncate=False)

print("Files present in Shallow Clone but not in Main Table:")
clone_only_files.show(truncate=False)
