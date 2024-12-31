from pyspark.sql.functions import col

# Define table names
main_table_name = "main_table_name"
shallow_clone_table_name = "shallow_clone_table_name"

# Retrieve the table locations using DESCRIBE DETAIL
main_table_location = spark.sql(f"DESCRIBE DETAIL {main_table_name}").select("location").collect()[0][0]
clone_table_location = spark.sql(f"DESCRIBE DETAIL {shallow_clone_table_name}").select("location").collect()[0][0]

# Load Delta log JSON files for both tables
main_log = spark.read.json(f"{main_table_location}/_delta_log/*.json")
clone_log = spark.read.json(f"{clone_table_location}/_delta_log/*.json")

# Extract "add" actions (representing data files)
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
