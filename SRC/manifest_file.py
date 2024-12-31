from pyspark.sql.functions import input_file_name

# Paths to the main table and shallow clone table
main_table_path = "path_to_main_table"
shallow_clone_table_path = "path_to_shallow_clone_table"

# Load the data files for both tables
main_table_df = spark.read.format("delta").load(main_table_path).withColumn("file_name", input_file_name())
shallow_clone_df = spark.read.format("delta").load(shallow_clone_table_path).withColumn("file_name", input_file_name())

# Extract and compare file names
main_files = main_table_df.select("file_name").distinct()
clone_files = shallow_clone_df.select("file_name").distinct()

# Find unmatched files
main_only_files = main_files.subtract(clone_files)
clone_only_files = clone_files.subtract(main_files)

# Show differences
print("Files present in Main Table but not in Shallow Clone:")
main_only_files.show(truncate=False)

print("Files present in Shallow Clone but not in Main Table:")
clone_only_files.show(truncate=False)
