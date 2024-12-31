from pyspark.sql.functions import col

# Load Delta tables
main_table = "path_to_main_table"
shallow_clone_table = "path_to_shallow_clone_table"

# Extract data files from Delta logs
main_table_files = spark.read.format("delta").load(main_table)._jdf.files().get()
shallow_clone_files = spark.read.format("delta").load(shallow_clone_table)._jdf.files().get()

# Compare files
main_files_df = spark.createDataFrame(main_table_files).select("path")
shallow_clone_files_df = spark.createDataFrame(shallow_clone_files).select("path")

unmatched_files = main_files_df.subtract(shallow_clone_files_df)
unmatched_files.show()
