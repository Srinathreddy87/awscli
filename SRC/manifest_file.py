from delta.tables import DeltaTable
from pyspark.sql.functions import col

# Table names
main_table_name = "main_table_name"
shallow_clone_table_name = "shallow_clone_table_name"

# Load Delta tables
main_table = DeltaTable.forName(spark, main_table_name)
shallow_clone_table = DeltaTable.forName(spark, shallow_clone_table_name)

# Extract data files for the main table and shallow clone
main_files_df = main_table.toDF().select(input_file_name().alias("file_path")).distinct()
clone_files_df = shallow_clone_table.toDF().select(input_file_name().alias("file_path")).distinct()

# Compare file paths
main_only_files = main_files_df.subtract(clone_files_df)
clone_only_files = clone_files_df.subtract(main_files_df)

# Display differences
print("Files present in Main Table but not in Shallow Clone:")
main_only_files.show(truncate=False)

print("Files present in Shallow Clone but not in Main Table:")
clone_only_files.show(truncate=False)
