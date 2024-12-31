from pyspark.sql.functions import col

# Table names
main_table_name = "en_poc_dev.sparta_work.user_data"
shallow_clone_name = "en_poc_dev.sparta_work.user_data_stage"

# Load Delta logs for the main table and shallow clone
main_table_logs = spark.sql(f"DESCRIBE DETAIL {main_table_name}").select("location").collect()[0][0] + "/_delta_log/*.json"
clone_table_logs = spark.sql(f"DESCRIBE DETAIL {shallow_clone_name}").select("location").collect()[0][0] + "/_delta_log/*.json"

main_files = spark.read.json(main_table_logs).filter(col("add").isNotNull()).select(col("add.path").alias("file_path")).distinct()
clone_files = spark.read.json(clone_table_logs).filter(col("add").isNotNull()).select(col("add.path").alias("file_path")).distinct()

# Compare file paths
main_only_files = main_files.subtract(clone_files)
clone_only_files = clone_files.subtract(main_files)

print("Files in Main Table but not in Shallow Clone:")
main_only_files.show(truncate=False)

print("Files in Shallow Clone but not in Main Table:")
clone_only_files.show(truncate=False)



-- Checksum comparison for the entire table
SELECT MD5(CONCAT_WS(',', *)) AS checksum FROM en_poc_dev.sparta_work.user_data;
SELECT MD5(CONCAT_WS(',', *)) AS checksum FROM en_poc_dev.sparta_work.user_data_stage;

-- Grouped checksum (useful for large tables)
SELECT SUM(CRC32(CONCAT_WS(',', *))) AS checksum FROM en_poc_dev.sparta_work.user_data;
SELECT SUM(CRC32(CONCAT_WS(',', *))) AS checksum FROM en_poc_dev.sparta_work.user_data_stage;
