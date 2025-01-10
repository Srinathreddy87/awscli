from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType

# Define the schema for the A/B test results table
ab_final_result_schema = StructType([
    StructField("test_name", StringType(), True),
    StructField("table_before", StringType(), True),
    StructField("table_after", StringType(), True),
    StructField("column_name", StringType(), True),
    StructField("schema_mismatch", BooleanType(), True),
    StructField("data_mismatch", BooleanType(), True),
    StructField("mismatch_count", IntegerType(), True),
    StructField("validation_errors", StringType(), True)
])
