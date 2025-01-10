from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType

# Initialize Spark session
spark = SparkSession.builder.appName("ABTestResults").getOrCreate()

# Define the schema for the A/B test results table
schema = StructType([
    StructField("test_name", StringType(), True),
    StructField("table_a", StringType(), True),
    StructField("table_b", StringType(), True),
    StructField("column_name", StringType(), True),
    StructField("schema_mismatch", BooleanType(), True),
    StructField("data_mismatch", BooleanType(), True),
    StructField("mismatch_count", IntegerType(), True),
    StructField("validation_errors", StringType(), True)
])

# Create an empty DataFrame with the defined schema
ab_test_results_df = spark.createDataFrame([], schema)

# Display the schema of the DataFrame
ab_test_results_df.printSchema()

# Save the DataFrame as a Delta table
ab_test_results_df.write.format("delta").mode("overwrite").saveAsTable("ab_test_results")

print("A/B test results table created successfully.")
