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
---------


WITH conversion_summary AS (
    SELECT
        experiment_id,
        variation,
        COUNT(user_id) AS total_users,
        SUM(conversion) AS total_conversions,
        AVG(time_on_site) AS avg_time_on_site,
        CAST(SUM(conversion) AS FLOAT) / COUNT(user_id) AS conversion_rate
    FROM
        ab_final_result_schema
    GROUP BY
        experiment_id,
        variation
),

daily_summary AS (
    SELECT
        experiment_id,
        variation,
        date,
        COUNT(user_id) AS daily_users,
        SUM(conversion) AS daily_conversions,
        AVG(time_on_site) AS daily_avg_time_on_site,
        CAST(SUM(conversion) AS FLOAT) / COUNT(user_id) AS daily_conversion_rate
    FROM
        ab_final_result_schema
    GROUP BY
        experiment_id,
        variation,
        date
)

SELECT
    cs.experiment_id,
    cs.variation,
    cs.total_users,
    cs.total_conversions,
    cs.avg_time_on_site,
    cs.conversion_rate,
    ds.date,
    ds.daily_users,
    ds.daily_conversions,
    ds.daily_avg_time_on_site,
    ds.daily_conversion_rate
FROM
    conversion_summary cs
    JOIN daily_summary ds ON cs.experiment_id = ds.experiment_id AND cs.variation = ds.variation
ORDER BY
    cs.experiment_id,
    cs.variation,
    ds.date;
