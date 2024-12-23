"""
This module contains the ABTestDeltaTables class which is used to perform A/B testing
on Delta tables in Databricks. It includes functionalities for schema comparison
and row-by-row data validation.
"""

import logging
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

# Set up a logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

class ABTestDeltaTables:
    """
    A class to perform A/B testing on Delta tables in Databricks.
    """

    def __init__(self, table_a, table_b, result_table, key_columns):
        """
        Initialize the A/B test with Delta table names.

        :param table_a: Name of the first Delta table (A variant)
        :param table_b: Name of the second Delta table (B variant)
        :param result_table: Name of the result Delta table to store comparison results
        :param key_columns: List of key column names used for joining the tables
        """
        self.spark = spark  # Use the existing Spark session in Databricks
        self.table_a = table_a
        self.table_b = table_b
        self.result_table = result_table
        self.key_columns = key_columns

    def compare_schemas(self):
        """
        Compare the schemas of the two Delta tables.
        """
        schema_a = self.spark.read.format("delta").table(self.table_a).schema
        schema_b = self.spark.read.format("delta").table(self.table_b).schema
        diff = set(schema_a) ^ set(schema_b)
        if not diff:
            logger.info("Schemas are identical.")
        else:
            logger.info("Schemas differ: %s", diff)

    def validate_data(self):
        """
        Validate data row-by-row and store results in the result Delta table.
        """
        df_a = self.spark.read.format("delta").table(self.table_a)
        df_b = self.spark.read.format("delta").table(self.table_b)

        # Rename columns in df_a and df_b to avoid conflicts
        for col in self.key_columns:
            df_a = df_a.withColumnRenamed(col, f"{col}_a")
            df_b = df_b.withColumnRenamed(col, f"{col}_b")

        # Create join condition based on key columns
        join_condition = [
            df_a[f"{col}_a"] == df_b[f"{col}_b"] for col in self.key_columns
        ]

        joined_df = df_a.join(df_b, join_condition, "outer").select(df_a["*"], df_b["*"])
        joined_df.createOrReplaceTempView("joined_view")

        result_df = self.spark.sql(
            """
            SELECT *,
            CASE
                WHEN {} THEN 'match'
                ELSE 'mismatch'
            END AS validation_result
            FROM joined_view
            """.format(" AND ".join(
                [
                    f"{col}_a IS NOT NULL AND {col}_b IS NOT NULL "
                    f"AND {col}_a = {col}_b" for col in self.key_columns
                ]
            ))
        )
        result_df.write.format("delta").mode("overwrite").saveAsTable(self.result_table)
        logger.info(
            "Data validation complete. Results stored in table: %s", self.result_table
        )


if __name__ == "__main__":
    ab_test = ABTestDeltaTables(
        "table_a",
        "table_b",
        "result_table",
        ["key_column1", "key_column2"]
    )
    ab_test.compare_schemas()
    ab_test.validate_data()
