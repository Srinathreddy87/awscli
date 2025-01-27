"""
This module contains the ABTestDeltaTables class which is used to perform
A/B testing on Delta tables in Databricks. It includes functionalities for
schema comparison and row-by-row data validation.
"""

import logging
import traceback
from dataclasses import dataclass
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

# Set up a logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

@dataclass
class ABTestConfig:
    """
    Dataclass for A/B test configuration.

    Attributes:
        table_a (str): Name of the first Delta table (A variant).
        post_fix (str): Postfix for the table name.
        result_table (str): Name of the result Delta table to store
                            comparison results.
    """
    table_a: str
    post_fix: str
    result_table: str

    def __init__(self, table_a, post_fix, result_table):
        self.table_a = table_a
        self.post_fix = post_fix
        self.result_table = result_table

class ABTestDeltaTables:
    """
    A class to perform A/B testing on Delta tables in Databricks.
    """
    def __init__(self, spark, dbutils, config: ABTestConfig):
        """
        Initialize the A/B test with Delta table names.

        :param spark: Spark session.
        :param dbutils: Databricks utilities.
        :param config: An instance of ABTestConfig containing the configuration.
        """
        self.spark = spark
        self.dbutils = dbutils
        self.config = config

    def get_schema_from_table(self, table_name: str) -> StructType:
        """
        Retrieve the schema from a specified table.

        :param table_name: Name of the table from which to retrieve the schema.
        :return: The schema as a StructType object.
        """
        # Extract catalog and schema names and append with "sparta_audit_result"
        parts = table_name.split(".")
        if len(parts) != 3:
            raise ValueError(
                "Table name must be in the format 'catalog.schema.table'"
            )

        catalog_name, schema_name, _ = parts
        audit_table_name = f"{catalog_name}.{schema_name}.sparta_audit_result"

        # Retrieve the schema from the audit table
        return self.spark.read.table(audit_table_name).schema

    def compare_schemas(self, before_table, after_table):
        """
        Compare the schemas of the two Delta tables.
        """
        schema_a = self.spark.read.format("delta").table(before_table).schema
        schema_b = self.spark.read.format("delta").table(after_table).schema
        diff = set(schema_a) ^ set(schema_b)
        if not diff:
            logger.info("Schemas are identical.")
        else:
            logger.info("Schemas differ: %s", diff)

    def validate_data(self, before_table, after_table):
        """
        Validate data row-by-row and store results in the result Delta table.
        """
        df_a = self.spark.read.format("delta").table(before_table)
        df_b = self.spark.read.format("delta").table(after_table)

        # Rename columns in both DataFrames to avoid conflicts
        df_a = self.rename_columns(df_a, suffix="_a")
        df_b = self.rename_columns(df_b, suffix="_b")

        # Create join condition based on all columns
        join_condition = self.create_join_condition(df_a, df_b)

        joined_df = df_a.join(
            df_b, join_condition, "outer"
        ).select(df_a["*"], df_b["*"])
        joined_df.createOrReplaceTempView("joined_view")

        # Construct the query for comparison result for each column
        comparison_query = self.construct_comparison_query(df_a)

        comparison_df = self.spark.sql(comparison_query)

        # Debugging: Show the schema and first few rows of the DataFrame
        comparison_df.printSchema()
        comparison_df.show(10)

        # Derive the audit table name
        audit_table_name = (
            f"{before_table.rsplit('.', 1)[0]}.sparta_audit_result"
        )

        # Get the schema from the audit table
        ab_final_result_schema = self.get_schema_from_table(audit_table_name)

        # Prepare data for the audit table
        results = self.prepare_results(
            comparison_df, df_a.columns, before_table, after_table
        )

        results_df = self.spark.createDataFrame(results, ab_final_result_schema)

        # Write the comparison results to the audit table
        self.write_results(results_df, audit_table_name)

        # Save the full comparison result DataFrame to the result_table
        self.write_comparison_results(comparison_df)

    def rename_columns(self, df, suffix):
        """
        Rename columns in the DataFrame to avoid conflicts.
        """
        renamed_columns = {col: f"{col}{suffix}" for col in df.columns}
        for col, new_col in renamed_columns.items():
            df = df.withColumnRenamed(col, new_col)
        return df

    def create_join_condition(self, df_a, df_b):
        """
        Create join condition based on all columns.
        """
        return [
            df_a[f"{col}_a"] == df_b[f"{col}_b"]
            for col in df_a.columns
            if col.endswith("_a")
        ]

    def construct_comparison_query(self, df_a):
        """
        Construct the query for comparison result for each column.
        """
        comparison_columns = [
            f"""
            CASE
                WHEN {col}_a IS NULL OR {col}_b IS NULL THEN 'unmatch'
                WHEN {col}_a = {col}_b THEN 'match'
                ELSE 'unmatch'
            END AS {col.replace('_a', '')}_result
            """
            for col in df_a.columns
            if col.endswith("_a")
        ]

        return f"""
        SELECT
            {', '.join(df_a.columns)},
            {', '.join(df_b.columns)},
            {', '.join(comparison_columns)},
            CASE
                WHEN {' OR '.join(
                    [
                        f"{col.replace('_a', '')}_result = 'unmatch'"
                        for col in df_a.columns
                        if col.endswith("_a")
                    ]
                )}
                THEN 'unmatch'
                ELSE 'match'
            END AS validation_result
        FROM joined_view
        """

    def prepare_results(
        self, comparison_df, columns, before_table, after_table
    ):
        """
        Prepare data for the audit table.
        """
        results = []
        run_date = datetime.now()
        for col in columns:
            if col.endswith("_a"):
                mismatch_count = comparison_df.filter(
                    f"{col.replace('_a', '')}_result = 'unmatch'"
                ).count()
                results.append(
                    {
                        "test_name": "ABTest",
                        "table_a": before_table,
                        "table_b": after_table,
                        "column_name": col.replace("_a", ""),
                        "schema_mismatch": col.replace(
                            "_a", ""
                        )
                        not in columns,
                        "data_mismatch": mismatch_count > 0,
                        "mismatch_count": mismatch_count,
                        "validation_errors": None,
                        "run_date": run_date,
                    }
                )
        return results

    def write_results(self, results_df, audit_table_name):
        """
        Write the comparison results to the audit table.
        """
        try:
            results_df.write.format("delta").mode("append").saveAsTable(
                audit_table_name
            )
            logger.info(
                "Data validation complete. Comparison results stored "
                "in table: %s",
                audit_table_name,
            )
        except Exception as e:
            logger.error("Failed to save comparison results: %s", e)
            logger.error(traceback.format_exc())

    def write_comparison_results(self, comparison_df):
        """
        Save the full comparison result DataFrame to the result_table.
        """
        try:
            comparison_df.write.format("delta").mode("overwrite").saveAsTable(
                self.config.result_table
            )
            logger.info(
                "Full comparison results stored in table: %s",
                self.config.result_table,
            )
        except Exception as e:
            logger.error("Failed to save full comparison results: %s", e)
            logger.error(traceback.format_exc())

def update_audit_table(spark, audit_table_name, data, branch_name):
    # Add the branch name as a new column to the DataFrame
    data = data.withColumn("story_name", F.lit(branch_name))
    
    try:
        # Write the updated DataFrame to the audit table
        data.write.format("delta").mode("append").saveAsTable(audit_table_name)
        print(f"Data successfully inserted into {audit_table_name}")
    except Exception as e:
        print(f"Error inserting data into {audit_table_name}: {str(e)}")
        print(traceback.format_exc())

def main():
    # Parameters
    audit_table_name = 'table_audit'
    branch_name = "branch"
    s3_path = "s3://path/story.yml"
    result_table = "result_table"
    table_a = "catalog.schema.table_a"
    post_fix = "_b"

    # Initialize Spark session
    spark = SparkSession.builder.appName("ABGenericScript").getOrCreate()

    # Initialize dbutils (this is required in Databricks)
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)

    # Create ABTestConfig and ABTestDeltaTables instances
    config = ABTestConfig(table_a, post_fix, result_table)
    ab_test = ABTestDeltaTables(spark, dbutils, config)

    try:
        # Perform schema comparison
        table_b = f"{table_a.split('.')[0]}.{table_a.split('.')[1]}{post_fix}"
        ab_test.compare_schemas(table_a, table_b)

        # Perform data validation
        ab_test.validate_data(table_a, table_b)

        # Read the result table
        result_df = spark.read.format("delta").table(result_table)

        # Update the audit table with the A/B test results and the branch name
        update_audit_table(spark, audit_table_name, result_df, branch_name)
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        print(traceback.format_exc())

if __name__ == "__main__":
    main()
