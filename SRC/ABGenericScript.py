"""
This module contains the ABTestDeltaTables class which is used to perform
A/B testing on Delta tables in Databricks. It includes functionalities for
schema comparison and row-by-row data validation.
"""

import logging
from dataclasses import dataclass
from typing import List, Dict
import src.domain_reference as dr
from SRC.logging_setup import get_logger
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, TimestampType

# Set up a logger
logger = get_logger(__name__, "DEBUG")

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
        :param config: An instance of ABTestConfig containing the 
                       configuration.
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
        parts = table_name.split('.')
        if len(parts) != 3:
            raise ValueError("Table name must be in the format 'catalog.schema.table'")
        
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

        # Rename all columns in df_a and df_b to avoid conflicts
        renamed_columns_a = {col: f"{col}_a" for col in df_a.columns}
        renamed_columns_b = {col: f"{col}_b" for col in df_b.columns}

        for col, new_col in renamed_columns_a.items():
            df_a = df_a.withColumnRenamed(col, new_col)
        for col, new_col in renamed_columns_b.items():
            df_b = df_b.withColumnRenamed(col, new_col)

        # Create join condition based on all columns
        join_condition = [
            df_a[f"{col}_a"] == df_b[f"{col}_b"]
            for col in df_a.columns if col.endswith("_a")
        ]

        joined_df = df_a.join(df_b, join_condition, "outer").select(
            df_a["*"], df_b["*"]
        )
        joined_df.createOrReplaceTempView("joined_view")

        # Construct the query for comparison result for each column
        comparison_columns = [
            f"""
            CASE
                WHEN {renamed_columns_a[col]} IS NULL OR 
                     {renamed_columns_b[col]} IS NULL THEN 'unmatch'
                WHEN {renamed_columns_a[col]} = 
                     {renamed_columns_b[col]} THEN 'match'
                ELSE 'unmatch'
            END AS {col.replace('_a', '')}_result
            """
            for col in renamed_columns_a.keys()
        ]

        comparison_query = f"""
        SELECT
            {', '.join(renamed_columns_a.values())},
            {', '.join(renamed_columns_b.values())},
            {', '.join(comparison_columns)},
            CASE
                WHEN {' OR '.join(
                    [f'{col.replace("_a", "")}_result = \'unmatch\''
                     for col in renamed_columns_a.keys()])} 
                     THEN 'unmatch'
                ELSE 'match'
            END AS validation_result
        FROM joined_view
        """

        comparison_df = self.spark.sql(comparison_query)

        # Debugging: Show the schema and first few rows of the DataFrame
        comparison_df.printSchema()
        comparison_df.show(10)

        # Get the schema from the result table
        ab_final_result_schema = self.get_schema_from_table(before_table)

        # Prepare data for ab_final_result table
        results = []
        run_date = datetime.now()  # Current run date and time
        for col in renamed_columns_a.keys():
            mismatch_count = comparison_df.filter(
                comparison_df[f"{col.replace('_a', '')}_result"] == 'unmatch'
            ).count()
            results.append({
                "test_name": "ABTest",  # You can parameterize this
                "table_a": before_table,
                "table_b": after_table,
                "column_name": col.replace('_a', ''),
                "schema_mismatch": col not in renamed_columns_b,
                "data_mismatch": mismatch_count > 0,
                "mismatch_count": mismatch_count,
                "validation_errors": None,  # Add more detailed error messages if needed
                "run_date": run_date  # Add run_date to results
            })

        results_df = self.spark.createDataFrame(results, ab_final_result_schema)

        # Write the comparison results to the ab_final_result table
        try:
            results_df.write.format("delta").mode("append")\
                .saveAsTable("ab_final_result")
            logger.info(
                "Data validation complete. Comparison results stored in table: ab_final_result"
            )
        except Exception as e:
            logger.error("Failed to save comparison results: %s", e)

        # Save the full comparison result DataFrame to the result_table
        try:
            comparison_df.write.format("delta").mode("overwrite")\
                .saveAsTable(self.config.result_table)
            logger.info(
                "Full comparison results stored in table: %s",
                self.config.result_table,
            )
        except Exception as e:
            logger.error("Failed to save full comparison results: %s", e)

    def _get_table_config(self, table_a: str):
        """
        Retrieve the table configuration for the specific table.

        :param table_a: The name of the table.
        :return: The config of the table.
        """
        domain_ref = dr.Domainreference(self.spark, self.dbutils)
        return domain_ref.get_table_config(table_a)

    def table_b_table_path(self, table_config, before_table, post_fix):
        """
        Determine table_b path for data comparison.
        """
        stage_path = table_config["stage_table_path"]
        if post_fix == "feature":
            table_b = stage_path.replace("_stage", "")
            return f"{before_table}_{post_fix}"
        return stage_path

    def _parse_table_settings(self, table):
        """
        Parse table settings and derive before_table and after_table.
        """
        table_a = table["table"]
        post_fix = next(
            (opt["post_fix"] for opt in table.get("options", []) 
             if "post_fix" in opt),
            "",
        )
        # Derive result_table
        result_table = table_a + "_ab_comparison_result"
        self.config.result_table = result_table

        # Get table config
        table_config = self._get_table_config(table_a)

        # Get before_table and after_table
        before_table = table_config.get("table_path")
        after_table = self.table_b_table_path(
            table_config, before_table, post_fix
        )

        return before_table, after_table
