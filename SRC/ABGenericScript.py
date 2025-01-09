"""
This module contains the ABTestDeltaTables class which is used to perform
A/B testing on Delta tables in Databricks. It includes functionalities for
schema comparison and row-by-row data validation.
"""

import logging
from dataclasses import dataclass
from typing import List
import src.domain_reference as dr
from SRC.logging_setup import get_logger

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
        key_columns (List[str]): List of key column names used for 
                                 joining the tables.
    """

    table_a: str
    post_fix: str
    result_table: str
    key_columns: List[str]

    def __init__(self, table_a, post_fix, result_table, key_columns):
        self.table_a = table_a
        self.post_fix = post_fix
        self.result_table = result_table
        self.key_columns = key_columns


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

        # Create join condition based on key columns
        join_condition = [
            df_a[f"{col}_a"] == df_b[f"{col}_b"]
            for col in self.config.key_columns
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

        # Write the comparison results to the result table
        try:
            comparison_df.write.format("delta").mode("overwrite")\
                .saveAsTable(self.config.result_table)
            logger.info(
                "Data validation complete. Comparison results stored in table: %s",
                self.config.result_table,
            )
        except Exception as e:
            logger.error("Failed to save comparison results: %s", e)

    def _get_table_config(self, table_a: str):
        """
        Retrieve the table configuration for the specific table.

        :param table_a: The name of the table.
        :return: The config of the table.
        """
        domain_ref = dr.Domainreference(self.spark, self.dbutils)
        table_config = domain_ref.get_table_config(table_a)
        return table_config

    def table_b_table_path(self, table_config, before_table, post_fix):
        """
        Determine table_b path for data comparison.
        """
        stage_path = table_config["stage_table_path"]
        if post_fix == "feature":
            table_b = stage_path.replace("_stage", "")
            return f"{before_table}_{post_fix}"
        else:
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
