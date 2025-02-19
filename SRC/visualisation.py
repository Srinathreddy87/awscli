"""
This module reads data from a Delta table, identifies mismatches,
and creates visualizations for schema and data issues.
"""

import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession


def read_table(spark, table_name):
    """
    Read data from a Delta table.

    Parameters:
    spark (SparkSession): The Spark session.
    table_name (str): The name of the table to read.

    Returns:
    DataFrame: The Pandas DataFrame containing the table data.
    """
    return spark.read.format("delta").table(table_name).toPandas()


def identify_mismatches(df):
    """
    Identify mismatches for each unique test_name and return
    mismatched records.

    Parameters:
    df (DataFrame): The Pandas DataFrame containing the table data.

    Returns:
    DataFrame: The Pandas DataFrame containing mismatched records.
    """
    mismatches = []

    for test_name, group in df.groupby("test_name"):
        for index, row in group.iterrows():
            if (
                row["schema_mismatch"]
                or row["data_mismatch"]
                or row["mismatch_count"] > 0
                or row["validation_errors"] != "No errors"
            ):
                mismatches.append(
                    {
                        "test_name": row["test_name"],
                        "table_a": row["table_a"],
                        "table_b": row["table_b"],
                        "column_name": row["column_name"],
                        "schema_mismatch": row["schema_mismatch"],
                        "data_mismatch": row["data_mismatch"],
                        "mismatch_count": row["mismatch_count"],
                        "validation_errors": row["validation_errors"],
                        "run_date": row["run_date"],
                    }
                )

    return pd.DataFrame(mismatches)


def plot_mismatches(df):
    """
    Plot mismatches as bar charts and tables for schema and data issues.

    Parameters:
    df (DataFrame): The Pandas DataFrame containing mismatched records.
    """
    if df.empty:
        print("No mismatches found.")
        return

    # Separate schema mismatches and data mismatches
    schema_mismatches = df[df["schema_mismatch"]]
    data_mismatches = df[df["data_mismatch"]]

    # Plot schema mismatches
    if not schema_mismatches.empty:
        schema_counts = (
            schema_mismatches.groupby("test_name")[["table_a", "table_b"]]
            .first()
            .reset_index()
        )
        schema_counts["tables"] = (
            schema_counts["table_a"] + " / " + schema_counts["table_b"]
        )

        plt.figure(figsize=(10, 6))
        plt.bar(
            schema_counts["test_name"], schema_counts["tables"], color="orange"
        )
        plt.xlabel("Test Name")
        plt.ylabel("Tables")
        plt.title("Schema Mismatch Tables by Test Name")
        plt.xticks(rotation=45)
        plt.show()

        # Table for detailed schema mismatches
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.axis("tight")
        ax.axis("off")
        table = ax.table(
            cellText=schema_mismatches.values,
            colLabels=schema_mismatches.columns,
            cellLoc="center",
            loc="center",
        )
        plt.title("Detailed Schema Mismatches")
        plt.show()

    # Plot data mismatches
    if not data_mismatches.empty:
        data_counts = (
            data_mismatches.groupby("test_name")[["table_a", "table_b"]]
            .first()
            .reset_index()
        )
        data_counts["tables"] = (
            data_counts["table_a"] + " / " + data_counts["table_b"]
        )

        plt.figure(figsize=(10, 6))
        plt.bar(data_counts["test_name"], data_counts["tables"], color="red")
        plt.xlabel("Test Name")
        plt.ylabel("Tables")
        plt.title("Data Mismatch Tables by Test Name")
        plt.xticks(rotation=45)
        plt.show()

        # Table for detailed data mismatches
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.axis("tight")
        ax.axis("off")
        table = ax.table(
            cellText=data_mismatches.values,
            colLabels=data_mismatches.columns,
            cellLoc="center",
            loc="center",
        )
        plt.title("Detailed Data Mismatches")
        plt.show()


# Use the existing Spark session (Databricks automatically provides a Spark session)
spark = SparkSession.builder.getOrCreate()

table_name = "spart_ab_audit_test"  # Adjust this to the actual table name
df = read_table(spark, table_name)

# Print columns for debugging
print("DataFrame Columns:", df.columns)

# Identify mismatches and get mismatched records
mismatches_df = identify_mismatches(df)

# Plot mismatches
plot_mismatches(mismatches_df)
