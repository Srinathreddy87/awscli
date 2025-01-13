import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession

def read_table(spark, table_name):
    """
    Read data from a Spark table.
    """
    return spark.read.table(table_name).toPandas()

def plot_bar_chart(df):
    """
    Plot a bar chart of unique records grouped by test_name.
    """
    grouped_df = df.groupby('test_name').size().reset_index(name='counts')
    plt.figure(figsize=(10, 6))
    plt.bar(grouped_df['test_name'], grouped_df['counts'], color='skyblue')
    plt.xlabel('Test Name')
    plt.ylabel('Counts')
    plt.title('Bar Chart of Unique Records by Test Name')
    plt.xticks(rotation=45)
    plt.show()

def plot_table(df):
    """
    Plot a DataFrame as a table.
    """
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.axis('tight')
    ax.axis('off')
    table = ax.table(cellText=df.values, colLabels=df.columns, cellLoc='center', loc='center')
    plt.title('Table of Unique Records by Test Name')
    plt.show()

def main():
    """
    Main function to read a table and create visualizations.
    """
    # Use the existing Spark session (Databricks automatically provides a Spark session)
    spark = SparkSession.builder.getOrCreate()

    table_name = 'spart_ab_audit_test'  # Adjust this to the actual table name
    df = read_table(spark, table_name)

    # Print columns for debugging
    print("DataFrame Columns:", df.columns)

    # Group the DataFrame by test_name and get unique records
    unique_records_df = df.drop_duplicates(subset=['test_name'])

    # Plot bar chart of unique records grouped by test_name
    plot_bar_chart(unique_records_df)

    # Plot the DataFrame as a table
    plot_table(unique_records_df)

if __name__ == "__main__":
    main()
