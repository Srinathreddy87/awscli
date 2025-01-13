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
    Plot a bar chart of mismatched records grouped by test_name.
    """
    # Filter for mismatched records
    mismatched_df = df[df['data_mismatch'] == True]
    
    # Group by test_name and count mismatches
    grouped_df = mismatched_df.groupby('test_name').size().reset_index(name='mismatch_count')
    
    plt.figure(figsize=(10, 6))
    plt.bar(grouped_df['test_name'], grouped_df['mismatch_count'], color='skyblue')
    plt.xlabel('Test Name')
    plt.ylabel('Mismatch Count')
    plt.title('Bar Chart of Data Mismatches by Test Name')
    plt.xticks(rotation=45)
    plt.show()

def plot_table(df):
    """
    Plot a DataFrame as a table.
    """
    # Filter for mismatched records
    mismatched_df = df[df['data_mismatch'] == True]
    
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.axis('tight')
    ax.axis('off')
    table = ax.table(cellText=mismatched_df.values, colLabels=mismatched_df.columns, cellLoc='center', loc='center')
    plt.title('Table of Data Mismatches by Test Name')
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

    # Plot bar chart of mismatched records grouped by test_name
    plot_bar_chart(df)

    # Plot the DataFrame as a table
    plot_table(df)

if __name__ == "__main__":
    main()
