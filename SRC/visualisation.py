import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession

def read_table(spark, table_name):
    """
    Read data from a Spark table.
    """
    return spark.read.table(table_name).toPandas()

def validate_columns(df):
    """
    Validate columns for each unique test_name and return mismatched records.
    """
    mismatches = []
    
    for test_name, group in df.groupby('test_name'):
        for index, row in group.iterrows():
            if row['schema_mismatch'] or row['data_mismatch'] or row['mismatch_count'] > 0 or row['validation_errors'] != 'No errors':
                mismatches.append({
                    'test_name': row['test_name'],
                    'table_a': row['table_a'],
                    'table_b': row['table_b'],
                    'column_name': row['column_name'],
                    'schema_mismatch': row['schema_mismatch'],
                    'data_mismatch': row['data_mismatch'],
                    'mismatch_count': row['mismatch_count'],
                    'validation_errors': row['validation_errors'],
                    'run_date': row['run_date']
                })
    
    return pd.DataFrame(mismatches)

def plot_mismatches(df):
    """
    Plot mismatches as a bar chart and a table.
    """
    if df.empty:
        print("No mismatches found.")
        return

    # Bar chart for mismatch count per test_name
    counts = df['test_name'].value_counts().reset_index(name='count')
    plt.figure(figsize=(10, 6))
    plt.bar(counts['index'], counts['count'], color='skyblue')
    plt.xlabel('Test Name')
    plt.ylabel('Mismatch Count')
    plt.title('Mismatch Count by Test Name')
    plt.xticks(rotation=45)
    plt.show()

    # Table for detailed mismatches
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.axis('tight')
    ax.axis('off')
    table = ax.table(cellText=df.values, colLabels=df.columns, cellLoc='center', loc='center')
    plt.title('Detailed Mismatches')
    plt.show()

def main():
    """
    Main function to read a table, validate columns, and create visualizations.
    """
    # Use the existing Spark session (Databricks automatically provides a Spark session)
    spark = SparkSession.builder.getOrCreate()

    table_name = 'spart_ab_audit_test'  # Adjust this to the actual table name
    df = read_table(spark, table_name)

    # Print columns for debugging
    print("DataFrame Columns:", df.columns)

    # Validate columns and get mismatches
    mismatches_df = validate_columns(df)

    # Plot mismatches
    plot_mismatches(mismatches_df)

if __name__ == "__main__":
    main()
