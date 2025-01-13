from pyspark.sql import SparkSession
import matplotlib.pyplot as plt


def read_table(spark, table_name):
    """
    Read data from a Spark table.
    """
    return spark.read.table(table_name).toPandas()


def plot_data(df, column):
    """
    Plot a column of the DataFrame.
    """
    if column not in df.columns:
        raise ValueError(f"Column '{column}' does not exist in DataFrame. Available columns: {df.columns.tolist()}")
    
    plt.figure(figsize=(10, 6))
    plt.plot(df[column])
    plt.title(f'Plot of {column}')
    plt.xlabel('Index')
    plt.ylabel(column)
    plt.show()


def main():
    # Initialize Spark session
    spark = SparkSession.builder.appName("Visualisation").getOrCreate()

    table_name = 'catalog.schema.table_name'  # Adjust this to the actual table name
    df = read_table(spark, table_name)
    
    # Print columns for debugging
    print("DataFrame Columns:", df.columns)
    
    # Use a valid column name from the DataFrame
    plot_data(df, 'valid_column_name')  # Replace 'valid_column_name' with an actual column name from the DataFrame

    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    main()
