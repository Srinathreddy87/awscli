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
    """
    Main function to read a table and plot data.
    """
    # Use the existing Spark session (Databricks automatically provides a Spark session)
    spark = SparkSession.builder.getOrCreate()

    table_name = 'catalog.schema.table_name'  # Adjust this to the actual table name
    df = read_table(spark, table_name)
    
    # Print columns for debugging
    print("DataFrame Columns:", df.columns)
    
    # Dynamically use the first column from the DataFrame for plotting
    first_column = df.columns[0]
    print(f"Using column '{first_column}' for plotting")
    
    # Plot data using the first column
    plot_data(df, first_column)

if __name__ == "__main__":
    main()
