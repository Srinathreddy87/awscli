# Databricks notebook source

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Initialize Spark session
spark = SparkSession.builder.appName("ABTestReport").getOrCreate()

# Define the path to the Delta table (adjust this path based on your setup)
delta_table_path = "mocks/ab_final_result_schema.json"

# Read the Delta table into a DataFrame
df = spark.read.format("delta").load(delta_table_path)

# Convert the Spark DataFrame to a Pandas DataFrame for easier plotting
pdf = df.toPandas()

# Display the first few rows of the DataFrame
display(pdf)

# Function to plot the mismatch count for each test
def plot_mismatch_count(pdf):
    plt.figure(figsize=(10, 6))
    sns.barplot(x="column_name", y="mismatch_count", hue="test_name", data=pdf)
    plt.title("Mismatch Count for Each Test")
    plt.xlabel("Column Name")
    plt.ylabel("Mismatch Count")
    plt.xticks(rotation=45)
    plt.legend(title="Test Name")
    plt.show()

# Function to plot the data mismatch percentage for each test
def plot_data_mismatch_percentage(pdf):
    pdf['data_mismatch_percentage'] = (pdf['data_mismatch'].astype(int) / pdf['mismatch_count']) * 100
    plt.figure(figsize=(10, 6))
    sns.barplot(x="column_name", y="data_mismatch_percentage", hue="test_name", data=pdf)
    plt.title("Data Mismatch Percentage for Each Test")
    plt.xlabel("Column Name")
    plt.ylabel("Data Mismatch Percentage")
    plt.xticks(rotation=45)
    plt.legend(title="Test Name")
    plt.show()

# Plot mismatch count
plot_mismatch_count(pdf)

# Plot data mismatch percentage
plot_data_mismatch_percentage(pdf)
