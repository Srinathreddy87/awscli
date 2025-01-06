import logging
import pytest
from pyspark.sql import SparkSession
from SRC.ABGenericScript import ABTestDeltaTables, ABTestConfig


@pytest.fixture(scope="module")
def spark():
    # Initialize a Spark session for testing
    spark = (
        SparkSession.builder.appName("TestABTestDeltaTables")
        .master("local[*]")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def config():
    # Example configuration for testing
    return ABTestConfig(
        table_a="test_table_a",
        table_b="test_table_b",
        result_table="test_result_table",
        key_columns=["key_column1", "key_column2"],
    )


@pytest.fixture
def ab_test(spark, config):
    # Initialize ABTestDeltaTables with the test config
    return ABTestDeltaTables(config)


def test_compare_schemas(spark, ab_test, caplog):
    # Create sample DataFrames for table_a and table_b
    data_a = [("value1", "value2")]
    schema_a = ["key_column1", "key_column2"]
    df_a = spark.createDataFrame(data_a, schema_a)
    df_a.write.format("delta").mode("overwrite").saveAsTable("test_table_a")

    data_b = [("value1", "value2")]
    schema_b = ["key_column1", "key_column2"]
    df_b = spark.createDataFrame(data_b, schema_b)
    df_b.write.format("delta").mode("overwrite").saveAsTable("test_table_b")

    # Compare schemas
    with caplog.at_level(logging.INFO):
        ab_test.compare_schemas()

    # Check if schemas are logged as identical
    assert "Schemas are identical." in caplog.text


def test_validate_data(spark, ab_test):
    # Create sample DataFrames for table_a and table_b
    data_a = [(1, "value1_a", "value2_a")]
    schema_a = ["key_column1", "key_column2", "column_a"]
    df_a = spark.createDataFrame(data_a, schema_a)
    df_a.write.format("delta").mode("overwrite").saveAsTable("test_table_a")

    data_b = [(1, "value1_b", "value2_b")]
    schema_b = ["key_column1", "key_column2", "column_b"]
    df_b = spark.createDataFrame(data_b, schema_b)
    df_b.write.format("delta").mode("overwrite").saveAsTable("test_table_b")

    # Validate data
    ab_test.validate_data()

    # Check if the result table is created
    result_df = spark.read.format("delta").table("test_result_table")
    assert result_df.count() > 0


def test_construct_unmatched_query(ab_test):
    # Construct the unmatched query
    query = ab_test.construct_unmatched_query()
    expected_conditions = (
        "key_column1_a IS NULL OR key_column1_b IS NULL OR "
        "key_column1_a != key_column1_b OR key_column2_a IS NULL OR "
        "key_column2_b IS NULL OR key_column2_a != key_column2_b"
    )
    expected_query = f"""
        SELECT *,
        CASE
            WHEN {expected_conditions} THEN 'unmatched'
            ELSE 'matched'
        END AS validation_result
        FROM joined_view
        WHERE {expected_conditions}
    """
    assert query.strip() == expected_query.strip()


if __name__ == "__main__":
    pytest.main()
