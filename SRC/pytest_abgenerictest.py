import pytest
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    IntegerType,
    TimestampType,
)
from SRC.ABGenericScript import ABTestDeltaTables, ABTestConfig
from unittest.mock import MagicMock
from mocks.mock_spark import mock_spark_session


@pytest.fixture(scope="module")
def spark():
    """
    Fixture for creating a Spark session using the mock spark session.
    """
    return mock_spark_session


@pytest.fixture
def ab_test_config():
    """
    Fixture for creating the ABTestConfig object.
    """
    return ABTestConfig(
        table_a="catalog.schema.table_a",
        post_fix="feature",
        result_table="catalog.schema.ab_test_result",
    )


@pytest.fixture
def ab_test(spark, ab_test_config):
    """
    Fixture for creating the ABTestDeltaTables object.
    """
    dbutils = MagicMock()
    return ABTestDeltaTables(spark, dbutils, ab_test_config)


def test_get_schema_from_table(spark, ab_test):
    """
    Test the get_schema_from_table method.
    """
    table_name = "catalog.schema.table"  # Adjust this to a valid table name
    schema = ab_test.get_schema_from_table(table_name)
    assert isinstance(schema, StructType)


def test_compare_schemas(spark, ab_test):
    """
    Test the compare_schemas method.
    """
    ab_test.compare_schemas("catalog.schema.table_a", "catalog.schema.table_b")


def test_validate_data(spark, ab_test):
    """
    Test the validate_data method.
    """
    ab_test.validate_data("catalog.schema.before_table", "catalog.schema.after_table")


def test_rename_columns(spark, ab_test):
    """
    Test the rename_columns method.
    """
    schema = StructType([StructField("col1", StringType(), True)])
    df = spark.createDataFrame([("value1",)], schema)
    df_renamed = ab_test.rename_columns(df, "_suffix")
    assert "col1_suffix" in df_renamed.columns


def test_create_join_condition(spark, ab_test):
    """
    Test the create_join_condition method.
    """
    schema = StructType([StructField("col1_a", StringType(), True)])
    df_a = spark.createDataFrame([("value1",)], schema)
    df_b = spark.createDataFrame([("value1",)], schema)
    join_condition = ab_test.create_join_condition(df_a, df_b)
    assert len(join_condition) == 1


def test_construct_comparison_query(spark, ab_test):
    """
    Test the construct_comparison_query method.
    """
    schema = StructType([StructField("col1_a", StringType(), True)])
    df_a = spark.createDataFrame([("value1",)], schema)
    df_b = spark.createDataFrame([("value1",)], schema)
    query = ab_test.construct_comparison_query(df_a, df_b)
    assert "SELECT" in query


def test_prepare_results(spark, ab_test):
    """
    Test the prepare_results method.
    """
    schema = StructType([StructField("col1_a", StringType(), True)])
    comparison_df = spark.createDataFrame([("value1",)], schema)
    results = ab_test.prepare_results(
        comparison_df, ["col1_a"], "catalog.schema.before_table", "catalog.schema.after_table"
    )
    assert len(results) > 0


def test_write_results(spark, ab_test):
    """
    Test the write_results method.
    """
    schema = StructType([StructField("col1", StringType(), True)])
    results_df = spark.createDataFrame([("value1",)], schema)
    ab_test.write_results(results_df, "catalog.schema.audit_table_name")


def test_write_comparison_results(spark, ab_test):
    """
    Test the write_comparison_results method.
    """
    schema = StructType([StructField("col1", StringType(), True)])
    comparison_df = spark.createDataFrame([("value1",)], schema)
    ab_test.write_comparison_results(comparison_df)
