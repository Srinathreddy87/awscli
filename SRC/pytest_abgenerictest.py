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
    return mock_spark_session

@pytest.fixture
def ab_test_config():
    return ABTestConfig(
        table_a="en_poc_dev.sparta_works.sparta_dev",
        post_fix="feature",
        result_table="ab_test_result"
    )

@pytest.fixture
def ab_test(spark, ab_test_config):
    dbutils = MagicMock()
    return ABTestDeltaTables(spark, dbutils, ab_test_config)

def test_get_schema_from_table(spark, ab_test):
    schema = ab_test.get_schema_from_table("en_poc_dev.sparta_works.sparta_dev")
    assert isinstance(schema, StructType)

def test_compare_schemas(spark, ab_test):
    ab_test.compare_schemas("table_a", "table_b")

def test_validate_data(spark, ab_test):
    ab_test.validate_data("before_table", "after_table")

def test_rename_columns(spark, ab_test):
    schema = StructType([StructField("col1", StringType(), True)])
    df = spark.createDataFrame([("value1",)], schema)
    df_renamed = ab_test.rename_columns(df, "_suffix")
    assert "col1_suffix" in df_renamed.columns

def test_create_join_condition(spark, ab_test):
    schema = StructType([StructField("col1_a", StringType(), True)])
    df_a = spark.createDataFrame([("value1",)], schema)
    df_b = spark.createDataFrame([("value1",)], schema)
    join_condition = ab_test.create_join_condition(df_a, df_b)
    assert len(join_condition) == 1

def test_construct_comparison_query(spark, ab_test):
    schema = StructType([StructField("col1_a", StringType(), True)])
    df_a = spark.createDataFrame([("value1",)], schema)
    df_b = spark.createDataFrame([("value1",)], schema)
    query = ab_test.construct_comparison_query(df_a, df_b)
    assert "SELECT" in query

def test_prepare_results(spark, ab_test):
    schema = StructType([StructField("col1_a", StringType(), True)])
    comparison_df = spark.createDataFrame([("value1",)], schema)
    results = ab_test.prepare_results(comparison_df, ["col1_a"], "before_table", "after_table")
    assert len(results) > 0

def test_write_results(spark, ab_test):
    schema = StructType([StructField("col1", StringType(), True)])
    results_df = spark.createDataFrame([("value1",)], schema)
    ab_test.write_results(results_df, "audit_table_name")

def test_write_comparison_results(spark, ab_test):
    schema = StructType([StructField("col1", StringType(), True)])
    comparison_df = spark.createDataFrame([("value1",)], schema)
    ab_test.write_comparison_results(comparison_df)
