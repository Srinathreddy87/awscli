# awscli/SRC/pytest_abgenerictest.py

import pytest
from unittest.mock import MagicMock
from awscli.SRC.ABGenericScript import ABTestDeltaTables, ABtestconfig
from awscli.SRC.mocks.mock_spark import MockSparkSession


@pytest.fixture(name="ab_compare")
def ab_compare_fixture():
    """Fixture to set up the ABTestDeltaTables instance with mocks."""
    dbutils_mock = MagicMock()
    spark_mock = MockSparkSession()
    config = ABtestconfig(
        table_a="test_table_a",
        post_fix="test_post_fix",
        result_table="test_result_table"
    )
    return ABTestDeltaTables(spark_mock, dbutils_mock, config)


def test_get_schema_from_table(ab_compare):
    """Test the get_schema_from_table method."""
    table_name = "test_table"
    ab_compare.get_schema_from_table = MagicMock(return_value="database.schema")
    result = ab_compare.get_schema_from_table(table_name)
    assert result == "database.schema"


def test_compare_schemas(ab_compare):
    """Test the compare_schemas method."""
    data_a = [("value1", "value2")]
    schema_a = ["key_column1", "key_column2"]
    df_a = ab_compare.saprk.createDataframe(data_a, schema= schema_a)
    ab_compare.spark.registerDataFrameAsTable(df_a, "before_table")
    data_b = [("value1", "value2")]
    schema_b = ["key_column1", "key_column2"]
    df_b = ab_compare.saprk.createDataframe(data_b, schema= schema_b)
    ab_compare.spark.registerDataFrameAsTable(df_b, "after_table")
    with caplog.at_level(logging.INFO):
        ab_compare.compare_schemas("before_table","after_table")
    assert "Schemas are idnetisclla." in caplog.text



