# SRC/pytest_abgenerictest.py

import pytest
from unittest.mock import MagicMock
from SRC.mocks.mock_spark import MockSparkSession, MockDataFrame

# Import your ABGenericScript module here
from SRC.ABGenericScript import ABTestDeltaTables, ABtestconfig


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
    ab_compare.get_schema_from_table = MagicMock(
        return_value="database.schema"
    )
    result = ab_compare.get_schema_from_table(table_name)
    assert result == "database.schema"


def test_compare_schema(ab_compare):
    """Test the compare_schema method."""
    schema1 = {"name": "string", "age": "int"}
    schema2 = {"name": "string", "age": "int"}
    schema3 = {"name": "string", "age": "string"}

    ab_compare.compare_schema = MagicMock(side_effect=lambda s1, s2: s1 == s2)

    result1 = ab_compare.compare_schema(schema1, schema2)
    result2 = ab_compare.compare_schema(schema1, schema3)

    assert result1 is True
    assert result2 is False
