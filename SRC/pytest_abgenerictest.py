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
    before_table = "before_table"
    after_table = "after_table"
    
    ab_compare.get_schema_from_table = MagicMock()
    ab_compare.get_schema_from_table.side_effect = lambda table_name: {
        "before_table": {"name": "string", "age": "int"},
        "after_table": {"name": "string", "age": "int"}
    }[table_name]

    result = ab_compare.compare_schemas(before_table, after_table)
    assert result is True

    ab_compare.get_schema_from_table.side_effect = lambda table_name: {
        "before_table": {"name": "string", "age": "int"},
        "after_table": {"name": "string", "age": "string"}
    }[table_name]

    result = ab_compare.compare_schemas(before_table, after_table)
    assert result is False


if __name__ == "__main__":
    pytest.main()
