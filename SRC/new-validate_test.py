import pytest
from unittest.mock import patch, MagicMock
from sparta import ABTestDeltaTables  # Assuming the class is in sparta.py

@pytest.fixture
def mock_spark():
    """Fixture to provide a mock SparkSession with simplified mocks."""
    mock_spark = MagicMock()
    mock_spark.read.format.return_value.table.return_value = MagicMock()
    mock_spark.read.table.return_value = MagicMock(schema="mock_schema")
    return mock_spark

@patch('sparta.ABTestDeltaTables.spark', new_callable=mock_spark)
def test_get_schema_from_table(mock_spark):
    """Test the get_schema_from_table method."""
    ab_test = ABTestDeltaTables(mock_spark)

    # Test with valid table name
    schema = ab_test.get_schema_from_table("catalog.schema.table")
    assert schema == "mock_schema"
    mock_spark.read.table.assert_called_once_with("catalog.schema.sparta_audit_result")

    # Test with invalid table name
    with pytest.raises(ValueError) as exc_info:
        ab_test.get_schema_from_table("invalid_table_name")
    assert str(exc_info.value) == "Table name must be in the format 'catalog.schema.table'"

@patch('sparta.ABTestDeltaTables.spark', new_callable=mock_spark)
def test_compare_schemas(mock_spark):
    """Test the compare_schemas method."""
    ab_test = ABTestDeltaTables(mock_spark)

    # Test with identical schemas
    mock_spark.read.format.return_value.table.return_value.schema = {1, 2, 3}
    ab_test.compare_schemas("before_table", "after_table")
    # Assert logging message (you'll need to capture logs for this)

    # Test with different schemas
    mock_spark.read.format.return_value.table.side_effect = [
        MagicMock(schema={1, 2, 3}),
        MagicMock(schema={1, 2, 4})
    ]
    ab_test.compare_schemas("before_table", "after_table")
    # Assert logging message (you'll need to capture logs for this)
