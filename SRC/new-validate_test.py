import pytest
from unittest.mock import patch, MagicMock
from sparta import ABTestDeltaTables  # Assuming the class is in sparta.py

@pytest.fixture
def mock_spark():
    """Fixture to provide a mock SparkSession with simplified mocks."""
    mock_spark = MagicMock()
    mock_spark.read.format.return_value.table.return_value = MagicMock(columns=['col1', 'col2'])
    mock_spark.sql.return_value = MagicMock()
    mock_spark.createDataFrame.return_value = MagicMock()
    return mock_spark

@patch('sparta.ABTestDeltaTables.rename_columns', side_effect=lambda df, suffix: df)
@patch('sparta.ABTestDeltaTables.create_join_condition', return_value="join_condition")
@patch('sparta.ABTestDeltaTables.construct_comparison_query', return_value="comparison_query")
@patch('sparta.ABTestDeltaTables.get_schema_from_table', return_value="schema")
@patch('sparta.ABTestDeltaTables.prepare_results', return_value="results")
@patch('sparta.ABTestDeltaTables.write_results')
def test_validate_data(mock_write_results, mock_prepare_results, mock_get_schema_from_table, mock_construct_comparison_query, mock_create_join_condition, mock_rename_columns, mock_spark):
    """Test the validate_data method with simplified mocks."""
    ab_test = ABTestDeltaTables(mock_spark)
    ab_test.validate_data("before_table", "after_table")

    # Assertions (simplified)
    mock_spark.read.format.assert_called_with("delta")
    mock_spark.read.format().table.assert_any_call("before_table")
    mock_spark.read.format().table.assert_any_call("after_table")
    mock_spark.sql.assert_called_once_with("comparison_query")
    mock_get_schema_from_table.assert_called_once_with("before_table.sparta_audit_result")
    mock_spark.createDataFrame.assert_called_once_with("results", "schema")
    mock_write_results.assert_called_once()
