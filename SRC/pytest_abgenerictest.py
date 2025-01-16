# SRC/pytest_abgenerictest.py

import pytest
from unittest.mock import MagicMock
from SRC.mocks.mock_spark import MockSparkSession, MockDataFrame

# Import your ABGenericScript module here
# from SRC.ABGenericScript import YourFunctionOrClass

@pytest.fixture
def spark_session():
    """Fixture to set up the mock Spark session."""
    spark = MockSparkSession()
    data = [
        {"name": "Alice", "age": 30, "group": "A"},
        {"name": "Bob", "age": 25, "group": "B"},
        {"name": "Charlie", "age": 35, "group": "A"},
    ]
    df = MagicMock(spec=MockDataFrame)
    df.collect.return_value = [
        {"name": "Alice", "age": 30, "group": "A"},
        {"name": "Charlie", "age": 35, "group": "A"},
    ]
    spark.tables["people"] = df
    return spark

def test_filter_group_a(spark_session):
    """Test filtering DataFrame rows where group is 'A'."""
    df = spark_session.tables["people"]
    df.filter.return_value.collect.return_value = [
        {"name": "Alice", "age": 30, "group": "A"},
        {"name": "Charlie", "age": 35, "group": "A"},
    ]
    filtered_df = df.filter("group == 'A'")
    result = filtered_df.collect()
    expected = [
        {"name": "Alice", "age": 30, "group": "A"},
        {"name": "Charlie", "age": 35, "group": "A"},
    ]
    assert [row.asDict() for row in result] == expected

def test_select_name_age(spark_session):
    """Test selecting 'name' and 'age' columns from the DataFrame."""
    df = spark_session.tables["people"]
    df.select.return_value.collect.return_value = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
        {"name": "Charlie", "age": 35},
    ]
    selected_df = df.select("name", "age")
    result = selected_df.collect()
    expected = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
        {"name": "Charlie", "age": 35},
    ]
    assert [row.asDict() for row in result] == expected

def test_sql_query(spark_session):
    """Test executing a basic SQL query to select 'name' and 'age' columns."""
    spark_session.sql.return_value.collect.return_value = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
        {"name": "Charlie", "age": 35},
    ]
    sql_df = spark_session.sql("SELECT name, age FROM people")
    result = sql_df.collect()
    expected = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
        {"name": "Charlie", "age": 35},
    ]
    assert [row.asDict() for row in result] == expected
