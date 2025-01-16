# SRC/pytest_abgenerictest.py

import unittest
from mock_spark import MockSparkSession, MockDataFrame

# Import your ABGenericScript module here
# from SRC.ABGenericScript import YourFunctionOrClass


class TestABGenericScript(unittest.TestCase):
    """Unit test class for testing ABGenericScript functionalities."""

    def setUp(self):
        """Set up the mock Spark session and mock DataFrame for testing."""
        self.spark = MockSparkSession()

        # Create a mock DataFrame
        data = [
            {"name": "Alice", "age": 30, "group": "A"},
            {"name": "Bob", "age": 25, "group": "B"},
            {"name": "Charlie", "age": 35, "group": "A"},
        ]
        self.df = self.spark.createDataFrame(data, ["name", "age", "group"])
        self.spark.tables["people"] = self.df.data

    def test_filter_group_a(self):
        """Test filtering DataFrame rows where group is 'A'."""
        filtered_df = self.df.filter("group == 'A'")
        result = filtered_df.collect()
        expected = [
            {"name": "Alice", "age": 30, "group": "A"},
            {"name": "Charlie", "age": 35, "group": "A"},
        ]
        self.assertEqual([row.asDict() for row in result], expected)

    def test_select_name_age(self):
        """Test selecting 'name' and 'age' columns from the DataFrame."""
        selected_df = self.df.select("name", "age")
        result = selected_df.collect()
        expected = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35},
        ]
        self.assertEqual([row.asDict() for row in result], expected)

    def test_sql_query(self):
        """Test executing a basic SQL query to select 'name' and 'age' columns."""
        sql_df = self.spark.sql("SELECT name, age FROM people")
        result = sql_df.collect()
        expected = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
            {"name": "Charlie", "age": 35},
        ]
        self.assertEqual([row.asDict() for row in result], expected)


if __name__ == "__main__":
    unittest.main()
