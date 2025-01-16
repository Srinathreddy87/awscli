import pandas as pd
import json
import sqlparse
from typing import List, Any, Dict

# 1. MockRow Class
class MockRow:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def asDict(self) -> Dict[str, Any]:
        return self.__dict__

# 2. MockDataFrame Class
class MockDataFrame:
    def __init__(self, data: pd.DataFrame):
        self.data = data

    def columns(self) -> List[str]:
        return self.data.columns.tolist()

    def schema(self) -> pd.Series:
        return self.data.dtypes

    def display(self, n: int = 5):
        print(self.data.head(n))

    def filter(self, condition: str) -> 'MockDataFrame':
        return MockDataFrame(self.data.query(condition))

    def select(self, *cols: List[str]) -> 'MockDataFrame':
        return MockDataFrame(self.data[list(cols)])

    def collect(self) -> List[MockRow]:
        return [MockRow(**row) for row in self.data.to_dict(orient='records')]

    def head(self, n: int = 1) -> List[Dict[str, Any]]:
        return self.data.head(n).to_dict(orient='records')

    def first(self) -> Dict[str, Any]:
        return self.head(1)[0]

    def isEmpty(self) -> bool:
        return self.data.empty

# 3. MockSparkSession Class
class MockSparkSession:
    def __init__(self):
        self.tables = {}

    def read_json(self, path: str) -> MockDataFrame:
        if path not in self.tables:
            self.tables[path] = pd.read_json(path)
        return MockDataFrame(self.tables[path])

    def createDataFrame(self, data: List[Dict[str, Any]], schema: List[str]) -> MockDataFrame:
        df = pd.DataFrame(data, columns=schema)
        return MockDataFrame(df)

    def sql(self, query: str) -> MockDataFrame:
        parsed = sqlparse.parse(query)[0]
        tokens = [token for token in parsed.tokens if not token.is_whitespace]
        if len(tokens) < 4 or tokens[0].value.upper() != 'SELECT' or tokens[2].value.upper() != 'FROM':
            raise ValueError("Only basic SELECT queries are supported")
        
        columns = tokens[1].value.split(',')
        table_name = tokens[3].value
        
        if table_name in self.tables:
            df = self.tables[table_name]
            if '*' in columns:
                return MockDataFrame(df)
            else:
                return MockDataFrame(df[columns])
        else:
            raise ValueError(f"Table {table_name} not loaded")

# Usage example
if __name__ == "__main__":
    # Create a mock Spark session
    spark = MockSparkSession()

    # Create a DataFrame from JSON
    json_data = '[{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]'
    with open('data.json', 'w') as f:
        f.write(json_data)

    df = spark.read_json('data.json')

    # Display the first 5 rows
    df.display(5)

    # Filter rows where age is greater than 25
    filtered_df = df.filter('age > 25')
    filtered_df.display(5)

    # Select specific columns
    selected_df = df.select('name', 'age')
    selected_df.display(5)

    # Collect rows
    rows = df.collect()
    for row in rows:
        print(row.asDict())

    # Check if the DataFrame is empty
    print("Is DataFrame empty?", df.isEmpty())

    # Execute SQL query
    df = spark.createDataFrame([{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}], ["name", "age"])
    spark.tables['people'] = df.data
    sql_df = spark.sql("SELECT name FROM people")
    sql_df.display()
