# awscli/SRC/mocks/mock_spark.py
# awscli/SRC/mocks/mock_spark.py

class MockDataFrame:
    def __init__(self, data, columns):
        self.data = data  # List of tuples representing rows
        self._columns = columns  # List of strings representing column names
    
    @property
    def columns(self):
        return self._columns
    
    @columns.setter
    def columns(self, value):
        self._columns = value
    
    def createOrReplaceTempView(self, name):
        """Mock method for createOrReplaceTempView"""
        print(f"Mock createOrReplaceTempView called with name: {name}")
    
    def count(self):
        """Mock method for count"""
        return len(self.data)
    
    def collect(self):
        """Mock method for collect"""
        return self.data
    
    def withColumnRenamed(self, existing, new):
        """Mock method for withColumnRenamed"""
        if existing in self._columns:
            index = self._columns.index(existing)
            self._columns[index] = new
        return self
    
    def _parse_join_clause(self, on):
        """Mock method for parsing join clause"""
        if isinstance(on, str):
            return on
        elif isinstance(on, list):
            return " AND ".join(on)
        else:
            raise ValueError("Unsupported join clause type")
    
    def _apply_join(self, other, on, how):
        """Mock method for applying join"""
        join_clause = self._parse_join_clause(on)
        # Simplified join logic for demonstration purposes
        join_column = join_clause.split(" = ")[0].split(".")[1]
        joined_data = []
        for row1 in self.data:
            for row2 in other.data:
                if row1[self._columns.index(join_column)] == row2[other._columns.index(join_column)]:
                    joined_data.append(row1 + row2)
        return MockDataFrame(joined_data, self._columns + other._columns)

    def join(self, other, on, how="inner"):
        """Mock method for join"""
        return self._apply_join(other, on, how)

class MockRead:
    def format(self, format_type):
        return self

    def table(self, table_name):
        return MockDataFrame([("value1", "value2")], ["key_column1", "key_column2"])


class MockSparkSession:
    def __init__(self):
        self.read = MockRead()

    def createDataFrame(self, data, schema):
        """Mock method for createDataFrame"""
        return MockDataFrame(data, schema)

    def registerDataFrameAsTable(self, df, name):
        """Mock method for registerDataFrameAsTable"""
        df.createOrReplaceTempView(name)
