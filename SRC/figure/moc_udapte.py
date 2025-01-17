# awscli/SRC/mocks/mock_spark.py

class MockDataFrame:
    def __init__(self, data, schema):
        self.data = data  # List of tuples representing rows
        self.schema = schema  # List of strings representing column names
        self._columns = schema
    
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
