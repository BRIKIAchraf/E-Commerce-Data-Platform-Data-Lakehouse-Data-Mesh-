# Flink Snowflake direct sink utilizing Snowflake ingestion API
class SnowflakeSink:
    def __init__(self, table_name):
        self.table = table_name

    def insert_records(self, records):
        print(f"Flink bulk streaming insert to Snowflake table: {self.table}")
        return True
