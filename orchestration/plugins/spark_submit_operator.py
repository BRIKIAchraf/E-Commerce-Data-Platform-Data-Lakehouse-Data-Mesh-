from airflow.models.baseoperator import BaseOperator

class CustomSparkSubmitOperator(BaseOperator):
    def __init__(self, application, spark_master='spark://spark-master:7077', **kwargs):
        super().__init__(**kwargs)
        self.application = application
        self.spark_master = spark_master

    def execute(self, context):
        print(f"Submitting PySpark job: {self.application} to Master: {self.spark_master}")
        return True
