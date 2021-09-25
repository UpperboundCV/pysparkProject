from pyspark.sql import SparkSession


class SparkIngestion:
    def __init__(self, spark_session: SparkSession) -> None:
        self.spark_session = spark_session

    def get_spark_version(self) -> str:
        return self.spark_session.version
