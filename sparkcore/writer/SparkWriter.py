import pyspark
from pyspark.sql import SparkSession
import ColumnDescriptor
import TableProperty
from typing import List

class SparkWriter:
    def __init__(self, spark_session: SparkSession) -> None:
        self.spark_session = spark_session

    def get_spark_version(self) -> str:
        return self.spark_session.version

    def table_property(self) -> TableProperty:
