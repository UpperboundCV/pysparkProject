from pyspark.sql import SparkSession
import TableProperty


class SparkWriter:
    def __init__(self, spark_session: SparkSession) -> None:
        self.spark_session = spark_session

    def get_spark_version(self) -> str:
        return self.spark_session.version

    def does_table_exist(self, database: str, table: str) -> bool:
        # this will assume that database already exists
        # table_lst = self.spark_session.catalog.listTable(dbName=database)
        # if table_lst
        pass

    def create_table(self) -> None:
        #check if table exist if exist, then don't create, otherwise create it
        pass
