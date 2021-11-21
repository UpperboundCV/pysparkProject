from pyspark.sql import SparkSession
from .TableProperty import TableProperty
import os


class SparkWriter:
    def __init__(self, spark_session: SparkSession) -> None:
        self.spark_session = spark_session

    def get_spark_version(self) -> str:
        return self.spark_session.version

    def does_database_exist(self, database: str):
        db_lst = self.spark_session.sql('show databases').collect()
        if hasattr(db_lst[0],'databaseName'):
            return database in [db.databaseName for db in db_lst]
        else:
            return database in [db.namespace for db in db_lst]

    def does_table_exist(self, database: str, checked_table: str) -> bool:
        # this will assume that database already exists
        # table_lst = self.spark_session.catalog.listTable(dbName=database)
        table_lst = self.spark_session.catalog.listTables(database)
        return checked_table in [table.name for table in table_lst]

    def create_hdfs_path(self, hdfs_path: str) -> None:
        exist_result_stream = os.popen(hdfs_path)
        exist_result = exist_result_stream.read()
        try:
            if exist_result == '':
                os.system(f"hdfs dfs -mkdir {hdfs_path}")
        except Exception as e:
            print(f"cannot create path for table: Unexpected {e}, {type(e)}")
            raise

    def create_table(self, table_property: TableProperty) -> None:
        # check if table exist if exist, then don't create, otherwise create it
        if not (self.does_database_exist(table_property.database)):
           self.spark_session.sql(f"create database if not exists {table_property.database}")
        if not (self.does_table_exist(table_property.database, table_property.table)):
            # create hdfs path for table
            try:
                self.create_hdfs_path(table_property.table_path)
                sql_statement = table_property.create_table_sql(table_property.ORC_FORMAT)
                print(sql_statement)
                self.spark_session.sql(sql_statement)
            except Exception as e:
                print(f"cannot create table: Unexpected {e}, {type(e)}")
                raise
        else:
           pass
