from configProvider.TableConfig import TableConfig
from SparkCore import SparkCore
from writer.SparkWriter import SparkWriter
from writer.TableProperty import TableProperty
from sys import platform
import os
if platform == 'linux':
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64/"

if __name__ == '__main__':
    config_path = "../data/config/"
    bluebook_table_config = TableConfig(config_path, 'local', 'bluebook')
    bluebook_table_property = TableProperty(db_name=bluebook_table_config.db_name,
                                            tb_name=bluebook_table_config.tb_name,
                                            table_path=bluebook_table_config.table_path,
                                            fields=bluebook_table_config.fields,
                                            partitions=bluebook_table_config.partitions)
    print(bluebook_table_property.database)
    print(bluebook_table_property.table)
    print(bluebook_table_property.table_path)
    print(bluebook_table_property.create_table_sql(
        table_format=bluebook_table_property.ORC_FORMAT,
        delimitor=None
    ))
    # to be able to write table: D:\winutils\bin\winutils.exe chmod 777 D:\tmp\hive
    # spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    #  FindFileOwnerAndPermission error (1789): The trust relationship between this workstation and the primary domain failed.
    spark_core = SparkCore('local')
    spark_writer = SparkWriter(spark_core.spark_session)
    spark_writer.create_table(bluebook_table_property)
    bluebook_df = spark_core.spark_session.\
        table(f'{bluebook_table_property.database}.{bluebook_table_property.table}')
    bluebook_df.show(truncate=False)
    # mock data to write into table
    bluebook_df.describe().show(truncate=False)
    spark_core.close_session()


