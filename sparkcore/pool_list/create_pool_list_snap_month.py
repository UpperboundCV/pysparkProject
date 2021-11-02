import os
import sys
import argparse
sys.path.append('/home/up_python/PycharmProjects/pysparkProject/sparkcore')
os.environ['PYTHONPATH'] = '/home/up_python/PycharmProjects/pysparkProject/sparkcore'
from reader.SparkReader import SparkReader
from SparkCore import SparkCore
from configProvider.TableConfig import TableConfig
from writer.TableProperty import TableProperty
from writer.SparkWriter import SparkWriter
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType, IntegerType, IntegralType, \
    DoubleType
from pyspark.sql.functions import lit, col
from typing import List
from sys import platform

if platform == 'linux':
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64/"

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-e", "--env", required=True, help="environment: local, dev, or prod")
    args = vars(ap.parse_args())

    config_path = "/config/"
    try:
        spark_core = SparkCore(args['env'])
        snap_monthly_table_config = TableConfig(config_path, args['env'], 'pool_list_curate')
        snap_monthly_table_property = TableProperty(db_name=snap_monthly_table_config.db_name,
                                                    tb_name=snap_monthly_table_config.tb_name,
                                                    table_path=snap_monthly_table_config.table_path,
                                                    fields=snap_monthly_table_config.fields,
                                                    partitions=snap_monthly_table_config.partitions)
        print(snap_monthly_table_property.database)
        print(snap_monthly_table_property.table)
        print(snap_monthly_table_property.table_path)
        print(snap_monthly_table_property.create_table_sql(
            table_format=snap_monthly_table_property.ORC_FORMAT,
            delimitor=None
        ))
        snap_monthly_writer = SparkWriter(spark_core.spark_session)
        snap_monthly_writer.create_table(snap_monthly_table_property)
        empty_snap_monthly_df = spark_core.spark_session \
            .table(f"{snap_monthly_table_property.database}.{snap_monthly_table_property.table}")
        empty_snap_monthly_df.show(truncate=False)
    except Exception as e:
        raise TypeError(f"error to create snap month: {e}")
