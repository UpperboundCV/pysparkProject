import pytest
import pyspark.sql
from sys import platform
import os

from sparkcore.TableCreator import TableCreator
from sparkcore.SparkCore import SparkCore
from sparkcore.table_health.TableHealth import TableHealth
from sparkcore.writer.TableProperty import TableProperty
from sparkcore.writer.SparkWriter import SparkWriter
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from sparkcore.ColumnDescriptor import ColumnDescriptor
from pyspark.sql.functions import to_json, count, col, min, max, mean

if platform == 'linux':
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64/"


@pytest.fixture
def spark_session() -> pyspark.sql.SparkSession:
    return SparkCore(mode='local').spark_session


@pytest.fixture
def mock_test_data(spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    date = '2021-09-15'
    # columns = ["start_date", "account_no"]
    account_no = 'account_no'
    schema = StructType([
        StructField('data_date', StringType(), True),
        StructField(account_no, StringType(), True),
        StructField('price', DoubleType(), True),
        StructField('brand', StringType(), True)
    ])
    data = [(date, "a01", 2000.50, 'Mitsu'), (date, "a02", 12345.99, 'Mitsu'), (date, "a03", 10.0, 'Honda')]
    df = spark_session.createDataFrame(data, schema)
    return df


def test_table_health(spark_session: pyspark.sql.SparkSession,
                      mock_test_data: pyspark.sql.dataframe.DataFrame) -> None:
    # create table cl_repo
    db_name = 'kadev_collection'
    tb_name = 'mock_cl'
    table_path = '/tmp/mock_cl'
    fields = [ColumnDescriptor('account_no', 'string', '"none"'),
              ColumnDescriptor('price', 'integer', '"none"'),
              ColumnDescriptor('brand', 'string', '"none"')]
    partitions = [ColumnDescriptor('data_date', 'string', '"none"')]
    table_property = TableProperty(db_name=db_name, tb_name=tb_name, table_path=table_path, fields=fields,
                                   partitions=partitions)
    spark_writer = SparkWriter(spark_session)
    spark_writer.create_table(table_property)
    empty_mock_df = spark_session.table(f'{db_name}.{tb_name}')
    empty_mock_df.show(truncate=False)
    mock_test_data.write.format('orc').partitionBy('data_date').mode('overwrite').saveAsTable(f'{db_name}.{tb_name}')

    mock_df = spark_session.table(f'{db_name}.{tb_name}')
    mock_df.show(truncate=False)
    spark_session.sql(f'describe {db_name}.{tb_name}').show(truncate=False)
    # create health table
    table_health = TableHealth(spark_session=spark_session, source_schema=db_name, source_table_name=tb_name)
    print(table_health.source_partition)
    table_health.create_health_table()

    health_df = spark_session.table(f'{table_health.schema}.{table_health.table_name}_health')
    health_df.show(truncate=False)
    spark_session.sql(f'describe {table_health.schema}.{table_health.table_name}_health').show(truncate=False)

    par_val_dist = [par[table_health.source_partition] for par in
                    mock_df.select(table_health.source_partition).distinct().collect()]
    # print(par_val_dist)
    print(','.join(par_val_dist))
    print(','.join(health_df.columns))

    # table_health.aggregate_on_column(mock_df, 'account_no').show(truncate=False)

    table_health.aggregate_on_column(mock_df, 'brand').show(truncate=False)
    table_health.aggregate_on_column(mock_df, 'price').show(truncate=False)
    table_health.summary_by_column().show(truncate=False)

