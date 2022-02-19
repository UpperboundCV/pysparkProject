import pytest
import pyspark.sql
from sys import platform
import sys
import os

# local PySpark Environment
# sys.path.append('/home/up_python/PycharmProjects/pysparkProject/sparkcore/')
sys.path.append('../../')

from SparkCore import SparkCore
from TableHealth import TableHealth
from writer.TableProperty import TableProperty
from writer.SparkWriter import SparkWriter
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import to_timestamp, col, lit, when
from ColumnDescriptor import ColumnDescriptor

if platform == 'linux':
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64/"


def are_dfs_schema_equal(df1: pyspark.sql.dataframe.DataFrame, df2: pyspark.sql.dataframe.DataFrame) -> bool:
    return False if df1.schema != df2.schema else True


def are_dfs_data_equal(df1: pyspark.sql.dataframe.DataFrame, df2: pyspark.sql.dataframe.DataFrame) -> bool:
    return False if df1.collect() != df2.collect() else True


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
        StructField('brand', StringType(), True),
        StructField('buy_time', StringType(), True)
    ])
    data = [(date, "a01", 2000.50, 'Mitsu', '2022-02-01 00:00:01'),
            (date, "a02", 12345.99, 'Mitsu', '2022-02-02 00:00:02'),
            (date, "a03", None, 'Honda', None),
            (date, "a04", float("NaN"), None, "")]
    df = spark_session.createDataFrame(data, schema) \
        .withColumn('buy_time', to_timestamp(col('buy_time'), 'yyyy-MM-dd HH:mm:ss'))
    return df


@pytest.fixture
def mock_health_table_result(spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    schema = StructType([
        StructField('column', StringType(), True),
        StructField('d_type', StringType(), True),
        StructField('d_min', StringType(), True),
        StructField('d_max', StringType(), True),
        StructField('d_mean', StringType(), True),
        StructField('d_median', StringType(), True),
        StructField('d_sum', StringType(), True),
        StructField('null_cnt', StringType(), True),
        StructField('nan_cnt', StringType(), True),
        StructField('empty_cnt', StringType(), True),
        StructField('cnt_distinct', StringType(), True),
        StructField('crunch_date', StringType(), True),
        StructField('data_date', StringType(), True)
    ])
    data = [('price', 'numeric', '10.0', '12345.99', '4785.5', '2000.5', '14356.49', '0', '0', '0', '3', '2022-02-8',
             '2021-09-15'),
            ('data_date', 'category', '2021-09-15', '2021-09-15', None, None, None, '0', '0', '0', "{'2021-09-15': 3}",
             '2022-02-8', '2021-09-15'),
            ('brand', 'category', 'Honda', 'Mitsu', None, None, None, '0', '0', '0', "{'Mitsu': 2, 'Honda': 1}",
             '2022-02-08', '2021-09-15'),
            ('account_no', 'string', 'a01', 'a03', None, None, None, '0', '0', '0', '3', '2022-02-08', '2021-09-15')
            ]
    df = spark_session.createDataFrame(data, schema)
    return df


def test_table_health(spark_session: pyspark.sql.SparkSession,
                      mock_test_data: pyspark.sql.dataframe.DataFrame,
                      mock_health_table_result: pyspark.sql.dataframe.DataFrame) -> None:
    # create table cl_repo
    db_name = 'kadev_collection'
    tb_name = 'mock_cl'
    table_path = '/tmp/mock_cl'
    fields = [ColumnDescriptor('account_no', 'string', '"none"'),
              ColumnDescriptor('price', 'integer', '"none"'),
              ColumnDescriptor('brand', 'string', '"none"'),
              ColumnDescriptor('buy_time', 'timestamp', 'none')]
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
    table_health = TableHealth(spark_session=spark_session, source_schema=db_name, source_table_name=tb_name,
                               env='local')

    table_health.save()

    table_health_df = spark_session.table(f'{table_health.schema}.{table_health.health_table_name}')
    table_health_df.show(truncate=False)
    table_health_df.printSchema()

    # assert are_dfs_schema_equal(table_health_df, mock_health_table_result)
    # assert are_dfs_data_equal(table_health_df, mock_health_table_result)
    # +----------+--------+----------+----------+------+--------+--------+--------+-------+---------+------------------------+-----------+----------+
    # | column | d_type | d_min | d_max | d_mean | d_median | d_sum | null_cnt | nan_cnt | empty_cnt | cnt_distinct | crunch_date | data_date |
    # +----------+--------+----------+----------+------+--------+--------+--------+-------+---------+------------------------+-----------+----------+
    # | price | numeric | 10.0 | 12345.99 | 4785.5 | 2000.5 | 14356.49 | 0 | 0 | 0 | 3 | 2022 - 02 - 0
    # 8 | 2021 - 0
    # 9 - 15 |
    # | data_date | category | 2021 - 0
    # 9 - 15 | 2021 - 0
    # 9 - 15 | null | null | null | 0 | 0 | 0 | {'2021-09-15': 3} | 2022 - 02 - 0
    # 8 | 2021 - 0
    # 9 - 15 |
    # | brand | category | Honda | Mitsu | null | null | null | 0 | 0 | 0 | {'Mitsu': 2, 'Honda': 1} | 2022 - 02 - 0
    # 8 | 2021 - 0
    # 9 - 15 |
    # | account_no | string | a01 | a03 | null | null | null | 0 | 0 | 0 | 3 | 2022 - 02 - 0
    # 8 | 2021 - 0
    # 9 - 15 |
    # +----------+--------+----------+----------+------+--------+--------+--------+-------+---------+------------------------+-----------+----------+
