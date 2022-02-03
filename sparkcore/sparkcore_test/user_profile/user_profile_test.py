import pytest
from sparkcore.SparkCore import SparkCore
from sparkcore.configProvider.TableConfig import TableConfig
from sparkcore.ColumnDescriptor import ColumnDescriptor
from sparkcore.writer.TableProperty import TableProperty
from sparkcore.writer.SparkWriter import SparkWriter
from sparkcore.helper.DataFrameHelper import DataFrameHelper
from sparkcore.helper.DateHelper import DateHelper
from sys import platform
import pyspark
import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, lit, substring, regexp_replace, count, to_timestamp, date_trunc

import random
import string
import itertools

if platform == 'linux':
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64/"


def are_dfs_schema_equal(df1: pyspark.sql.dataframe.DataFrame, df2: pyspark.sql.dataframe.DataFrame) -> bool:
    return False if df1.schema != df2.schema else True


def are_dfs_data_equal(df1: pyspark.sql.dataframe.DataFrame, df2: pyspark.sql.dataframe.DataFrame) -> bool:
    return False if df1.collect() != df2.collect() else True


@pytest.fixture
def spark_session() -> pyspark.sql.SparkSession:
    return SparkCore(mode='local').spark_session


def get_random_ka_user(length: int) -> str:
    # With combination of lower and upper case
    return ''.join(random.choice(string.ascii_letters) for i in range(length))


def get_random_cr_user() -> str:
    # get random string of 8 digits
    source = string.digits
    return ''.join((random.choice(source) for i in range(8)))


@pytest.fixture
def mock_month_key_df(spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    data = [(321, "September", 9, "Q3", "2021", "2021 Q3", "2021/09"),
            (322, "October", 10, "Q4", "2021", "2021 Q4", "2021/10"),
            (323, "November", 11, "Q4", "2021", "2021 Q4", "2021/11"),
            (324, "December", 12, "Q4", "2021", "2021 Q4", "2021/12"),
            (325, "January", 1, "Q1", "2022", "2022 Q1", "2022/01")]
    schema = StructType([
        StructField("month_key", IntegerType(), False),
        StructField("month_text", StringType(), False),
        StructField("month_number", IntegerType(), False),
        StructField("fiscal_period", StringType(), False),
        StructField("year", StringType(), False),
        StructField("period_and_year", StringType(), False),
        StructField("month_and_year", StringType(), False)
    ])
    df = spark_session.createDataFrame(data, schema)
    return df


@pytest.fixture
def mock_ay_user_entity(spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    user_login = 'user_login'
    user_status = 'status'
    entity_code = 'entity_code'
    schema = StructType([
        StructField(user_login, StringType(), True),
        StructField(user_status, StringType(), True),
        StructField(entity_code, StringType(), True)
    ])
    entity = 'AYCAL'
    data = [(30000618, 'Active', entity),
            ('KULYAWKE', 'Active', entity),
            (10000001, 'Inactive', entity),
            ('RATCHANOK', 'Inactive', entity),
            (20000002, 'Active', entity),
            ('SOMBOON', 'Active', entity),
            (30000003, 'Active', entity),
            ('onnut', 'Active', entity),
            (40000004, 'Inactive', entity),
            ('SUKUMVIT', 'Inactive', entity),
            (50000005, None, entity),
            ('PRAYUT', None, entity),
            (get_random_cr_user(), 'Active', entity),
            (get_random_ka_user(9), 'Active', entity),
            (get_random_cr_user(), 'Inactive', entity),
            (get_random_ka_user(9), 'Inactive', entity),
            (get_random_cr_user(), None, entity),
            (get_random_ka_user(9), None, entity)
            ]

    df = spark_session.createDataFrame(data, schema)
    return df


@pytest.fixture
def mock_initial_ext_ay_user_entity(
        mock_ay_user_entity: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    return mock_ay_user_entity.withColumn('sent_date',
                                          to_timestamp(lit('2022-01-31 01:02:59'), DateHelper.HIVE_TIMESTAMP_FORMAT))


@pytest.fixture
def mock_ka_user_entity(spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    user_login = 'user_login'
    user_status = 'status'
    entity_code = 'entity_code'
    schema = StructType([
        StructField(user_login, StringType(), True),
        StructField(user_status, StringType(), True),
        StructField(entity_code, StringType(), True)
    ])
    entity = 'BAY'
    data = [(30000618, 'Active', entity),
            ('KULYAWKE', 'Active', entity),
            (10000001, 'Inactive', entity),
            ('RATCHANOK', 'Inactive', entity),
            (20000002, 'Inactive', entity),
            ('SOMBOON', 'Inactive', entity),
            (30000003, None, entity),
            ('onnut', None, entity),
            (40000004, 'Active', entity),
            ('SUKUMVIT', 'Active', entity),
            (50000005, 'Active', entity),
            ('PRAYUT', 'Active', entity),
            (get_random_cr_user(), 'Active', entity),
            (get_random_ka_user(9), 'Active', entity),
            (get_random_cr_user(), 'Inactive', entity),
            (get_random_ka_user(9), 'Inactive', entity),
            (get_random_cr_user(), None, entity),
            (get_random_ka_user(9), None, entity)
            ]

    df = spark_session.createDataFrame(data, schema)
    return df


@pytest.fixture
def mock_initial_ext_ka_user_entity(
        mock_ka_user_entity: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    return mock_ka_user_entity.withColumn('sent_date',
                                          to_timestamp(lit('2022-01-31 15:30:45'), DateHelper.HIVE_TIMESTAMP_FORMAT))


@pytest.fixture()
def mock_entity_uam_result(spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    entity_uam = 'entity_uam'
    entity_total = 'entity_total'
    schema = StructType([StructField(entity_uam, StringType(), True),
                         StructField(entity_total, IntegerType(), False)])
    data = [(None, 10), ('Auto', 2), ('Bay', 6), ('Aycal', 6)]

    df = spark_session.createDataFrame(data, schema)
    return df


@pytest.fixture()
def mock_user_type_result(spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    user_type = 'user_type'
    user_type_total = 'user_type_total'
    schema = StructType([StructField(user_type, StringType(), False),
                         StructField(user_type_total, IntegerType(), False)])
    data = [('STAFF', 9), ('CR', 9)]

    df = spark_session.createDataFrame(data, schema)
    return df


def test_entity_uam(mock_ay_user_entity: pyspark.sql.dataframe.DataFrame,
                    mock_ka_user_entity: pyspark.sql.dataframe.DataFrame,
                    mock_entity_uam_result: pyspark.sql.dataframe.DataFrame) -> None:
    mock_ay_user_entity.show(truncate=False)
    mock_ka_user_entity.show(truncate=False)
    entities = ['ka', 'ay']
    entity_status = ['status', 'entity']
    key_list = ['user_login']
    entity_uam = 'entity_uam'
    ay_user_entity_df = mock_ay_user_entity \
        .withColumnRenamed('status', 'ay_status') \
        .withColumnRenamed('entity_code', 'ay_entity')
    ka_user_entity_df = mock_ka_user_entity \
        .withColumnRenamed('status', 'ka_status') \
        .withColumnRenamed('entity_code', 'ka_entity')
    all_entities_df = DataFrameHelper.combine_entity_df(ay_df=ay_user_entity_df, ka_df=ka_user_entity_df,
                                                        join_key=key_list)
    uam_cal_list = key_list + [entities + "_" + entity_status for
                               entity_status, entities in
                               itertools.product(entity_status, entities)]
    print('\n'.join(uam_cal_list))
    entity_status_labeled_df = all_entities_df.selectExpr(*uam_cal_list)
    entity_status_labeled_df.show(truncate=False)
    print(f'num_row = {entity_status_labeled_df.count()}')
    uam_df = entity_status_labeled_df.withColumn(entity_uam, DataFrameHelper.add_entity_uam())
    uam_df.show(n=100, truncate=False)
    assert uam_df.count() == 24
    entity_uam_summary_df = uam_df.groupBy(entity_uam).agg((count("*").cast(IntegerType())).alias('entity_total'))
    entity_uam_summary_df.show(n=100, truncate=False)
    assert are_dfs_schema_equal(entity_uam_summary_df, mock_entity_uam_result)
    assert are_dfs_data_equal(entity_uam_summary_df, mock_entity_uam_result)


def test_user_type(mock_ay_user_entity: pyspark.sql.dataframe.DataFrame,
                   mock_user_type_result: pyspark.sql.dataframe.DataFrame) -> None:
    mock_ay_user_entity.show(truncate=False)
    user_type_df = mock_ay_user_entity.withColumn('user_type', DataFrameHelper.add_user_type())
    assert user_type_df.count() == 18
    user_type_summary_df = user_type_df.groupBy('user_type').agg(
        count('*').cast(IntegerType()).alias('user_type_total'))
    user_type_summary_df.show(n=10, truncate=False)
    user_type_summary_df.printSchema()
    mock_user_type_result.printSchema()
    assert are_dfs_schema_equal(user_type_summary_df, mock_user_type_result)
    assert are_dfs_data_equal(user_type_summary_df, mock_user_type_result)


def test_snap_monthly_without_status(mock_initial_ext_ay_user_entity: pyspark.sql.dataframe.DataFrame,
                                     mock_initial_ext_ka_user_entity: pyspark.sql.dataframe.DataFrame,
                                     mock_month_key_df: pyspark.sql.dataframe.DataFrame,
                                     spark_session: pyspark.sql.SparkSession) -> None:
    spark_session.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark_session.sparkContext.setCheckpointDir("/tmp/checkpoint")
    config_path = '../sparkcore_test/user_profile/config/'
    snap_monthly_table_config = TableConfig(config_path, 'local', 'ay_irepo_user_profile')
    snap_monthly_table_property = TableProperty(db_name=snap_monthly_table_config.db_name,
                                                tb_name=snap_monthly_table_config.tb_name,
                                                table_path=snap_monthly_table_config.table_path,
                                                fields=snap_monthly_table_config.fields,
                                                partitions=snap_monthly_table_config.partitions)
    print(
        snap_monthly_table_property.create_table_sql(table_format=snap_monthly_table_property.ORC_FORMAT,
                                                     delimitor=None))
    spark_writer = SparkWriter(spark_session)
    spark_writer.create_table(snap_monthly_table_property)
    snap_month_table = f'{snap_monthly_table_property.database}.{snap_monthly_table_property.table}'
    snap_monthly_df = spark_session.table(snap_month_table)
    snap_monthly_df.printSchema()
    snap_monthly_df.show(truncate=False)

    entities = ['ka', 'ay']
    entity_status = ['status', 'entity']
    key_list = ['user_login', 'sent_date']
    entity_uam = 'entity_uam'
    # remove time from sent_date
    ext_ay_input_user_profile = mock_initial_ext_ay_user_entity.withColumn('sent_date',
                                                                           date_trunc("day", col('sent_date')))
    ext_ay_input_user_profile.show(truncate=False)

    ext_ka_input_user_profile = mock_initial_ext_ka_user_entity.withColumn('sent_date',
                                                                           date_trunc("day", col('sent_date')))
    ext_ka_input_user_profile.show(truncate=False)
    # combine entity ay and ka
    ay_user_entity_df = ext_ay_input_user_profile \
        .withColumnRenamed('status', 'ay_status') \
        .withColumnRenamed('entity_code', 'ay_entity')
    ka_user_entity_df = ext_ka_input_user_profile \
        .withColumnRenamed('status', 'ka_status') \
        .withColumnRenamed('entity_code', 'ka_entity')

    all_entities_df = DataFrameHelper.combine_entity_df(ay_df=ay_user_entity_df, ka_df=ka_user_entity_df,
                                                        join_key=key_list)
    # label entity to status and entity
    uam_cal_list = key_list + [entities + "_" + entity_status for
                               entity_status, entities in
                               itertools.product(entity_status, entities)]
    print('\n'.join(uam_cal_list))
    entity_status_labeled_df = all_entities_df.selectExpr(*uam_cal_list)
    entity_status_labeled_df.show(n=100, truncate=False)
    # Get user_login entity_uam
    uam_df = entity_status_labeled_df.withColumn(entity_uam, DataFrameHelper.add_entity_uam()).select('user_login',
                                                                                                      'entity_uam')
    uam_df.show(n=100, truncate=False)
    # add entity_uam back to transaction_df
    entity = 'ay'
    ay_transaction_w_entity_uam_df = mock_initial_ext_ay_user_entity.join(uam_df, on=['user_login'], how='inner')
    ay_transaction_w_entity_uam_df.show(n=100, truncate=False)
    # add user_type
    ay_transaction_w_user_type_df = ay_transaction_w_entity_uam_df \
        .withColumn('user_type',
                    DataFrameHelper.add_user_type()) \
        .withColumnRenamed('sent_date', 'data_date') \
        .withColumnRenamed('status', 'user_status')
    DataFrameHelper.update_insert_snap_monthly_to_table(transaction_df=ay_transaction_w_user_type_df,
                                                        process_date='2022-01-31', today_date='2022-02-01',
                                                        month_key_df=mock_month_key_df,
                                                        data_date_col_name='data_date',
                                                        spark_session=spark_session,
                                                        snap_month_table=snap_month_table
                                                        )
    snap_month_table_df = spark_session.table(snap_month_table)
    snap_month_table_df.show(truncate=False)
