import datetime

import pytest
from sparkcore.SparkCore import SparkCore
from sparkcore.helper.DataFrameHelper import DataFrameHelper
from sparkcore.helper.DateHelper import DateHelper
import pyspark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from sys import platform
from sparkcore.writer.TableProperty import TableProperty
from sparkcore.ColumnDescriptor import ColumnDescriptor
from sparkcore.writer.SparkWriter import SparkWriter
from sparkcore.configProvider.TableConfig import TableConfig
import os

if platform == 'linux':
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64/"


@pytest.fixture
def spark_session() -> pyspark.sql.SparkSession:
    return SparkCore(mode='local').spark_session


@pytest.fixture
def mock_transaction_20210915(spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    date = '2021-09-15'
    # columns = ["start_date", "account_no"]
    account_no = 'account_no'
    schema = StructType([
        StructField('data_date', StringType(), True),
        StructField(account_no, StringType(), True),
    ])
    data = [(date, "a01"), (date, "a02"), (date, "a03")]
    df = spark_session.createDataFrame(data, schema)
    return df


@pytest.fixture
def mock_transaction_20211001(mock_transaction_20210915: pyspark.sql.SparkSession,
                              spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    date = '2021-10-01'
    # columns = ["start_date", "account_no"]
    account_no = 'account_no'
    schema = StructType([
        StructField('data_date', StringType(), True),
        StructField(account_no, StringType(), True),
    ])
    data = [(date, "a01"), (date, "a04"), (date, "a05")]
    df = spark_session.createDataFrame(data, schema)
    return df.union(mock_transaction_20210915)


@pytest.fixture
def mock_transaction_20211002(mock_transaction_20211001: pyspark.sql.SparkSession,
                              spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    date = '2021-10-02'
    # columns = ["start_date", "account_no"]
    account_no = 'account_no'
    schema = StructType([
        StructField('data_date', StringType(), True),
        StructField(account_no, StringType(), True),
    ])
    data = [(date, "a01"), (date, "a02"), (date, "a03")]
    df = spark_session.createDataFrame(data, schema)
    return df.union(mock_transaction_20211001)


@pytest.fixture
def mock_transaction_multiple_key_20210901(spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    date = '2021-09-01'
    account_no = 'account_no'
    branch_no = 'branch_no'
    schema = StructType([
        StructField('data_date', StringType(), True),
        StructField(account_no, StringType(), True),
        StructField(branch_no, StringType(), True)
    ])
    data = [(date, "a01", "b01"), (date, "a02", "b02"), (date, "a03", "b02")]
    df = spark_session.createDataFrame(data, schema)
    return df


@pytest.fixture
def mock_transaction_multiple_key_20210903(mock_transaction_multiple_key_20210901: pyspark.sql.dataframe.DataFrame,
                                           spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    date = '2021-09-03'
    account_no = 'account_no'
    branch_no = 'branch_no'
    schema = StructType([
        StructField('data_date', StringType(), True),
        StructField(account_no, StringType(), True),
        StructField(branch_no, StringType(), True)
    ])
    data = [(date, "a01", "b01"), (date, "a04", "b02"), (date, "a05", "b02")]
    df = spark_session.createDataFrame(data, schema)
    return df.union(mock_transaction_multiple_key_20210901)


@pytest.fixture
def mock_transaction_multiple_key_20210902(mock_transaction_multiple_key_20210903: pyspark.sql.dataframe.DataFrame,
                                           spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    date = '2021-09-02'
    account_no = 'account_no'
    branch_no = 'branch_no'
    schema = StructType([
        StructField('data_date', StringType(), True),
        StructField(account_no, StringType(), True),
        StructField(branch_no, StringType(), True)
    ])
    data = [(date, "a06", "b01")]
    df = spark_session.createDataFrame(data, schema)
    return df.union(mock_transaction_multiple_key_20210903)


@pytest.fixture
def mock_transaction_multiple_key_20211003(mock_transaction_multiple_key_20210902: pyspark.sql.dataframe.DataFrame,
                                           spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    date = '2021-10-03'
    account_no = 'account_no'
    branch_no = 'branch_no'
    schema = StructType([
        StructField('data_date', StringType(), True),
        StructField(account_no, StringType(), True),
        StructField(branch_no, StringType(), True)
    ])
    data = [(date, "a07", "b01")]
    df = spark_session.createDataFrame(data, schema)
    return df.union(mock_transaction_multiple_key_20210902)


@pytest.fixture
def mock_month_key_df(spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    data = [(321, "September", 9, "Q3", "2021", "2021 Q3", "2021/09"),
            (322, "October", 10, "Q4", "2021", "2021 Q4", "2021/10"),
            (323, "November", 11, "Q4", "2021", "2021 Q4", "2021/11"),
            (324, "December", 12, "Q4", "2021", "2021 Q4", "2021/12")]
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


def are_dfs_schema_equal(df1: pyspark.sql.dataframe.DataFrame, df2: pyspark.sql.dataframe.DataFrame) -> bool:
    return False if df1.schema != df2.schema else True


def are_dfs_data_equal(df1: pyspark.sql.dataframe.DataFrame, df2: pyspark.sql.dataframe.DataFrame) -> bool:
    return False if df1.collect() != df2.collect() else True


def test_update_insert_empty_snap_monthly(mock_transaction_20210915: pyspark.sql.dataframe.DataFrame,
                                          mock_month_key_df: pyspark.sql.dataframe.DataFrame,
                                          spark_session: pyspark.sql.SparkSession) -> None:
    # spark_session.sparkContext.setCheckpointDir("/tmp/spark/checkpoint")
    # spark_session.conf.set("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
    mock_transaction_20210915.show(truncate=False)
    process_date = '2021-09-15'
    today_date = '2021-09-15'
    is_active = 'is_active'
    account_no = 'account_no'
    data_date = 'data_date'
    schema = StructType([
        StructField(data_date, StringType(), True),
        StructField(account_no, StringType(), True),
        StructField(DataFrameHelper.MONTH_KEY, IntegerType(), True),
        StructField(DataFrameHelper.UPDATE_DATE, StringType(), True),
        StructField(is_active, StringType(), True)
    ])
    data = []
    snap_monthly_df = spark_session.createDataFrame(data, schema)
    snap_monthly_df.show(truncate=False)
    keys = [account_no, ]
    actual_df = DataFrameHelper().update_insert_status_snap_monthly(transaction_df=mock_transaction_20210915,
                                                                    snap_monthly_df=snap_monthly_df,
                                                                    status_column=is_active,
                                                                    key_columns=keys,
                                                                    process_date=process_date,
                                                                    today_date=today_date,
                                                                    month_key_df=mock_month_key_df,
                                                                    data_date_col_name=data_date)
    actual_df.show(truncate=False)
    month_key = 0
    expected_first_stage = [(process_date, "a01", today_date, month_key, DataFrameHelper.ACTIVE),
                            (process_date, "a02", today_date, month_key, DataFrameHelper.ACTIVE),
                            (process_date, "a03", today_date, month_key, DataFrameHelper.ACTIVE)]
    expected_schema = StructType([
        StructField(data_date, StringType(), True),
        StructField(account_no, StringType(), True),
        StructField(DataFrameHelper.UPDATE_DATE, StringType(), False),
        StructField(DataFrameHelper.MONTH_KEY, IntegerType(), False),
        StructField(is_active, StringType(), False)
    ])
    expected_df = spark_session.createDataFrame(expected_first_stage, expected_schema)
    expected_df.show(truncate=False)
    print(actual_df.schema)
    print("=============")
    print(expected_df.schema)
    assert are_dfs_schema_equal(actual_df, expected_df)
    assert are_dfs_data_equal(actual_df, expected_df)
    spark_session.stop()


def test_update_insert_status_snap_monthly_new_month(mock_transaction_20210915: pyspark.sql.dataframe.DataFrame,
                                                     mock_transaction_20211001: pyspark.sql.dataframe.DataFrame,
                                                     mock_month_key_df: pyspark.sql.dataframe.DataFrame,
                                                     spark_session: pyspark.sql.SparkSession) -> None:
    mock_transaction_20210915.show(truncate=False)
    process_date = "2021-09-15"
    today_date = "2021-09-15"
    is_active = 'is_active'
    account_no = 'account_no'
    data_date = 'data_date'
    schema = StructType([
        StructField(account_no, StringType(), True),
        StructField(DataFrameHelper.MONTH_KEY, IntegerType(), True),
        StructField(DataFrameHelper.UPDATE_DATE, StringType(), True),
        StructField(is_active, StringType(), True)
    ])
    data = []
    snap_monthly_df = spark_session.createDataFrame(data, schema)
    snap_monthly_df.show(truncate=False)
    keys = [account_no, ]
    actual_first_stage_df = DataFrameHelper().update_insert_status_snap_monthly(
        transaction_df=mock_transaction_20210915,
        snap_monthly_df=snap_monthly_df,
        status_column=is_active,
        key_columns=keys,
        process_date=process_date,
        today_date=today_date,
        month_key_df=mock_month_key_df,
        data_date_col_name=data_date)
    actual_first_stage_df.show(truncate=False)
    mock_month_key_df.show(truncate=False)
    month_key = 0
    process_date = "2021-10-01"
    today_date = "2021-10-03"
    actual_second_stage_df = DataFrameHelper().update_insert_status_snap_monthly(
        transaction_df=mock_transaction_20211001,
        snap_monthly_df=actual_first_stage_df,
        status_column=is_active,
        key_columns=keys,
        process_date=process_date,
        today_date=today_date,
        month_key_df=mock_month_key_df,
        data_date_col_name=data_date)
    actual_second_stage_df.show(truncate=False)
    expected_second_stage = [("2021-10-01", "a01", "2021-10-03", 0, DataFrameHelper.ACTIVE),
                             ("2021-10-01", "a04", "2021-10-03", 0, DataFrameHelper.ACTIVE),
                             ("2021-10-01", "a05", "2021-10-03", 0, DataFrameHelper.ACTIVE),
                             ("2021-09-15", "a01", "2021-09-15", 321, DataFrameHelper.ACTIVE),
                             ("2021-09-15", "a02", "2021-09-15", 321, DataFrameHelper.ACTIVE),
                             ("2021-09-15", "a03", "2021-09-15", 321, DataFrameHelper.ACTIVE)]
    expected_schema = StructType([
        StructField(data_date, StringType(), True),
        StructField(account_no, StringType(), True),
        StructField(DataFrameHelper.UPDATE_DATE, StringType(), False),
        StructField(DataFrameHelper.MONTH_KEY, IntegerType(), False),
        StructField(is_active, StringType(), False)
    ])
    expected_second_df = spark_session.createDataFrame(expected_second_stage, expected_schema)
    expected_second_df.show(truncate=False)
    actual_second_stage_df.printSchema()
    assert are_dfs_schema_equal(actual_second_stage_df, expected_second_df)
    assert are_dfs_data_equal(actual_second_stage_df, expected_second_df)
    spark_session.stop()


def test_find_month_key_of_process_date(spark_session: pyspark.sql.SparkSession) -> None:
    data_date = 'date_date'
    expected_first_stage = [("2021-10-02", "a01", "2021-10-02", 1, DataFrameHelper.ACTIVE),
                            ("2021-10-31", "a02", "2021-10-31", 1, DataFrameHelper.ACTIVE),
                            ("2021-11-01", "a03", "2021-11-01", 0, DataFrameHelper.ACTIVE)]
    expected_schema = StructType([
        StructField(data_date, StringType(), True),
        StructField('account_no', StringType(), True),
        StructField(DataFrameHelper.UPDATE_DATE, StringType(), False),
        StructField(DataFrameHelper.MONTH_KEY, IntegerType(), False),
        StructField('is_active', StringType(), False)
    ])
    expected_df = spark_session.createDataFrame(expected_first_stage, expected_schema)
    expected_df.show(truncate=False)
    process_date = '2021-10-05'
    target_month_key = DataFrameHelper().find_month_key_of_process_date(process_date, expected_df, data_date)
    assert target_month_key == 1
    process_date = '2021-11-05'
    target_month_key = DataFrameHelper().find_month_key_of_process_date(process_date, expected_df, data_date)
    assert target_month_key == 0


def test_update_insert_status_snap_monthly_existing_month(mock_transaction_20210915: pyspark.sql.dataframe.DataFrame,
                                                          mock_transaction_20211001: pyspark.sql.dataframe.DataFrame,
                                                          mock_transaction_20211002: pyspark.sql.dataframe.DataFrame,
                                                          mock_month_key_df: pyspark.sql.dataframe.DataFrame,
                                                          spark_session: pyspark.sql.SparkSession) -> None:
    process_date = "2021-09-15"
    today_date = "2021-09-15"
    is_active = 'is_active'
    account_no = 'account_no'
    data_date = 'data_date'

    schema = StructType([
        StructField(data_date, StringType(), True),
        StructField(account_no, StringType(), True),
        StructField(DataFrameHelper.MONTH_KEY, IntegerType(), True),
        StructField(DataFrameHelper.UPDATE_DATE, StringType(), True),
        StructField(is_active, StringType(), True)
    ])
    data = []
    snap_monthly_df = spark_session.createDataFrame(data, schema)
    snap_monthly_df.show(truncate=False)
    mock_transaction_20210915.show(truncate=False)
    keys = [account_no, ]
    actual_first_stage_df = DataFrameHelper().update_insert_status_snap_monthly(
        transaction_df=mock_transaction_20210915,
        snap_monthly_df=snap_monthly_df,
        status_column=is_active,
        key_columns=keys,
        process_date=process_date,
        today_date=today_date,
        month_key_df=mock_month_key_df,
        data_date_col_name=data_date)
    actual_first_stage_df.show(truncate=False)

    process_date = "2021-10-01"
    today_date = "2021-10-01"
    actual_second_stage_df = DataFrameHelper().update_insert_status_snap_monthly(
        transaction_df=mock_transaction_20211001,
        snap_monthly_df=actual_first_stage_df,
        status_column=is_active,
        key_columns=keys,
        process_date=process_date,
        today_date=today_date,
        month_key_df=mock_month_key_df,
        data_date_col_name=data_date)
    actual_second_stage_df.orderBy(DataFrameHelper().MONTH_KEY, data_date).show(truncate=False)
    process_date = "2021-10-02"
    today_date = "2021-10-02"
    mock_transaction_20211002.show(truncate=False)
    print("mock_transaction_20211002.show(truncate=False)")
    actual_third_stage_df = DataFrameHelper().update_insert_status_snap_monthly(
        transaction_df=mock_transaction_20211002,
        snap_monthly_df=actual_second_stage_df,
        status_column=is_active,
        key_columns=keys,
        process_date=process_date,
        today_date=today_date,
        month_key_df=mock_month_key_df,
        data_date_col_name=data_date)
    actual_third_stage_df.show(truncate=False)

    expected_third_stage = [("2021-09-15", "a01", "2021-09-15", 321, DataFrameHelper.ACTIVE),
                            ("2021-09-15", "a02", "2021-09-15", 321, DataFrameHelper.ACTIVE),
                            ("2021-09-15", "a03", "2021-09-15", 321, DataFrameHelper.ACTIVE),
                            ("2021-10-02", "a01", "2021-10-02", 0, DataFrameHelper.ACTIVE),
                            ("2021-10-01", "a04", "2021-10-01", 0, DataFrameHelper.INACTIVE),
                            ("2021-10-02", "a03", "2021-10-02", 0, DataFrameHelper.ACTIVE),
                            ("2021-10-02", "a02", "2021-10-02", 0, DataFrameHelper.ACTIVE),
                            ("2021-10-01", "a05", "2021-10-01", 0, DataFrameHelper.INACTIVE)]
    expected_schema = StructType([
        StructField(data_date, StringType(), True),
        StructField(account_no, StringType(), True),
        StructField(DataFrameHelper.UPDATE_DATE, StringType(), True),
        StructField(DataFrameHelper.MONTH_KEY, IntegerType(), True),
        StructField(is_active, StringType(), True)
    ])
    expected_third_df = spark_session.createDataFrame(expected_third_stage, expected_schema)
    expected_third_df.show(truncate=False)
    assert are_dfs_schema_equal(actual_third_stage_df, expected_third_df)
    assert are_dfs_data_equal(actual_third_stage_df, expected_third_df)
    spark_session.stop()


def test_does_month_exist(spark_session: pyspark.sql.SparkSession) -> None:
    is_active = 'is_active'
    account_no = 'account_no'
    data_date = 'data_date'
    schema = StructType([
        StructField(data_date, StringType(), True),
        StructField(account_no, StringType(), True),
        StructField(DataFrameHelper.UPDATE_DATE, StringType(), True),
        StructField(DataFrameHelper.MONTH_KEY, IntegerType(), True),
        StructField(is_active, StringType(), True)
    ])
    data = []
    empty_df = spark_session.createDataFrame(data, schema)
    empty_df.show(truncate=False)
    does_exist = DataFrameHelper().does_month_exist('2021-05-01', empty_df, data_date)
    assert not (does_exist)
    test_data = [("2021-09-15", "a01", "2021-09-15", 1, DataFrameHelper.ACTIVE),
                 ("2021-09-15", "a02", "2021-09-15", 1, DataFrameHelper.ACTIVE),
                 ("2021-09-15", "a03", "2021-09-15", 1, DataFrameHelper.ACTIVE),
                 ("2021-10-02", "a01", "2021-10-02", 0, DataFrameHelper.ACTIVE),
                 ("2021-10-01", "a04", "2021-10-01", 0, DataFrameHelper.INACTIVE),
                 ("2021-10-02", "a03", "2021-10-02", 0, DataFrameHelper.ACTIVE),
                 ("2021-10-02", "a02", "2021-10-02", 0, DataFrameHelper.ACTIVE),
                 ("2021-10-01", "a05", "2021-10-01", 0, DataFrameHelper.INACTIVE)]
    test_df = spark_session.createDataFrame(test_data, schema)
    test_df.show(truncate=False)
    does_exist = DataFrameHelper().does_month_exist('2021-10-01', test_df, data_date)
    assert does_exist


def test_rerun(mock_transaction_multiple_key_20210902: pyspark.sql.dataframe.DataFrame,
               mock_transaction_multiple_key_20211003: pyspark.sql.dataframe.DataFrame,
               mock_month_key_df: pyspark.sql.dataframe.DataFrame,
               spark_session: pyspark.sql.SparkSession) -> None:
    # spark_session.sparkContext.setCheckpointDir("file:///tmp/checkpoint")
    # config_path = '../sparkcore_test/helper/config/'
    # transaction_table_config = TableConfig(config_path, 'local', 'transaction')
    # transaction_table_property = TableProperty(db_name=transaction_table_config.db_name,
    #                                            tb_name=transaction_table_config.tb_name,
    #                                            table_path=transaction_table_config.table_path,
    #                                            fields=transaction_table_config.fields,
    #                                            partitions=transaction_table_config.partitions)
    # print(
    #     transaction_table_property.create_table_sql(table_format=transaction_table_property.ORC_FORMAT, delimitor=None))
    # spark_writer = SparkWriter(spark_session)
    # spark_writer.create_table(transaction_table_property)
    # transaction_df = spark_session.table(f'{transaction_table_property.database}.{transaction_table_property.table}')
    # transaction_df.show(truncate=False)
    # mock_transaction_multiple_key_20210901.write \
    #     .format("orc") \
    #     .mode("overwrite") \
    #     .partitionBy('start_date') \
    #     .saveAsTable(f'{transaction_table_property.database}.{transaction_table_property.table}')
    # transaction_df = spark_session.table(f'{transaction_table_property.database}.{transaction_table_property.table}')
    # transaction_df.show(truncate=False)
    # snap_monthly_table_config = TableConfig(config_path, 'local', 'snap_monthly')
    # snap_monthly_table_property = TableProperty(db_name=snap_monthly_table_config.db_name,
    #                                             tb_name=snap_monthly_table_config.tb_name,
    #                                             table_path=snap_monthly_table_config.table_path,
    #                                             fields=snap_monthly_table_config.fields,
    #                                             partitions=snap_monthly_table_config.partitions)
    # print(
    #     snap_monthly_table_property.create_table_sql(table_format=snap_monthly_table_property.ORC_FORMAT,
    #                                                  delimitor=None))
    # spark_writer.create_table(snap_monthly_table_property)
    # snap_monthly_df = spark_session.table(f'{snap_monthly_table_property.database}.{snap_monthly_table_property.table}')
    # snap_monthly_df.show(truncate=False)

    account_no = 'account_no'
    branch_no = 'branch_no'
    status_column = 'is_active'
    data_date = 'data_date'
    keys = [account_no, branch_no]
    data = []
    schema = StructType([
        StructField(data_date, StringType(), True),
        StructField(account_no, StringType(), True),
        StructField(branch_no, StringType(), True),
        StructField(DataFrameHelper.UPDATE_DATE, StringType(), True),
        StructField(DataFrameHelper.MONTH_KEY, IntegerType(), False),
        StructField(status_column, StringType(), True)
    ])
    snap_monthly_df = spark_session.createDataFrame(data, schema)

    snap_monthly_df = DataFrameHelper().update_insert_status_snap_monthly(
        transaction_df=mock_transaction_multiple_key_20210902,
        snap_monthly_df=snap_monthly_df,
        status_column=status_column,
        key_columns=keys,
        process_date='2021-09-01',
        today_date='2021-09-04',
        month_key_df=mock_month_key_df,
        data_date_col_name=data_date)
    snap_monthly_df.show(truncate=False)

    snap_monthly_df = DataFrameHelper().update_insert_status_snap_monthly(
        transaction_df=mock_transaction_multiple_key_20210902,
        snap_monthly_df=snap_monthly_df,
        status_column=status_column,
        key_columns=keys,
        process_date='2021-09-02',
        today_date='2021-09-04',
        month_key_df=mock_month_key_df,
        data_date_col_name=data_date)
    snap_monthly_df.show(truncate=False)

    snap_monthly_df = DataFrameHelper().update_insert_status_snap_monthly(
        transaction_df=mock_transaction_multiple_key_20210902,
        snap_monthly_df=snap_monthly_df,
        status_column=status_column,
        key_columns=keys,
        process_date='2021-09-03',
        today_date='2021-09-04',
        month_key_df=mock_month_key_df,
        data_date_col_name=data_date)
    snap_monthly_df.show(truncate=False)

    snap_monthly_df = DataFrameHelper().update_insert_status_snap_monthly(
        transaction_df=mock_transaction_multiple_key_20211003,
        snap_monthly_df=snap_monthly_df,
        status_column=status_column,
        key_columns=keys,
        process_date='2021-10-03',
        today_date='2021-09-04',
        month_key_df=mock_month_key_df,
        data_date_col_name=data_date)
    snap_monthly_df.show(truncate=False)
    test_data = [("2021-10-03", "a07", "b01", "2021-09-04", 0, DataFrameHelper.ACTIVE),
                 ("2021-09-03", "a04", "b02", "2021-09-04", 321, DataFrameHelper.ACTIVE),
                 ("2021-09-01", "a02", "b02", "2021-09-04", 321, DataFrameHelper.INACTIVE),
                 ("2021-09-03", "a01", "b01", "2021-09-04", 321, DataFrameHelper.ACTIVE),
                 ("2021-09-01", "a03", "b02", "2021-09-04", 321, DataFrameHelper.INACTIVE),
                 ("2021-09-03", "a05", "b02", "2021-09-04", 321, DataFrameHelper.ACTIVE),
                 ("2021-09-02", "a06", "b01", "2021-09-04", 321, DataFrameHelper.INACTIVE)]
    test_df = spark_session.createDataFrame(test_data, schema)
    test_df.show(truncate=False)
    assert are_dfs_schema_equal(snap_monthly_df, test_df)
    assert are_dfs_data_equal(snap_monthly_df, test_df)
    spark_session.stop()

