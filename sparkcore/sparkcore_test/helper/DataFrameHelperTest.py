import datetime

import pytest
from sparkcore.SparkCore import SparkCore
from sparkcore.helper.DataFrameHelper import DataFrameHelper
from sparkcore.helper.DateHelper import DateHelper
import pyspark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, lit, substring, regexp_replace
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


@pytest.fixture
def mock_transaction_df_w_keys(spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    date = '2021-10-27'
    data = [("BAY", "GECAL", "HP", "13", "1118860", date),
            ("AYCAL", "GECAL", "HP", "11", "AB77053", date),
            ("AYCAL", "GECAL", "MC", "12", "AB77070", date),
            ("BAY", "GECAL", "HP", "11", "1122978", date)]
    schema = StructType([
        StructField("entity_code", StringType(), False),
        StructField("company_code", StringType(), False),
        StructField("product_code", StringType(), False),
        StructField("branch_code", StringType(), False),
        StructField("contract_code", StringType(), False),
        StructField("data_date", StringType(), False)
    ])
    df = spark_session.createDataFrame(data, schema)
    return df


@pytest.fixture
def mock_transaction_pst_df_w_keys(spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    start_date = '2021-10-27'
    date = '641027'
    data = [("BAY", "GECAL", "HP", "13", "1118860", date, start_date),
            ("AYCAL", "GECAL", "HP", "11", "AB77053", date, start_date),
            ("AYCAL", "GECAL", "MC", "12", "AB77070", date, start_date),
            ("BAY", "GECAL", "HP", "11", "1122978", date, start_date)]
    schema = StructType([
        StructField("entity_code", StringType(), False),
        StructField("company", StringType(), False),
        StructField("product", StringType(), False),
        StructField("branch", StringType(), False),
        StructField("contract", StringType(), False),
        StructField("data_date", StringType(), False),
        StructField("start_date", StringType(), False),
    ])
    df = spark_session.createDataFrame(data, schema)
    return df


@pytest.fixture
def mock_transaction_pst_df_w_keys_20211028(spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    start_date = '2021-10-28'
    date = '641028'
    data = [("AYCAL", "GECAL", "HP", "43", "BB00055", date, start_date),
            ("AYCAL", "GECAL", "MC", "12", "AB77070", date, start_date),
            ("BAY", "GECAL", "HP", "11", "1122978", date, start_date)]
    schema = StructType([
        StructField("entity_code", StringType(), False),
        StructField("company", StringType(), False),
        StructField("product", StringType(), False),
        StructField("branch", StringType(), False),
        StructField("contract", StringType(), False),
        StructField("data_date", StringType(), False),
        StructField("start_date", StringType(), False),
    ])
    df = spark_session.createDataFrame(data, schema)
    return df


@pytest.fixture
def mock_transaction_pst_df_w_key_20211101(spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    start_date = '2021-11-01'
    date = '641101'
    data = [("AYCAL", "GECAL", "HP", "43", "BB00055", date, start_date),
            ("AYCAL", "GECAL", "MC", "12", "AB77070", date, start_date),
            ("AYCAL", "GECAL", "MC", "12", "ABCD000", date, start_date),
            ("BAY", "GECAL", "HP", "11", "1122978", date, start_date),
            ("BAY", "GECAL", "HP", "11", "1234567", date, start_date)]
    schema = StructType([
        StructField("entity_code", StringType(), False),
        StructField("company", StringType(), False),
        StructField("product", StringType(), False),
        StructField("branch", StringType(), False),
        StructField("contract", StringType(), False),
        StructField("data_date", StringType(), False),
        StructField("start_date", StringType(), False),
    ])
    df = spark_session.createDataFrame(data, schema)
    return df


@pytest.fixture
def mock_transaction_pst_df_w_key_20211102(spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    start_date = '2021-11-02'
    date = '641102'
    data = [("AYCAL", "GECAL", "HP", "43", "BB00055", date, start_date),
            ("AYCAL", "GECAL", "HP", "13", "ABCDEFG", date, start_date),
            ("BAY", "GECAL", "HP", "12", "JKLMNOP", date, start_date),
            ("BAY", "GECAL", "HP", "11", "1234567", date, start_date)]
    schema = StructType([
        StructField("entity_code", StringType(), False),
        StructField("company", StringType(), False),
        StructField("product", StringType(), False),
        StructField("branch", StringType(), False),
        StructField("contract", StringType(), False),
        StructField("data_date", StringType(), False),
        StructField("start_date", StringType(), False),
    ])
    df = spark_session.createDataFrame(data, schema)
    return df


@pytest.fixture
def mock_transaction_pst_df_w_key_20211201(spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    start_date = '2021-12-01'
    date = '641201'
    data = [("AYCAL", "GECAL", "HP", "43", "QWERTYU", date, start_date),
            ("AYCAL", "GECAL", "HP", "13", "ZXCVBNM", date, start_date),
            ("BAY", "GECAL", "HP", "11", "ASDFGHJ", date, start_date)]
    schema = StructType([
        StructField("entity_code", StringType(), False),
        StructField("company", StringType(), False),
        StructField("product", StringType(), False),
        StructField("branch", StringType(), False),
        StructField("contract", StringType(), False),
        StructField("data_date", StringType(), False),
        StructField("start_date", StringType(), False),
    ])
    df = spark_session.createDataFrame(data, schema)
    return df


@pytest.fixture
def mock_transaction_pst_df_w_key_20211202(spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    start_date = '2021-12-02'
    date = '641202'
    data = [("AYCAL", "GECAL", "HP", "43", "QWERTYU", date, start_date),
            ("BAY", "GECAL", "HP", "11", "ASDFGHJ", date, start_date),
            ("BAY", "GECAL", "HP", "12", "POIUYTR", date, start_date)]
    schema = StructType([
        StructField("entity_code", StringType(), False),
        StructField("company", StringType(), False),
        StructField("product", StringType(), False),
        StructField("branch", StringType(), False),
        StructField("contract", StringType(), False),
        StructField("data_date", StringType(), False),
        StructField("start_date", StringType(), False),
    ])
    df = spark_session.createDataFrame(data, schema)
    return df


def test_with_entity(mock_transaction_df_w_keys: pyspark.sql.dataframe.DataFrame,
                     spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    mock_transaction_df_w_keys.show(truncate=False)
    actual = DataFrameHelper().with_entity(mock_transaction_df_w_keys)
    actual.show(truncate=False)
    date = '2021-10-27'
    data = [("BAY", "GECAL", "HP", "13", "1118860", date, 'KA'),
            ("AYCAL", "GECAL", "HP", "11", "AB77053", date, 'AY'),
            ("AYCAL", "GECAL", "MC", "12", "AB77070", date, 'AY'),
            ("BAY", "GECAL", "HP", "11", "1122978", date, 'KA')]
    schema = StructType([
        StructField("entity_code", StringType(), False),
        StructField("company_code", StringType(), False),
        StructField("product_code", StringType(), False),
        StructField("branch_code", StringType(), False),
        StructField("contract_code", StringType(), False),
        StructField("data_date", StringType(), False),
        StructField("entity", StringType(), True)
    ])
    actual.printSchema()
    expected = spark_session.createDataFrame(data, schema)
    assert are_dfs_schema_equal(actual, expected)
    assert are_dfs_data_equal(actual, expected)
    spark_session.stop()


@pytest.fixture
def mock_lookup_product_keys(spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    date = '2021-10-27'
    data = [("1234abc", "MC"),
            ("567xyz", "HP")]
    schema = StructType([
        StructField("product_key", StringType(), False),
        StructField("product_id", StringType(), False)
    ])
    df = spark_session.createDataFrame(data, schema)
    return df


def test_with_product_key(mock_transaction_df_w_keys: pyspark.sql.dataframe.DataFrame,
                          mock_lookup_product_keys: pyspark.sql.dataframe.DataFrame,
                          spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    mock_transaction_df_w_keys.show(truncate=False)
    actual = DataFrameHelper().with_product_key(mock_transaction_df_w_keys, mock_lookup_product_keys)
    actual.show(truncate=False)
    date = '2021-10-27'
    p1 = '567xyz'
    p2 = '1234abc'
    data = [("BAY", "GECAL", "HP", "13", "1118860", date, p1),
            ("AYCAL", "GECAL", "HP", "11", "AB77053", date, p1),
            ("BAY", "GECAL", "HP", "11", "1122978", date, p1),
            ("AYCAL", "GECAL", "MC", "12", "AB77070", date, p2)]
    schema = StructType([
        StructField("entity_code", StringType(), False),
        StructField("company_code", StringType(), False),
        StructField("product_code", StringType(), False),
        StructField("branch_code", StringType(), False),
        StructField("contract_code", StringType(), False),
        StructField("data_date", StringType(), False),
        StructField("product_key", StringType(), True)
    ])
    actual.printSchema()
    expected = spark_session.createDataFrame(data, schema)
    assert are_dfs_schema_equal(actual, expected)
    assert are_dfs_data_equal(actual, expected)
    spark_session.stop()


def test_with_branch_key(mock_transaction_df_w_keys: pyspark.sql.dataframe.DataFrame,
                         spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    mock_transaction_df_w_keys.show(truncate=False)
    transaction_w_entity_df = DataFrameHelper().with_entity(mock_transaction_df_w_keys)
    transaction_w_entity_df.show(truncate=False)
    transaction_w_gecid_df = DataFrameHelper().with_gecid(transaction_w_entity_df)
    transaction_w_gecid_df.show(truncate=False)
    actual = DataFrameHelper().with_branch_key(transaction_w_gecid_df)
    actual.show(truncate=False)
    date = '2021-10-27'
    ka_entity = 'KA'
    ay_entity = 'AY'
    ka_gecid = '52800000'
    ay_gecid = '60000000'
    data = [("BAY", "GECAL", "HP", "13", "1118860", date, ka_entity, ka_gecid, '568b9ffc497efc1543c92f7c928bfdc8'),
            ("AYCAL", "GECAL", "HP", "11", "AB77053", date, ay_entity, ay_gecid, '63f913db121dde37ba6f5e9186b8c65d'),
            ("AYCAL", "GECAL", "MC", "12", "AB77070", date, ay_entity, ay_gecid, '0a04899648265150cd6d5f6081dc67a5'),
            ("BAY", "GECAL", "HP", "11", "1122978", date, ka_entity, ka_gecid, 'eae08e28ce562a8fa2a20b4c92f284f8')]
    ay_gecid = '60000000'
    ka_gecid = '52800000'
    schema = StructType([
        StructField("entity_code", StringType(), False),
        StructField("company_code", StringType(), False),
        StructField("product_code", StringType(), False),
        StructField("branch_code", StringType(), False),
        StructField("contract_code", StringType(), False),
        StructField("data_date", StringType(), False),
        StructField("entity", StringType(), True),
        StructField("gecid", StringType(), True),
        StructField("branch_key", StringType(), True)
    ])
    actual.printSchema()
    expected = spark_session.createDataFrame(data, schema)
    assert are_dfs_schema_equal(actual, expected)
    assert are_dfs_data_equal(actual, expected)
    spark_session.stop()


def test_with_account_key(mock_transaction_df_w_keys: pyspark.sql.dataframe.DataFrame,
                          spark_session: pyspark.sql.SparkSession) -> None:
    mock_transaction_df_w_keys.show(truncate=False)
    transaction_w_account_df = DataFrameHelper().with_account(mock_transaction_df_w_keys)
    transaction_w_account_df.show(truncate=False)
    actual = DataFrameHelper().with_account_key(transaction_w_account_df)
    actual.show(truncate=False)
    date = '2021-10-27'
    data = [("BAY", "GECAL", "HP", "13", "1118860", date, "1118860", '413d7ccfd29c29a9788812229d58b04c'),
            ("AYCAL", "GECAL", "HP", "11", "AB77053", date, "AB77053", '1fb3718882e90b64b55e824961fe3080'),
            ("AYCAL", "GECAL", "MC", "12", "AB77070", date, "AB77070", 'cbf006981d303927e3140e617ea1fceb'),
            ("BAY", "GECAL", "HP", "11", "1122978", date, "1122978", '3d6cf29da9fa4142d4c1f1fe2fdf0e52')]
    schema = StructType([
        StructField("entity_code", StringType(), False),
        StructField("company_code", StringType(), False),
        StructField("product_code", StringType(), False),
        StructField("branch_code", StringType(), False),
        StructField("contract_code", StringType(), False),
        StructField("data_date", StringType(), False),
        StructField("account_code", StringType(), True),
        StructField("account_key", StringType(), True)
    ])
    expected = spark_session.createDataFrame(data, schema)
    assert are_dfs_schema_equal(actual, expected)
    assert are_dfs_data_equal(actual, expected)
    spark_session.stop()


def test_rerun_with_table(mock_transaction_pst_df_w_keys: pyspark.sql.dataframe.DataFrame,
                          mock_transaction_pst_df_w_keys_20211028: pyspark.sql.dataframe.DataFrame,
                          mock_transaction_pst_df_w_key_20211101: pyspark.sql.dataframe.DataFrame,
                          mock_transaction_pst_df_w_key_20211102: pyspark.sql.dataframe.DataFrame,
                          mock_transaction_pst_df_w_key_20211201: pyspark.sql.dataframe.DataFrame,
                          mock_transaction_pst_df_w_key_20211202: pyspark.sql.dataframe.DataFrame,
                          mock_lookup_product_keys: pyspark.sql.dataframe.DataFrame,
                          mock_month_key_df: pyspark.sql.dataframe.DataFrame,
                          spark_session: pyspark.sql.SparkSession) -> None:
    spark_session.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark_session.sparkContext.setCheckpointDir("/tmp/checkpoint")
    config_path = '../sparkcore_test/helper/config/'
    transaction_table_config = TableConfig(config_path, 'local', 'transaction')
    transaction_table_property = TableProperty(db_name=transaction_table_config.db_name,
                                               tb_name=transaction_table_config.tb_name,
                                               table_path=transaction_table_config.table_path,
                                               fields=transaction_table_config.fields,
                                               partitions=transaction_table_config.partitions)
    print(
        transaction_table_property.create_table_sql(table_format=transaction_table_property.ORC_FORMAT, delimitor=None))
    spark_writer = SparkWriter(spark_session)
    spark_writer.create_table(transaction_table_property)
    transaction_df = spark_session.table(f'{transaction_table_property.database}.{transaction_table_property.table}')
    transaction_df.show(truncate=False)
    mock_transaction_pst_df_w_keys.write \
        .format("orc") \
        .mode("overwrite") \
        .partitionBy('start_date') \
        .saveAsTable(f'{transaction_table_property.database}.{transaction_table_property.table}')
    transaction_df = spark_session.table(f'{transaction_table_property.database}.{transaction_table_property.table}')
    transaction_df.show(truncate=False)
    snap_monthly_table_config = TableConfig(config_path, 'local', 'snap_monthly')
    snap_monthly_table_property = TableProperty(db_name=snap_monthly_table_config.db_name,
                                                tb_name=snap_monthly_table_config.tb_name,
                                                table_path=snap_monthly_table_config.table_path,
                                                fields=snap_monthly_table_config.fields,
                                                partitions=snap_monthly_table_config.partitions)
    print(
        snap_monthly_table_property.create_table_sql(table_format=snap_monthly_table_property.ORC_FORMAT,
                                                     delimitor=None))
    spark_writer.create_table(snap_monthly_table_property)
    snap_monthly_df = spark_session.table(f'{snap_monthly_table_property.database}.{snap_monthly_table_property.table}')
    snap_monthly_df.show(truncate=False)

    intermediate_transaction_df = transaction_df \
        .withColumnRenamed("company", "company_code") \
        .withColumnRenamed("product", "product_code") \
        .withColumnRenamed("branch", "branch_code") \
        .withColumnRenamed("contract", "account_code") \
        .drop('start_date')
    data_date = 'data_date'
    intermediate_transaction_df = DataFrameHelper().data_date_convert(intermediate_transaction_df, data_date)
    intermediate_transaction_df = DataFrameHelper().with_gecid(intermediate_transaction_df)
    intermediate_transaction_df = DataFrameHelper().with_entity(intermediate_transaction_df)
    intermediate_transaction_w_keys = DataFrameHelper() \
        .with_all_keys(intermediate_transaction_df, mock_lookup_product_keys) \
        .withColumn("contract_code", col("account_code")) \
        .drop("account_code") \
        .drop("entity") \
        .drop("gecid")
    intermediate_transaction_w_keys.show(truncate=False)
    intermediate_transaction_w_keys.printSchema()
    status_column = 'is_active'
    account_key = 'account_key'
    product_key = 'product_key'
    branch_key = 'branch_key'
    process_date = '2021-10-27'
    today_date = '2021-10-28'
    key_columns = [account_key, product_key, branch_key]
    snap_month_tb = f'{snap_monthly_table_property.database}.{snap_monthly_table_property.table}'
    DataFrameHelper().update_insert_status_snap_monthly_to_table(intermediate_transaction_w_keys,
                                                                 status_column,
                                                                 key_columns,
                                                                 process_date,
                                                                 today_date,
                                                                 month_key_df=mock_month_key_df,
                                                                 data_date_col_name=data_date,
                                                                 spark_session=spark_session,
                                                                 snap_month_table=snap_month_tb)
    snap_monthly_cond1 = spark_session.table(
        f'{snap_monthly_table_property.database}.{snap_monthly_table_property.table}')
    snap_monthly_cond1.show(truncate=False)
    # ==========end first time insert into snap monthly ================================================================
    # ==========start update snap monthly with 20211028 ================================================================
    mock_transaction_pst_df_w_keys_20211028.write \
        .format("orc") \
        .mode("overwrite") \
        .insertInto(f'{transaction_table_property.database}.{transaction_table_property.table}')
    process_date = '2021-10-28'
    today_date = '2021-10-29'
    transaction_df = spark_session.table(f'{transaction_table_property.database}.{transaction_table_property.table}')
    transaction_df.show(truncate=False)

    intermediate_transaction_df = transaction_df \
        .withColumnRenamed("company", "company_code") \
        .withColumnRenamed("product", "product_code") \
        .withColumnRenamed("branch", "branch_code") \
        .withColumnRenamed("contract", "account_code") \
        .drop('start_date')
    data_date = 'data_date'
    intermediate_transaction_df = DataFrameHelper().data_date_convert(intermediate_transaction_df, data_date)
    intermediate_transaction_df.show(truncate=False)
    # .where(col(data_date) == process_date)
    intermediate_transaction_df = DataFrameHelper().with_gecid(intermediate_transaction_df)
    intermediate_transaction_df = DataFrameHelper().with_entity(intermediate_transaction_df)
    intermediate_transaction_w_keys = DataFrameHelper() \
        .with_all_keys(intermediate_transaction_df, mock_lookup_product_keys) \
        .withColumn("contract_code", col("account_code")) \
        .drop("account_code") \
        .drop("entity") \
        .drop("gecid")
    print("transaction with joining keys")
    intermediate_transaction_w_keys.show(truncate=False)

    DataFrameHelper() \
        .update_insert_status_snap_monthly_to_table(transaction_df=intermediate_transaction_w_keys,
                                                    status_column=status_column,
                                                    key_columns=key_columns,
                                                    process_date=process_date,
                                                    today_date=today_date,
                                                    month_key_df=mock_month_key_df,
                                                    data_date_col_name=data_date,
                                                    spark_session=spark_session,
                                                    snap_month_table=f'{snap_monthly_table_property.database}.{snap_monthly_table_property.table}')
    snap_monthly_cond2 = spark_session.table(
        f'{snap_monthly_table_property.database}.{snap_monthly_table_property.table}')
    snap_monthly_cond2.show(truncate=False)
    # ==========end second insert into snap monthly ================================================================
    # # ==========start update snap monthly with 20211101 ================================================================
    mock_transaction_pst_df_w_key_20211101.write \
        .format("orc") \
        .mode("overwrite") \
        .insertInto(f'{transaction_table_property.database}.{transaction_table_property.table}')
    process_date = '2021-11-01'
    today_date = '2021-11-02'
    transaction_df = spark_session.table(f'{transaction_table_property.database}.{transaction_table_property.table}')
    transaction_df.show(truncate=False)

    intermediate_transaction_df = transaction_df \
        .withColumnRenamed("company", "company_code") \
        .withColumnRenamed("product", "product_code") \
        .withColumnRenamed("branch", "branch_code") \
        .withColumnRenamed("contract", "account_code") \
        .drop('start_date')
    data_date = 'data_date'
    intermediate_transaction_df = DataFrameHelper().data_date_convert(intermediate_transaction_df, data_date)
    intermediate_transaction_df.show(truncate=False)
    # .where(col(data_date) == process_date)
    intermediate_transaction_df = DataFrameHelper().with_gecid(intermediate_transaction_df)
    intermediate_transaction_df = DataFrameHelper().with_entity(intermediate_transaction_df)
    intermediate_transaction_w_keys = DataFrameHelper() \
        .with_all_keys(intermediate_transaction_df, mock_lookup_product_keys) \
        .withColumn("contract_code", col("account_code")) \
        .drop("account_code") \
        .drop("entity") \
        .drop("gecid")
    print("transaction with joining keys")
    intermediate_transaction_w_keys.show(truncate=False)

    DataFrameHelper() \
        .update_insert_status_snap_monthly_to_table(transaction_df=intermediate_transaction_w_keys,
                                                    status_column=status_column,
                                                    key_columns=key_columns,
                                                    process_date=process_date,
                                                    today_date=today_date,
                                                    month_key_df=mock_month_key_df,
                                                    data_date_col_name=data_date,
                                                    spark_session=spark_session,
                                                    snap_month_table=f'{snap_monthly_table_property.database}.{snap_monthly_table_property.table}')
    snap_monthly_cond3 = spark_session.table(
        f'{snap_monthly_table_property.database}.{snap_monthly_table_property.table}')
    snap_monthly_cond3.orderBy('month_key').show(truncate=False)
    # # ==========end second insert into snap monthly ================================================================
    # # ==========start update snap monthly with 20211102 ================================================================
    mock_transaction_pst_df_w_key_20211102.write \
        .format("orc") \
        .mode("overwrite") \
        .insertInto(f'{transaction_table_property.database}.{transaction_table_property.table}')
    process_date = '2021-11-02'
    today_date = '2021-11-03'
    transaction_df = spark_session.table(f'{transaction_table_property.database}.{transaction_table_property.table}')
    print('transaction_df 2021-11-02')
    transaction_df.orderBy(data_date).show(truncate=False)

    intermediate_transaction_df = transaction_df \
        .withColumnRenamed("company", "company_code") \
        .withColumnRenamed("product", "product_code") \
        .withColumnRenamed("branch", "branch_code") \
        .withColumnRenamed("contract", "account_code") \
        .drop('start_date')
    data_date = 'data_date'
    intermediate_transaction_df = DataFrameHelper().data_date_convert(intermediate_transaction_df, data_date)
    intermediate_transaction_df.orderBy(data_date).show(truncate=False)
    # .where(col(data_date) == process_date)
    intermediate_transaction_df = DataFrameHelper().with_gecid(intermediate_transaction_df)
    intermediate_transaction_df = DataFrameHelper().with_entity(intermediate_transaction_df)
    intermediate_transaction_w_keys = DataFrameHelper() \
        .with_all_keys(intermediate_transaction_df, mock_lookup_product_keys) \
        .withColumn("contract_code", col("account_code")) \
        .drop("account_code") \
        .drop("entity") \
        .drop("gecid")
    print("transaction with joining keys")
    print('transaction_df 2021-11-02 with key columns')
    intermediate_transaction_w_keys.orderBy(data_date).show(truncate=False)

    DataFrameHelper() \
        .update_insert_status_snap_monthly_to_table(transaction_df=intermediate_transaction_w_keys,
                                                    status_column=status_column,
                                                    key_columns=key_columns,
                                                    process_date=process_date,
                                                    today_date=today_date,
                                                    month_key_df=mock_month_key_df,
                                                    data_date_col_name=data_date,
                                                    spark_session=spark_session,
                                                    snap_month_table=f'{snap_monthly_table_property.database}.{snap_monthly_table_property.table}')
    snap_monthly_cond4 = spark_session.table(
        f'{snap_monthly_table_property.database}.{snap_monthly_table_property.table}')
    snap_monthly_cond4.orderBy('month_key').show(truncate=False)
    # ==========end second insert into snap monthly ================================================================
    # ==========start update snap monthly with 20211201 ================================================================
    mock_transaction_pst_df_w_key_20211201.write \
        .format("orc") \
        .mode("overwrite") \
        .insertInto(f'{transaction_table_property.database}.{transaction_table_property.table}')
    process_date = '2021-12-01'
    today_date = '2021-12-02'
    transaction_df = spark_session.table(f'{transaction_table_property.database}.{transaction_table_property.table}')
    print('transaction_df 2021-12-01')
    transaction_df.orderBy(data_date).show(truncate=False)

    intermediate_transaction_df = transaction_df \
        .withColumnRenamed("company", "company_code") \
        .withColumnRenamed("product", "product_code") \
        .withColumnRenamed("branch", "branch_code") \
        .withColumnRenamed("contract", "account_code") \
        .drop('start_date')
    data_date = 'data_date'
    intermediate_transaction_df = DataFrameHelper().data_date_convert(intermediate_transaction_df, data_date)
    intermediate_transaction_df.orderBy(data_date).show(truncate=False)
    # .where(col(data_date) == process_date)
    intermediate_transaction_df = DataFrameHelper().with_gecid(intermediate_transaction_df)
    intermediate_transaction_df = DataFrameHelper().with_entity(intermediate_transaction_df)
    intermediate_transaction_w_keys = DataFrameHelper() \
        .with_all_keys(intermediate_transaction_df, mock_lookup_product_keys) \
        .withColumn("contract_code", col("account_code")) \
        .drop("account_code") \
        .drop("entity") \
        .drop("gecid")
    print("transaction with joining keys")
    print('transaction_df 2021-12-01 with key columns')
    intermediate_transaction_w_keys.orderBy(data_date).show(truncate=False)

    DataFrameHelper() \
        .update_insert_status_snap_monthly_to_table(transaction_df=intermediate_transaction_w_keys,
                                                    status_column=status_column,
                                                    key_columns=key_columns,
                                                    process_date=process_date,
                                                    today_date=today_date,
                                                    month_key_df=mock_month_key_df,
                                                    data_date_col_name=data_date,
                                                    spark_session=spark_session,
                                                    snap_month_table=f'{snap_monthly_table_property.database}.{snap_monthly_table_property.table}')
    snap_monthly_cond5 = spark_session.table(
        f'{snap_monthly_table_property.database}.{snap_monthly_table_property.table}')
    snap_monthly_cond5.orderBy('month_key').show(truncate=False)
    # ==========end second insert into snap monthly ================================================================
    # ==========start update snap monthly with 20211202 ================================================================
    mock_transaction_pst_df_w_key_20211202.write \
        .format("orc") \
        .mode("overwrite") \
        .insertInto(f'{transaction_table_property.database}.{transaction_table_property.table}')
    process_date = '2021-12-02'
    today_date = '2021-12-03'
    transaction_df = spark_session.table(f'{transaction_table_property.database}.{transaction_table_property.table}')
    print('transaction_df 2021-12-02')
    transaction_df.orderBy(data_date).show(truncate=False)

    intermediate_transaction_df = transaction_df \
        .withColumnRenamed("company", "company_code") \
        .withColumnRenamed("product", "product_code") \
        .withColumnRenamed("branch", "branch_code") \
        .withColumnRenamed("contract", "account_code") \
        .drop('start_date')
    data_date = 'data_date'
    intermediate_transaction_df = DataFrameHelper().data_date_convert(intermediate_transaction_df, data_date)
    intermediate_transaction_df.orderBy(data_date).show(truncate=False)
    # .where(col(data_date) == process_date)
    intermediate_transaction_df = DataFrameHelper().with_gecid(intermediate_transaction_df)
    intermediate_transaction_df = DataFrameHelper().with_entity(intermediate_transaction_df)
    intermediate_transaction_w_keys = DataFrameHelper() \
        .with_all_keys(intermediate_transaction_df, mock_lookup_product_keys) \
        .withColumn("contract_code", col("account_code")) \
        .drop("account_code") \
        .drop("entity") \
        .drop("gecid")
    print("transaction with joining keys")
    print('transaction_df 2021-12-02 with key columns')
    intermediate_transaction_w_keys.orderBy(data_date).show(truncate=False)

    DataFrameHelper() \
        .update_insert_status_snap_monthly_to_table(transaction_df=intermediate_transaction_w_keys,
                                                    status_column=status_column,
                                                    key_columns=key_columns,
                                                    process_date=process_date,
                                                    today_date=today_date,
                                                    month_key_df=mock_month_key_df,
                                                    data_date_col_name=data_date,
                                                    spark_session=spark_session,
                                                    snap_month_table=f'{snap_monthly_table_property.database}.{snap_monthly_table_property.table}')
    snap_monthly_cond6 = spark_session.table(
        f'{snap_monthly_table_property.database}.{snap_monthly_table_property.table}')
    snap_monthly_cond6.orderBy('month_key').show(truncate=False)


@pytest.fixture
def mock_intermediate_mix_two_days_data_with_keys(
        spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    date20211027 = '2021-10-27'
    date20211028 = '2021-10-28'
    bay_entity = 'BAY'
    aycal_entity = 'AYCAL'
    company_code = 'GECAL'
    hp_code = 'HP'
    mc_code = 'MC'
    product_key1 = '567xyz'
    product_key2 = '1234abc'
    data = [(bay_entity, company_code, hp_code, '13', date20211027, product_key1, '568b9ffc497efc1543c92f7c928bfdc8',
             '413d7ccfd29c29a9788812229d58b04c', '1118860'),
            (bay_entity, company_code, hp_code, '11', date20211027, product_key1, 'eae08e28ce562a8fa2a20b4c92f284f8',
             '3d6cf29da9fa4142d4c1f1fe2fdf0e52', '1122978'),
            (bay_entity, company_code, hp_code, '11', date20211028, product_key1, 'eae08e28ce562a8fa2a20b4c92f284f8',
             '3d6cf29da9fa4142d4c1f1fe2fdf0e52', '1122978'),
            (aycal_entity, company_code, hp_code, '43', date20211028, product_key1, '3c0a855ab5fc9f4276a16f4c37961adf',
             '116781c1ce55d6cd0be159f56e18760e', 'BB00055'),
            (aycal_entity, company_code, hp_code, '11', date20211027, product_key1, '63f913db121dde37ba6f5e9186b8c65d',
             '1fb3718882e90b64b55e824961fe3080', 'AB77053'),
            (aycal_entity, company_code, mc_code, '12', date20211027, product_key2, '0a04899648265150cd6d5f6081dc67a5',
             'cbf006981d303927e3140e617ea1fceb', 'AB77070'),
            (aycal_entity, company_code, mc_code, '12', date20211028, product_key2, '0a04899648265150cd6d5f6081dc67a5',
             'cbf006981d303927e3140e617ea1fceb', 'AB77070')]
    schema = StructType([
        StructField("entity_code", StringType(), False),
        StructField("company_code", StringType(), False),
        StructField("product_code", StringType(), False),
        StructField("branch_code", StringType(), False),
        StructField("data_date", StringType(), False),
        StructField("product_key", StringType(), False),
        StructField("branch_key", StringType(), False),
        StructField("account_key", StringType(), False),
        StructField("contract_code", StringType(), False)
    ])
    df = spark_session.createDataFrame(data, schema)
    return df


@pytest.fixture
def mock_snap_monthly_with_keys(spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    date20211027 = '2021-10-27'
    date20211028 = '2021-10-28'
    bay_entity = 'BAY'
    aycal_entity = 'AYCAL'
    company_code = 'GECAL'
    hp_code = 'HP'
    mc_code = 'MC'
    product_key1 = '567xyz'
    product_key2 = '1234abc'
    active = 'active'
    data = [(bay_entity, company_code, hp_code, '13', '1118860', '413d7ccfd29c29a9788812229d58b04c', product_key1,
             '568b9ffc497efc1543c92f7c928bfdc8', date20211027, date20211028, active, 0),
            (bay_entity, company_code, hp_code, '11', '1122978', '3d6cf29da9fa4142d4c1f1fe2fdf0e52', product_key1,
             'eae08e28ce562a8fa2a20b4c92f284f8', date20211027, date20211028, active, 0),
            (aycal_entity, company_code, hp_code, '11', 'AB77053', '1fb3718882e90b64b55e824961fe3080', product_key1,
             '0a04899648265150cd6d5f6081dc67a5', date20211027, date20211028, active, 0),
            (aycal_entity, company_code, mc_code, '12', 'AB77070', 'cbf006981d303927e3140e617ea1fceb', product_key2,
             '0a04899648265150cd6d5f6081dc67a5', date20211027, date20211028, active, 0)]
    schema = StructType([
        StructField("entity_code", StringType(), False),
        StructField("company_code", StringType(), False),
        StructField("product_code", StringType(), False),
        StructField("branch_code", StringType(), False),
        StructField("contract_code", StringType(), False),
        StructField("account_key", StringType(), False),
        StructField("product_key", StringType(), False),
        StructField("branch_key", StringType(), False),
        StructField("data_date", StringType(), False),
        StructField("update_date", StringType(), False),
        StructField("is_active", StringType(), False),
        StructField("month_key", IntegerType(), False)])
    df = spark_session.createDataFrame(data, schema)
    return df


def test_update_insert(mock_intermediate_mix_two_days_data_with_keys: pyspark.sql.dataframe.DataFrame,
                       mock_snap_monthly_with_keys: pyspark.sql.dataframe.DataFrame,
                       spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    print('transaction df')
    mock_intermediate_mix_two_days_data_with_keys.show(truncate=False)
    print('snap month df')
    mock_snap_monthly_with_keys.show(truncate=False)
    data_date_col_name = 'data_date'
    LAST_UPDATE_DATE = 'last_update_date'
    MONTH_KEY = 'month_key'
    CURRENT_STATUS = 'current_status'
    TRANSACTION_START_DATE = 'transaction_start_date'
    process_date = '2021-10-28'
    today_date = '2021-10-29'
    target_month_key = 0
    ACTIVE = 'active'
    IS_ACTIVE = 'is_active'
    key_columns = ['account_key', 'product_key', 'branch_key', MONTH_KEY]
    UPDATE_DATE = 'update_date'
    hp_code = 'HP'
    mc_code = 'MC'
    transaction_of_month_key = mock_intermediate_mix_two_days_data_with_keys \
        .where(col(data_date_col_name) == process_date) \
        .withColumn(LAST_UPDATE_DATE, lit(today_date)) \
        .withColumn(MONTH_KEY, lit(target_month_key)) \
        .withColumn(CURRENT_STATUS, lit(ACTIVE)) \
        .withColumnRenamed(data_date_col_name, TRANSACTION_START_DATE)
    print('transaction with addition columns df')
    transaction_of_month_key.show(truncate=False)
    target_snap_month_key_df = mock_snap_monthly_with_keys.where(col(MONTH_KEY) == target_month_key).cache()
    additional_cols = [UPDATE_DATE, data_date_col_name, IS_ACTIVE]
    actual_df = DataFrameHelper().update_insert(transaction_df=transaction_of_month_key,
                                                snap_monthly_df=target_snap_month_key_df, status_column=IS_ACTIVE,
                                                key_columns=key_columns, data_date_col_name=data_date_col_name,
                                                additional_cols=additional_cols)
    actual_df.show(truncate=False)
    product_key1 = '567xyz'
    product_key2 = '1234abc'
    date20211027 = '2021-10-27'
    bay_entity = 'BAY'
    aycal_entity = 'AYCAL'
    company_code = 'GECAL'
    data = [
        ('cbf006981d303927e3140e617ea1fceb', product_key2, '0a04899648265150cd6d5f6081dc67a5', 0, today_date,
         process_date, 'active', aycal_entity, company_code, mc_code, '12', 'AB77070'),
        ('116781c1ce55d6cd0be159f56e18760e', product_key1, '3c0a855ab5fc9f4276a16f4c37961adf', 0, today_date,
         process_date, 'active', aycal_entity, company_code, hp_code, '43', 'BB00055'),
        ('1fb3718882e90b64b55e824961fe3080', product_key1, '0a04899648265150cd6d5f6081dc67a5', 0, process_date,
         date20211027, 'inactive', aycal_entity, company_code, hp_code, '11', 'AB77053'),
        ('3d6cf29da9fa4142d4c1f1fe2fdf0e52', product_key1, 'eae08e28ce562a8fa2a20b4c92f284f8', 0, today_date,
         process_date, 'active', bay_entity, company_code, hp_code, '11', '1122978'),
        ('413d7ccfd29c29a9788812229d58b04c', product_key1, '568b9ffc497efc1543c92f7c928bfdc8', 0, process_date,
         date20211027, 'inactive', bay_entity, company_code, hp_code, '13', '1118860')
    ]
    schema = StructType([
        StructField("account_key", StringType(), True),
        StructField("product_key", StringType(), True),
        StructField("branch_key", StringType(), True),
        StructField("month_key", IntegerType(), True),
        StructField("update_date", StringType(), True),
        StructField("data_date", StringType(), True),
        StructField("is_active", StringType(), True),
        StructField("entity_code", StringType(), True),
        StructField("company_code", StringType(), True),
        StructField("product_code", StringType(), True),
        StructField("branch_code", StringType(), True),
        StructField("contract_code", StringType(), True)
    ])
    expected_df = spark_session.createDataFrame(data, schema)
    actual_df.printSchema()
    assert are_dfs_schema_equal(actual_df, expected_df)
    assert are_dfs_data_equal(actual_df, expected_df)
