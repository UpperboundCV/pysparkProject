import pytest
from sparkcore.SparkCore import SparkCore
from sparkcore.helper.DataFrameHelper import DataFrameHelper
from sparkcore.helper.DateHelper import DateHelper
import pyspark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from sys import platform
import os

if platform == 'linux':
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64/"


@pytest.fixture
def spark_session() -> pyspark.sql.SparkSession:
    return SparkCore(mode='local').spark_session


@pytest.fixture
def mock_transaction_20211015(spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    date = "2021-10-15"
    # columns = ["start_date", "account_no"]
    account_no = 'account_no'
    schema = StructType([
        StructField(DataFrameHelper.START_DATE, StringType(), True),
        StructField(account_no, StringType(), True),
        ])
    data = [(date, "a01"), (date, "a02"), (date, "a03")]
    df = spark_session.createDataFrame(data,schema)
    return df

def are_dfs_schema_equal(df1: pyspark.sql.dataframe.DataFrame, df2: pyspark.sql.dataframe.DataFrame) -> bool:
    return False if df1.schema != df2.schema else True


def are_dfs_data_equal(df1: pyspark.sql.dataframe.DataFrame, df2: pyspark.sql.dataframe.DataFrame) -> bool:
    return False if df1.collect() != df2.collect() else True


def test_update_insert_status_snap_monthly(mock_transaction_20211015: pyspark.sql.dataframe.DataFrame,
                                           spark_session: pyspark.sql.SparkSession) -> None:
    mock_transaction_20211015.show(truncate=False)
    process_date = "2021-10-15"
    today_date = DateHelper().today_date()
    is_active = 'is_active'
    account_no = 'account_no'
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
    actual_df = DataFrameHelper().update_insert_status_snap_monthly(mock_transaction_20211015, snap_monthly_df,
                                                                    is_active, keys, process_date)
    actual_df.show(truncate=False)
    month_key = 0
    expected_first_stage = [(process_date, "a01", today_date, month_key,  DataFrameHelper.ACTIVE),
                            (process_date, "a02", today_date, month_key,  DataFrameHelper.ACTIVE),
                            (process_date, "a03", today_date, month_key,  DataFrameHelper.ACTIVE)]
    expected_schema = StructType([
        StructField(DataFrameHelper.START_DATE, StringType(), True),
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


