import os
import sys
from functools import reduce

sys.path.append('/home/up_python/PycharmProjects/pysparkProject/sparkcore')

from reader.SparkReader import SparkReader
from SparkCore import SparkCore
from helper.DataFrameHelper import DataFrameHelper
from configProvider.TableConfig import TableConfig
from writer.TableProperty import TableProperty
from writer.SparkWriter import SparkWriter
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType, IntegerType, IntegralType, \
    DoubleType
from pyspark.sql.functions import lit, col, count, countDistinct
from pyspark.sql import DataFrame
from typing import List, Optional
from sys import platform
import pyspark

if platform == 'linux':
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64/"


def ext_user_profile_schema() -> List[str]:
    return ['end_date',
            'business_date',
            'load_date',
            'record_deleted_flag',
            'pipeline_name',
            'execution_id',
            'user_login',
            'user_status',
            'entity_code',
            'org_code',
            'product_code',
            'branch_code',
            'main_cr_code',
            'sub_cr_code',
            'col_id',
            'corporate_name',
            'position',
            'sup_code',
            'enabled_app',
            'user_id',
            'icollect_id',
            'irepo_type',
            'issue_date',
            'expire_date',
            'atd_flag',
            'col_team',
            'job_limit',
            'job_current',
            'job_available',
            'updated_by',
            'updated_date',
            'create_date',
            'sent_date']


def user_profile_transaction_df(spark_session: pyspark.sql.SparkSession,
                                source_txt_path: str,
                                process_date: str,
                                today_date: str) -> pyspark.sql.dataframe.DataFrame:
    end_date = '9000-12-31'
    business_date = process_date + ' 00:00:00'
    load_date = process_date + ' 23:59:59'
    record_deleted_flag = 0
    pipeline_name = 'test ingestion in local'
    execution_id = '1'
    spark_reader = SparkReader(spark_session)
    df = spark_reader.read_txt_file(txt_path=source_txt_path,
                                    have_header=False,
                                    delimiter='~', is_infershema=True)
    col_names = df.columns
    user_profile_cols = ext_user_profile_schema()
    expr_str = [f'{col_names[i]} as {user_profile_cols[i]}' for i in range(len(col_names))]
    ext_user_profile_df = df.selectExpr(*expr_str)
    return ext_user_profile_df \
        .withColumn("start_date", lit(today_date).cast(DateType())) \
        .withColumn('end_date', lit(end_date).cast(DateType())) \
        .withColumn('business_date', lit(business_date).cast(TimestampType())) \
        .withColumn('load_date', lit(load_date).cast(TimestampType())) \
        .withColumn('record_deleted_flag', lit(record_deleted_flag).cast(IntegerType())) \
        .withColumn('pipeline_name', lit(pipeline_name)) \
        .withColumn('execution_id', lit(execution_id).cast(IntegerType())) \
        .withColumn('updated_date', lit('updated_date').cast(TimestampType())) \
        .withColumn('create_date', lit('create_date').cast(TimestampType())) \
        .withColumn('sent_date', lit('sent_date').cast(TimestampType()))


def mock_ay_lookup_product_keys(spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    date = '2021-10-27'
    data = [("1234abc", "MC"),
            ("567xyz", "HP")]
    schema = StructType([
        StructField("product_key", StringType(), False),
        StructField("product_id", StringType(), False)
    ])
    df = spark_session.createDataFrame(data, schema)
    return df


def mock_ka_lookup_product_keys(spark_session: pyspark.sql.SparkSession) -> pyspark.sql.dataframe.DataFrame:
    date = '2021-10-27'
    data = [("67894abc", "MC"),
            ("12345qwe", "HP")]
    schema = StructType([
        StructField("product_key", StringType(), False),
        StructField("product_id", StringType(), False)
    ])
    df = spark_session.createDataFrame(data, schema)
    return df


def get_look_up_product_keys_by_entity(entity: str, spark_session: pyspark.sql.SparkSession) -> Optional[
    pyspark.sql.dataframe.DataFrame]:
    if entity == 'ay':
        return mock_ay_lookup_product_keys(spark_session)
    elif entity == 'ka':
        return mock_ka_lookup_product_keys(spark_session)
    else:
        raise TypeError("entity is not recognized.")


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


def to_user_profile_df(transaction_df: pyspark.sql.dataframe.DataFrame,
                       product_keys_df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    intermediate_transaction_df = transaction_df \
        .withColumnRenamed('user_status', 'status') \
        .withColumnRenamed('updated_date', 'web_updated_date') \
        .withColumnRenamed('create_date', 'web_create_date') \
        .withColumnRenamed('sent_date', 'data_date')


if __name__ == '__main__':
    print('\n'.join(sys.path))
