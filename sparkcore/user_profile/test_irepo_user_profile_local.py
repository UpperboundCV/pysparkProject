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
import itertools
from datetime import datetime

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


def to_intermediate_user_profile_df(transaction_df: pyspark.sql.dataframe.DataFrame,
                                    product_keys_df: pyspark.sql.dataframe.DataFrame,
                                    today_date: str) -> pyspark.sql.dataframe.DataFrame:
    intermediate_df = transaction_df \
        .withColumnRenamed('user_status', 'status') \
        .withColumnRenamed('updated_date', 'web_updated_date') \
        .withColumnRenamed('create_date', 'web_create_date') \
        .withColumnRenamed('sent_date', 'data_date')
    intermediate_w_product_key_df = DataFrameHelper.with_product_key(intermediate_df, product_keys_df)
    intermediate_w_gecid_df = DataFrameHelper.with_gecid(intermediate_w_product_key_df)
    intermediate_w_entity_df = DataFrameHelper.with_entity(intermediate_w_gecid_df)
    intermediate_w_branch_key_df = DataFrameHelper.with_branch_key(intermediate_w_entity_df)
    # process date is in timestamp format YYYY-MM-DD HH:MM:SS
    intermediate_w_update_date_df = intermediate_w_branch_key_df.withColumn('update_date', lit(today_date))
    intermediate_w_user_type_df = intermediate_w_update_date_df.withColumn('user_type', DataFrameHelper.add_user_type())
    return intermediate_w_update_date_df


def get_raw_ay_user_profile(spark_session: pyspark.sql.SparkSession, process_dates: List[str],
                            today_dates: List[str]) -> pyspark.sql.dataframe.DataFrame:
    source_txt_path1 = '../pysparkProject/sparkcore/user_profile/data/ay_user_profile_20211231_013020.txt'
    source_txt_path2 = '../pysparkProject/sparkcore/user_profile/data/ay_user_profile_20220101_132932.txt'
    source_txt_path3 = '../pysparkProject/sparkcore/user_profile/data/ay_user_profile_20220102_144013.txt'
    source_txt_paths = [source_txt_path1, source_txt_path2, source_txt_path3]
    dfs = [
        user_profile_transaction_df(spark_session, source_txt_path=source_txt_paths[i], process_date=process_dates[i],
                                    today_date=today_dates[i]) for i in range(len(process_dates))]
    df = reduce(DataFrame.unionByName, dfs)
    return df


def get_raw_ka_user_profile(spark_session: pyspark.sql.SparkSession, process_dates: List[str],
                            today_dates: List[str]) -> pyspark.sql.dataframe.DataFrame:
    source_txt_path1 = '../pysparkProject/sparkcore/user_profile/data/ka_user_profile_20211231_013020.txt'
    source_txt_path2 = '../pysparkProject/sparkcore/user_profile/data/ka_user_profile_20220101_171403.txt'
    source_txt_path3 = '../pysparkProject/sparkcore/user_profile/data/ka_user_profile_20220102_144022.txt'
    source_txt_paths = [source_txt_path1, source_txt_path2, source_txt_path3]
    dfs = [
        user_profile_transaction_df(spark_session, source_txt_path=source_txt_paths[i], process_date=process_dates[i],
                                    today_date=today_dates[i]) for i in range(len(process_dates))]
    df = reduce(DataFrame.unionByName, dfs)
    return df


def get_raw_pool_list_by_entity(entity: str, spark_session: pyspark.sql.SparkSession, process_dates: List[str],
                                today_dates: List[str]) -> Optional[pyspark.sql.dataframe.DataFrame]:
    if entity == 'ay':
        return get_raw_ay_user_profile(spark_session, process_dates, today_dates)
    elif entity == 'ka':
        return get_raw_ka_user_profile(spark_session, process_dates, today_dates)
    else:
        raise TypeError("entity is not recognized.")


def user_login_uam(ay_transaction_df: pyspark.sql.dataframe.DataFrame,
                   ka_transaction_df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    entities = ['ka', 'ay']
    entity_status = ['status', 'entity']
    key_list = ['user_login']
    entity_uam = 'entity_uam'
    ay_user_entity_df = ay_transaction_df.select('user_login', 'user_status', 'entity_code') \
        .withColumnRenamed('user_status', 'ay_status') \
        .withColumnRenamed('entity_code', 'ay_entity')
    ka_user_entity_df = ka_transaction_df.select('user_login', 'user_status', 'entity_code') \
        .withColumnRenamed('user_status', 'ka_status') \
        .withColumnRenamed('entity_code', 'ka_entity')
    all_entities_df = DataFrameHelper.combine_entity_df(ay_df=ay_user_entity_df, ka_df=ka_user_entity_df,
                                                        join_key=key_list)
    uam_cal_list = key_list + [entities + "_" + entity_status for
                               entity_status, entities in
                               itertools.product(entity_status, entities)]
    print('\n'.join(uam_cal_list))
    entity_status_labeled_df = all_entities_df.selectExpr(*uam_cal_list)
    # entity_status_labeled_df.show(truncate=False)
    # print(f'num_row = {entity_status_labeled_df.count()}')
    return entity_status_labeled_df.withColumn(entity_uam, DataFrameHelper.add_entity_uam())


def to_user_profile_uam_df(ay_transaction_df: pyspark.sql.dataframe.DataFrame,
                           ka_transaction_df: pyspark.sql.dataframe.DataFrame,
                           ay_product_keys_df: pyspark.sql.dataframe.DataFrame,
                           ka_product_keys_df: pyspark.sql.dataframe.DataFrame, entity: str,
                           today_date: str) -> pyspark.sql.dataframe.DataFrame:
    ay_intermediate_df = to_intermediate_user_profile_df(ay_transaction_df, ay_product_keys_df, today_date)
    ka_intermediate_df = to_intermediate_user_profile_df(ka_transaction_df, ka_product_keys_df, today_date)
    intermediate_df = ay_intermediate_df if entity == 'ay' else ka_intermediate_df
    user_login_uam_df = user_login_uam(ay_transaction_df, ka_transaction_df).select('user_login', 'entity_uam')
    intermediate_user_profile_uam_df = intermediate_df.join(user_login_uam_df, on=['user_login'], how='inner')
    return intermediate_user_profile_uam_df


def process_user_profile_from_pst_to_crt(spark_session: pyspark.sql.SparkSession, entity: str, process_date: str,
                                         today_date: str,
                                         ay_transaction_df: pyspark.sql.dataframe.DataFrame,
                                         ka_transaction_df: pyspark.sql.dataframe.DataFrame,
                                         ay_product_key_df: pyspark.sql.dataframe.DataFrame,
                                         ka_product_key_df: pyspark.sql.dataframe.DataFrame,
                                         month_key_df: pyspark.sql.dataframe.DataFrame,
                                         environment: str) -> None:
    intermediate_user_profile_df = to_user_profile_uam_df(ay_transaction_df, ka_transaction_df, ay_product_key_df,
                                                          ka_product_key_df, entity, today_date)

    config_path = "../user_profile/config/"
    snap_monthly_table_config = TableConfig(config_path, environment, f'{entity}_user_profile_curate')
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
    data_date_col = 'data_date'
    user_login = 'user_login'
    product_key = 'product_key'
    branch_key = 'branch_key'
    key_columns = [user_login, product_key, branch_key]

    snap_monthly_table = f"{snap_monthly_table_property.database}.{snap_monthly_table_property.table}"
    DataFrameHelper.update_insert_status_snap_monthly_to_table(transaction_df=intermediate_user_profile_df,
                                                               status_column='status',
                                                               key_columns=key_columns,
                                                               process_date=process_date,
                                                               today_date=today_date,
                                                               month_key_df=month_key_df,
                                                               data_date_col_name=data_date_col,
                                                               spark_session=spark_session,
                                                               snap_month_table=snap_monthly_table
                                                               )


if __name__ == '__main__':
    print('\n'.join(sys.path))
    env = 'local'
    spark_core = SparkCore('local')
    spark_core.spark_session.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark_core.spark_session.sparkContext.setCheckpointDir("/tmp/checkpoint")
    process_date_lst = ['2021-12-31', '2022-01-01', '2022-01-02']
    today_date_lst = ['2022-01-01', '2022-01-02', '2022-01-03']
    today_date = datetime.today().strftime('%Y-%m-%d')
    entities = ['ka', 'ay']
    # test single case

    ay_transaction_df = user_profile_transaction_df(spark_session=spark_core.spark_session,
                                                    source_txt_path='../pysparkProject/sparkcore/user_profile/data/ay_user_profile_20211231_013020.txt',
                                                    process_date='2021-12-31',
                                                    today_date='2022-01-01')
    ka_transaction_df = user_profile_transaction_df(spark_session=spark_core.spark_session,
                                                    source_txt_path='../pysparkProject/sparkcore/user_profile/data/ka_user_profile_20211231_013020.txt',
                                                    process_date='2021-12-31',
                                                    today_date='2022-01-01')
    ay_transaction_df.show(truncate=False)
    ka_transaction_df.show(truncate=False)

    # process_user_profile_from_pst_to_crt(spark_session=spark_core.spark_session, entity='ay', process_date='2021-12-31',
    #                                      today_date=today_date,
    #                                      ay_transaction_df=ay_transaction_df, ka_transaction_df=ka_transaction_df,
    #                                      ay_product_key_df=mock_ay_lookup_product_keys(spark_core.spark_session),
    #                                      ka_product_key_df=mock_ka_lookup_product_keys(spark_core.spark_session),
    #                                      month_key_df=mock_month_key_df(spark_core.spark_session),
    #                                      environment=env)
    #
    # snap_monthly_df = spark_core.spark_session.table('aydev_test.irepo_user_profile')
    # snap_monthly_df.groupBy('month_key','ptn_month_key').agg(count('*').cast(IntegerType()).alias('total')).show(truncate=False)
