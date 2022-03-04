# --------------------------------------------------
# OBJECTIVE : user profile pyspark
# CREATE BY : Krisorn Chunhapongpipat [UP]
# CREATE DATE : 2022/02/04
# --------------------------------------------------

# IMPORT
import os
import sys
import argparse
from datetime import datetime
from pyspark.sql.functions import lit, col, count
import pyspark
from sys import platform
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType, IntegerType, IntegralType, \
    DoubleType
from pyspark.sql.functions import date_trunc, to_timestamp, when, length
from typing import List, Optional, Dict

sys.path.append('/home/up_python/PycharmProjects/pysparkProject/sparkcore')

from reader.SparkReader import SparkReader
from SparkCore import SparkCore
from helper.DataFrameHelper import DataFrameHelper
from helper.DateHelper import DateHelper
from configProvider.TableConfig import TableConfig
from writer.TableProperty import TableProperty
from writer.SparkWriter import SparkWriter
import itertools

if platform == 'linux':
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64/"


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


def cdp_product_key(config_path: str, env: str, config_name: str) -> pyspark.sql.dataframe.DataFrame:
    product_key_config = TableConfig(config_path, env, config_name)
    product_key_table = f'{product_key_config.db_name}.{product_key_config.tb_name}'
    return spark_core.spark_session.table(product_key_table)


def cdp_month_key(config_path: str, env: str, config_name: str) -> pyspark.sql.dataframe.DataFrame:
    month_key_table_config = TableConfig(config_path, env, config_name)
    month_key_table = f'{month_key_table_config.db_name}.{month_key_table_config.tb_name}'
    return spark_core.spark_session.table(month_key_table)


def get_entity_uam_df(ext_ay_input_user_profile: pyspark.sql.dataframe.DataFrame,
                      ext_ka_input_user_profile: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    user_status = 'user_status'
    user_login = 'user_login'
    entities = ['ka', 'ay']
    entity_status = ['status', 'entity']
    key_list = [user_login, 'sent_date']
    entity_uam = 'entity_uam'
    entity_code = 'entity_code'
    # ext_ka_input_user_profile.show(truncate=False)
    # combine entity ay and ka
    ay_user_entity_df = ext_ay_input_user_profile \
        .withColumn(user_status,
                    when((col(user_status).isNull()) | (col(user_status) == ' '), lit('Inactive')).otherwise(
                        col(user_status))) \
        .withColumnRenamed(user_status, 'ay_status') \
        .withColumnRenamed(entity_code, 'ay_entity') \
        .withColumn(user_login, when((col(user_login).isNull()) | (col(user_login) == ' ') | (col(user_login) == ''),
                                     lit('ay_none')).otherwise(col(user_login)))

    ka_user_entity_df = ext_ka_input_user_profile \
        .withColumn(user_status,
                    when((col(user_status).isNull()) | (col(user_status) == ' '), lit('Inactive')).otherwise(
                        col(user_status))) \
        .withColumnRenamed(user_status, 'ka_status') \
        .withColumnRenamed(entity_code, 'ka_entity') \
        .withColumn(user_login, when((col(user_login).isNull()) | (col(user_login) == ' ') | (col(user_login) == ''),
                                     lit('ka_none')).otherwise(col(user_login)))
    all_entities_df = DataFrameHelper.combine_entity_df(ay_df=ay_user_entity_df, ka_df=ka_user_entity_df,
                                                        join_key=key_list)

    # label entity to status and entity
    uam_cal_list = key_list + [entities + "_" + entity_status for
                               entity_status, entities in
                               itertools.product(entity_status, entities)]
    print('\n'.join(uam_cal_list))
    entity_status_labeled_df = all_entities_df.selectExpr(*uam_cal_list)

    # Get user_login entity_uam
    uam_df = entity_status_labeled_df.withColumn(entity_uam, DataFrameHelper.add_entity_uam())

    final_result = uam_df \
        .select(user_login, entity_uam) \
        .withColumn(user_login, when(col(user_login).contains("_none"), lit(None)).otherwise(col(user_login)))

    return final_result


def ay_mapping_date_and_source_file() -> Dict[str, str]:
    return {'2021-12-31': '../pysparkProject/sparkcore/user_profile/data/ay_user_profile_20211231_184543.txt',
            '2022-01-01': '../pysparkProject/sparkcore/user_profile/data/ay_user_profile_20220101_115726.txt',
            '2022-01-02': '../pysparkProject/sparkcore/user_profile/data/ay_user_profile_20220102_115934.txt'}


def ka_mapping_date_and_source_file() -> Dict[str, str]:
    return {'2021-12-31': '../pysparkProject/sparkcore/user_profile/data/ka_user_profile_20211231_185223.txt',
            '2022-01-01': '../pysparkProject/sparkcore/user_profile/data/ka_user_profile_20220101_115733.txt',
            '2022-01-02': '../pysparkProject/sparkcore/user_profile/data/ka_user_profile_20220102_115920.txt'}


def ext_user_profile_schema() -> List[str]:
    return ['user_login',
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
    expr_str = [f'{col_names[i]} as {user_profile_cols[i]}' for i in range(len(user_profile_cols))]
    ext_user_profile_df = df.selectExpr(*expr_str)
    return ext_user_profile_df \
        .withColumn("start_date", lit(today_date).cast(DateType())) \
        .withColumn('end_date', lit(end_date).cast(DateType())) \
        .withColumn('business_date', lit(business_date).cast(TimestampType())) \
        .withColumn('load_date', lit(load_date).cast(TimestampType())) \
        .withColumn('record_deleted_flag', lit(record_deleted_flag).cast(IntegerType())) \
        .withColumn('pipeline_name', lit(pipeline_name)) \
        .withColumn('execution_id', lit(execution_id).cast(IntegerType())) \
        .withColumn('updated_date', to_timestamp(col('updated_date'), DateHelper.HIVE_TIMESTAMP_FORMAT)) \
        .withColumn('create_date', to_timestamp(col('create_date'), DateHelper.HIVE_TIMESTAMP_FORMAT)) \
        .withColumn('sent_date', to_timestamp(col('sent_date'), DateHelper.HIVE_TIMESTAMP_FORMAT))


def get_user_profile_transaction_df(spark_session: pyspark.sql.SparkSession,
                                    process_date: str,
                                    today_date: str,
                                    entity: str) -> pyspark.sql.dataframe.DataFrame:
    print(f'file path name ay: {ay_mapping_date_and_source_file()[process_date]}')
    print(f'file path name ka: {ka_mapping_date_and_source_file()[process_date]}')
    ay_trans_df = user_profile_transaction_df(spark_session,
                                              ay_mapping_date_and_source_file()[process_date],
                                              process_date,
                                              today_date)
    ka_trans_df = user_profile_transaction_df(spark_session,
                                              ka_mapping_date_and_source_file()[process_date],
                                              process_date,
                                              today_date)
    return ay_trans_df if entity == 'ay' else ka_trans_df


def process_user_profile_from_pst_to_crt(spark_session: pyspark.sql.SparkSession,
                                         ay_transaction_df: pyspark.sql.dataframe.DataFrame,
                                         ka_transaction_df: pyspark.sql.dataframe.DataFrame,
                                         product_key_df: pyspark.sql.dataframe.DataFrame,
                                         month_key_df: pyspark.sql.dataframe.DataFrame,
                                         entity: str,
                                         process_date: str,
                                         today_date: str,
                                         data_date_col_name: str,
                                         snap_month_table: str) -> None:
    ext_ay_input_user_profile = ay_transaction_df.withColumn('sent_date',
                                                             date_trunc("day", col('sent_date')))
    # ext_ay_input_user_profile.show(truncate=False)

    ext_ka_input_user_profile = ka_transaction_df.withColumn('sent_date',
                                                             date_trunc("day", col('sent_date')))
    # ext_ka_input_user_profile.show(truncate=False)
    uam_df = get_entity_uam_df(ext_ay_input_user_profile, ext_ka_input_user_profile)

    # add entity_uam back to transaction_df
    transaction_w_entity_uam_df = ext_ay_input_user_profile.join(uam_df, on=['user_login'],
                                                                 how='left') if entity == 'ay' else ext_ka_input_user_profile.join(
        uam_df, on=['user_login'], how='left')
    # transaction_w_entity_uam_df.show(n=100, truncate=False)
    # add user_type
    transaction_w_user_type_df = transaction_w_entity_uam_df \
        .withColumn('user_type', DataFrameHelper.add_user_type()) \
        .withColumnRenamed('user_status', 'status') \
        .withColumnRenamed('updated_date', 'web_updated_date') \
        .withColumnRenamed('create_date', 'web_create_date') \
        .withColumnRenamed('sent_date', 'data_date')
    print('rename column: user_status -> status')
    print('rename column: updated_date -> web_updated_date')
    print('rename column: create_date -> web_create_date')
    print('rename column: sent_date -> data_date')
    # transaction_w_user_type_df.show(n=7, truncate=False)
    intermediate_w_product_key_df = DataFrameHelper.with_short_product_key(transaction_w_user_type_df).repartition(
        col('product_key'))
    print('add product key')
    intermediate_w_gecid_df = DataFrameHelper.with_gecid(intermediate_w_product_key_df)
    print('add gecid')
    intermediate_w_entity_df = DataFrameHelper.with_entity(intermediate_w_gecid_df)
    print('add entity')
    intermediate_w_branch_key_df = DataFrameHelper.with_branch_key(intermediate_w_entity_df).cache()
    print('add branch key')

    DataFrameHelper.update_insert_snap_monthly_to_table(transaction_df=intermediate_w_branch_key_df,
                                                        process_date=process_date, today_date=today_date,
                                                        month_key_df=month_key_df,
                                                        data_date_col_name=data_date_col_name,
                                                        spark_session=spark_session,
                                                        snap_month_table=snap_month_table
                                                        )


if __name__ == '__main__':
    env = 'local'
    config_path = "../user_profile/config/"
    today_date = datetime.now().strftime('%Y-%m-%d')
    process_dates = ay_mapping_date_and_source_file().keys()
    entities = ['ka', 'ay']
    # instantiate spark session
    spark_core = SparkCore(env, f"process_user_profile_local")
    spark_core.spark_session.sparkContext.setLogLevel("ERROR")
    spark_core.spark_session.sql(f'drop table if exists kadev_pst_test.ext_irepo_user_profile')
    spark_core.spark_session.sql(f'drop table if exists aydev_pst_test.ext_irepo_user_profile')
    for entity, process_date in itertools.product(entities, process_dates):
        # Get transaction user profile Df
        # ay
        ay_trans_user_profile_conf = TableConfig(config_path, env, f"ay_user_profile_persist")
        ay_trans_user_profile_table = f'{ay_trans_user_profile_conf.db_name}.{ay_trans_user_profile_conf.tb_name}'
        ay_trans_user_profile_df = (
            get_user_profile_transaction_df(spark_core.spark_session, process_date, today_date,
                                            'ay') if env == 'local' else spark_core.spark_session.table(
                ay_trans_user_profile_table)).where(DateHelper.timestamp2str(col('sent_date')) == process_date)
        num_row_ay_input_row = ay_trans_user_profile_df.count()

        print(f'num_row_ay_row: {num_row_ay_input_row}')
        # ka
        ka_trans_user_profile_conf = TableConfig(config_path, env, f"ka_user_profile_persist")
        ka_trans_user_profile_table = f'{ka_trans_user_profile_conf.db_name}.{ka_trans_user_profile_conf.tb_name}'
        ka_trans_user_profile_df = (
            get_user_profile_transaction_df(spark_core.spark_session, process_date, today_date,
                                            'ka') if env == 'local' else spark_core.spark_session.table(
                ka_trans_user_profile_table)).where(DateHelper.timestamp2str(col('sent_date')) == process_date)
        num_row_ka_input_row = ka_trans_user_profile_df.count()

        print(f'num_row_ka_row: {num_row_ka_input_row}')
        if num_row_ay_input_row == 0 | num_row_ka_input_row == 0:
            ay_report = f'{ay_trans_user_profile_table}:{num_row_ay_input_row}'
            ka_report = f'{ka_trans_user_profile_table}:{num_row_ka_input_row}'
            raise TypeError(f"one of transaction input df's is empty => {ay_report}  | {ka_report} ")
        # # # #
        # # # # Get product key look up DF
        product_key_df = (get_look_up_product_keys_by_entity(entity,
                                                             spark_core.spark_session) if env == 'local' else cdp_product_key(
            f'{entity}_product_key'))
        # # # #
        # # # # # Get month key look up DF
        month_key_df = (
            mock_month_key_df(spark_core.spark_session) if env == 'local' else cdp_month_key(
                f'{entity}_month_key'))
        # # #
        # # # # Get snap month table name
        snap_monthly_table_config = TableConfig(config_path, env, f'{entity}_user_profile_curate')
        snap_monthly_table = f'{snap_monthly_table_config.db_name}.{snap_monthly_table_config.tb_name}'
        # # # Create snap monthly table if it doesn't exist
        snap_monthly_table_property = TableProperty(db_name=snap_monthly_table_config.db_name,
                                                    tb_name=snap_monthly_table_config.tb_name,
                                                    table_path=snap_monthly_table_config.table_path,
                                                    fields=snap_monthly_table_config.fields,
                                                    partitions=snap_monthly_table_config.partitions)
        print(snap_monthly_table_property.database)
        print(snap_monthly_table_property.table)
        print(snap_monthly_table_property.table_path)
        print(snap_monthly_table_property.create_table_sql(table_format=snap_monthly_table_property.ORC_FORMAT,
                                                           delimitor=None))
        snap_monthly_writer = SparkWriter(spark_core.spark_session)
        snap_monthly_writer.create_table(snap_monthly_table_property)
        # # # #
        process_user_profile_from_pst_to_crt(spark_session=spark_core.spark_session,
                                             ay_transaction_df=ay_trans_user_profile_df,
                                             ka_transaction_df=ka_trans_user_profile_df,
                                             product_key_df=product_key_df,
                                             month_key_df=month_key_df,
                                             entity=entity,
                                             process_date=process_date,
                                             today_date=today_date,
                                             data_date_col_name='data_date',
                                             snap_month_table=snap_monthly_table)
        # #
        snap_month_df = spark_core.spark_session.table(snap_monthly_table)
        snap_month_df.show(n=5, truncate=False)
        snap_month_df.groupBy('entity_code','ptn_month_key', 'user_type', 'status', 'entity_uam', 'data_date').agg(
            count('*').cast(IntegerType()).alias('total')).sort(col('ptn_month_key'), col('user_type'),
                                                                col('entity_uam')).show(n=1000,truncate=False)
    spark_core.close_session()