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
import itertools

# cdp PySpark Environment
os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/CDH/lib/spark/"
os.environ["PYTHONPATH"] = "/opt/cloudera/parcels/CDH/lib/spark/python"
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_232-cloudera/"
sys.path.append("/opt/cloudera/parcels/CDH/lib/spark/python")
sys.path.append('/nfs/msa/dapscripts/ka/pln/dev/tfm/pys/collection/user_profile/sparkcore/')
sys.path.append("/opt/cloudera/parcels/CDH/lib/spark/python/lib/py4j-0.10.7-src.zip")

from reader.SparkReader import SparkReader
from SparkCore import SparkCore
from helper.DataFrameHelper import DataFrameHelper
from helper.DateHelper import DateHelper
from configProvider.TableConfig import TableConfig
from writer.TableProperty import TableProperty
from writer.SparkWriter import SparkWriter


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

    ext_ka_input_user_profile = ka_transaction_df.withColumn('sent_date',
                                                             date_trunc("day", col('sent_date')))
    # ext_ka_input_user_profile.show(truncate=False)
    uam_df = get_entity_uam_df(ext_ay_input_user_profile, ext_ka_input_user_profile)

    # add entity_uam back to transaction_df
    transaction_w_entity_uam_df = ext_ay_input_user_profile \
        .join(uam_df, on=['user_login'], how='left') if entity == 'ay' else ext_ka_input_user_profile \
        .join(uam_df, on=['user_login'], how='left')
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
    ap = argparse.ArgumentParser()
    ap.add_argument("-v", "--env", required=True, help="environment: local, dev, or prod")
    ap.add_argument("-e", "--entity", required=True, help="entity: it can be only ka or ay (lower case)")
    ap.add_argument("-p", "--process_date", required=True, help="data date to process")
    args = vars(ap.parse_args())
    try:
        if args['env'] == 'dev' or args['env'] == 'prod':
            env = args['env']
            entity = args['entity']
            process_date = args['process_date']
            config_path = "../user_profile/config/"
            today_date = datetime.now().strftime('%Y-%m-%d')
            # instantiate spark session
            spark_core = SparkCore(env, f"{entity}_user_profile_{process_date}")

            spark_core.spark_session.sparkContext.setLogLevel("ERROR")

            # Get transaction user profile Df
            # ay
            ay_trans_user_profile_conf = TableConfig(config_path, env, f"ay_user_profile_persist")
            ay_trans_user_profile_table = f'{ay_trans_user_profile_conf.db_name}.{ay_trans_user_profile_conf.tb_name}'
            ay_trans_user_profile_df = spark_core.spark_session.table(ay_trans_user_profile_table) \
                .where(DateHelper.timestamp2str(col('sent_date')) == process_date)
            num_row_ay_input_row = ay_trans_user_profile_df.count()
            print(f'num_row_ay_row: {num_row_ay_input_row}')
            # ka
            ka_trans_user_profile_conf = TableConfig(config_path, env, f"ka_user_profile_persist")
            ka_trans_user_profile_table = f'{ka_trans_user_profile_conf.db_name}.{ka_trans_user_profile_conf.tb_name}'
            ka_trans_user_profile_df = spark_core.spark_session.table(ka_trans_user_profile_table) \
                .where(DateHelper.timestamp2str(col('sent_date')) == process_date)
            num_row_ka_input_row = ka_trans_user_profile_df.count()
            print(f'num_row_ka_row: {num_row_ka_input_row}')
            if num_row_ay_input_row == 0 | num_row_ka_input_row == 0:
                ay_report = f'{ay_trans_user_profile_table}:{num_row_ay_input_row}'
                ka_report = f'{ka_trans_user_profile_table}:{num_row_ka_input_row}'
                raise TypeError(f"one of transaction input df's is empty => {ay_report}  | {ka_report} ")

            # # # # Get product key look up DF
            product_key_df = cdp_product_key(f'{entity}_product_key')

            # # # # # Get month key look up DF
            month_key_df = cdp_month_key(f'{entity}_month_key')

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

            snap_month_df = spark_core.spark_session.table(snap_monthly_table)
            snap_month_df.show(n=5, truncate=False)
            snap_month_df.groupBy('ptn_month_key', 'user_type', 'status', 'entity_uam').agg(
                count('*').cast(IntegerType()).alias('total')).show(truncate=False)
            spark_core.close_session()
        else:
            raise TypeError(f"input environment is not supported: {args['env']}")
    except Exception as e:
        raise TypeError(f"Process on process date: {args['process_date']} error: {e}")
