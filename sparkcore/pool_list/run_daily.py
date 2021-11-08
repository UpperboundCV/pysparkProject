# --------------------------------------------------
# OBJECTIVE : pool_list pyspark
# CREATE BY : Krisorn Chunhapongpipat [UP]
# CREATE DATE : 2021/11/02
# --------------------------------------------------

# IMPORT
import os
import sys
import argparse

# PySpark Environment
os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/CDH/lib/spark/"
os.environ["PYTHONPATH"] = "/opt/cloudera/parcels/CDH/lib/spark/python"
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_232-cloudera/"
sys.path.append("/opt/cloudera/parcels/CDH/lib/spark/python")
sys.path.append('/nfs/msa/dapscripts/ka/pln/dev/tfm/pys/collection/irepo/sparkcore')
sys.path.append("/opt/cloudera/parcels/CDH/lib/spark/python/lib/py4j-0.10.7-src.zip")
from reader.SparkReader import SparkReader
from SparkCore import SparkCore
from helper.DataFrameHelper import DataFrameHelper
from configProvider.TableConfig import TableConfig
from writer.TableProperty import TableProperty
from writer.SparkWriter import SparkWriter
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType, IntegerType, IntegralType, \
    DoubleType
from pyspark.sql.functions import lit, col
import pyspark
from typing import List
from sys import platform
from datetime import datetime


def intermediate_df(transaction_df: pyspark.sql.dataframe.DataFrame,
                    product_keys_df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    intermediate_transaction_df = transaction_df \
        .withColumnRenamed("company", "company_code") \
        .withColumnRenamed("product", "product_code") \
        .withColumnRenamed("branch", "branch_code") \
        .withColumnRenamed("contract", "account_code") \
        .withColumnRenamed("engine_no", "engine_number") \
        .withColumnRenamed("body", "body_number") \
        .withColumnRenamed("od_amount", "overdue_amount") \
        .withColumnRenamed("os_balance", "outstanding_balance") \
        .withColumnRenamed("due", "payment_due_date") \
        .withColumnRenamed("haircut_reason_desc", "haircut_reason_description") \
        .withColumnRenamed("last_paydate", "last_pay_date") \
        .withColumnRenamed("last_payperiod", "last_pay_period") \
        .withColumnRenamed("no_period_passdue", "number_period_pass_due") \
        .withColumnRenamed("bill_code_wo", "bill_code_scom") \
        .withColumnRenamed("bill_desc_wo", "bill_code_scom_description") \
        .withColumnRenamed("overdue_threshold", "collection_overdue_threshold") \
        .withColumnRenamed("req_flag", "ocpb_flag") \
        .drop('start_date')
    data_date = 'data_date'
    intermediate_transaction_df = DataFrameHelper().data_date_convert(intermediate_transaction_df, data_date)
    intermediate_transaction_df.orderBy(data_date).show(truncate=False)
    # .where(col(data_date) == process_date)
    intermediate_transaction_df = DataFrameHelper().with_gecid(intermediate_transaction_df)
    intermediate_transaction_df = DataFrameHelper().with_entity(intermediate_transaction_df)
    intermediate_transaction_w_keys = DataFrameHelper() \
        .with_all_keys(intermediate_transaction_df, product_keys_df) \
        .withColumn("contract_code", col("account_code")) \
        .drop("account_code") \
        .drop("entity") \
        .drop("gecid")
    print("transaction with joining keys")
    print('transaction_df 2021-12-02 with key columns')
    intermediate_transaction_w_keys.orderBy(data_date).show(truncate=False)
    return intermediate_transaction_w_keys

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-e", "--env", required=True, help="environment: local, dev, or prod")
    ap.add_argument("-p", "--process_date", required=True, help="data date to process")
    args = vars(ap.parse_args())
    try:
        if args['env'] == 'dev' or args['env'] == 'prod':
            env = args['env']
            process_date = args['process_date']
            config_path = "../pool_list/config/"
            ka_transaction_table_config = TableConfig(config_path, env, 'ka_pool_list_persist')
            ka_transaction_table = f'{ka_transaction_table_config.db_name}.{ka_transaction_table_config.tb_name}'
            ay_transaction_table_config = TableConfig(config_path, env, 'ay_pool_list_persist')
            ay_transaction_table = f'{ay_transaction_table_config.db_name}.{ay_transaction_table_config.tb_name}'
            snap_monthly_table_config = TableConfig(config_path, env, 'pool_list_curate')
            snap_month_table = f'{snap_monthly_table_config.db_name}.{snap_monthly_table_config.tb_name}'
            ka_product_key_config = TableConfig(config_path, env, 'ka_product_key')
            ka_product_key_table = f'{ka_product_key_config.db_name}.{ka_product_key_config.tb_name}'
            ay_product_key_config = TableConfig(config_path, env, 'ay_product_key')
            ay_product_key_table = f'{ay_product_key_config.db_name}.{ay_product_key_config.tb_name}'
            spark_core = SparkCore(env, f"irepo_{process_date}")
            os.system(f'hdfs dfs -mkdir -p {snap_monthly_table_config.check_point_path}')
            spark_core.spark_session.sparkContext.setCheckpointDir(snap_monthly_table_config.check_point_path)
            raw_ka_transaction_df = spark_core.spark_session.table(ka_transaction_table)
            raw_ka_transaction_df.show(truncate=False)
            raw_ay_transaction_df = spark_core.spark_session.table(ay_transaction_table)
            raw_ay_transaction_df.show(truncate=False)
            raw_transaction_df = raw_ka_transaction_df.unionByName(raw_ay_transaction_df)
            raw_transaction_df.show(truncate=False)
            # todo: check whether ka_month_key and ay_month_key have duplicated key to each other or not.
            ka_product_key_df = spark_core.spark_session.table(ka_product_key_table)
            ay_product_key_df = spark_core.spark_session.table(ay_product_key_table)
            product_key_df = ka_product_key_df.unionByName(ay_product_key_df)
            product_key_df.show(truncate=False)
            intermediate_transaction_df = intermediate_df(transaction_df  = raw_transaction_df,
                                                          product_keys_df = product_key_df)
            intermediate_transaction_df.show(truncate=False) 
            snap_monthly_table_config = TableConfig(config_path, env, 'pool_list_curate')
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
            #snap_monthly_writer=SparkWriter(spark_core.spark_session)
            #snap_monthly_writer.create_table(snap_monthly_table_property)
            snap_monthly_db = f'{snap_monthly_table_property.database}'
            snap_monthly_tb = f'{snap_monthly_table_property.table}'
            snap_monthly_table = f'{snap_monthly_db}.{snap_monthly_tb}' 
            empty_snap_monthly_df = spark_core.spark_session.table(snap_monthly_table)
            empty_snap_monthly_df.show(truncate=False)
            
            data_date_col = 'data_date'
            status_column = 'is_active'
            account_key   = 'account_key'
            product_key   = 'product_key'
            branch_key    = 'branch_key'
            key_columns   = [account_key, product_key, branch_key]

            month_key_table_config = TableConfig(config_path, env, 'ka_month_key')
            month_key_table_property = TableProperty(db_name=month_key_table_config.db_name,
                                                     tb_name=month_key_table_config.tb_name,
                                                     table_path=month_key_table_config.table_path,
                                                     fields=month_key_table_config.fields,
                                                     partitions=month_key_table_config.partitions)
            month_key_db = f'{month_key_table_property.database}'
            month_key_tb = f'{month_key_table_property.table}'
            month_key_df = spark_core.spark_session.table(f'{month_key_db}.{month_key_tb}')
            
            
            current_process_date = process_date
            current_today_date = datetime.now().strftime('%Y-%m-%d')
            DataFrameHelper().update_insert_status_snap_monthly_to_table(transaction_df=intermediate_transaction_df,
                                                                         status_column=status_column,
                                                                         key_columns=key_columns,
                                                                         process_date=current_process_date,
                                                                         today_date=current_today_date,
                                                                         month_key_df=month_key_df,
                                                                         data_date_col_name=data_date_col,
                                                                         spark_session=spark_core.spark_session,
                                                                         snap_month_table=snap_monthly_table)
            
            snap_monthly_final = spark_core.spark_session.table(snap_monthly_table)
            snap_monthly_final.show(truncate=False)
        else:
            raise TypeError(f"input environment is not right: {args['env']}")
    except Exception as e:
            raise TypeError(f"Process on process date: {args['process_date']} error: {e}")
