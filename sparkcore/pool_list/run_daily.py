# --------------------------------------------------
# OBJECTIVE : pool_list pyspark
# CREATE BY : Krisorn Chunhapongpipat [UP]
# CREATE DATE : 2021/11/10
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


def to_intermediate_pool_list_df(transaction_df: pyspark.sql.dataframe.DataFrame,
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
    contract_date = 'contract_date'
    last_paydate = 'last_pay_date'
    last_pay_date_wo = 'last_pay_date_wo'
    intermediate_transaction_df = DataFrameHelper().data_date_convert(intermediate_transaction_df, data_date)
    intermediate_transaction_df = DataFrameHelper().data_date_convert(intermediate_transaction_df, contract_date)
    intermediate_transaction_df = DataFrameHelper().data_date_convert(intermediate_transaction_df, last_paydate)
    intermediate_transaction_df = DataFrameHelper().data_date_convert(intermediate_transaction_df, last_pay_date_wo)
    intermediate_transaction_df.orderBy(data_date).show(truncate=False)
    # .where(col(data_date) == process_date)
    intermediate_transaction_df = DataFrameHelper().with_gecid(intermediate_transaction_df)
    intermediate_transaction_df = DataFrameHelper().with_entity(intermediate_transaction_df)
    intermediate_transaction_w_keys = DataFrameHelper() \
        .with_all_keys(intermediate_transaction_df, product_keys_df) \
        .withColumn("contract_code", col("account_code")) \
        .withColumn(data_date, col(data_date).cast(DateType())) \
        .withColumn(contract_date, col(contract_date).cast(DateType())) \
        .withColumn(last_paydate, col(last_paydate).cast(DateType())) \
        .withColumn(last_pay_date_wo, col(last_pay_date_wo).cast(DateType())) \
        .drop("account_code") \
        .drop("entity") \
        .drop("gecid")
    intermediate_transaction_final = DataFrameHelper().to_entity_dwh(intermediate_transaction_w_keys)
    intermediate_transaction_final.orderBy(data_date).show(truncate=False)
    return intermediate_transaction_final


def process_pool_list_from_pst_to_crt(process_date: str, today_date: str,
                                      raw_transaction_df: pyspark.sql.dataframe.DataFrame,
                                      product_key_df: pyspark.sql.dataframe.DataFrame,
                                      month_key_df: pyspark.sql.dataframe.DataFrame,
                                      spark_session: pyspark.sql.SparkSession,
                                      snap_monthly_table: str) -> None:
    intermediate_pool_list_df = to_intermediate_pool_list_df(raw_transaction_df, product_key_df)

    data_date_col = 'data_date'
    status_column = 'is_active'
    account_key = 'account_key'
    product_key = 'product_key'
    branch_key = 'branch_key'
    key_columns = [account_key, product_key, branch_key]

    DataFrameHelper().update_insert_status_snap_monthly_to_table(transaction_df=intermediate_pool_list_df,
                                                                 status_column=status_column,
                                                                 key_columns=key_columns,
                                                                 process_date=process_date,
                                                                 today_date=today_date,
                                                                 month_key_df=month_key_df,
                                                                 data_date_col_name=data_date_col,
                                                                 spark_session=spark_session,
                                                                 snap_month_table=snap_monthly_table)


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
            config_path = "../pool_list/config/"
            today_date = datetime.now().strftime('%Y-%m-%d')
            # instantiate spark session
            spark_core = SparkCore(env, f"{entity}_pool_list_{process_date}")
            # Get transaction pool list DF
            transaction_pool_list_table_config = TableConfig(config_path, env, f'{entity}_pool_list_persist')
            transaction_pool_list_table = f'{transaction_pool_list_table_config.db_name}.{transaction_pool_list_table_config.tb_name}'
            transaction_pool_list_df = spark_core.spark_session.table(transaction_pool_list_table)
            # Get product key look up DF
            product_key_config = TableConfig(config_path, env, f'{entity}_product_key')
            product_key_table = f'{product_key_config.db_name}.{product_key_config.tb_name}'
            product_key_df = spark_core.spark_session(product_key_table)
            # Get month key look up DF
            month_key_table_config = TableConfig(config_path, env, f'{entity}_month_key')
            month_key_table = f'{month_key_table_config.db_name}.{month_key_table_config.tb_name}'
            month_key_df = spark_core.spark_session(month_key_table)
            # Get snap monthly table name
            snap_monthly_table_config = TableConfig(config_path, env, f'{entity}_pool_list_curate')
            snap_monthly_table = f'{snap_monthly_table_config.db_name}.{snap_monthly_table_config.tb_name}'
            # Create snap monthly table if it doesn't exist
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
            # process pool list snap monthly
            process_pool_list_from_pst_to_crt(process_date=process_date, today_date=today_date,
                                              raw_transaction_df=transaction_pool_list_df,
                                              product_key_df=product_key_df,
                                              month_key_df=product_key_df,
                                              spark_session=spark_core.spark_session,
                                              snap_monthly_table=snap_monthly_table)
        else:
            raise TypeError(f"input environment is not right: {args['env']}")
    except Exception as e:
        raise TypeError(f"Process on process date: {args['process_date']} error: {e}")
