# --------------------------------------------------
# OBJECTIVE : cl_repo_temp pyspark
# CREATE BY : Krisorn Chunhapongpipat [UP]
# CREATE DATE : 2021/11/22
# MODIFIED DATE: 2021/11/22
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
from pyspark.sql.functions import lit, col, count, countDistinct, max, when
from pyspark.sql import DataFrame
from typing import List, Optional, Dict
from sys import platform
import pyspark
from datetime import datetime


def sfwrpo00_schema() -> List[str]:
    return [
        "O2PDTH",
        "O2BRNO",
        "O2CTNO",
        "O2TRDT",
        "O2BILL",
        "O2BIDT",
        "O2PBIL",
        "O2PBDT",
        "O2ADTE",
        "O2BKVL",
        "O2SAMT",
        "O2STS",
        "O2SDAT",
        "O2HLDT",
        "O2B7DT",
        "O2LOSS",
        "O2RECV",
        "O2SYST",
        "O2OACD",
        "O2SCOD",
        "O2SCDS",
        "O2CCDT",
        "O2EPFL",
        "O2RFG2",
        "O2BDPR",
    ]


def column_mapping() -> Dict[str, str]:
    return {
        "O2PDTH": "product_code",
        "O2BRNO": "branch_code",
        "O2CTNO": "contract_code",
        "O2TRDT": "data_date",
        "O2BILL": "bill_code",
        "O2BIDT": "bill_code_date",
        "O2PBIL": "prev_bill_code",
        "O2PBDT": "prev_bill_code_date",
        "O2ADTE": "status_date",
        "O2BKVL": "book_value",
        "O2SAMT": "sale_amount",
        "O2STS": "status",
        "O2SDAT": "sale_date",
        "O2HLDT": "hold_date",
        "O2B7DT": "change_bill7_date",
        "O2LOSS": "loss_value",
        "O2RECV": "success_recovery_amt",
        "O2SYST": "group_data_key",
        "O2OACD": "cr_code",
        "O2SCOD": "s_code",
        "O2SCDS": "s_code_desc",
        "O2CCDT": "cut_cost_date",
        "O2EPFL": "expand_flag",
        "O2RFG2": "car_grade",
        "O2BDPR": "bid_price",
    }


def pretty_dict_print(printed_dict: Dict[str, str]) -> None:
    return print("\n".join("{}\t{}".format(k, v) for k, v in printed_dict.items()))


def expr_str(col_source: str, col_type: str, col_name: str) -> str:
    expr = f'cast({col_source} as {col_type}) as {col_name}'
    print(expr)
    return expr


def sfwrpo00_cols_to_car_price_cols(sfwpo00_date_cols: List[str], car_price_col_desc: Dict[str, str]) -> List[str]:
    return [expr_str(sfwrpo00_col, "string", column_mapping()[sfwrpo00_col])
            if sfwrpo00_col in sfwpo00_date_cols
            else expr_str(sfwrpo00_col, car_price_col_desc[column_mapping()[sfwrpo00_col]],
                          column_mapping()[sfwrpo00_col])
            for sfwrpo00_col in column_mapping().keys()]


def to_car_price_df(sfwrpo00_df: pyspark.sql.dataframe.DataFrame,
                    look_up_product_df: pyspark.sql.dataframe.DataFrame,
                    cl_repo_table_config: TableConfig,
                    entity: str) -> pyspark.sql.dataframe.DataFrame:
    sfwrpo00_date_group = ['O2TRDT', 'O2BIDT', 'O2PBDT', 'O2ADTE', 'O2SDAT', 'O2HLDT', 'O2B7DT', 'O2CCDT']
    data_date_col = 'data_date'
    max_date_col = 'max_date'
    col_desc_lst = cl_repo_table_config.column_to_data_type()
    pretty_dict_print(col_desc_lst)
    expr_lst = sfwrpo00_cols_to_car_price_cols(sfwpo00_date_cols=sfwrpo00_date_group, car_price_col_desc=col_desc_lst)
    map_cols_df = sfwrpo00_df.selectExpr(*expr_lst)
    max_data_date = map_cols_df.agg(max(col(data_date_col)).alias(max_date_col)).select(max_date_col).collect()[0][
        max_date_col]
    non_empty_data_date_df = map_cols_df.withColumn(data_date_col,
                                                    when((col(data_date_col) == '0') | (col(data_date_col).isNull()),
                                                         lit(max_data_date)).otherwise(col(data_date_col)))
    car_price_date_group = [column_mapping()[sfwrpo00_date_col] for sfwrpo00_date_col in sfwrpo00_date_group]
    convert_date_col_group_df = DataFrameHelper().convert_as400_data_date_to_timestamp(non_empty_data_date_df,
                                                                                       car_price_date_group)
    add_entity_code_df = DataFrameHelper().with_entity_code(convert_date_col_group_df, entity)
    add_gecid_df = DataFrameHelper().with_gecid(add_entity_code_df)
    add_company_df = DataFrameHelper().with_company(add_gecid_df)
    add_entity_df = DataFrameHelper().with_entity(add_company_df)
    add_account_code_df = DataFrameHelper().with_account(add_entity_df)
    add_join_keys_df = DataFrameHelper().with_all_keys(transaction_df=add_account_code_df,
                                                       look_up_product_df=look_up_product_df)
    final_df = add_join_keys_df.drop(DataFrameHelper().ENTITY_CODE) \
        .drop(DataFrameHelper().ACCOUNT_CODE) \
        .drop('company_code') \
        .drop('entity')
    return final_df


if __name__ == "__main__":
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
            config_path = "../cl_repo_temp/config/"
            # instantiate spark session
            spark_core = SparkCore(env, f"{entity}_cl_repo_temp_{process_date}")
            # Get product key look up DF
            product_key_config = TableConfig(config_path, env, f'{entity}_product_key')
            product_key_table = f'{product_key_config.db_name}.{product_key_config.tb_name}'
            product_key_df = spark_core.spark_session.table(product_key_table)
            # Get transaction cl_repo df
            sfwrpo00_config = TableConfig(config_path, env, f'{entity}_pst_cl_repo_temp')
            sfwrpo00_table = f'{sfwrpo00_config.db_name}.{sfwrpo00_config.tb_name}'
            # note that col('O2TRDT') can not have 0 or null.
            sfwrpo00_df = spark_core.spark_session.table(sfwrpo00_table).where(col('start_date') == process_date)
            # create cl_repo_temp
            cl_repo_temp_config = TableConfig(config_path, env, f'{entity}_crt_cl_repo_temp')
            cl_repo_temp_property = TableProperty(db_name=cl_repo_temp_config.db_name,
                                                  tb_name=cl_repo_temp_config.tb_name,
                                                  table_path=cl_repo_temp_config.table_path,
                                                  fields=cl_repo_temp_config.fields,
                                                  partitions=cl_repo_temp_config.partitions)
            cl_repo_temp_writer = SparkWriter(spark_core.spark_session)
            cl_repo_temp_writer.create_table(cl_repo_temp_property)
            # process cl_repo_temp data
            intermediate_df = to_car_price_df(sfwrpo00_df=sfwrpo00_df, look_up_product_df=product_key_df,
                                              cl_repo_table_config=cl_repo_temp_config, entity=entity)
            cl_repo_cols = cl_repo_temp_config.column_to_data_type().keys()
            cl_repo_temp_df = intermediate_df.select(*cl_repo_cols)
            # preview data
            cl_repo_temp_df.show(5, truncate=False)
            cl_repo_temp_df.groupby('data_date').agg(count("*").alias("total")).show(truncate=False)
            cl_repo_temp_df.printSchema()
            cl_repo_temp_table = f'{cl_repo_temp_config.db_name}.{cl_repo_temp_config.tb_name}'
            cl_repo_temp_df.write.format("orc").insertInto(cl_repo_temp_table, overwrite=True)
        else:
            raise TypeError(f"input environment is not right: {args['env']}")
    except Exception as e:
        raise TypeError(f"Process date: {args['process_date']} error: {e}")
