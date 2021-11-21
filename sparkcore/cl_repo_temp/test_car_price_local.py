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
from pyspark.sql.functions import lit, col, count, countDistinct, max, when
from pyspark.sql import DataFrame
from typing import List, Optional, Dict
from sys import platform
import pyspark

if platform == 'linux':
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64/"


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


def sfwrpo00_transaction_df(spark_session: pyspark.sql.SparkSession,
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
    sfwrpo00_cols = sfwrpo00_schema()
    expr_str = [f'{col_names[i]} as {sfwrpo00_cols[i]}' for i in range(len(col_names))]
    sfwrpo00_df = df.selectExpr(*expr_str)
    return sfwrpo00_df \
        .withColumn("start_date", lit(today_date).cast(DateType())) \
        .withColumn('end_date', lit(end_date).cast(DateType())) \
        .withColumn('business_date', lit(business_date).cast(TimestampType())) \
        .withColumn('load_date', lit(load_date).cast(TimestampType())) \
        .withColumn('record_deleted_flag', lit(record_deleted_flag).cast(IntegerType())) \
        .withColumn('pipeline_name', lit(pipeline_name)) \
        .withColumn('execution_id', lit(execution_id).cast(IntegerType()))


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


def get_process_lst_dates(entity: str) -> List[str]:
    return ['2021-10-02', '2021-10-05'] if entity == 'ay' else ['2021-05-01', '2021-07-08']


def get_today_date_lst(entity: str) -> List[str]:
    return ['2021-10-03', '2021-10-06'] if entity == 'ay' else ['2021-05-02', '2021-07-09']


def get_raw_ay_sfwrpo00(spark_session: pyspark.sql.SparkSession,
                        process_dates: List[str]) -> pyspark.sql.dataframe.DataFrame:
    source_txt_path1 = '../pysparkProject/sparkcore/cl_repo_temp/data/ay_sfwrpo00_20211002_113737.txt'
    source_txt_path2 = '../pysparkProject/sparkcore/cl_repo_temp/data/AY_SFWRPO00_20211005_171148.txt'
    source_txt_paths = [source_txt_path1, source_txt_path2]
    today_date_lst = get_today_date_lst('ay')
    dfs = [sfwrpo00_transaction_df(spark_session=spark_session, source_txt_path=source_txt_paths[i],
                                   process_date=process_dates[i], today_date=today_date_lst[i]) for i in
           range(len(today_date_lst))]
    df = reduce(DataFrame.unionByName, dfs)
    return df


def get_raw_ka_sfwpo00(spark_session: pyspark.sql.SparkSession,
                       process_dates: List[str]) -> pyspark.sql.dataframe.DataFrame:
    source_txt_path1 = '../pysparkProject/sparkcore/cl_repo_temp/data/KA_SFWRPO00_20210501_194921.txt'
    source_txt_path2 = '../pysparkProject/sparkcore/cl_repo_temp/data/KA_SFWRPO00_20210708_164256.txt'
    source_txt_paths = [source_txt_path1, source_txt_path2]
    today_date_lst = get_today_date_lst('ka')
    dfs = [sfwrpo00_transaction_df(spark_session=spark_session, source_txt_path=source_txt_paths[i],
                                   process_date=process_dates[i], today_date=today_date_lst[i]) for i in
           range(len(today_date_lst))]
    df = reduce(DataFrame.unionByName, dfs)
    return df


def get_sfwpo00_by_entity(entity: str, spark_session: pyspark.sql.SparkSession, process_dates: List[str],
                          today_dates: List[str]) -> Optional[pyspark.sql.dataframe.DataFrame]:
    if entity == 'ay':
        return get_raw_ay_sfwrpo00(spark_session, process_dates)
    elif entity == 'ka':
        return get_raw_ka_sfwpo00(spark_session, process_dates)
    else:
        raise TypeError('entity is not recognized.')


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
    print('\n'.join(sys.path))
    environment = 'local'
    spark_core = SparkCore(environment)
    spark_reader = SparkReader(spark_core.spark_session)

    config_path = "../cl_repo_temp/config/"

    entity = 'ay'
    cl_repo_temp_config = TableConfig(config_path, environment, f'{entity}_crt_cl_repo_temp')
    cl_repo_temp_property = TableProperty(db_name=cl_repo_temp_config.db_name,
                                          tb_name=cl_repo_temp_config.tb_name,
                                          table_path=cl_repo_temp_config.table_path,
                                          fields=cl_repo_temp_config.fields,
                                          partitions=cl_repo_temp_config.partitions)
    process_dates = get_process_lst_dates(entity)
    today_date_lst = get_today_date_lst(entity)
    sfwrpo00_df = get_sfwpo00_by_entity(entity, spark_session=spark_core.spark_session, process_dates=process_dates,
                                        today_dates=today_date_lst)
    sfwrpo00_df.groupby('start_date', 'O2TRDT').agg(count("*").alias("total")).show(truncate=False)
    look_up_product_key_df = get_look_up_product_keys_by_entity(entity, spark_core.spark_session)
    intermediate_df = to_car_price_df(sfwrpo00_df=sfwrpo00_df, look_up_product_df=look_up_product_key_df,
                                      cl_repo_table_config=cl_repo_temp_config, entity=entity)

    cl_repo_cols = cl_repo_temp_config.column_to_data_type().keys()
    cl_repo_temp_df = intermediate_df.select(*cl_repo_cols)

    cl_repo_temp_df.show(5,truncate=False)
    cl_repo_temp_df.groupby('data_date').agg(count("*").alias("total")).show(truncate=False)
    cl_repo_temp_df.printSchema()

    cl_repo_temp_writer = SparkWriter(spark_core.spark_session)
    cl_repo_temp_writer.create_table(cl_repo_temp_property)
    cl_repo_temp_table = f'{cl_repo_temp_config.db_name}.{cl_repo_temp_config.tb_name}'
    cl_repo_temp_df.write.format("orc").insertInto(cl_repo_temp_table,overwrite=True)

    result_df = spark_core.spark_session.table(cl_repo_temp_table)
    result_df.groupby('data_date').agg(count("*").alias("total")).show(truncate=False)

