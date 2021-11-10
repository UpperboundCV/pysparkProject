import os
import sys
from functools import reduce

sys.path.append('/home/up_python/PycharmProjects/pysparkProject/sparkcore')
# os.environ['PYTHONPATH'] = '/home/up_python/PycharmProjects/pysparkProject/sparkcore'
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


def irepo_schema() -> List[str]:
    return ["entity_code",
            "status",
            "company",
            "product",
            "branch",
            "contract",
            "car_registration",
            "car_register_province",
            "brand",
            "model",
            "cyear",
            "color",
            "engine_no",
            "body",
            "overdue_period",
            "od_amount",
            "os_balance",
            "contract_date",
            "bill_code",
            "program_id",
            "data_date",
            "legal_type",
            "judge_type",
            "legal_step",
            "due",
            "haircut_flag",
            "haircut_reason_code",
            "haircut_reason_desc",
            "a1",
            "fpd_flag",
            "term",
            "last_paydate",
            "last_payperiod",
            "no_period_passdue",
            "bill_code_wo",
            "bill_desc_wo",
            "last_pay_date_wo",
            "last_pay_amount_wo",
            "overdue_threshold",
            "auto_model_group",
            "auto_type_group",
            "req_flag"
            ]


def irepo_transaction_df(spark_session: pyspark.sql.SparkSession,
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
    irepo_cols = irepo_schema()
    expr_str = [f'{col_names[i]} as {irepo_cols[i]}' for i in range(len(col_names))]
    irepo_df = df.selectExpr(*expr_str)
    return irepo_df \
        .withColumn("start_date", lit(today_date).cast(DateType())) \
        .withColumn('end_date', lit(end_date).cast(DateType())) \
        .withColumn('business_date', lit(business_date).cast(TimestampType())) \
        .withColumn('load_date', lit(load_date).cast(TimestampType())) \
        .withColumn('record_deleted_flag', lit(record_deleted_flag).cast(IntegerType())) \
        .withColumn('pipeline_name', lit(pipeline_name)) \
        .withColumn('execution_id', lit(execution_id).cast(IntegerType())) \
        .withColumn('od_amount', col('od_amount').cast(DoubleType())) \
        .withColumn('os_amount', col('os_balance').cast(DoubleType())) \
        .withColumn('last_pay_amount_wo', col('last_pay_amount_wo').cast(DoubleType())) \
        .withColumn('overdue_threshold', col('overdue_threshold').cast(DoubleType()))


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
    print("transaction with joining keys")
    print('transaction_df 2021-12-02 with key columns')
    intermediate_transaction_final.orderBy(data_date).show(truncate=False)
    return intermediate_transaction_final


def get_raw_pool_list_by_entity(entity: str, spark_session: pyspark.sql.SparkSession, process_dates: List[str],
                                today_dates: List[str]) -> Optional[pyspark.sql.dataframe.DataFrame]:
    if entity == 'ay':
        return get_raw_ay_pool_list(spark_session, process_dates, today_dates)
    elif entity == 'ka':
        return get_raw_ka_pool_list(spark_session, process_dates, today_dates)
    else:
        raise TypeError("entity is not recognized.")


def get_raw_ay_pool_list(spark_session: pyspark.sql.SparkSession, process_dates: List[str],
                         today_dates: List[str]) -> pyspark.sql.dataframe.DataFrame:
    source_txt_path1 = '../pysparkProject/sparkcore/pool_list/data/aycal_irepo_pool_20211020_134420.txt'
    source_txt_path2 = '../pysparkProject/sparkcore/pool_list/data/aycal_irepo_pool_20211021_134522.txt'
    source_txt_path3 = '../pysparkProject/sparkcore/pool_list/data/aycal_irepo_pool_20211022_134913.txt'
    source_txt_path4 = '../pysparkProject/sparkcore/pool_list/data/aycal_irepo_pool_20211023_134949.txt'
    source_txt_path5 = '../pysparkProject/sparkcore/pool_list/data/aycal_irepo_pool_20211030_135026.txt'
    source_txt_path6 = '../pysparkProject/sparkcore/pool_list/data/aycal_irepo_pool_20211031_135108.txt'
    source_txt_path7 = '../pysparkProject/sparkcore/pool_list/data/aycal_irepo_pool_20211101_082844.txt'
    source_txt_path8 = '../pysparkProject/sparkcore/pool_list/data/aycal_irepo_pool_20211102_082929.txt'
    source_txt_path9 = '../pysparkProject/sparkcore/pool_list/data/aycal_irepo_pool_20211103_082955.txt'

    source_txt_paths = [source_txt_path1, source_txt_path2, source_txt_path3, source_txt_path4, source_txt_path5,
                        source_txt_path6, source_txt_path7, source_txt_path8, source_txt_path9]
    dfs = [irepo_transaction_df(spark_session=spark_core.spark_session, source_txt_path=source_txt_paths[i],
                                process_date=process_date_lst[i], today_date=today_date_lst[i]) for i in
           range(len(process_date_lst))]
    df = reduce(DataFrame.unionByName, dfs)
    return df


def get_raw_ka_pool_list(spark_session: pyspark.sql.SparkSession, process_dates: List[str],
                         today_dates: List[str]) -> pyspark.sql.dataframe.DataFrame:
    source_txt_path1 = '../pysparkProject/sparkcore/pool_list/data/ka_irepo_pool_20211020_135218.txt'
    source_txt_path2 = '../pysparkProject/sparkcore/pool_list/data/ka_irepo_pool_20211021_135237.txt'
    source_txt_path3 = '../pysparkProject/sparkcore/pool_list/data/ka_irepo_pool_20211022_135306.txt'
    source_txt_path4 = '../pysparkProject/sparkcore/pool_list/data/ka_irepo_pool_20211023_135336.txt'
    source_txt_path5 = '../pysparkProject/sparkcore/pool_list/data/ka_irepo_pool_20211030_135408.txt'
    source_txt_path6 = '../pysparkProject/sparkcore/pool_list/data/ka_irepo_pool_20211031_135456.txt'
    source_txt_path7 = '../pysparkProject/sparkcore/pool_list/data/ka_irepo_pool_20211101_082856.txt'
    source_txt_path8 = '../pysparkProject/sparkcore/pool_list/data/ka_irepo_pool_20211102_082939.txt'
    source_txt_path9 = '../pysparkProject/sparkcore/pool_list/data/ka_irepo_pool_20211103_083004.txt'

    source_txt_paths = [source_txt_path1, source_txt_path2, source_txt_path3, source_txt_path4, source_txt_path5,
                        source_txt_path6, source_txt_path7, source_txt_path8, source_txt_path9]
    dfs = [irepo_transaction_df(spark_session=spark_core.spark_session, source_txt_path=source_txt_paths[i],
                                process_date=process_date_lst[i], today_date=today_date_lst[i]) for i in
           range(len(process_date_lst))]
    df = reduce(DataFrame.unionByName, dfs)
    return df


def process_pool_list_from_pst_to_crt(entity: str, process_date: str, today_date: str,
                                      raw_transaction_df: pyspark.sql.dataframe.DataFrame,
                                      product_key_df: pyspark.sql.dataframe.DataFrame,
                                      month_key_df: pyspark.sql.dataframe.DataFrame,
                                      environment: str) -> None:
    intermediate_pool_list_df = to_intermediate_pool_list_df(raw_transaction_df, product_key_df)

    config_path = "../pool_list/config/"
    snap_monthly_table_config = TableConfig(config_path, environment, f'{entity}_pool_list_curate')
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
    # empty_snap_monthly_df = spark_core.spark_session \
    #     .table(f"{snap_monthly_table_property.database}.{snap_monthly_table_property.table}")
    # empty_snap_monthly_df.show(truncate=False)
    # show_snap_monthly = spark_core.spark_session \
    #     .sql(f"show create table {snap_monthly_table_property.database}.{snap_monthly_table_property.table}")
    # print(show_snap_monthly.collect()[0])
    data_date_col = 'data_date'
    status_column = 'is_active'
    account_key = 'account_key'
    product_key = 'product_key'
    branch_key = 'branch_key'
    key_columns = [account_key, product_key, branch_key]

    snap_monthly_table = f"{snap_monthly_table_property.database}.{snap_monthly_table_property.table}"

    DataFrameHelper().update_insert_status_snap_monthly_to_table(transaction_df=intermediate_pool_list_df,
                                                                 status_column=status_column,
                                                                 key_columns=key_columns,
                                                                 process_date=process_date,
                                                                 today_date=today_date,
                                                                 month_key_df=month_key_df,
                                                                 data_date_col_name=data_date_col,
                                                                 spark_session=spark_core.spark_session,
                                                                 snap_month_table=snap_monthly_table)

    # snap_monthly_final = spark_core.spark_session.table(
    #     f'{snap_monthly_table_property.database}.{snap_monthly_table_property.table}')
    # snap_monthly_final.printSchema()
    #
    # snap_monthly_final.groupBy(data_date_col, 'update_date', status_column, 'month_key').agg(
    #     count("*")).orderBy('month_key', data_date_col,
    #                         status_column).show(truncate=False)


if __name__ == '__main__':
    print('\n'.join(sys.path))
    spark_core = SparkCore('local')
    spark_core.spark_session.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark_core.spark_session.sparkContext.setCheckpointDir("/tmp/checkpoint")
    spark_reader = SparkReader(spark_core.spark_session)
    process_date_lst = ['2021-10-20', '2021-10-21', '2021-10-22', '2021-10-23', '2021-10-30', '2021-10-31',
                        '2021-11-01', '2021-11-02', '2021-11-03']
    today_date_lst = ['2021-10-21', '2021-10-22', '2021-10-23', '2021-10-30', '2021-10-31', '2021-11-01', '2021-11-02',
                      '2021-11-03', '2021-11-04']
    entities = ['ka', 'ay']
    for entity in entities:
        raw_pool_list = get_raw_pool_list_by_entity(entity=entity, spark_session=spark_core.spark_session,
                                                    process_dates=process_date_lst,
                                                    today_dates=today_date_lst)
        month_key_df = mock_month_key_df(spark_session=spark_core.spark_session)
        product_key_df = get_look_up_product_keys_by_entity(entity=entity, spark_session=spark_core.spark_session)
        for i in range(len(process_date_lst)):
            env = 'local'

            current_process_date = process_date_lst[i]
            current_today_date = today_date_lst[i]

            process_pool_list_from_pst_to_crt(entity=entity,
                                              process_date=current_process_date,
                                              today_date=current_today_date,
                                              raw_transaction_df=raw_pool_list,
                                              product_key_df=product_key_df,
                                              month_key_df=month_key_df,
                                              environment=env)

    data_date_col = 'data_date'
    status_column = 'is_active'
    entity_code = 'entity_code'
    ka_pool_list_snap_month = spark_core.spark_session.table("kadev_test.ext_irepo_snap_monthly")
    ay_pool_list_snap_month = spark_core.spark_session.table("aydev_test.ext_irepo_snap_monthly")
    pool_list_snap_month = ka_pool_list_snap_month.unionByName(ay_pool_list_snap_month)
    pool_list_snap_month.groupBy(entity_code, data_date_col, 'update_date', status_column, 'month_key').agg(
        count("*")).orderBy(entity_code, 'month_key', data_date_col, 'update_date', status_column).show(truncate=False)

    # +-----------+----------+-----------+---------+---------+--------+
    # | entity_code | data_date | update_date | is_active | month_key | count(1) |
    # +-----------+----------+-----------+---------+---------+--------+
    # | AY | 2021 - 11 - 01 | 2021 - 11 - 02 | inactive | 0 | 32 |
    # | AY | 2021 - 11 - 02 | 2021 - 11 - 03 | inactive | 0 | 104 |
    # | AY | 2021 - 11 - 03 | 2021 - 11 - 04 | active | 0 | 8583 |
    # | AY | 2021 - 10 - 20 | 2021 - 10 - 21 | inactive | 322 | 704 |
    # | AY | 2021 - 10 - 21 | 2021 - 10 - 22 | inactive | 322 | 383 |
    # | AY | 2021 - 10 - 22 | 2021 - 10 - 23 | inactive | 322 | 326 |
    # | AY | 2021 - 10 - 30 | 2021 - 10 - 31 | inactive | 322 | 250 |
    # | AY | 2021 - 10 - 31 | 2021 - 11 - 01 | active | 322 | 8280 |
    # | KA | 2021 - 11 - 01 | 2021 - 11 - 02 | inactive | 0 | 4 |
    # | KA | 2021 - 11 - 02 | 2021 - 11 - 03 | inactive | 0 | 9 |
    # | KA | 2021 - 11 - 03 | 2021 - 11 - 04 | active | 0 | 406 |
    # | KA | 2021 - 10 - 20 | 2021 - 10 - 21 | inactive | 322 | 59 |
    # | KA | 2021 - 10 - 21 | 2021 - 10 - 22 | inactive | 322 | 25 |
    # | KA | 2021 - 10 - 22 | 2021 - 10 - 23 | inactive | 322 | 21 |
    # | KA | 2021 - 10 - 30 | 2021 - 10 - 31 | inactive | 322 | 8 |
    # | KA | 2021 - 10 - 31 | 2021 - 11 - 01 | active | 322 | 394 |
    # +-----------+----------+-----------+---------+---------+--------+

