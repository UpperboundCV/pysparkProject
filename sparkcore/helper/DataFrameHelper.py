from typing import List, Optional

import pyspark
from pyspark.sql.functions import col, lit, max, when, md5, concat, lower, to_date, upper, to_timestamp, unix_timestamp
from pyspark.sql.functions import months_between, coalesce, last_day, date_format, substring, regexp_replace, lower, \
    broadcast
from pyspark.sql.types import IntegerType, StringType, DateType, TimestampType

from .DateHelper import DateHelper


class DataFrameHelper:
    PTN_MONTH_KEY = 'ptn_month_key'
    MONTH_KEY = 'month_key'
    UPDATE_DATE = 'update_date'
    ACTIVE = 'active'
    INACTIVE = 'inactive'
    CURRENT_STATUS = "current_status"
    TRANSACTION_START_DATE = "transaction_start_date"
    LAST_UPDATE_DATE = 'last_update_date'
    PRODUCT_KEY = "product_key"
    BRANCH_CODE = 'branch_code'
    CONTRACT_CODE = 'contract_code'
    ACCOUNT_CODE = 'account_code'
    ENTITY_CODE = 'entity_code'

    @classmethod
    def combine_entity_df(cls, ay_df: pyspark.sql.dataframe.DataFrame,
                          ka_df: pyspark.sql.dataframe.DataFrame,
                          join_key: List[str]) -> pyspark.sql.dataframe.DataFrame:
        return ay_df.join(ka_df, join_key, how='outer')

    @classmethod
    def add_entity_uam(cls) -> pyspark.sql.functions.col:
        AY_ENTITY = 'ay_entity'
        KA_ENTITY = 'ka_entity'
        AY_STATUS = 'ay_status'
        KA_STATUS = 'ka_status'
        ay_entity_flag = (lower(col(AY_ENTITY)) == 'aycal')
        ka_entity_flag = (lower(col(KA_ENTITY)) == 'bay')
        is_ay_entity_null = ((lower(col(AY_ENTITY)).isNull()) | (col(AY_ENTITY) == ' '))
        is_ka_entity_null = ((lower(col(KA_ENTITY)).isNull()) | (col(KA_ENTITY) == ' '))
        ay_status = (lower(col(AY_STATUS)) == cls.ACTIVE)
        ka_status = (lower(col(KA_STATUS)) == cls.ACTIVE)
        is_ay_status_null = ((lower(col(AY_STATUS)).isNull()) | (col(AY_STATUS) == ' '))
        is_ka_status_null = ((lower(col(KA_STATUS)).isNull()) | (col(KA_STATUS) == ' '))
        return when(ay_entity_flag & ay_status & ka_entity_flag & ka_status, lit('Auto')) \
            .when(~(ay_entity_flag & ay_status) & ka_entity_flag & ka_status, lit('Bay')) \
            .when(ay_entity_flag & ay_status & ~(ka_entity_flag & ka_status), lit('Aycal')) \
            .when(ay_entity_flag & ay_status & (is_ka_entity_null | is_ka_status_null), lit('Aycal')) \
            .when((is_ay_entity_null | is_ay_status_null) & ka_entity_flag & ka_status, lit('Bay')) \
            .when((is_ay_entity_null | is_ay_status_null) & (is_ka_entity_null | is_ka_status_null)) \
            .otherwise(lit('blank').cast(StringType()))


@classmethod
def add_user_type(cls) -> pyspark.sql.functions.col:
    return when(col('user_login').rlike("\D"), lit('CR')) \
        .when(col('user_login').rlike("\d"), lit('STAFF')) \
        .otherwise(lit('Undefined'))


@classmethod
def config_type_to_df_type(cls, config_data_type: str) -> Optional[str]:
    if config_data_type == 'integer':
        return "int"
    elif config_data_type == 'string':
        return "string"
    elif config_data_type == 'date':
        return "date"
    elif config_data_type == 'timestamp':
        return "timestamp"
    else:
        raise TypeError("data type does not exist.")


@classmethod
def convert_as400_data_date_to_yyyyMMdd(cls, data_date_col_name: str) -> pyspark.sql.functions.col:
    return (col(data_date_col_name).cast(IntegerType()) + lit(20000000) - lit(430000)).cast(StringType())


@classmethod
def convert_yyyyMMdd_col_to_spark_date(cls, yyyyMMdd_col: pyspark.sql.functions.col) -> pyspark.sql.functions.col:
    extracted_year = concat(substring(yyyyMMdd_col, 1, 4), lit('-'))
    extracted_year_month = concat(extracted_year, concat(substring(yyyyMMdd_col, 5, 2), lit('-')))
    extracted_year_month_day = concat(extracted_year_month, concat(substring(yyyyMMdd_col, 7, 2)))
    return concat(to_date(extracted_year_month_day, "yyyy-MM-dd").cast(StringType()), lit(' 00:00:00'))


@classmethod
def convert_as400_data_date_to_timestamp(cls, df: pyspark.sql.dataframe.DataFrame,
                                         data_date_cols: List[str]) -> pyspark.sql.dataframe.DataFrame:
    def convert_all_in_group(df_concat: pyspark.sql.dataframe.DataFrame,
                             col_index: int) -> pyspark.sql.dataframe.DataFrame:
        if len(data_date_cols) == 0:
            return df_concat
        else:
            if col_index < len(data_date_cols):
                yyyyMMdd_str_col = cls.convert_as400_data_date_to_yyyyMMdd(data_date_cols[col_index])
                date_timestamp = cls.convert_yyyyMMdd_col_to_spark_date(yyyyMMdd_col=yyyyMMdd_str_col)
                df_attach_col = df_concat.withColumn(data_date_cols[col_index],
                                                     to_timestamp(date_timestamp, 'yyyy-MM-dd HH:mm:ss'))
                return convert_all_in_group(df_attach_col, col_index + 1)
            else:
                return df_concat

    return convert_all_in_group(df, 0)


@classmethod
def data_date_convert(cls, transaction_df: pyspark.sql.dataframe.DataFrame,
                      data_date_col_name: str) -> pyspark.sql.dataframe.DataFrame:
    yyyy_mm_dd_col = (col(data_date_col_name).cast(IntegerType()) + lit(20000000) - lit(430000)).cast(StringType())
    return transaction_df.withColumn(data_date_col_name, to_date(yyyy_mm_dd_col, "yyyyMMdd").cast(StringType()))


@classmethod
def with_entity_code(cls, transaction_df: pyspark.sql.dataframe.DataFrame,
                     entity: str) -> pyspark.sql.dataframe.DataFrame:
    if entity == 'ay':
        return transaction_df.withColumn(cls.ENTITY_CODE, lit('AYCAL'))
    else:
        return transaction_df.withColumn(cls.ENTITY_CODE, lit('BAY'))


@classmethod
def to_entity_dwh(cls, transaction_df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    return transaction_df.withColumn(cls.ENTITY_CODE, when(col(cls.ENTITY_CODE) == "AYCAL", lit("AY"))
                                     .when(col(cls.ENTITY_CODE) == "BAY", lit("KA"))
                                     .otherwise(None))


@classmethod
def with_entity(cls, transaction_df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    return transaction_df.withColumn("entity",
                                     when(col(cls.ENTITY_CODE) == "AYCAL", lit("AY"))
                                     .when(col(cls.ENTITY_CODE) == "BAY", lit("KA"))
                                     .otherwise(None))


@classmethod
def with_gecid(cls, transaction_df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    return transaction_df.withColumn("gecid",
                                     when(col(cls.ENTITY_CODE) == "AYCAL", lit("60000000"))
                                     .when(col(cls.ENTITY_CODE) == "BAY", lit("52800000"))
                                     .otherwise(None))


@classmethod
def with_company(cls, transaction_df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    return transaction_df.withColumn("company_code", lit('GECAL'))


@classmethod
def with_account(cls, transaction_df: pyspark.sql.dataframe.DataFrame,
                 contract_code: str = 'contract_code') -> pyspark.sql.dataframe.DataFrame:
    return transaction_df.withColumn(cls.ACCOUNT_CODE,
                                     when(col(contract_code).isNotNull(), col(contract_code)).otherwise(
                                         None))


@classmethod
def with_short_product_key(cls, transaction_df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    return transaction_df.withColumn('product_key', when(col('product_code') == 'MC', lit("1234abc"))
                                     .when(col("product_code") == 'HP', lit("567xyz")).otherwise(
        lit(None)))


@classmethod
def with_broadcast_product_key(cls, transaction_df: pyspark.sql.dataframe.DataFrame,
                               look_up_product_df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    product_df = look_up_product_df.withColumnRenamed(cls.PRODUCT_KEY, "lookup_product_key") \
        .withColumnRenamed("product_id", "lookup_product_id")
    transaction_df_w_product_key = transaction_df.alias("t") \
        .join(broadcast(product_df.alias("p")), on=[col("t.product_code") == col("p.lookup_product_id")],
              how="left") \
        .withColumn(cls.PRODUCT_KEY, col("p.lookup_product_key")) \
        .select("t.*", col(cls.PRODUCT_KEY))
    return transaction_df_w_product_key


@classmethod
def with_product_key(cls, transaction_df: pyspark.sql.dataframe.DataFrame,
                     look_up_product_df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    product_df = look_up_product_df.withColumnRenamed(cls.PRODUCT_KEY, "lookup_product_key") \
        .withColumnRenamed("product_id", "lookup_product_id")
    transaction_df_w_product_key = transaction_df.alias("t") \
        .join(product_df.alias("p"), on=[col("t.product_code") == col("p.lookup_product_id")], how="left") \
        .withColumn(cls.PRODUCT_KEY, col("p.lookup_product_key")) \
        .select("t.*", col(cls.PRODUCT_KEY))
    return transaction_df_w_product_key


@classmethod
def with_branch_key(cls, transaction_df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    return transaction_df \
        .withColumn("branch_key",
                    when(col(cls.BRANCH_CODE).isNotNull() & col("gecid").isNotNull(),
                         upper(md5(concat(lower(col("entity")), lower(col(cls.BRANCH_CODE)))))))


@classmethod
def with_account_key(cls, transaction_df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    product_company_code = concat(lower(col("product_code")), lower(col("company_code")))
    branch_product_company_code = concat(lower(col("branch_code")), product_company_code)
    account_branch_product_company_code = concat(lower(col(cls.ACCOUNT_CODE)), branch_product_company_code)
    return transaction_df.withColumn("account_key", upper(md5(account_branch_product_company_code)))


@classmethod
def with_all_keys(cls, transaction_df: pyspark.sql.dataframe.DataFrame,
                  look_up_product_df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
    transaction_df_w_product_key = cls.with_product_key(transaction_df, look_up_product_df)
    transaction_df_w_branch_key = cls.with_branch_key(transaction_df_w_product_key)
    transaction_df_w_account_key = cls.with_account_key(transaction_df_w_branch_key)
    return transaction_df_w_account_key


@classmethod
def to_ptn_month_key(cls, yyyy_mm_dd_col_name: str) -> pyspark.sql.functions.col:
    return substring(regexp_replace(yyyy_mm_dd_col_name, '-', ''), 0, 6)


@classmethod
def update_insert_snap_monthly_to_table(cls, transaction_df: pyspark.sql.dataframe.DataFrame,
                                        process_date: str,  # value of data_date_col_name
                                        today_date: str,
                                        month_key_df: pyspark.sql.dataframe.DataFrame,
                                        data_date_col_name: str,
                                        spark_session: pyspark.sql.SparkSession,
                                        snap_month_table: str) -> None:
    snap_monthly_df = spark_session.table(snap_month_table)
    snap_monthly_df_cols = [c for c in snap_monthly_df.columns]
    print('inside')
    print(','.join(snap_monthly_df_cols))

    if snap_monthly_df.where(col(cls.MONTH_KEY) == 0).count() == 0:
        print('!fresh insert')
        writed_df = transaction_df.where(DateHelper.timestamp2str(col(data_date_col_name)) == process_date) \
            .withColumn(cls.UPDATE_DATE, DateHelper.data_date2timestamp(today_date)) \
            .withColumn(cls.MONTH_KEY, lit(0).cast(IntegerType())) \
            .withColumn(cls.PTN_MONTH_KEY, cls.to_ptn_month_key(data_date_col_name)) \
            .selectExpr(snap_monthly_df_cols).cache()
        writed_df.show(n=5, truncate=False)
        print(writed_df.rdd.getNumPartitions())
        writed_df.repartition(800).write.format("orc").insertInto(snap_month_table, overwrite=True)
        # writed_df.repartition(500).write.partitionBy(cls.PTN_MONTH_KEY).format("orc").mode("overwrite").saveAsTable(
        #     snap_month_table)
        # writed_df.coalesce(5).write.partitionBy(cls.PTN_MONTH_KEY).format("orc").mode("overwrite").saveAsTable(
        #     snap_month_table)
    else:
        current_snap_month = snap_monthly_df.where(col(cls.MONTH_KEY) == 0).select(
            max(to_date(data_date_col_name)).cast(StringType())).first()[0]
        print(f"process_date:{process_date}")
        print(f"current_snap_month:{current_snap_month}")
        print(f"diff month:{DateHelper().date_str_num_month_diff(process_date, current_snap_month)}")

        if DateHelper.date_str_num_month_diff(process_date, current_snap_month) > 0:
            # This condition will use when there is newer process_date than current_snap_month.
            # 1. update from current monthkey = 0 to month key of process_date.
            print("[1/2] start: update on month key")
            updated_month_key_df = cls.update_month_key_zero(snap_monthly_df, month_key_df, data_date_col_name)
            updated_month_key_df.selectExpr(snap_monthly_df_cols) \
                .write.format("orc").insertInto(snap_month_table, overwrite=True)
            print("[1/2] end: update on month key")
            # 2. save process_date in monthkey = 0
            print("[2/2] start: save new transaction data to month key = 0")
            writed_df = transaction_df.where(DateHelper.timestamp2str(col(data_date_col_name)) == process_date) \
                .withColumn(cls.UPDATE_DATE, DateHelper.data_date2timestamp(today_date)) \
                .withColumn(cls.MONTH_KEY, lit(0).cast(IntegerType())) \
                .withColumn(cls.PTN_MONTH_KEY, cls.to_ptn_month_key(data_date_col_name)) \
                .selectExpr(snap_monthly_df_cols)
            writed_df.write.format("orc").insertInto(snap_month_table, overwrite=True)
            print("[2/2] end: save new transaction data to month key = 0")
        else:
            # rerun case: please rerun for the beginning of the month
            # elif DateHelper().date_str_num_month_diff(process_date, current_snap_month) < 0:
            target_month_key = cls.find_month_key_of_process_date(process_date, snap_monthly_df, data_date_col_name)
            print(f"[1/1] start: update on month key = {target_month_key}")
            writed_df = transaction_df.where(DateHelper.timestamp2str(col(data_date_col_name)) == process_date) \
                .withColumn(cls.UPDATE_DATE, DateHelper.data_date2timestamp(today_date)) \
                .withColumn(cls.MONTH_KEY, lit(0).cast(IntegerType())) \
                .withColumn(cls.PTN_MONTH_KEY, cls.to_ptn_month_key(data_date_col_name)) \
                .selectExpr(snap_monthly_df_cols)
            writed_df.write.format("orc").insertInto(snap_month_table, overwrite=True)
            print(f"[1/1] end: update on month key = {target_month_key}")


@classmethod
def update_insert_status_snap_monthly_to_table(cls, transaction_df: pyspark.sql.dataframe.DataFrame,
                                               status_column: str,
                                               key_columns: List[str],
                                               process_date: str,  # value of data_date_col_name
                                               today_date: str,
                                               month_key_df: pyspark.sql.dataframe.DataFrame,
                                               data_date_col_name: str,
                                               spark_session: pyspark.sql.SparkSession,
                                               snap_month_table: str) -> None:
    snap_monthly_df = spark_session.table(snap_month_table)
    snap_monthly_df_cols = [c for c in snap_monthly_df.columns]
    print("inside")
    transaction_df.show(truncate=False)

    if snap_monthly_df.where(col(cls.MONTH_KEY) == 0).count() == 0:
        writed_df = transaction_df.where(col(data_date_col_name) == process_date) \
            .withColumn(cls.UPDATE_DATE, lit(today_date).cast(DateType())) \
            .withColumn(cls.MONTH_KEY, lit(0).cast(IntegerType())) \
            .withColumn(cls.PTN_MONTH_KEY, cls.to_ptn_month_key(data_date_col_name)) \
            .withColumn(status_column, lit(cls.ACTIVE)) \
            .selectExpr(snap_monthly_df_cols)

        writed_df.write.format("orc").insertInto(snap_month_table, overwrite=True)
    else:
        current_snap_month = \
            snap_monthly_df.where(col(cls.MONTH_KEY) == 0).select(
                max(col(data_date_col_name).cast(StringType()))).first()[0]
        print(f"process_date:{process_date}")
        print(f"current_snap_month:{current_snap_month}")
        print(f"diff month:{DateHelper().date_str_num_month_diff(process_date, current_snap_month)}")

        if DateHelper().date_str_num_month_diff(process_date, current_snap_month) > 0:
            # It should update the month key
            print("start: update on month key")
            updated_month_key_df = cls.update_month_key_zero(snap_monthly_df, month_key_df, data_date_col_name)
            updated_month_key_df.selectExpr(snap_monthly_df_cols) \
                .write.format("orc").insertInto(snap_month_table, overwrite=True)
            # process_date is in monthkey = 0
            transaction_df \
                .where(col(data_date_col_name) == process_date) \
                .withColumn(cls.UPDATE_DATE, lit(today_date).cast(DateType())) \
                .withColumn(cls.MONTH_KEY, lit(0).cast(IntegerType())) \
                .withColumn(cls.PTN_MONTH_KEY, cls.to_ptn_month_key(data_date_col_name)) \
                .withColumn(status_column, lit(cls.ACTIVE)) \
                .selectExpr(snap_monthly_df_cols) \
                .write.format("orc").insertInto(snap_month_table, overwrite=True)
            print("start: update on month key")
        else:
            # rerun case
            # elif DateHelper().date_str_num_month_diff(process_date, current_snap_month) < 0:
            target_month_key = cls.find_month_key_of_process_date(process_date, snap_monthly_df, data_date_col_name)
            print(f"start: month key = {target_month_key}")
            # rerun the whole monthkey
            transaction_of_month_key = transaction_df.where(col(data_date_col_name) == process_date) \
                .withColumn(cls.LAST_UPDATE_DATE, lit(today_date).cast(DateType())) \
                .withColumn(cls.MONTH_KEY, lit(target_month_key).cast(IntegerType())) \
                .withColumn(cls.PTN_MONTH_KEY, cls.to_ptn_month_key(data_date_col_name)) \
                .withColumn(cls.CURRENT_STATUS, lit(cls.ACTIVE)) \
                .withColumnRenamed(data_date_col_name, cls.TRANSACTION_START_DATE)
            if cls.MONTH_KEY not in key_columns:
                key_columns.append(cls.MONTH_KEY)
            if cls.PTN_MONTH_KEY not in key_columns:
                key_columns.append(cls.PTN_MONTH_KEY)
            target_snap_month_key_df = snap_monthly_df \
                .where((col(cls.MONTH_KEY) == target_month_key) &
                       (col(cls.PTN_MONTH_KEY) == cls.to_ptn_month_key(data_date_col_name))) \
                .cache()
            # target_month_key_df.show(truncate=False)
            updated_df = cls.update_insert(transaction_df=transaction_of_month_key,
                                           snap_monthly_df=target_snap_month_key_df, status_column=status_column,
                                           key_columns=key_columns, data_date_col_name=data_date_col_name,
                                           additional_cols=[cls.UPDATE_DATE, data_date_col_name, status_column])
            updated_df.selectExpr(snap_monthly_df_cols) \
                .withColumn(cls.UPDATE_DATE, col(cls.UPDATE_DATE).cast(DateType())) \
                .write.insertInto(snap_month_table, overwrite=True)
            print(f"end: month key = {target_month_key}")


@classmethod
def update_insert_status_snap_monthly(cls, transaction_df: pyspark.sql.dataframe.DataFrame,
                                      snap_monthly_df: pyspark.sql.dataframe.DataFrame,
                                      status_column: str,
                                      key_columns: List[str],
                                      process_date: str,
                                      today_date: str,
                                      month_key_df: pyspark.sql.dataframe.DataFrame,
                                      data_date_col_name: str) -> pyspark.sql.dataframe.DataFrame:
    # cannot save the result here since the order of the columns after process is the same every time
    # it needs to be saved outside function
    if snap_monthly_df.where(col(cls.MONTH_KEY) == 0).count() == 0:
        return transaction_df.where(col(data_date_col_name) == process_date) \
            .withColumn(cls.UPDATE_DATE, lit(today_date)) \
            .withColumn(cls.MONTH_KEY, lit(0)) \
            .withColumn(status_column, lit(cls.ACTIVE))
    else:
        current_snap_month = \
            snap_monthly_df.where(col(cls.MONTH_KEY) == 0).select(max(col(data_date_col_name))).first()[0]
        print(f"process_date:{process_date}")
        print(f"current_snap_month:{current_snap_month}")
        print(f"diff month:{DateHelper().date_str_num_month_diff(process_date, current_snap_month)}")

        if DateHelper().date_str_num_month_diff(process_date, current_snap_month) > 0:
            # It should update the month key
            updated_month_key_df = cls.update_month_key_zero(snap_monthly_df, month_key_df, data_date_col_name)
            # process_date is in monthkey = 0
            temp_df = transaction_df \
                .where(col(data_date_col_name) == process_date) \
                .withColumn(cls.UPDATE_DATE, lit(today_date)) \
                .withColumn(cls.MONTH_KEY, lit(0)) \
                .withColumn(status_column, lit(cls.ACTIVE))
            temp_df.show(truncate=False)
            return temp_df.unionByName(updated_month_key_df)
        # elif DateHelper().date_str_num_month_diff(process_date, current_snap_month) < 0:
        else:
            target_month_key = cls.find_month_key_of_process_date(process_date, snap_monthly_df, data_date_col_name)
            print(f"target_month_key: {target_month_key}")
            transaction_of_month_key = transaction_df.where(col(data_date_col_name) == process_date) \
                .withColumn(cls.LAST_UPDATE_DATE, lit(today_date)) \
                .withColumn(cls.MONTH_KEY, lit(target_month_key)) \
                .withColumn(cls.CURRENT_STATUS, lit(cls.ACTIVE)) \
                .withColumnRenamed(data_date_col_name, cls.TRANSACTION_START_DATE)
            print("transaction_of_month_key")
            transaction_of_month_key.show(truncate=False)
            if cls.MONTH_KEY not in key_columns:
                key_columns.append(cls.MONTH_KEY)
            non_target_month_key_df = snap_monthly_df.where(col(cls.MONTH_KEY) != target_month_key)
            target_snap_month_key_df = snap_monthly_df.where(col(cls.MONTH_KEY) == target_month_key).cache()
            print("target_snap_month_key_df")
            target_snap_month_key_df.show(truncate=False)
            # target_month_key_df.show(truncate=False)
            updated_df = cls.update_insert(transaction_df=transaction_of_month_key,
                                           snap_monthly_df=target_snap_month_key_df, status_column=status_column,
                                           key_columns=key_columns, data_date_col_name=data_date_col_name,
                                           additional_cols=[cls.UPDATE_DATE, data_date_col_name, status_column])
            updated_df.show(truncate=False)

            return updated_df  # non_target_month_key_df.unionByName(updated_df)


@classmethod
def does_month_exist(cls, process_date: str, snap_monthly_df: pyspark.sql.dataframe.DataFrame,
                     data_date_col_name: str) -> bool:
    return snap_monthly_df.where(
        months_between(last_day(col(data_date_col_name)), last_day(lit(process_date))) == 0).count() > 0


@classmethod
def find_month_key_of_process_date(cls, process_date: str, snap_monthly_df: pyspark.sql.dataframe.DataFrame,
                                   data_date_col_name: str) -> int:
    return \
        snap_monthly_df.where(months_between(last_day(col(data_date_col_name)), last_day(lit(process_date))) == 0) \
            .select(max(cls.MONTH_KEY)).first()[0]


@classmethod
def get_month_keys(cls, snap_monthly_df: pyspark.sql.dataframe.DataFrame) -> List[int]:
    return [int(collect_monthkey[cls.MONTH_KEY]) for collect_monthkey in
            snap_monthly_df.select(col(cls.MONTH_KEY)).collect()]


@classmethod
def update_month_key_zero(cls, snap_monthly_df: pyspark.sql.dataframe.DataFrame,
                          month_key_df: pyspark.sql.dataframe.DataFrame,
                          data_date_col_name: str) -> pyspark.sql.dataframe.DataFrame:
    zero_month_key_last_date = snap_monthly_df.where(col(cls.MONTH_KEY) == 0) \
        .select(last_day(col(data_date_col_name))).first()[0]
    print(f'zero_month_key_last_date: {zero_month_key_last_date} and {type(zero_month_key_last_date)}')
    print(f"month_number: {zero_month_key_last_date.month} and year: {zero_month_key_last_date.year}")
    month_key = month_key_df.where(col("month_number").cast(IntegerType()) == int(zero_month_key_last_date.month)) \
        .where(col("year").cast(IntegerType()) == int(zero_month_key_last_date.year)) \
        .select(max(cls.MONTH_KEY)).first()[0]
    print(f"month key: {month_key}")
    return snap_monthly_df.where(col(cls.MONTH_KEY) == 0).withColumn(cls.MONTH_KEY, lit(month_key)) \
        .withColumn(cls.PTN_MONTH_KEY, cls.to_ptn_month_key(data_date_col_name))


@classmethod
def update_insert(cls, transaction_df: pyspark.sql.dataframe.DataFrame,
                  snap_monthly_df: pyspark.sql.dataframe.DataFrame, status_column: str, key_columns: List[str],
                  data_date_col_name: str, additional_cols: List[str]) -> pyspark.sql.dataframe.DataFrame:
    base = 'base'
    current = 'current'
    main_cols = key_columns + additional_cols
    minor_col_expr = [f'coalesce(base.{minor_col}, current.{minor_col}) as ' + minor_col for minor_col in
                      snap_monthly_df.columns if minor_col not in main_cols]
    select_expr_lst = main_cols + minor_col_expr
    result = snap_monthly_df.alias(base) \
        .join(transaction_df.alias(current), key_columns, how='outer') \
        .withColumn(cls.UPDATE_DATE, coalesce(col(f'{cls.LAST_UPDATE_DATE}'), col(f'{cls.UPDATE_DATE}'))) \
        .withColumn(data_date_col_name,
                    coalesce(col(f'{cls.TRANSACTION_START_DATE}'), col(f'{data_date_col_name}'))) \
        .withColumn(status_column, when(col(cls.CURRENT_STATUS) == cls.ACTIVE, col(cls.CURRENT_STATUS))
                    .otherwise(lit(cls.INACTIVE))) \
        .drop(cls.TRANSACTION_START_DATE) \
        .drop(cls.LAST_UPDATE_DATE) \
        .drop(cls.CURRENT_STATUS) \
        .selectExpr(*select_expr_lst)
    return result
