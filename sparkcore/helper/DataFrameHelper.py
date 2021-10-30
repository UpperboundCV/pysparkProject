from typing import List

import pyspark
from pyspark.sql.functions import col, lit, max, when, md5, concat, lower, to_date
from pyspark.sql.functions import months_between, coalesce, last_day
from pyspark.sql.types import IntegerType, StringType

from .DateHelper import DateHelper


class DataFrameHelper:
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

    def data_date_convert(cls, transaction_df: pyspark.sql.dataframe.DataFrame,
                          data_date_col_name: str) -> pyspark.sql.dataframe.DataFrame:
        yyyy_mm_dd_col = (col(data_date_col_name).cast(IntegerType()) + lit(20000000) - lit(430000)).cast(StringType())
        return transaction_df.withColumn(data_date_col_name, to_date(yyyy_mm_dd_col, "yyyyMMdd").cast(StringType()))

    def with_entity(cls, transaction_df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
        return transaction_df.withColumn("entity",
                                         when(col(cls.ENTITY_CODE) == "AYCAL", lit("AY"))
                                         .when(col(cls.ENTITY_CODE) == "BAY", lit("KA"))
                                         .otherwise(None))

    def with_gecid(cls, transaction_df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
        return transaction_df.withColumn("gecid",
                                         when(col(cls.ENTITY_CODE) == "AYCAL", lit("60000000"))
                                         .when(col(cls.ENTITY_CODE) == "BAY", lit("52800000"))
                                         .otherwise(None))

    def with_account(cls, transaction_df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
        return transaction_df.withColumn(cls.ACCOUNT_CODE,
                                         when(col(cls.CONTRACT_CODE).isNotNull(), col(cls.CONTRACT_CODE)).otherwise(
                                             None))

    def with_product_key(cls, transaction_df: pyspark.sql.dataframe.DataFrame,
                         look_up_product_df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
        product_df = look_up_product_df.withColumnRenamed(cls.PRODUCT_KEY, "lookup_product_key") \
            .withColumnRenamed("product_id", "lookup_product_id")
        transaction_df_w_product_key = transaction_df.alias("t") \
            .join(product_df.alias("p"), on=[col("t.product_code") == col("p.lookup_product_id")], how="left") \
            .withColumn(cls.PRODUCT_KEY, col("p.lookup_product_key")) \
            .select("t.*", col(cls.PRODUCT_KEY))
        return transaction_df_w_product_key

    def with_branch_key(cls, transaction_df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
        return transaction_df \
            .withColumn("branch_key",
                        when(col(cls.BRANCH_CODE).isNotNull() & col("gecid").isNotNull(),
                             md5(concat(lower(col("entity")), lower(col(cls.BRANCH_CODE))))))

    def with_account_key(cls, transaction_df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
        product_company_code = concat(lower(col("product_code")), lower(col("company_code")))
        branch_product_company_code = concat(lower(col("branch_code")), product_company_code)
        account_branch_product_company_code = concat(lower(col(cls.ACCOUNT_CODE)), branch_product_company_code)
        return transaction_df.withColumn("account_key", md5(account_branch_product_company_code))

    def with_all_keys(cls, transaction_df: pyspark.sql.dataframe.DataFrame,
                      look_up_product_df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
        transaction_df_w_product_key = cls.with_product_key(transaction_df, look_up_product_df)
        transaction_df_w_branch_key = cls.with_branch_key(transaction_df_w_product_key)
        transaction_df_w_account_key = cls.with_account_key(transaction_df_w_branch_key)
        return transaction_df_w_account_key

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
        # cannot save the result here since the order of the columns after process is the same every time
        # it needs to be saved outside function
        if snap_monthly_df.where(col(cls.MONTH_KEY) == 0).count() == 0:
            writed_df = transaction_df.where(col(data_date_col_name) == process_date) \
                .withColumn(cls.UPDATE_DATE, lit(today_date)) \
                .withColumn(cls.MONTH_KEY, lit(0).cast(IntegerType())) \
                .withColumn(status_column, lit(cls.ACTIVE)) \
                .selectExpr(snap_monthly_df_cols)
            writed_df.show(truncate=False)
            writed_df.printSchema()
            print("hello")
            writed_df.write.format("orc").insertInto(snap_month_table, overwrite=True)
        else:
            current_snap_month = \
                snap_monthly_df.where(col(cls.MONTH_KEY) == 0).select(max(col(data_date_col_name))).first()[0]
            print(f"process_date:{process_date}")
            print(f"current_snap_month:{current_snap_month}")
            print(f"diff month:{DateHelper().date_str_num_month_diff(process_date, current_snap_month)}")

            if DateHelper().date_str_num_month_diff(process_date, current_snap_month) > 0:
                # It should update the month key
                updated_month_key_df = cls.update_month_key_zero(snap_monthly_df, month_key_df, data_date_col_name)
                updated_month_key_df.write.format("orc").insertInto(snap_month_table, overwrite=True)
                # process_date is in monthkey = 0
                temp_df = transaction_df \
                    .where(col(data_date_col_name) == process_date) \
                    .withColumn(cls.UPDATE_DATE, lit(today_date)) \
                    .withColumn(cls.MONTH_KEY, lit(0)) \
                    .withColumn(status_column, lit(cls.ACTIVE)) \
                    .write.format("orc").insertInto(snap_month_table, overwrite=True)
            # elif DateHelper().date_str_num_month_diff(process_date, current_snap_month) < 0:
            else:
                # this process_date is not in monthkey = 0 but it is in another monthkey
                target_month_key = cls.find_month_key_of_process_date(process_date, snap_monthly_df, data_date_col_name)
                # rerun the whole monthkey
                transaction_of_month_key = transaction_df.where(col(data_date_col_name) == process_date) \
                    .withColumn(cls.LAST_UPDATE_DATE, lit(today_date)) \
                    .withColumn(cls.MONTH_KEY, lit(target_month_key)) \
                    .withColumn(cls.CURRENT_STATUS, lit(cls.ACTIVE)) \
                    .withColumnRenamed(data_date_col_name, cls.TRANSACTION_START_DATE)
                if cls.MONTH_KEY not in key_columns:
                    key_columns.append(cls.MONTH_KEY)
                non_target_month_key_df = snap_monthly_df.where(col(cls.MONTH_KEY) != target_month_key)
                target_snap_month_key_df = snap_monthly_df.where(col(cls.MONTH_KEY) == target_month_key).cache()
                # target_month_key_df.show(truncate=False)
                updated_df = cls.update_insert(transaction_df=transaction_of_month_key,
                                               snap_monthly_df=target_snap_month_key_df, status_column=status_column,
                                               key_columns=key_columns, data_date_col_name=data_date_col_name,
                                               additional_cols="")
                updated_df.show(truncate=False)
                updated_df.write.insertInto(snap_month_table, overwrite=True)

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

    def does_month_exist(cls, process_date: str, snap_monthly_df: pyspark.sql.dataframe.DataFrame,
                         data_date_col_name: str) -> bool:
        return snap_monthly_df.where(
            months_between(last_day(col(data_date_col_name)), last_day(lit(process_date))) == 0).count() > 0

    def find_month_key_of_process_date(cls, process_date: str, snap_monthly_df: pyspark.sql.dataframe.DataFrame,
                                       data_date_col_name: str) -> int:
        return \
            snap_monthly_df.where(months_between(last_day(col(data_date_col_name)), last_day(lit(process_date))) == 0) \
                .select(max(cls.MONTH_KEY)).first()[0]

    def get_month_keys(cls, snap_monthly_df: pyspark.sql.dataframe.DataFrame) -> List[int]:
        return [int(collect_monthkey[cls.MONTH_KEY]) for collect_monthkey in
                snap_monthly_df.select(col(cls.MONTH_KEY)).collect()]

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
        return snap_monthly_df.where(col(cls.MONTH_KEY) == 0).withColumn(cls.MONTH_KEY, lit(month_key))

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

    def update_insert2(cls, transaction_df: pyspark.sql.dataframe.DataFrame,
                       snap_monthly_df: pyspark.sql.dataframe.DataFrame,
                       status_column: str,
                       key_columns: List[str],
                       data_date_col_name: str) -> pyspark.sql.dataframe.DataFrame:
        # todo: use left and left anti join instead of outer join to avoid null in column and
        # todo: duplicated column after outer join
        return transaction_df
