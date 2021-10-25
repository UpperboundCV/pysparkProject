from typing import List

import pyspark
from pyspark.sql.functions import col, lit, max, when
from pyspark.sql.functions import months_between, coalesce, last_day
from pyspark.sql.types import IntegerType

from .DateHelper import DateHelper


class DataFrameHelper:
    MONTH_KEY = 'month_key'
    UPDATE_DATE = 'update_date'
    ACTIVE = 'active'
    INACTIVE = 'inactive'
    CURRENT_STATUS = "current_status"
    TRANSACTION_START_DATE = "transaction_start_date"
    LAST_UPDATE_DATE = 'last_update_date'

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
                                               snap_monthly_df=target_snap_month_key_df,
                                               status_column=status_column,
                                               key_columns=key_columns,
                                               data_date_col_name=data_date_col_name)
                updated_df.show(truncate=False)
                return non_target_month_key_df.unionByName(updated_df)

    def does_month_exist(cls, process_date: str, snap_monthly_df: pyspark.sql.dataframe.DataFrame, data_date_col_name: str) -> bool:
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
                      snap_monthly_df: pyspark.sql.dataframe.DataFrame,
                      status_column: str,
                      key_columns: List[str],
                      data_date_col_name: str) -> pyspark.sql.dataframe.DataFrame:
        base = 'base'
        current = 'current'
        return snap_monthly_df.alias(base) \
            .join(transaction_df.alias(current), key_columns, how='outer') \
            .withColumn(cls.UPDATE_DATE, coalesce(col(f'{cls.LAST_UPDATE_DATE}'), col(f'{cls.UPDATE_DATE}'))) \
            .withColumn(data_date_col_name,
                        coalesce(col(f'{cls.TRANSACTION_START_DATE}'), col(f'{data_date_col_name}'))) \
            .withColumn(status_column, when(col(cls.CURRENT_STATUS) == cls.ACTIVE, col(cls.CURRENT_STATUS))
                        .otherwise(lit(cls.INACTIVE))) \
            .drop(cls.TRANSACTION_START_DATE) \
            .drop(cls.LAST_UPDATE_DATE) \
            .drop(cls.CURRENT_STATUS)
