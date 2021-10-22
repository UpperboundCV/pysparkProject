from typing import List
import pyspark
from pyspark.sql.functions import col, lit, max, first, when
from .DateHelper import DateHelper
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, months_between, ceil, to_date, coalesce, floor, abs, last_day
from pyspark.sql.types import DateType, IntegerType


class DataFrameHelper:
    MONTH_KEY = 'month_key'
    START_DATE = 'start_date'
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
                                          today_date: str) -> pyspark.sql.dataframe.DataFrame:
        if snap_monthly_df.where(col(cls.MONTH_KEY) == 0).count() == 0:
            return transaction_df.where(col(cls.START_DATE)==process_date) \
                .withColumn(cls.UPDATE_DATE, lit(today_date)) \
                .withColumn(cls.MONTH_KEY, lit(0)) \
                .withColumn(status_column, lit(cls.ACTIVE))
        else:
            current_snap_month = \
                snap_monthly_df.where(col(cls.MONTH_KEY) == 0).select(max(col(cls.START_DATE))).first()[0]
            print(f"process_date:{process_date}")
            print(f"current_snap_month:{current_snap_month}")
            print(f"diff month:{DateHelper().date_str_num_month_diff(process_date, current_snap_month)}")

            if DateHelper().date_str_num_month_diff(process_date, current_snap_month) > 0:
                # It should update the month key
                updated_month_key_df = cls.update_month_key(process_date, snap_monthly_df)
                updated_month_key_df.show(truncate=False)
                # process_date is in monthkey = 0
                temp_df = transaction_df \
                    .where(col(cls.START_DATE) == process_date) \
                    .withColumn(cls.UPDATE_DATE, lit(today_date)) \
                    .withColumn(cls.MONTH_KEY, lit(0)) \
                    .withColumn(status_column, lit(cls.ACTIVE))
                temp_df.show(truncate=False)
                return updated_month_key_df.union(temp_df)
            # elif DateHelper().date_str_num_month_diff(process_date, current_snap_month) < 0:
            else:
                # this process_date is not in monthkey = 0 but it is in another monthkey
                target_month_key = cls.find_month_key_of_process_date(process_date, snap_monthly_df)
                # rerun the whole monthkey
                transaction_of_month_key = transaction_df.where(col(cls.START_DATE) == process_date) \
                    .withColumn(cls.LAST_UPDATE_DATE, lit(today_date)) \
                    .withColumn(cls.MONTH_KEY, lit(target_month_key)) \
                    .withColumn(cls.CURRENT_STATUS, lit(cls.ACTIVE)) \
                    .withColumnRenamed(cls.START_DATE, cls.TRANSACTION_START_DATE)
                if cls.MONTH_KEY not in key_columns:
                    key_columns.append(cls.MONTH_KEY)
                non_target_month_key_df = snap_monthly_df.where(col(cls.MONTH_KEY) != target_month_key)
                target_snap_month_key_df = snap_monthly_df.where(col(cls.MONTH_KEY) == target_month_key).cache()
                # target_month_key_df.show(truncate=False)
                updated_df = cls.update_insert(transaction_of_month_key,
                                               target_snap_month_key_df,
                                               status_column,
                                               key_columns,
                                               process_date,
                                               target_month_key)
                updated_df.show(truncate=False)
                return non_target_month_key_df.unionByName(updated_df)

    def does_month_exist(cls, process_date: str, snap_monthly_df: pyspark.sql.dataframe.DataFrame) -> bool:
        return snap_monthly_df.where(
            months_between(last_day(col(cls.START_DATE)), last_day(lit(process_date))) == 0).count() > 0

    def find_month_key_of_process_date(cls, process_date: str, snap_monthly_df: pyspark.sql.dataframe.DataFrame) -> int:
        return snap_monthly_df.where(months_between(last_day(col(cls.START_DATE)), last_day(lit(process_date))) == 0) \
            .select(max(cls.MONTH_KEY)).first()[0]

    def get_month_keys(cls, snap_monthly_df: pyspark.sql.dataframe.DataFrame) -> List[int]:
        return [int(collect_monthkey[cls.MONTH_KEY]) for collect_monthkey in
                snap_monthly_df.select(col(cls.MONTH_KEY)).collect()]

    def update_month_key(cls, process_date: str,
                         snap_monthly_df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
        modified_month_key = f"modified_{cls.MONTH_KEY}"
        month_keys = cls.get_month_keys(snap_monthly_df)
        process_datetime = "process_datetime"
        return snap_monthly_df \
            .withColumn(process_datetime, lit(process_date)) \
            .withColumn(cls.MONTH_KEY,
                        ceil(months_between(last_day(col(process_datetime)), last_day(col(cls.START_DATE)))).cast(
                            IntegerType())) \
            .drop(process_datetime)

    def update_insert(cls, transaction_df: pyspark.sql.dataframe.DataFrame,
                      snap_monthly_df: pyspark.sql.dataframe.DataFrame,
                      status_column: str,
                      key_columns: List[str],
                      process_date: str,
                      month_key: int) -> pyspark.sql.dataframe.DataFrame:
        base = 'base'
        current = 'current'
        transaction_df.printSchema()
        snap_monthly_df.printSchema()
        return snap_monthly_df.alias(base) \
            .join(transaction_df.alias(current), key_columns, how='outer') \
            .withColumn(cls.UPDATE_DATE, coalesce(col(f'{cls.LAST_UPDATE_DATE}'), col(f'{cls.UPDATE_DATE}'))) \
            .withColumn(cls.START_DATE, coalesce(col(f'{cls.TRANSACTION_START_DATE}'), col(f'{cls.START_DATE}'))) \
            .withColumn(status_column, when(col(cls.CURRENT_STATUS) == cls.ACTIVE, col(cls.CURRENT_STATUS))
                        .otherwise(lit(cls.INACTIVE))) \
            .drop(cls.TRANSACTION_START_DATE) \
            .drop(cls.LAST_UPDATE_DATE) \
            .drop(cls.CURRENT_STATUS)
