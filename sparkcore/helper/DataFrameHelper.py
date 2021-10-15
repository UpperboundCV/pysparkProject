from typing import List
import pyspark
from pyspark.sql.functions import col, lit, max, first, when
from .DateHelper import DateHelper


class DataFrameHelper:
    MONTH_KEY = 'month_key'
    START_DATE = 'start_date'
    UPDATE_DATE = 'update_date'
    ACTIVE = 'active'
    INACTIVE = 'inactive'

    def update_insert_status_snap_monthly(cls, transaction_df: pyspark.sql.dataframe.DataFrame,
                                          snap_monthly_df: pyspark.sql.dataframe.DataFrame,
                                          status_column: str,
                                          key_columns: List[str]) -> pyspark.sql.dataframe.DataFrame:

        if snap_monthly_df.where(col(cls.MONTH_KEY) == 0).count() == 0:
            return transaction_df \
                .withColumn(cls.UPDATE_DATE, col(cls.START_DATE)) \
                .withColumn(cls.MONTH_KEY, lit(0)) \
                .withColumn(status_column, lit(cls.ACTIVE))
        else:
            lastest_snap_monthly_df_date = snap_monthly_df \
                .where(col(cls.MONTH_KEY) == 0) \
                .select(max(cls.UPDATE_DATE)) \
                .first()[0]

            unprocess_transaction_df_date = transaction_df \
                .where(col(cls.START_DATE) > lastest_snap_monthly_df_date) \
                .select(col(cls.START_DATE)) \
                .collect()

            running_dates = [running_date[cls.START_DATE] for running_date in unprocess_transaction_df_date]

            if len(unprocess_transaction_df_date) > 0:
                for process_date in running_dates:
                    update_insert_df = cls.update_insert(transaction_df, snap_monthly_df, status_column, key_columns)
                    if DateHelper.is_today_last_day_of_month(process_date):
                        update_insert_df.withColumn(cls.MONTH_KEY, col(cls.MONTH_KEY) + 1)
            else:
                raise TypeError("No new data date")

    def update_insert(cls, transaction_df: pyspark.sql.dataframe.DataFrame,
                      snap_monthly_df: pyspark.sql.dataframe.DataFrame,
                      status_column: str,
                      key_columns: List[str],
                      process_date: str) -> pyspark.sql.dataframe.DataFrame:
        BASE = 'base'
        CURRENT = 'current'
        CURRENT_STATUS = "current_status"
        process_date_df = transaction_df \
            .where(col(cls.START_DATE) == process_date) \
            .withColumn(CURRENT_STATUS, lit(cls.ACTIVE)) \
            .cache()
        return snap_monthly_df.alias(BASE) \
            .join(process_date_df.alias(CURRENT), key_columns, how='outer') \
            .withColumn(status_column,
                        when(col(f"{CURRENT}.{CURRENT_STATUS}") == col(f"{BASE}.{status_column}"), lit(cls.ACTIVE))
                        .otherwise(lit(cls.INACTIVE))) \
            .drop(CURRENT_STATUS)
