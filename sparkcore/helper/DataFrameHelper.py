from typing import List
import pyspark
from pyspark.sql.functions import col, lit, max, first, when
from DateHelper import DateHelper


class DataFrameHelper:
    MONTH_KEY = 'month_key'
    BUSINESS_DATE = 'business_date'
    START_DATE = 'start_date'
    UPDATE_DATE = 'update_date'
    ACTIVE = 'active'
    INACTIVE = 'inactive'

    def update_insert_status_snap_monthly(self, transaction_df: pyspark.sql.dataframe.DataFrame,
                                          snap_monthly_df: pyspark.sql.dataframe.DataFrame,
                                          status_column: str,
                                          key_columns: List[str]) -> pyspark.sql.dataframe.DataFrame:

        if snap_monthly_df.where(col(self.MONTH_KEY) == 0).count() == 0:
            return transaction_df \
                .withColumn(self.UPDATE_DATE, col(self.BUSINESS_DATE)) \
                .withColumn(self.MONTH_KEY, lit(0)) \
                .withColumn(status_column, lit(status_column))
        else:
            lastest_snap_monthly_df_date = snap_monthly_df \
                .where(col(self.MONTH_KEY) == 0) \
                .select(max(self.UPDATE_DATE)) \
                .first()[0]

            unprocess_transaction_df_date = transaction_df \
                .where(col(self.START_DATE) > lastest_snap_monthly_df_date) \
                .select(col(self.START_DATE)) \
                .collect()

            if len(unprocess_transaction_df_date) > 0:
                for running_date in unprocess_transaction_df_date:
                    process_date = running_date[self.START_DATE]
                    update_insert_df = self.update_insert(transaction_df, snap_monthly_df, status_column, key_columns)
                    if DateHelper.is_today_last_day_of_month(process_date):
                        update_insert_df.withColumn(self.MONTH_KEY, col(self.MONTH_KEY)+1)
            else:
                raise TypeError("No new data date")

    def update_insert(self, transaction_df: pyspark.sql.dataframe.DataFrame,
                      snap_monthly_df: pyspark.sql.dataframe.DataFrame,
                      status_column: str,
                      key_columns: List[str],
                      process_date: str) -> pyspark.sql.dataframe.DataFrame:
        BASE = 'base'
        CURRENT = 'current'
        CURRENT_STATUS = "current_status"
        process_date_df = transaction_df \
            .where(col(self.START_DATE) == process_date) \
            .withColumn(CURRENT_STATUS, lit(self.ACTIVE)) \
            .cache()
        process_df = snap_monthly_df.alias(BASE) \
            .join(process_date_df.alias(CURRENT), key_columns, how='outer') \
            .withColumn(status_column,
                        when(col(f"{CURRENT}.{CURRENT_STATUS}") == col(f"{BASE}.{status_column}"), lit(self.ACTIVE))
                        .otherwise(lit(self.INACTIVE))) \
            .drop(CURRENT_STATUS)
