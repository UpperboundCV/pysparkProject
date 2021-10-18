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

    # def __init__(self, spark_session: pyspark.sql.SparkSession) -> None:
    #     self.spark_session = spark_session

    def update_insert_status_snap_monthly(cls, transaction_df: pyspark.sql.dataframe.DataFrame,
                                          snap_monthly_df: pyspark.sql.dataframe.DataFrame,
                                          status_column: str,
                                          key_columns: List[str],
                                          process_date: str) -> pyspark.sql.dataframe.DataFrame:

        if snap_monthly_df.where(col(cls.MONTH_KEY) == 0).count() == 0:
            return transaction_df \
                .withColumn(cls.UPDATE_DATE, lit(DateHelper().today_date())) \
                .withColumn(cls.MONTH_KEY, lit(0)) \
                .withColumn(status_column, lit(cls.ACTIVE))
        else:
            current_snap_month = \
                snap_monthly_df.where(col(cls.MONTH_KEY) == 0).select(max(col(cls.START_DATE))).first()[0]

            if DateHelper().date_str_num_month_diff(process_date, current_snap_month) > 0:
                # It should update the month key
                updated_month_key_df = cls.update_month_key(process_date, snap_monthly_df)
                updated_month_key_df.show(truncate=False)
                # process_date is in monthkey = 0
                temp_df = transaction_df \
                    .where(col(cls.START_DATE) == process_date) \
                    .withColumn(cls.UPDATE_DATE, lit(DateHelper().today_date())) \
                    .withColumn(cls.MONTH_KEY, lit(0)) \
                    .withColumn(status_column, lit(cls.ACTIVE))
                temp_df.show(truncate=False)
                return updated_month_key_df.union(temp_df)
            elif DateHelper().date_str_num_month_diff(process_date, current_snap_month) < 0:
                # this process_date is not in monthkey = 0 but it is in another monthkey
                target_month_key = cls.find_month_key_of_process_date(process_date, snap_monthly_df)
                # rerun the whole monthkey
                transaction_of_month_key = transaction_df \
                    .where(months_between(last_day(col(cls.START_DATE)), last_day(lit(process_date)))==0) \
                    .orderBy(col(cls.START_DATE))
            #     todo: using update_insert
            else:
                # this process_date is in monthkey==0
                # rerun from process_date to the latest start_date
                pass

    def find_month_key_of_process_date(cls, process_date: str, snap_monthly_df: pyspark.sql.dataframe.DataFrame) -> int:
        return snap_monthly_df.where(months_between(last_day(col(cls.START_DATE)), last_day(lit(process_date)))==0) \
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
                        ceil(months_between(last_day(col(process_datetime)), last_day(col(cls.START_DATE)))).cast(IntegerType())) \
            .drop(process_datetime)

    def update_insert(cls, transaction_df: pyspark.sql.dataframe.DataFrame,
                      snap_monthly_df: pyspark.sql.dataframe.DataFrame,
                      status_column: str,
                      key_columns: List[str],
                      process_date: str,
                      month_key: int) -> pyspark.sql.dataframe.DataFrame:
        base = 'base'
        current = 'current'
        current_status = "current_status"
        transaction_start_date = "transaction_start_date"
        process_date_df = transaction_df \
            .where(col(cls.START_DATE) == process_date) \
            .withColumn(current_status, lit(cls.ACTIVE)) \
            .withColumnRenamed(cls.START_DATE, transaction_start_date) \
            .cache()
        process_date_df.show(truncate=False)
        return snap_monthly_df.alias(base) \
            .join(process_date_df.alias(current), key_columns, how='outer') \
            .withColumn(status_column,
                        when(col(f"{current}.{current_status}").isNotNull(), lit(cls.ACTIVE)) \
                        .otherwise(lit(cls.INACTIVE))) \
            .withColumn(cls.MONTH_KEY, coalesce(col(cls.MONTH_KEY), lit(month_key))) \
            .withColumn(cls.UPDATE_DATE, coalesce(col(cls.UPDATE_DATE), lit(DateHelper().today_date()))) \
            .withColumn(cls.START_DATE, coalesce(col(f'{base}.{cls.START_DATE}'), col(f'{transaction_start_date}'))) \
            .drop(transaction_start_date) \
            .drop(current_status)
