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

    def __init__(self, spark_session: pyspark.sql.SparkSession) -> None:
        self.spark_session = spark_session

    def update_insert_status_snap_monthly(self, transaction_df: pyspark.sql.dataframe.DataFrame,
                                          snap_monthly_df: pyspark.sql.dataframe.DataFrame,
                                          status_column: str,
                                          key_columns: List[str],
                                          process_date: str) -> pyspark.sql.dataframe.DataFrame:

        if snap_monthly_df.where(col(self.MONTH_KEY) == 0).count() == 0:
            return transaction_df \
                .withColumn(self.UPDATE_DATE, lit(DateHelper().today_date())) \
                .withColumn(self.MONTH_KEY, lit(0)) \
                .withColumn(status_column, lit(self.ACTIVE))
        else:
            current_snap_month = \
                snap_monthly_df.where(col(self.MONTH_KEY) == 0).select(max(col(self.START_DATE))).first()[
                    self.START_DATE]

            if DateHelper().date_str_num_month_diff(process_date, current_snap_month) > 0:
                # It should update the month key

                # process_date is in monthkey = 0
                pass
            elif DateHelper().date_str_num_month_diff(process_date, current_snap_month) < 0:
                # this process_date is not in monthkey but it is in some monthkey
                # rerun the whole monthkey
                pass
            else:
                # this process_date is in monthkey==0
                # rerun from process_date to the latest start_date
                pass

    def get_month_key(self, snap_monthly_df: pyspark.sql.dataframe.DataFrame) -> List[int]:
        return [int(collect_monthkey[self.MONTH_KEY]) for collect_monthkey in
                snap_monthly_df.select(col(self.MONTH_KEY)).collect()]

    def union_dfs(self, dfs: List[pyspark.sql.dataframe.DataFrame]) -> pyspark.sql.dataframe.DataFrame:
        def union_dfs_index(dfs: List[pyspark.sql.dataframe.DataFrame]
                            , index: int
                            , result_df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
            if index == len(dfs) - 1:
                return result_df
            else:
                union_dfs_index(dfs, index + 1, result_df.union(dfs[index]))

        df_columns = dfs[0].columns
        data = []
        result_df = self.spark_session.createDataFrame(data).toDF(*df_columns)
        return union_dfs_index(dfs, 0, result_df)

    def update_monthkey(self, process_date: str,
                        snap_monthly_df: pyspark.sql.dataframe.DataFrame) -> pyspark.sql.dataframe.DataFrame:
        modified_month_key = f"modified_{self.MONTH_KEY}"
        month_keys = self.get_month_key(snap_monthly_df)
        process_snap_monthly_df = snap_monthly_df.withColumn(modified_month_key, lit(-1))
        process_month_key_dfs = []
        for month_key in month_keys:
            df = process_snap_monthly_df \
                .where(col(self.MONTH_KEY) == month_key)
            mx_date = df.select(max(self.START_DATE)).first()[self.START_DATE]
            num_month_diff = DateHelper().date_str_num_month_diff(process_date, mx_date)
            process_month_key_dfs.append(df.withColumn(modified_month_key, lit(num_month_diff)))
        process_month_key_df = self.union_dfs(process_month_key_dfs)
        return process_month_key_df.drop(modified_month_key)

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
        return snap_monthly_df.alias(BASE) \
            .join(process_date_df.alias(CURRENT), key_columns, how='outer') \
            .withColumn(status_column,
                        when(col(f"{CURRENT}.{CURRENT_STATUS}") == col(f"{BASE}.{status_column}"), lit(self.ACTIVE))
                        .otherwise(lit(self.INACTIVE))) \
            .drop(CURRENT_STATUS)
