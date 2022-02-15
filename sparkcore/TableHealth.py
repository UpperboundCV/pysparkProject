import pyspark
import sys

from .writer.SparkWriter import SparkWriter
from .TableCreator import TableCreator
from .ColumnDescriptor import ColumnDescriptor
from .helper.DateHelper import DateHelper
from typing import List, Optional
import itertools
from pyspark.sql.functions import col, to_json, collect_list, create_map, min, mean, isnull, isnan, count, expr, lit, \
    StringType, map_from_entries, collect_list, struct, round, when
from pyspark.sql.functions import max as spark_max, min as spark_min
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql import DataFrame
from pyspark.sql import Window
from functools import reduce


class TableHealth:

    def __init__(self, spark_session: pyspark.sql.SparkSession, source_schema: str, source_table_name: str,
                 env: str = 'local') -> None:
        self.spark_session = spark_session
        self.schema = source_schema
        self.table_name = source_table_name
        self.source_partition = self.get_table_partition()
        self.health_table_name = f'{self.table_name}_health' if self.source_partition is not None else None
        self.env = env

    def get_table_partition(self) -> Optional[str]:
        # todo: please aware that table can have more than one partition
        if len(self.spark_session.sql(f'show partitions {self.schema}.{self.table_name}').collect()) > 0:
            return self.spark_session.sql(f'show partitions {self.schema}.{self.table_name}') \
                .collect()[0]['partition'].split('=')[0]
        else:
            return None

    def health_table_columns(self) -> Optional[List[ColumnDescriptor]]:
        spark_writer = SparkWriter(self.spark_session)
        if spark_writer.does_table_exist(self.schema, self.table_name):
            base_columns = [ColumnDescriptor(column_name='column', data_type='string',
                                             comment=f'"column in {self.schema}.{self.table_name}"'),
                            ColumnDescriptor(column_name='d_type', data_type='string',
                                             comment=f'"type of column in {self.schema}.{self.table_name}"'),
                            ColumnDescriptor(column_name='d_min', data_type='string',
                                             comment=f'"min of column in {self.schema}.{self.table_name}"'),
                            ColumnDescriptor(column_name='d_max', data_type='string',
                                             comment=f'"max of column in {self.schema}.{self.table_name}"'),
                            ColumnDescriptor(column_name='d_mean', data_type='string',
                                             comment=f'"mean of column in {self.schema}.{self.table_name}"'),
                            ColumnDescriptor(column_name='d_median', data_type='string',
                                             comment=f'"median of column in {self.schema}.{self.table_name}"'),
                            ColumnDescriptor(column_name='d_sum', data_type='string',
                                             comment=f'"sum of column in {self.schema}.{self.table_name}"'),
                            ColumnDescriptor(column_name='null_cnt', data_type='string',
                                             comment=f'"sum of column in {self.schema}.{self.table_name}"'),
                            ColumnDescriptor(column_name='nan_cnt', data_type='string',
                                             comment=f'"number of nan of column in {self.schema}.{self.table_name}"'),
                            ColumnDescriptor(column_name='empty_cnt', data_type='string',
                                             comment=f'"number of empty of column in {self.schema}.{self.table_name}"'),
                            ColumnDescriptor(column_name='cnt_distinct', data_type='string',
                                             comment=f'"number of empty of column in {self.schema}.{self.table_name}"')]
            return base_columns
        else:
            raise TypeError(f"Table {self.schema}.{self.table_name} does not exist.")

    def create_health_table(self) -> bool:
        crunch_date = ColumnDescriptor(column_name='crunch_date',
                                       data_type='string',
                                       comment=f'"partition column in {self.schema}.{self.table_name}"')
        source_partition = ColumnDescriptor(column_name=self.source_partition,
                                            data_type='string',
                                            comment=f'"partition column in {self.schema}.{self.table_name}"')
        partition_cols = [crunch_date, source_partition] if self.source_partition is not None else [crunch_date]
        print(f'partition_cols {partition_cols[0].name}  {partition_cols[1].name}')
        table_creator = TableCreator(spark_session=self.spark_session, schema=self.schema,
                                     table_name=f'{self.table_name}_health',
                                     fields=self.health_table_columns(),
                                     partition_cols=partition_cols,
                                     env=self.env)
        return table_creator.create()

    def aggregate_on_column(self, df: pyspark.sql.dataframe.DataFrame, at_col: str) -> pyspark.sql.dataframe.DataFrame:
        type = 'numeric'
        print(f'process aggregate_on_column: {at_col} of type {str(df.schema[at_col].dataType).lower()}')
        summary_df = df.groupby(self.source_partition).agg(
            spark_min(at_col).cast(StringType()).alias('d_min'),
            spark_max(at_col).cast(StringType()).alias('d_max'),
            round(mean(col(at_col)), 2).cast(StringType()).alias('d_mean'),
            count(lit(1)).cast(StringType()).alias('d_median'),
            spark_sum(col(at_col)).cast(StringType()).alias('d_sum'),
            count(when(isnull(at_col), lit(True)).otherwise(lit(None))).cast(StringType()).alias('null_cnt'),
            count(lit(1)).cast(StringType()).alias('nan_cnt'),
            count(when(col(at_col) == "", lit(True)).otherwise(lit(None))).cast(StringType()).alias('empty_cnt'),
            count('*').cast(StringType()).alias('cnt_distinct')).withColumn('column', lit(at_col))
        if 'string' in str(df.schema[at_col].dataType).lower() or \
           'char' in str(df.schema[at_col].dataType).lower() or 'time' in str(df.schema[at_col].dataType).lower():
            d_vals = [d_val for d_val in df.select(at_col).distinct().collect()]
            num_dup = df.groupby(at_col).agg(count('*').alias('total')).select(spark_max(col('total'))).collect()[0][
                'max(total)']
            ratio = (len(d_vals) * 1.0) / df.count()
            cnt_distinct_at_col = df.groupby(self.source_partition, at_col).count()
            num_cnt_distinct_at_col = len(cnt_distinct_at_col.collect())
            type = 'category' if (ratio < 0.1) and num_cnt_distinct_at_col<100  else 'string'

            if type == 'string':
                print(f'{at_col} string type case')
                result_df = summary_df.withColumn('d_type', lit(type))
                # result_df.show(truncate=False) 
                return result_df
            else:
                print(f'{at_col} category type case')
                cnt_distinct_at_col = df.groupby(self.source_partition, at_col).count()
                cnt_distinct_str = ''
                try:
                    to_map = cnt_distinct_at_col.groupby(self.source_partition).agg(
                        map_from_entries(collect_list(struct(at_col, "count"))).alias("cnt_distinct"))
                    cnt_distinct_str = str(to_map.collect()[0]['cnt_distinct'])
                except Exception as e:
                    # print('error at cnt_distinct of struct type:',e)
                    cnt_distinct_str = 'internal error'
                result_df = summary_df.withColumn('cnt_distinct', lit(cnt_distinct_str)).withColumn('d_type', lit(type)) 
                # result_df.show(truncate=False)
                return result_df
        else:
            print(f'{at_col} numeric type case')
            result_df = summary_df.withColumn('d_type', lit(type)) 
            # result_df.show(truncate=False)
            return result_df

    def summary_by_column(self, par_conds: str) -> pyspark.sql.dataframe.DataFrame:
        if self.get_table_partition() is not None:
            # get distinct value on partition
            source_df = self.spark_session.table(f'{self.schema}.{self.table_name}').where(expr(par_conds))
            
            src_cols = source_df.columns
            summary_by_col_dfs = [
                self.aggregate_on_column(source_df, src_col).cache() for src_col in src_cols]
            today_date = DateHelper.today_date()
            summary_by_column_df = reduce(DataFrame.unionAll, summary_by_col_dfs).withColumn('crunch_date',
                                                                                             lit(today_date))            
            # summary_by_column_df.show(truncate=False)
            return summary_by_column_df.select('column', 'd_type', 'd_min', 'd_max', 'd_mean', 'd_median', 'd_sum',
                                               'null_cnt', 'nan_cnt', 'empty_cnt',
                                               'cnt_distinct', 'crunch_date', f'{self.source_partition}')


    def partitions_to_condition_col(self,par_val_col_row: str) -> str:    
        def add_front(val_col: str) -> str:
            return f'({val_col}'
        
        def add_middle(val_col: str) -> str:
            return val_col.replace('=','=="')
        
        def add_back(val_col: str) -> str:
            return f'{val_col}") '
        
        def str_col_cond(col_cond: str) -> str:
            front = add_front(col_cond)
            print(f'add front: {front}')
            middle = add_middle(front)
            print(f'add middle: {middle}')
            back = add_back(middle)
            print(f'add back: {back}')
            return back
        
        col_conds = [str_col_cond(par_val_col) for par_val_col in par_val_col_row.split('/')]
        
        return (' & '.join(col_conds))

    def save(self) -> None:
        if self.source_partition is not None:
            spark_writer = SparkWriter(spark_session=self.spark_session)
            if not (spark_writer.does_table_exist(database=self.schema, checked_table=self.health_table_name)):
                self.create_health_table()
            health_table_df = self.spark_session.table(f'{self.schema}.{self.health_table_name}')
            df_col_set = set(health_table_df.columns)
            print(f'df_col_set: {df_col_set}')
            par_val_col_rows = self.spark_session.sql(f'show partitions {self.schema}.{self.table_name}').select('partition').collect()
            par_val_col_rows = [par.__getitem__('partition') for par in par_val_col_rows]
            partition_col_conds = [self.partitions_to_condition_col(par_val_col_row) for par_val_col_row in par_val_col_rows]
            for par_conds in partition_col_conds:
                process_df = self.summary_by_column(par_conds=par_conds)
                # process_df.show(truncate=False)
                process_df_col_set = set(process_df.columns)
                print(f'process_df_col_set: {process_df_col_set}')
                if df_col_set == process_df_col_set:
                    process_df.select(*process_df.columns).write.format("orc").insertInto(
                        f'{self.schema}.{self.health_table_name}',
                        overwrite=True)
                else:
                    raise TypeError(f"""health table columns: {df_col_set} is equal to 
                                        process_df columns: {process_df_col_set}
                                        """)
